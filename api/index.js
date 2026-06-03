// ============================================================
//  JEMBATAN WEBHOOK SAWERIA  ->  ROBLOX
//  Versi 1.0
//
//  Tugas server ini:
//   1) Menerima "tembakan" donasi dari Saweria (webhook / POST).
//   2) Menyimpannya di antrian (Redis).
//   3) Memberikannya ke Roblox satu per satu saat Roblox bertanya
//      (GET /prime), supaya tiap donasi cuma diproses 1x.
//
//  Endpoint:
//   GET  /prime              -> ambil 1 donasi terbaru { donator, amount, message }
//   GET  /prime/leaderboard  -> SENGAJA kosong [] (lihat catatan di bawah)
//   POST /webhook            -> dipanggil Saweria saat ada donasi
//   POST /test-donate        -> kirim donasi palsu buat ngetes
//   GET  /                   -> halaman status (buka di browser)
//
//  CATATAN soal leaderboard yang kosong:
//   Script Roblox (SaweriaServer) SUDAH menjumlahkan tiap donasi ke
//   DataStore-nya sendiri (customLeaderboardEntries) lalu menampilkan
//   "data dari API + data DataStore". Kalau server ini JUGA mengirim
//   total donasi, tiap donasi bakal kehitung DUA KALI. Maka endpoint
//   /prime/leaderboard sengaja mengembalikan [] (kosong). Hasilnya:
//   leaderboard = persis jumlah donasi yang dicatat Roblox (tidak dobel),
//   tanpa perlu mengubah script Roblox-mu.
// ============================================================

const express = require("express");
const crypto = require("crypto");
const { Redis } = require("@upstash/redis");

// ---- Konfigurasi dari Environment Variables (diisi di Vercel) ----

// Stream Key dari Saweria. Dipakai untuk memastikan webhook benar2 dari
// Saweria (bukan orang iseng). Kalau dikosongkan -> verifikasi dilewati
// (mode tes, kurang aman). Isi setelah sistem terbukti jalan.
const STREAM_KEY = process.env.SAWERIA_STREAM_KEY || "";

// Koneksi Redis (Upstash). Nama env var-nya diisi OTOMATIS oleh integrasi
// Upstash di Vercel. Kita baca dua kemungkinan nama biar pasti kebaca.
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN,
});

const QUEUE_KEY = "saweria:queue"; // antrian donasi yang belum diambil Roblox
const LOG_KEY = "saweria:log";     // catatan 20 donasi terakhir (utk halaman status)
const LOG_MAX = 20;

const app = express();

// Tangkap RAW body (utk verifikasi tanda tangan) sambil tetap parse JSON.
app.use(
  express.json({
    verify: (req, res, buf) => {
      req.rawBody = buf;
    },
  })
);

// ---- Helper ----

function safeEqualHex(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  return ba.length === bb.length && crypto.timingSafeEqual(ba, bb);
}

// Verifikasi tanda tangan webhook Saweria (HMAC-SHA256 pakai Stream Key).
function verifySaweriaSignature(req) {
  if (!STREAM_KEY) return { ok: true, skipped: true }; // mode tes
  const got = req.get("Saweria-Callback-Signature") || "";
  if (!got) return { ok: false, reason: "no-signature-header" };

  const hmac = (str) =>
    crypto.createHmac("sha256", STREAM_KEY).update(str).digest("hex");

  // Coba cocokkan dengan body mentah ATAU body yang di-stringify ulang.
  const candidates = [];
  if (req.rawBody && req.rawBody.length) candidates.push(req.rawBody.toString("utf8"));
  try {
    candidates.push(JSON.stringify(req.body || {}));
  } catch (e) {}

  const ok = candidates.some((s) => safeEqualHex(hmac(s), got));
  return { ok, reason: ok ? null : "bad-signature" };
}

// Ambil field penting dari payload Saweria (defensif terhadap variasi nama).
function pickDonationFields(body) {
  body = body || {};
  const donator =
    body.donator_name || body.donatur_name || body.donator || body.name || "Anonim";
  const amount =
    Number(
      body.amount_raw != null
        ? body.amount_raw
        : body.amount != null
        ? body.amount
        : body.etc && body.etc.amount_to_display != null
        ? body.etc.amount_to_display
        : 0
    ) || 0;
  const message = body.message || "";
  return { donator: String(donator), amount, message: String(message) };
}

async function pushDonation(d, source) {
  const item = { donator: d.donator, amount: d.amount, message: d.message };
  await redis.rpush(QUEUE_KEY, JSON.stringify(item));
  const logItem = Object.assign({}, item, { source: source || "webhook", at: Date.now() });
  await redis.lpush(LOG_KEY, JSON.stringify(logItem));
  await redis.ltrim(LOG_KEY, 0, LOG_MAX - 1);
}

function parseMaybe(x) {
  if (x == null) return null;
  if (typeof x === "string") {
    try {
      return JSON.parse(x);
    } catch (e) {
      return null;
    }
  }
  return x;
}

// ============================================================
//  GET /prime  -> Roblox ambil 1 donasi terbaru
// ============================================================
app.get(["/prime", "/api/prime"], async (req, res) => {
  try {
    const raw = await redis.lpop(QUEUE_KEY);
    const item = parseMaybe(raw);
    if (!item) return res.type("application/json").send("{}"); // tidak ada -> {}
    return res.json({
      donator: item.donator,
      amount: Number(item.amount) || 0,
      message: item.message || "...",
    });
  } catch (e) {
    // Kalau error, kirim {} biar Roblox tidak memproses data rusak.
    return res.type("application/json").send("{}");
  }
});

// ============================================================
//  GET /prime/leaderboard  -> SENGAJA kosong (baca catatan di atas)
// ============================================================
app.get(["/prime/leaderboard", "/api/prime/leaderboard"], (req, res) => {
  return res.json([]);
});

// ============================================================
//  POST /webhook  -> dipanggil Saweria saat ada donasi
// ============================================================
app.post(["/webhook", "/api/webhook"], async (req, res) => {
  const sig = verifySaweriaSignature(req);
  if (!sig.ok) {
    console.warn("[webhook] ditolak:", sig.reason);
    return res.status(401).json({ ok: false, error: sig.reason });
  }
  const d = pickDonationFields(req.body);
  if (!d.donator || d.amount <= 0) {
    return res.status(400).json({ ok: false, error: "data donasi tidak lengkap" });
  }
  try {
    await pushDonation(d, sig.skipped ? "webhook(tanpa-verifikasi)" : "webhook");
    console.log("[webhook] donasi diterima:", d.donator, d.amount);
    return res.json({ ok: true });
  } catch (e) {
    console.error("[webhook] gagal simpan:", e);
    return res.status(500).json({ ok: false, error: "gagal simpan ke database" });
  }
});

// ============================================================
//  POST /test-donate  -> kirim donasi palsu (buat ngetes, boleh dihapus)
// ============================================================
app.post(["/test-donate", "/api/test-donate"], async (req, res) => {
  const d = pickDonationFields(req.body);
  if (!d.donator || d.donator === "Anonim") d.donator = "TestUser";
  if (d.amount <= 0) d.amount = 50000;
  if (!d.message) d.message = "[TES] dari halaman status";
  try {
    await pushDonation(d, "test");
    return res.json({ ok: true, donation: d });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e) });
  }
});

// ============================================================
//  GET /  -> halaman status
// ============================================================
function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => {
    return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
  });
}

app.get(["/", "/status"], async (req, res) => {
  let redisOk = false;
  let queueLen = 0;
  let logs = [];
  try {
    queueLen = await redis.llen(QUEUE_KEY);
    const raw = await redis.lrange(LOG_KEY, 0, LOG_MAX - 1);
    logs = (raw || []).map(parseMaybe).filter(Boolean);
    redisOk = true;
  } catch (e) {
    redisOk = false;
  }

  const proto = req.headers["x-forwarded-proto"] || "https";
  const base = proto + "://" + req.headers.host;

  const verifyOn = !!STREAM_KEY;
  const okBadge =
    '<span class="b ok">&#10003; tersambung</span>';
  const errBadge =
    '<span class="b err">&#10007; gagal — cek integrasi Upstash</span>';

  const logsHtml = logs.length
    ? logs
        .map((l) => {
          const amt = (Number(l.amount) || 0).toLocaleString("id-ID");
          return (
            "<tr><td>" +
            esc(l.donator) +
            '</td><td class="amt">Rp ' +
            amt +
            "</td><td>" +
            esc(l.message || "") +
            '</td><td><span class="src">' +
            esc(l.source || "") +
            '</span></td><td class="t" data-at="' +
            (l.at || Date.now()) +
            "\"></td></tr>"
          );
        })
        .join("")
    : '<tr><td colspan="5" class="muted">Belum ada donasi masuk. Klik tombol di atas buat tes.</td></tr>';

  const html =
    "<!doctype html><html lang=\"id\"><head><meta charset=\"utf-8\">" +
    '<meta name="viewport" content="width=device-width,initial-scale=1">' +
    "<title>Status Jembatan Saweria</title><style>" +
    "*{box-sizing:border-box}body{margin:0;background:#0f1115;color:#e7e7ea;font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;padding:20px}" +
    ".wrap{max-width:780px;margin:0 auto}h1{font-size:20px;margin:0 0 4px}.sub{color:#9aa0a6;font-size:13px;margin:0 0 18px}" +
    ".card{background:#171a21;border:1px solid #262b35;border-radius:12px;padding:16px;margin-bottom:14px}" +
    ".row{display:flex;justify-content:space-between;align-items:center;gap:10px;padding:6px 0;font-size:14px}" +
    ".row .k{color:#9aa0a6}.b{font-size:12px;padding:3px 9px;border-radius:20px;font-weight:600}" +
    ".b.ok{background:#102b18;color:#5fd07f;border:1px solid #1f5a32}.b.err{background:#2b1212;color:#f08a8a;border:1px solid #5a1f1f}" +
    ".b.warn{background:#2b2410;color:#e6c15f;border:1px solid #5a4a1f}.b.on{background:#10212b;color:#5fb8d0;border:1px solid #1f4a5a}" +
    "code{background:#0b0d11;border:1px solid #262b35;border-radius:6px;padding:2px 7px;font-size:13px;color:#9fd0ff;word-break:break-all}" +
    ".urls div{margin:8px 0;font-size:13px}.urls .lbl{color:#9aa0a6;display:block;margin-bottom:3px}" +
    "button{background:#1f5fff;color:#fff;border:0;border-radius:8px;padding:10px 16px;font-size:14px;font-weight:600;cursor:pointer}button:disabled{opacity:.5}" +
    "table{width:100%;border-collapse:collapse;font-size:13px;margin-top:6px}th,td{text-align:left;padding:8px 6px;border-bottom:1px solid #20242d}th{color:#9aa0a6;font-weight:600}" +
    ".amt{color:#5fd07f;font-weight:600;white-space:nowrap}.src{color:#9aa0a6;font-size:11px}.t{color:#9aa0a6;white-space:nowrap}.muted{color:#9aa0a6;text-align:center;padding:18px}" +
    "</style></head><body><div class=\"wrap\">" +
    "<h1>&#128225; Status Jembatan Saweria &#8594; Roblox</h1>" +
    '<p class="sub">Halaman ini buat ngecek semua jalan. Refresh untuk update.</p>' +
    '<div class="card">' +
    '<div class="row"><span class="k">Database (Redis)</span>' + (redisOk ? okBadge : errBadge) + "</div>" +
    '<div class="row"><span class="k">Verifikasi tanda tangan Saweria</span>' +
    (verifyOn ? '<span class="b on">aktif (aman)</span>' : '<span class="b warn">nonaktif (mode tes)</span>') +
    "</div>" +
    '<div class="row"><span class="k">Donasi di antrian (belum diambil Roblox)</span><b>' + queueLen + "</b></div>" +
    "</div>" +
    '<div class="card"><div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">' +
    "<div><b>Tes tanpa donasi beneran</b><div class=\"sub\" style=\"margin:2px 0 0\">Masukkan 1 donasi palsu ke antrian.</div></div>" +
    '<button id="tb" onclick="testDonate()">Kirim donasi tes</button></div></div>' +
    '<div class="card urls"><b>Tempel link berikut:</b>' +
    '<div><span class="lbl">Webhook Saweria (Settings &#8594; Integrations &#8594; Webhook):</span><code>' + esc(base) + "/webhook</code></div>" +
    '<div><span class="lbl">VERCEL_URL di SaweriaServer Roblox:</span><code>' + esc(base) + "/prime</code></div>" +
    '<div><span class="lbl">LEADERBOARD_API_URL di SaweriaServer Roblox:</span><code>' + esc(base) + "/prime/leaderboard</code></div>" +
    "</div>" +
    '<div class="card"><b>20 donasi terakhir diterima</b>' +
    '<table><thead><tr><th>Donatur</th><th>Jumlah</th><th>Pesan</th><th>Sumber</th><th>Waktu</th></tr></thead><tbody>' +
    logsHtml +
    "</tbody></table></div>" +
    "</div><script>" +
    "function testDonate(){var b=document.getElementById('tb');b.disabled=true;b.textContent='Mengirim...';" +
    "fetch('/test-donate',{method:'POST'}).then(function(r){return r.json()}).then(function(){location.reload()})" +
    ".catch(function(){b.disabled=false;b.textContent='Kirim donasi tes'})}" +
    "function ago(ms){var d=Math.floor((Date.now()-ms)/1000);if(d<60)return d+' dtk lalu';" +
    "if(d<3600)return Math.floor(d/60)+' mnt lalu';if(d<86400)return Math.floor(d/3600)+' jam lalu';return Math.floor(d/86400)+' hari lalu'}" +
    "document.querySelectorAll('[data-at]').forEach(function(el){el.textContent=ago(Number(el.getAttribute('data-at')))});" +
    "</script></body></html>";

  res.type("html").send(html);
});

// 404 yang membantu
app.use((req, res) => {
  res
    .status(404)
    .json({ ok: false, error: "rute tidak ditemukan", path: req.path });
});

// Untuk tes di komputer sendiri (Vercel tidak menjalankan ini).
if (require.main === module) {
  const port = process.env.PORT || 3000;
  app.listen(port, () => console.log("Jalan di http://localhost:" + port));
}

module.exports = app;
