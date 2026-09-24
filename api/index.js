// ============================================================
//  JEMBATAN WEBHOOK SAWERIA + BAGIBAGI + AMALSHOLEH  ->  ROBLOX
//  Versi 1.3 (tambah filter campaign opsional utk Amalsholeh via AMALSHOLEH_PROGRAM_ID,
//  jalur Saweria & BagiBagi TIDAK diubah, dan tanpa PROGRAM_ID perilaku Amalsholeh
//  TIDAK BERUBAH -- backward-compatible)
//
//  Tugas server ini:
//   1) Menerima "tembakan" donasi dari Saweria, BagiBagi & Amalsholeh (webhook / POST).
//   2) Menyimpannya di antrian (Redis) -- MASING-MASING PLATFORM PUNYA ANTRIAN SENDIRI.
//   3) Memberikannya ke Roblox satu per satu saat Roblox bertanya
//      (GET /prime untuk Saweria, GET /prime-bagibagi untuk BagiBagi,
//      GET /prime-amalsholeh untuk Amalsholeh), supaya tiap donasi cuma
//      diproses 1x per platform.
//
//  Endpoint Saweria (TIDAK BERUBAH dari versi 1.0):
//   GET  /prime              -> ambil 1 donasi Saweria terbaru { donator, amount, message }
//   GET  /prime/leaderboard  -> SENGAJA kosong [] (lihat catatan di bawah)
//   POST /webhook            -> dipanggil Saweria saat ada donasi
//   POST /test-donate        -> kirim donasi Saweria palsu buat ngetes
//
//  Endpoint BagiBagi (TIDAK BERUBAH dari versi 1.1):
//   GET  /prime-bagibagi          -> ambil 1 donasi BagiBagi terbaru { donator, amount, message }
//   POST /webhook-bagibagi        -> dipanggil BagiBagi saat ada donasi (Custom Webhook)
//   POST /test-donate-bagibagi    -> kirim donasi BagiBagi palsu buat ngetes
//
//  Endpoint Amalsholeh (BARU di versi 1.2):
//   GET  /prime-amalsholeh        -> ambil 1 donasi Amalsholeh terbaru { donator, amount, message }
//   POST /webhook-amalsholeh      -> dipanggil Amalsholeh saat ada donasi sukses
//   POST /test-donate-amalsholeh  -> kirim donasi Amalsholeh palsu buat ngetes
//
//  CATATAN KHUSUS Amalsholeh (beda dari Saweria/BagiBagi):
//   Amalsholeh TIDAK menyediakan mekanisme signature/HMAC/token verifikasi
//   apa pun di webhook-nya (sudah dicek di dokumentasi resmi mereka,
//   help.amalsholeh.com). Makanya proteksi endpoint /webhook-amalsholeh
//   MURNI dari query param ?key=... yang kita buat sendiri (lihat
//   AMALSHOLEH_WEBHOOK_SECRET di bawah) -- BUKAN dari Amalsholeh. Key ini
//   ditempel di URL yang didaftarkan sebagai webhook campaign, dan sengaja
//   TIDAK ditampilkan di halaman status (GET /) karena halaman itu publik.
//
//  GET  /                   -> halaman status (buka di browser)
//
//  CATATAN soal leaderboard yang kosong (BERLAKU JUGA utk BagiBagi & Amalsholeh):
//   Script Roblox (SaweriaServer) SUDAH menjumlahkan tiap donasi -- dari
//   platform MANAPUN -- ke DataStore-nya sendiri. Makanya server ini TIDAK
//   punya endpoint leaderboard terpisah utk BagiBagi/Amalsholeh -- leaderboard
//   gabungan cukup dihitung di sisi Roblox, sama seperti pola Saweria yang
//   sudah terbukti aman (baca README bagian lama).
// ============================================================

const express = require("express");
const crypto = require("crypto");
const { Redis } = require("@upstash/redis");

// ---- Konfigurasi dari Environment Variables (diisi di Vercel) ----

// Stream Key dari Saweria. (TIDAK BERUBAH)
const STREAM_KEY = process.env.SAWERIA_STREAM_KEY || "";

// Webhook Token dari BagiBagi (dashboard -> Overlay Integration -> Custom Webhook). (TIDAK BERUBAH)
const BAGIBAGI_WEBHOOK_TOKEN = process.env.BAGIBAGI_WEBHOOK_TOKEN || "";

// Secret key BUATAN SENDIRI untuk Amalsholeh (Amalsholeh TIDAK menyediakan
// signature/token verifikasi apa pun di webhook-nya). Ditempel sebagai query
// param ?key=... di URL webhook yang didaftarkan ke campaign Amalsholeh
// (lewat pihak yang bikin campaign-nya). Kalau dikosongkan -> verifikasi
// dilewati (mode tes, kurang aman), sama seperti STREAM_KEY & BAGIBAGI_WEBHOOK_TOKEN.
const AMALSHOLEH_WEBHOOK_SECRET = process.env.AMALSHOLEH_WEBHOOK_SECRET || "";

// ID Campaign/Program Amalsholeh yang BOLEH diteruskan ke Roblox (opsional, BARU).
// Amalsholeh cuma punya webhook di level LEMBAGA (bukan per-campaign), jadi kalau
// dikosongkan -> SEMUA campaign lembaga ini diterima (perilaku lama, backward-compatible,
// aman dipasang SEBELUM campaign-nya dibuat). Kalau nanti diisi (angka ID campaign, ambil
// dari "Program terakhir terdeteksi" di halaman status setelah 1x donasi tes/real masuk) ->
// HANYA donasi dari campaign itu yang diteruskan ke Roblox; donasi dari campaign lain di
// lembaga yang sama akan DIABAIKAN (tapi tetap dicatat programnya biar gampang dicek).
const AMALSHOLEH_PROGRAM_ID = process.env.AMALSHOLEH_PROGRAM_ID || "";

// Koneksi Redis (Upstash). (TIDAK BERUBAH)
const redis = new Redis({
  url: process.env.UPSTASH_REDIS_REST_URL || process.env.KV_REST_API_URL,
  token: process.env.UPSTASH_REDIS_REST_TOKEN || process.env.KV_REST_API_TOKEN,
});

const QUEUE_KEY = "saweria:queue"; // antrian donasi Saweria yang belum diambil Roblox (TIDAK BERUBAH)
const LOG_KEY = "saweria:log";     // catatan 20 donasi Saweria terakhir (TIDAK BERUBAH)
const LOG_MAX = 20;

// Antrian & log KHUSUS BagiBagi -- TERPISAH TOTAL dari punya Saweria. (TIDAK BERUBAH)
const BAGI_QUEUE_KEY = "bagibagi:queue";
const BAGI_LOG_KEY = "bagibagi:log";

// Antrian & log KHUSUS Amalsholeh -- TERPISAH TOTAL dari Saweria & BagiBagi,
// supaya tidak mungkin saling menimpa/ganggu.
const AMAL_QUEUE_KEY = "amalsholeh:queue";
const AMAL_LOG_KEY = "amalsholeh:log";
const AMAL_LASTPROGRAM_KEY = "amalsholeh:lastprogram"; // simpan campaign/program TERAKHIR yang lewat webhook (BARU, bantu cari programId yang benar)

const app = express();

// Tangkap RAW body (utk verifikasi tanda tangan) sambil tetap parse JSON. (TIDAK BERUBAH)
app.use(
  express.json({
    verify: (req, res, buf) => {
      req.rawBody = buf;
    },
  })
);

// ---- Helper (TIDAK BERUBAH) ----

function safeEqualHex(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  return ba.length === bb.length && crypto.timingSafeEqual(ba, bb);
}

// Verifikasi tanda tangan webhook Saweria (HMAC-SHA256 pakai Stream Key). (TIDAK BERUBAH)
function verifySaweriaSignature(req) {
  if (!STREAM_KEY) return { ok: true, skipped: true }; // mode tes
  const got = req.get("Saweria-Callback-Signature") || "";
  if (!got) return { ok: false, reason: "no-signature-header" };

  const hmac = (str) =>
    crypto.createHmac("sha256", STREAM_KEY).update(str).digest("hex");

  const candidates = [];
  if (req.rawBody && req.rawBody.length) candidates.push(req.rawBody.toString("utf8"));
  try {
    candidates.push(JSON.stringify(req.body || {}));
  } catch (e) {}

  const ok = candidates.some((s) => safeEqualHex(hmac(s), got));
  return { ok, reason: ok ? null : "bad-signature" };
}

// Verifikasi tanda tangan webhook BagiBagi (HMAC-SHA256 pakai Webhook Token). (TIDAK BERUBAH)
function verifyBagibagiSignature(req) {
  if (!BAGIBAGI_WEBHOOK_TOKEN) return { ok: true, skipped: true }; // mode tes
  const got = req.get("X-Bagibagi-Signature") || "";
  if (!got) return { ok: false, reason: "no-signature-header" };

  const hmac = (str) =>
    crypto.createHmac("sha256", BAGIBAGI_WEBHOOK_TOKEN).update(str).digest("hex");

  const candidates = [];
  if (req.rawBody && req.rawBody.length) candidates.push(req.rawBody.toString("utf8"));
  try {
    candidates.push(JSON.stringify(req.body || {}));
  } catch (e) {}

  const ok = candidates.some((s) => safeEqualHex(hmac(s), got));
  return { ok, reason: ok ? null : "bad-signature" };
}

// Verifikasi request webhook Amalsholeh. BEDA dari Saweria/BagiBagi: Amalsholeh
// TIDAK punya mekanisme signature/HMAC sama sekali di dokumentasi resminya,
// jadi proteksi endpoint ini murni dari query param ?key=... yang cuma KITA
// yang tahu (dibuat sendiri, BUKAN dari Amalsholeh), ditempel di URL webhook
// yang didaftarkan ke campaign.
function verifyAmalsholehSecret(req) {
  if (!AMALSHOLEH_WEBHOOK_SECRET) return { ok: true, skipped: true }; // mode tes
  const got = (req.query && req.query.key) || "";
  if (!got) return { ok: false, reason: "no-key-param" };
  const ok = safeEqualHex(got, AMALSHOLEH_WEBHOOK_SECRET);
  return { ok, reason: ok ? null : "bad-key" };
}

// Ambil field penting dari payload Saweria (TIDAK BERUBAH).
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

// Ambil field penting dari payload BagiBagi (TIDAK BERUBAH).
// Bentuk resmi (dari docs.bagibagi.co): { transaction_id, name, amount, message, mediaShareUrl, created_at }
function pickBagibagiDonationFields(body) {
  body = body || {};
  const donator = body.name || body.donator || "Anonim";
  const amount = Number(body.amount != null ? body.amount : 0) || 0;
  const message = body.message || "";
  return { donator: String(donator), amount, message: String(message) };
}

// Ambil field penting dari payload Amalsholeh.
// Bentuk resmi (dari help.amalsholeh.com):
//   { type: "donation", data: { donation: { amount, total, unique_code, ... },
//                                user: { name, email, phone, message } } }
// PENTING: pakai "amount" (nominal bersih yang diketik donatur), BUKAN
// "total" (amount + unique_code, itu cuma buat pencocokan transfer bank).
function pickAmalsholehDonationFields(body) {
  body = body || {};
  const data = body.data || {};
  const donation = data.donation || {};
  const user = data.user || {};
  const donator = user.name || "Hamba Allah";
  const amount = Number(donation.amount != null ? donation.amount : 0) || 0;
  const message = user.message || "";
  // BARU: info campaign/program (dari donation.program.{id,name,slug}), dipakai utk filter.
  const program = donation.program || {};
  const programId = program.id != null ? String(program.id) : "";
  const programName = program.name || "";
  return {
    donator: String(donator),
    amount,
    message: String(message),
    programId,
    programName: String(programName),
  };
}

async function pushDonation(d, source, queueKey, logKey) {
  queueKey = queueKey || QUEUE_KEY;
  logKey = logKey || LOG_KEY;
  const item = { donator: d.donator, amount: d.amount, message: d.message };
  await redis.rpush(queueKey, JSON.stringify(item));
  const logItem = Object.assign({}, item, { source: source || "webhook", at: Date.now() });
  await redis.lpush(logKey, JSON.stringify(logItem));
  await redis.ltrim(logKey, 0, LOG_MAX - 1);
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

// Cek apakah donasi Amalsholeh ini boleh diteruskan, berdasarkan filter AMALSHOLEH_PROGRAM_ID
// (BARU). Kosong -> terima semua campaign lembaga ini (perilaku lama, backward-compatible).
// Diisi -> harus persis sama dengan programId donasi yang masuk.
function isAllowedAmalsholehProgram(programId) {
  if (!AMALSHOLEH_PROGRAM_ID) return true; // filter belum diaktifkan -> terima semua
  return String(programId || "") === String(AMALSHOLEH_PROGRAM_ID);
}

// Catat program/campaign TERAKHIR yang lewat webhook Amalsholeh (BARU) -- dipanggil utk
// SETIAP donasi yang masuk (baik nanti diterima ataupun diabaikan filter), supaya gampang
// dicek dari halaman status program ID mana yang beneran datang dari campaign yang didaftarkan.
async function rememberAmalsholehProgram(programId, programName) {
  try {
    await redis.set(
      AMAL_LASTPROGRAM_KEY,
      JSON.stringify({ id: programId || "", name: programName || "", at: Date.now() })
    );
  } catch (e) {
    console.error("[webhook-amalsholeh] gagal simpan info program terakhir:", e);
  }
}

// ============================================================
//  GET /prime  -> Roblox ambil 1 donasi Saweria terbaru (TIDAK BERUBAH)
// ============================================================
app.get(["/prime", "/api/prime"], async (req, res) => {
  try {
    const raw = await redis.lpop(QUEUE_KEY);
    const item = parseMaybe(raw);
    if (!item) return res.type("application/json").send("{}");
    return res.json({
      donator: item.donator,
      amount: Number(item.amount) || 0,
      message: item.message || "...",
    });
  } catch (e) {
    return res.type("application/json").send("{}");
  }
});

// ============================================================
//  GET /prime/leaderboard  -> SENGAJA kosong (TIDAK BERUBAH)
// ============================================================
app.get(["/prime/leaderboard", "/api/prime/leaderboard"], (req, res) => {
  return res.json([]);
});

// ============================================================
//  GET /prime-bagibagi  -> Roblox ambil 1 donasi BagiBagi terbaru (TIDAK BERUBAH)
// ============================================================
app.get(["/prime-bagibagi", "/api/prime-bagibagi"], async (req, res) => {
  try {
    const raw = await redis.lpop(BAGI_QUEUE_KEY);
    const item = parseMaybe(raw);
    if (!item) return res.type("application/json").send("{}");
    return res.json({
      donator: item.donator,
      amount: Number(item.amount) || 0,
      message: item.message || "...",
    });
  } catch (e) {
    return res.type("application/json").send("{}");
  }
});

// ============================================================
//  GET /prime-amalsholeh  -> Roblox ambil 1 donasi Amalsholeh terbaru (BARU)
//  Pola identik /prime & /prime-bagibagi, TAPI ambil dari antrian Redis terpisah.
// ============================================================
app.get(["/prime-amalsholeh", "/api/prime-amalsholeh"], async (req, res) => {
  try {
    const raw = await redis.lpop(AMAL_QUEUE_KEY);
    const item = parseMaybe(raw);
    if (!item) return res.type("application/json").send("{}");
    return res.json({
      donator: item.donator,
      amount: Number(item.amount) || 0,
      message: item.message || "...",
    });
  } catch (e) {
    return res.type("application/json").send("{}");
  }
});

// ============================================================
//  POST /webhook  -> dipanggil Saweria saat ada donasi (TIDAK BERUBAH)
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
    await pushDonation(d, sig.skipped ? "webhook(tanpa-verifikasi)" : "webhook", QUEUE_KEY, LOG_KEY);
    console.log("[webhook] donasi diterima:", d.donator, d.amount);
    return res.json({ ok: true });
  } catch (e) {
    console.error("[webhook] gagal simpan:", e);
    return res.status(500).json({ ok: false, error: "gagal simpan ke database" });
  }
});

// ============================================================
//  POST /webhook-bagibagi  -> dipanggil BagiBagi saat ada donasi (TIDAK BERUBAH)
// ============================================================
app.post(["/webhook-bagibagi", "/api/webhook-bagibagi"], async (req, res) => {
  const sig = verifyBagibagiSignature(req);
  if (!sig.ok) {
    console.warn("[webhook-bagibagi] ditolak:", sig.reason);
    return res.status(401).json({ ok: false, error: sig.reason });
  }
  const d = pickBagibagiDonationFields(req.body);
  if (!d.donator || d.amount <= 0) {
    return res.status(400).json({ ok: false, error: "data donasi tidak lengkap" });
  }
  try {
    await pushDonation(
      d,
      sig.skipped ? "webhook-bagibagi(tanpa-verifikasi)" : "webhook-bagibagi",
      BAGI_QUEUE_KEY,
      BAGI_LOG_KEY
    );
    console.log("[webhook-bagibagi] donasi diterima:", d.donator, d.amount);
    return res.json({ ok: true });
  } catch (e) {
    console.error("[webhook-bagibagi] gagal simpan:", e);
    return res.status(500).json({ ok: false, error: "gagal simpan ke database" });
  }
});

// ============================================================
//  POST /webhook-amalsholeh  -> dipanggil Amalsholeh saat ada donasi sukses (BARU)
// ============================================================
app.post(["/webhook-amalsholeh", "/api/webhook-amalsholeh"], async (req, res) => {
  const sig = verifyAmalsholehSecret(req);
  if (!sig.ok) {
    console.warn("[webhook-amalsholeh] ditolak:", sig.reason);
    return res.status(401).json({ ok: false, error: sig.reason });
  }
  const body = req.body || {};
  if (body.type !== "donation" || !body.data) {
    // Tetap 200 biar Amalsholeh gak retry terus-terusan utk payload yang tidak dikenali.
    console.warn("[webhook-amalsholeh] payload tidak dikenali:", JSON.stringify(body));
    return res.json({ ok: true, ignored: true });
  }
  const d = pickAmalsholehDonationFields(body);

  // BARU: catat program/campaign yang barusan masuk -- dilakukan SELALU (diterima
  // ataupun diabaikan filter), supaya "Program terakhir terdeteksi" di halaman status
  // selalu up to date dan bisa dipakai buat cari tahu programId campaign yang benar.
  await rememberAmalsholehProgram(d.programId, d.programName);

  // BARU: kalau AMALSHOLEH_PROGRAM_ID sudah diisi, cuma terima donasi dari campaign itu.
  // Kalau masih kosong (belum diaktifkan) -> semua campaign lembaga ini tetap diterima,
  // persis seperti sebelum patch ini (backward-compatible).
  if (!isAllowedAmalsholehProgram(d.programId)) {
    console.warn(
      "[webhook-amalsholeh] donasi dari campaign lain diabaikan (filter aktif):",
      d.programId,
      d.programName
    );
    return res.json({ ok: true, ignored: true, reason: "program-tidak-sesuai-filter" });
  }

  if (!d.donator || d.amount <= 0) {
    return res.status(400).json({ ok: false, error: "data donasi tidak lengkap" });
  }
  try {
    await pushDonation(
      d,
      sig.skipped ? "webhook-amalsholeh(tanpa-verifikasi)" : "webhook-amalsholeh",
      AMAL_QUEUE_KEY,
      AMAL_LOG_KEY
    );
    console.log("[webhook-amalsholeh] donasi diterima:", d.donator, d.amount);
    return res.json({ ok: true });
  } catch (e) {
    console.error("[webhook-amalsholeh] gagal simpan:", e);
    return res.status(500).json({ ok: false, error: "gagal simpan ke database" });
  }
});

// ============================================================
//  POST /test-donate  -> kirim donasi Saweria palsu (TIDAK BERUBAH)
// ============================================================
app.post(["/test-donate", "/api/test-donate"], async (req, res) => {
  const d = pickDonationFields(req.body);
  if (!d.donator || d.donator === "Anonim") d.donator = "TestUser";
  if (d.amount <= 0) d.amount = 50000;
  if (!d.message) d.message = "[TES] dari halaman status";
  try {
    await pushDonation(d, "test", QUEUE_KEY, LOG_KEY);
    return res.json({ ok: true, donation: d });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e) });
  }
});

// ============================================================
//  POST /test-donate-bagibagi  -> kirim donasi BagiBagi palsu (TIDAK BERUBAH)
// ============================================================
app.post(["/test-donate-bagibagi", "/api/test-donate-bagibagi"], async (req, res) => {
  const d = pickBagibagiDonationFields(req.body);
  if (!d.donator || d.donator === "Anonim") d.donator = "TestUserBagiBagi";
  if (d.amount <= 0) d.amount = 50000;
  if (!d.message) d.message = "[TES BagiBagi] dari halaman status";
  try {
    await pushDonation(d, "test-bagibagi", BAGI_QUEUE_KEY, BAGI_LOG_KEY);
    return res.json({ ok: true, donation: d });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e) });
  }
});

// ============================================================
//  POST /test-donate-amalsholeh  -> kirim donasi Amalsholeh palsu (BARU)
// ============================================================
app.post(["/test-donate-amalsholeh", "/api/test-donate-amalsholeh"], async (req, res) => {
  const d = pickAmalsholehDonationFields(req.body);
  if (!d.donator || d.donator === "Hamba Allah") d.donator = "TestUserAmalsholeh";
  if (d.amount <= 0) d.amount = 50000;
  if (!d.message) d.message = "[TES Amalsholeh] dari halaman status";
  try {
    await pushDonation(d, "test-amalsholeh", AMAL_QUEUE_KEY, AMAL_LOG_KEY);
    return res.json({ ok: true, donation: d });
  } catch (e) {
    return res.status(500).json({ ok: false, error: String(e) });
  }
});

// ============================================================
//  GET /  -> halaman status (DIPERLUAS: sekarang nampilin Amalsholeh juga)
// ============================================================
function esc(s) {
  return String(s == null ? "" : s).replace(/[&<>"']/g, (c) => {
    return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
  });
}

app.get(["/", "/status"], async (req, res) => {
  let redisOk = false;
  let queueLen = 0;
  let bagiQueueLen = 0;
  let amalQueueLen = 0;
  let logs = [];
  let bagiLogs = [];
  let amalLogs = [];
  let amalLastProgram = null; // BARU
  try {
    queueLen = await redis.llen(QUEUE_KEY);
    bagiQueueLen = await redis.llen(BAGI_QUEUE_KEY);
    amalQueueLen = await redis.llen(AMAL_QUEUE_KEY);
    const raw = await redis.lrange(LOG_KEY, 0, LOG_MAX - 1);
    logs = (raw || []).map(parseMaybe).filter(Boolean);
    const bagiRaw = await redis.lrange(BAGI_LOG_KEY, 0, LOG_MAX - 1);
    bagiLogs = (bagiRaw || []).map(parseMaybe).filter(Boolean);
    const amalRaw = await redis.lrange(AMAL_LOG_KEY, 0, LOG_MAX - 1);
    amalLogs = (amalRaw || []).map(parseMaybe).filter(Boolean);
    const amalLastRaw = await redis.get(AMAL_LASTPROGRAM_KEY); // BARU
    amalLastProgram = parseMaybe(amalLastRaw); // BARU
    redisOk = true;
  } catch (e) {
    redisOk = false;
  }

  const proto = req.headers["x-forwarded-proto"] || "https";
  const base = proto + "://" + req.headers.host;

  const verifyOn = !!STREAM_KEY;
  const bagiVerifyOn = !!BAGIBAGI_WEBHOOK_TOKEN;
  const amalVerifyOn = !!AMALSHOLEH_WEBHOOK_SECRET;
  const amalFilterOn = !!AMALSHOLEH_PROGRAM_ID; // BARU
  const okBadge = '<span class="b ok">&#10003; tersambung</span>';
  const errBadge = '<span class="b err">&#10007; gagal — cek integrasi Upstash</span>';

  function renderLogRows(list) {
    return list.length
      ? list
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
  }

  const logsHtml = renderLogRows(logs);
  const bagiLogsHtml = renderLogRows(bagiLogs);
  const amalLogsHtml = renderLogRows(amalLogs);

  const html =
    "<!doctype html><html lang=\"id\"><head><meta charset=\"utf-8\">" +
    '<meta name="viewport" content="width=device-width,initial-scale=1">' +
    "<title>Status Jembatan Saweria + BagiBagi + Amalsholeh</title><style>" +
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
    "h2{font-size:15px;margin:0 0 10px}" +
    "</style></head><body><div class=\"wrap\">" +
    "<h1>&#128225; Status Jembatan Saweria + BagiBagi + Amalsholeh &#8594; Roblox</h1>" +
    '<p class="sub">Halaman ini buat ngecek semua jalan. Refresh untuk update.</p>' +
    '<div class="card"><h2>Saweria</h2>' +
    '<div class="row"><span class="k">Database (Redis)</span>' + (redisOk ? okBadge : errBadge) + "</div>" +
    '<div class="row"><span class="k">Verifikasi tanda tangan Saweria</span>' +
    (verifyOn ? '<span class="b on">aktif (aman)</span>' : '<span class="b warn">nonaktif (mode tes)</span>') +
    "</div>" +
    '<div class="row"><span class="k">Donasi di antrian (belum diambil Roblox)</span><b>' + queueLen + "</b></div>" +
    "</div>" +
    '<div class="card"><h2>BagiBagi</h2>' +
    '<div class="row"><span class="k">Verifikasi tanda tangan BagiBagi</span>' +
    (bagiVerifyOn ? '<span class="b on">aktif (aman)</span>' : '<span class="b warn">nonaktif (mode tes)</span>') +
    "</div>" +
    '<div class="row"><span class="k">Donasi di antrian (belum diambil Roblox)</span><b>' + bagiQueueLen + "</b></div>" +
    "</div>" +
    '<div class="card"><h2>Amalsholeh</h2>' +
    '<div class="row"><span class="k">Verifikasi key Amalsholeh (buatan sendiri)</span>' +
    (amalVerifyOn ? '<span class="b on">aktif (aman)</span>' : '<span class="b warn">nonaktif (mode tes)</span>') +
    "</div>" +
    '<div class="row"><span class="k">Donasi di antrian (belum diambil Roblox)</span><b>' + amalQueueLen + "</b></div>" +
    '<div class="row"><span class="k">Filter campaign (AMALSHOLEH_PROGRAM_ID)</span>' +
    (amalFilterOn
      ? '<span class="b on">aktif -- ID: ' + esc(AMALSHOLEH_PROGRAM_ID) + "</span>"
      : '<span class="b warn">nonaktif (semua campaign lembaga diterima)</span>') +
    "</div>" +
    '<div class="row"><span class="k">Program/campaign terakhir terdeteksi</span><b>' +
    (amalLastProgram && amalLastProgram.id
      ? esc(amalLastProgram.name || "(tanpa nama)") + " -- ID: " + esc(amalLastProgram.id)
      : "belum ada donasi masuk") +
    "</b></div>" +
    "</div>" +
    '<div class="card"><div style="display:flex;justify-content:space-between;align-items:center;gap:12px;flex-wrap:wrap">' +
    "<div><b>Tes tanpa donasi beneran</b><div class=\"sub\" style=\"margin:2px 0 0\">Masukkan 1 donasi palsu ke antrian.</div></div>" +
    '<div style="display:flex;gap:8px;flex-wrap:wrap">' +
    '<button id="tb" onclick="testDonate()">Tes Saweria</button>' +
    '<button id="tbBagi" onclick="testDonateBagi()">Tes BagiBagi</button>' +
    '<button id="tbAmal" onclick="testDonateAmal()">Tes Amalsholeh</button>' +
    "</div></div></div>" +
    '<div class="card urls"><b>Tempel link berikut:</b>' +
    '<div><span class="lbl">Webhook Saweria (Settings &#8594; Integrations &#8594; Webhook):</span><code>' + esc(base) + "/webhook</code></div>" +
    '<div><span class="lbl">VERCEL_URL di SaweriaServer Roblox:</span><code>' + esc(base) + "/prime</code></div>" +
    '<div><span class="lbl">LEADERBOARD_API_URL di SaweriaServer Roblox:</span><code>' + esc(base) + "/prime/leaderboard</code></div>" +
    '<div><span class="lbl">Custom Webhook BagiBagi (dashboard &#8594; Overlay Integration):</span><code>' + esc(base) + "/webhook-bagibagi</code></div>" +
    '<div><span class="lbl">Endpoint polling BagiBagi di Roblox:</span><code>' + esc(base) + "/prime-bagibagi</code></div>" +
    '<div><span class="lbl">Endpoint polling Amalsholeh di Roblox:</span><code>' + esc(base) + "/prime-amalsholeh</code></div>" +
    '<div><span class="lbl">Webhook Amalsholeh (demi keamanan, key ?key=... TIDAK ditampilkan di sini -- minta ke owner sistem):</span><code>' + esc(base) + "/webhook-amalsholeh?key=...</code></div>" +
    "</div>" +
    '<div class="card"><b>20 donasi Saweria terakhir</b>' +
    '<table><thead><tr><th>Donatur</th><th>Jumlah</th><th>Pesan</th><th>Sumber</th><th>Waktu</th></tr></thead><tbody>' +
    logsHtml +
    "</tbody></table></div>" +
    '<div class="card"><b>20 donasi BagiBagi terakhir</b>' +
    '<table><thead><tr><th>Donatur</th><th>Jumlah</th><th>Pesan</th><th>Sumber</th><th>Waktu</th></tr></thead><tbody>' +
    bagiLogsHtml +
    "</tbody></table></div>" +
    '<div class="card"><b>20 donasi Amalsholeh terakhir</b>' +
    '<table><thead><tr><th>Donatur</th><th>Jumlah</th><th>Pesan</th><th>Sumber</th><th>Waktu</th></tr></thead><tbody>' +
    amalLogsHtml +
    "</tbody></table></div>" +
    "</div><script>" +
    "function testDonate(){var b=document.getElementById('tb');b.disabled=true;b.textContent='Mengirim...';" +
    "fetch('/test-donate',{method:'POST'}).then(function(r){return r.json()}).then(function(){location.reload()})" +
    ".catch(function(){b.disabled=false;b.textContent='Tes Saweria'})}" +
    "function testDonateBagi(){var b=document.getElementById('tbBagi');b.disabled=true;b.textContent='Mengirim...';" +
    "fetch('/test-donate-bagibagi',{method:'POST'}).then(function(r){return r.json()}).then(function(){location.reload()})" +
    ".catch(function(){b.disabled=false;b.textContent='Tes BagiBagi'})}" +
    "function testDonateAmal(){var b=document.getElementById('tbAmal');b.disabled=true;b.textContent='Mengirim...';" +
    "fetch('/test-donate-amalsholeh',{method:'POST'}).then(function(r){return r.json()}).then(function(){location.reload()})" +
    ".catch(function(){b.disabled=false;b.textContent='Tes Amalsholeh'})}" +
    "function ago(ms){var d=Math.floor((Date.now()-ms)/1000);if(d<60)return d+' dtk lalu';" +
    "if(d<3600)return Math.floor(d/60)+' mnt lalu';if(d<86400)return Math.floor(d/3600)+' jam lalu';return Math.floor(d/86400)+' hari lalu'}" +
    "document.querySelectorAll('[data-at]').forEach(function(el){el.textContent=ago(Number(el.getAttribute('data-at')))});" +
    "</script></body></html>";

  res.type("html").send(html);
});

// 404 yang membantu (TIDAK BERUBAH)
app.use((req, res) => {
  res.status(404).json({ ok: false, error: "rute tidak ditemukan", path: req.path });
});

// Untuk tes di komputer sendiri (TIDAK BERUBAH)
if (require.main === module) {
  const port = process.env.PORT || 3000;
  app.listen(port, () => console.log("Jalan di http://localhost:" + port));
}

module.exports = app;
