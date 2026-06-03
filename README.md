# Jembatan Webhook Saweria → Roblox

Server kecil yang menjembatani donasi Saweria ke game Roblox.
Saweria nembak donasi ke server ini (webhook) → server simpan →
Roblox mengambilnya satu per satu lewat `GET /prime`.

## Endpoint

| Alamat | Fungsi |
| --- | --- |
| `GET /` | Halaman status (buka di browser untuk cek semuanya jalan) |
| `GET /prime` | Roblox mengambil 1 donasi terbaru: `{ donator, amount, message }` |
| `GET /prime/leaderboard` | Sengaja kosong `[]` (lihat catatan di bawah) |
| `POST /webhook` | Dipanggil Saweria saat ada donasi |
| `POST /test-donate` | Kirim donasi palsu untuk tes (boleh dihapus saat live) |

## Cara setup (ringkas)

1. Upload semua file ini ke sebuah repo GitHub.
2. Di Vercel: **Add New → Project → Import** repo tersebut, lalu **Deploy**.
3. Di project Vercel: tab **Storage → Create Database → Upstash (Redis)**.
   Ini otomatis mengisi environment variable koneksi Redis. **Redeploy** setelahnya.
4. Buka URL Vercel-mu (`https://NAMA.vercel.app`) → muncul halaman status.
   Klik **Kirim donasi tes** → harus muncul di daftar & "antrian" bertambah.
5. Pasang `https://NAMA.vercel.app/webhook` di Saweria (Settings → Integrations → Webhook).
6. Di script `SaweriaServer` Roblox, ganti:
   ```lua
   local VERCEL_URL          = "https://NAMA.vercel.app/prime"
   local LEADERBOARD_API_URL = "https://NAMA.vercel.app/prime/leaderboard"
   ```
7. (Opsional, untuk keamanan) Setelah jalan, isi environment variable
   `SAWERIA_STREAM_KEY` di Vercel dengan Stream Key dari Saweria, lalu redeploy.

## Kenapa `/prime/leaderboard` kosong?

Script `SaweriaServer` Roblox sudah menjumlahkan setiap donasi ke DataStore-nya
sendiri (`customLeaderboardEntries`) dan menampilkan **data API + data DataStore**.
Kalau server ini ikut mengirim total donasi, tiap donasi akan terhitung **dua kali**.
Maka endpoint ini sengaja mengembalikan `[]` agar leaderboard = persis jumlah yang
dicatat Roblox (tanpa dobel), dan **tanpa perlu mengubah script Roblox**.

## Tes di komputer sendiri (opsional)

```bash
npm install
npm start
# buka http://localhost:3000
```
