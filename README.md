# kb — papan tugas untuk tim agen AI

`kb` adalah papan kanban kecil (seperti Trello sederhana) untuk membagi tugas antar agen AI, atau antara kamu dan agen.
Tidak butuh internet, tidak butuh akun cloud, dan tidak memanggil model AI sendiri. Datanya satu file SQLite di komputermu.

- **Satu file**: Python saja, tanpa library tambahan.
- **Aman**: aturan siapa boleh apa ditegakkan oleh server, bukan sekadar kesepakatan di teks. Identitas diambil dari token, jadi agen tidak bisa mengaku orang lain.
- **Tahan mati**: papan tetap jalan walau salah satu agen mati.

## Cara pakai (5 menit)

### 1. Siapkan
Butuh Python 3 (sudah diuji di 3.12). Cek dengan `python3 --version`.

```bash
git clone https://github.com/zalldz/kb.git
cd kb
```

### 2. Setup pertama kali
Ganti `ani` dengan namamu, dan nama agen sesuka kamu:

```bash
python3 kb.py init --owner ani --agents rina,dodi
```

Perintah ini membuat:
- `~/.config/kb/config.json` berisi pengaturan,
- satu token rahasia per nama di `~/.config/kb/secrets/` (jangan dibagikan ke siapa pun).

### 3. Jalankan papan
```bash
python3 kb.py serve
```
Biarkan jalan. Buka terminal baru untuk langkah berikutnya.

### 4. Coba
```bash
python3 kb.py --as ani add "Tulis ringkasan minggu ini" --to rina --body "Maksimal 5 poin"
python3 kb.py --as rina next            # tugas untuk rina
python3 kb.py --as rina claim <id>      # rina mulai mengerjakan
python3 kb.py --as rina done <id> --result "Sudah, ini ringkasannya ..."
python3 kb.py --as ani ls --all         # lihat seluruh papan
```
Ganti `<id>` dengan kode 6 karakter yang muncul saat membuat kartu.

Kalau ingin memanggil `kb` tanpa `python3 kb.py`, buat tautan:
```bash
ln -s "$PWD/kb.py" ~/.local/bin/kb && chmod +x kb.py
```

## Siapa boleh apa

| Peran | Bisa | Tidak bisa |
|---|---|---|
| **owner** (kamu) | membuat kartu, memindahkan semua kartu, mengulang kartu (requeue) | — |
| **boss** (opsional, `--bosses`) | membuat kartu, mengulang kartu (maks 2x) | mengubah kartu yang sudah selesai |
| **worker** (agen) | mengklaim, menyelesaikan, dan menandai kartu sendiri sebagai terhambat | membuat kartu, mengambil kartu orang lain |

Kartu berjalan lewat kolom: `backlog → todo → doing → done` (atau `blocked` kalau terhambat). `done` adalah tahap akhir.

## Agen dibangunkan otomatis (opsional)
Kalau sebuah kartu masuk ke `todo`, kb bisa menjalankan perintah untuk membangunkan agen. Atur di `config.json`, bagian `notify`:

```json
"notify": {
  "rina": ["/path/ke/skrip-bangunkan.sh", "{id}"]
}
```

`{id}` diganti dengan kode kartu. Skrip itu bebas kamu isi, misalnya memanggil CLI agen kamu atau mengirim pesan Telegram. Kalau agen tidak mengklaim kartu dalam 3 kali bangun, kartu pindah ke `blocked` dan kamu diminta mengecek.

Agen yang berat bisa dibatasi agar tidak berjalan bersamaan lewat `serialize` (berguna kalau RAM terbatas).

## Jalankan sebagai layanan (agar hidup terus)
Di Linux dengan systemd, buat `~/.config/systemd/user/kb.service`:

```ini
[Unit]
Description=kb papan tugas

[Service]
ExecStart=/usr/bin/python3 /path/ke/kb/kb.py serve
Restart=always
MemoryMax=150M

[Install]
WantedBy=default.target
```

Lalu:
```bash
systemctl --user daemon-reload
systemctl --user enable --now kb
```

Backup otomatis: `python3 kb.py backup` (menyimpan 7 salinan terakhir di `~/.local/share/kb/backups/`). Jalankan lewat cron atau timer harian.

## Akses dari perangkat lain
Secara default papan hanya bisa diakses dari komputer yang sama (`127.0.0.1`). Untuk mengaksesnya dari perangkat lain, tambahkan alamat jaringanmu di `bind` pada `config.json`. Gunakan jaringan privat seperti Tailscale. **Jangan** membuka port ini ke internet publik: kb belum punya enkripsi transport.

## Tes
```bash
python3 test_kb.py
```
Menjalankan server sungguhan di port acak. Harus menampilkan `OK`.

## Istilah
- **kartu**: satu tugas.
- **token**: kata sandi per nama. Kalau bocor, jalankan `kb init --force` untuk membuat ulang semua token.
- **TTL**: batas waktu kartu di `doing` tanpa `touch`. Lewat batas, kartu otomatis pindah ke `blocked`.

## Lisensi
MIT, lihat [LICENSE](LICENSE).
