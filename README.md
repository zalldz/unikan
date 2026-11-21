# unikan — papan tugas untuk tim agen AI

**unikan** = *uni* + *kanban*: satu papan tugas bersama untuk semua agen dan kamu.
Tidak butuh internet, tidak butuh akun cloud, dan tidak memanggil model AI sendiri. Datanya satu file SQLite di komputermu.

- **Satu file**: Python saja, tanpa library tambahan.
- **Aman**: aturan siapa boleh apa ditegakkan oleh server, bukan sekadar kesepakatan di teks. Identitas diambil dari token, jadi agen tidak bisa mengaku orang lain.
- **Tahan mati**: papan tetap jalan walau salah satu agen mati.

## Coba dulu (30 detik)

```bash
git clone https://github.com/zalldz/unikan.git
cd unikan
python3 unikan.py demo
```

Perintah ini membuat papan sementara, menjalankan server sungguhan, lalu menghapus semuanya lagi. Tidak ada yang tersentuh di komputermu. Kamu akan melihat satu tugas berpindah dari `todo` ke `done`, dan satu percobaan yang ditolak server.

## Pakai untuk sungguhan (5 menit)

### 1. Siapkan
Butuh Python 3 (sudah diuji di 3.12). Cek dengan `python3 --version`.

```bash
git clone https://github.com/zalldz/unikan.git
cd unikan
```

### 2. Setup pertama kali
Ganti `ani` dengan namamu, dan nama agen sesuka kamu:

```bash
python3 unikan.py init --owner ani --agents rina,dodi
```

Perintah ini membuat:
- `~/.config/unikan/config.json` berisi pengaturan,
- satu token rahasia per nama di `~/.config/unikan/secrets/` (jangan dibagikan ke siapa pun).

### 3. Jalankan papan
```bash
python3 unikan.py serve
```
Biarkan jalan. Buka terminal baru untuk langkah berikutnya.

### 4. Coba
```bash
python3 unikan.py --as ani add "Tulis ringkasan minggu ini" --to rina --body "Maksimal 5 poin"
python3 unikan.py --as rina next            # tugas untuk rina
python3 unikan.py --as rina claim <id>      # rina mulai mengerjakan
python3 unikan.py --as rina done <id> --result "Sudah, ini ringkasannya ..."
python3 unikan.py --as ani ls --all         # lihat seluruh papan
```
Ganti `<id>` dengan kode 6 karakter yang muncul saat membuat kartu.

Kalau ingin memanggil `unikan` tanpa `python3 unikan.py`, buat tautan:
```bash
ln -s "$PWD/unikan.py" ~/.local/bin/unikan && chmod +x unikan.py
```

## Siapa boleh apa

| Peran | Bisa | Tidak bisa |
|---|---|---|
| **owner** (kamu) | membuat kartu, memindahkan semua kartu, mengulang kartu (requeue) | — |
| **boss** (opsional, `--bosses`) | membuat kartu, mengulang kartu (maks 2x) | mengubah kartu yang sudah selesai |
| **worker** (agen) | mengklaim, menyelesaikan, dan menandai kartu sendiri sebagai terhambat | membuat kartu, mengambil kartu orang lain |

Kartu berjalan lewat kolom: `backlog → todo → doing → done` (atau `blocked` kalau terhambat). `done` adalah tahap akhir.

## Agen dibangunkan otomatis (opsional)
Kalau sebuah kartu masuk ke `todo`, unikan bisa menjalankan perintah untuk membangunkan agen. Atur di `config.json`, bagian `notify`:

```json
"notify": {
  "rina": ["/path/ke/skrip-bangunkan.sh", "{id}"]
}
```

`{id}` diganti dengan kode kartu. Skrip itu bebas kamu isi, misalnya memanggil CLI agen kamu atau mengirim pesan Telegram. Kalau agen tidak mengklaim kartu dalam 3 kali bangun, kartu pindah ke `blocked` dan kamu diminta mengecek.

Agen yang berat bisa dibatasi agar tidak berjalan bersamaan lewat `serialize` (berguna kalau RAM terbatas).

## Jalankan sebagai layanan (agar hidup terus)
Di Linux dengan systemd, buat `~/.config/systemd/user/unikan.service`:

```ini
[Unit]
Description=unikan papan tugas

[Service]
ExecStart=/usr/bin/python3 /path/ke/unikan/unikan.py serve
Restart=always
MemoryMax=150M

[Install]
WantedBy=default.target
```

Lalu:
```bash
systemctl --user daemon-reload
systemctl --user enable --now unikan
```

Backup otomatis: `python3 unikan.py backup` (menyimpan 7 salinan terakhir di `~/.local/share/unikan/backups/`). Jalankan lewat cron atau timer harian.

## Akses dari perangkat lain
Secara default papan hanya bisa diakses dari komputer yang sama (`127.0.0.1`). Untuk mengaksesnya dari perangkat lain, tambahkan alamat jaringanmu di `bind` pada `config.json`. Gunakan jaringan privat seperti Tailscale. **Jangan** membuka port ini ke internet publik: unikan belum punya enkripsi transport.

## Untuk agen luar
Lihat [docs/AGEN-LUAR.md](docs/AGEN-LUAR.md): cara terhubung dan perintah harian untuk agen yang berjalan di luar server owner.

## Tes
```bash
python3 test_unikan.py
```
Menjalankan server sungguhan di port acak. Harus menampilkan `OK`.

## Istilah
- **kartu**: satu tugas.
- **token**: kata sandi per nama. Kalau bocor, jalankan `unikan init --force` untuk membuat ulang semua token.
- **TTL**: batas waktu kartu di `doing` tanpa `touch`. Lewat batas, kartu otomatis pindah ke `blocked`.

## Lisensi
MIT, lihat [LICENSE](LICENSE).
