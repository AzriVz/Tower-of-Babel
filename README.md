# Tower of Babel

Babel Gateway menghubungkan API HTTP/JSON publik dengan Service A (HTTP),
Service B (raw TCP framed JSON), dan Service C (UDP binary + CRC32). Gateway
berjalan di `http://localhost:8080` dan menyediakan:

- `POST /execute`
- `GET /services`
- `GET /status`

## Run

Image backend dari kit tugas perlu tersedia dengan tag `babel-go-*:local`. Jika
belum pernah dimuat:

```bash
docker load -i images/babel-go-images.tar
```

Jalankan stack sesuai perintah grader:

```bash
docker compose up -d
```

Untuk rebuild gateway setelah dilakukan perubahan source:

```bash
docker compose up -d --build
```

## Test dan demonstrasi

Video demo: https://drive.google.com/file/d/1AtSgqqSaFqOe_aZPYPrzU84i8xKedQ52/view?usp=sharing

Pasang dependensi development:

```bash
python -m pip install -r requirements-dev.txt
```

Jalankan test:

```bash
./demo/run_demo.sh
```

## Laporan dan Deklarasi AI

Ada pada folder 'docs/'.