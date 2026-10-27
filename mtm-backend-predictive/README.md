# MTM Maintenance - Backend Predictive (PdM)

Layanan pemantauan kondisi prediktif (*Predictive Maintenance & IoT Condition Monitoring*) untuk **PT Menara Terus Makmur (Astra Otoparts Group)**.

## Ruang Lingkup Layanan
1. **MQTT Telemetry Ingestion (Mesin Jingduan Forging Press 630T)**
   - Topik: `mtm/jingduanforging/#`
   - Parameter telemetri: Sumbu Z & X Vibration Velocity (`in/s` & `mm/s`), Peak/HF Acceleration (`g`), Kurtosis, Crest Factor, Dominant Frequency, Suhu Bantalan.
2. **Evaluasi Standar Internasional Getaran (ISO 10816-8 / ISO 20816-8)**
   - Normal (Zona A/B): `< 0.180 in/s` (`< 4.50 mm/s`)
   - Warning (Zona C): $\ge 0.180\text{ in/s}$ (`4.50 mm/s`)
   - Danger / Critical (Zona D): $\ge 0.280\text{ in/s}$ (`7.10 mm/s`)
3. **Penyimpanan Time-Series & Rollup**
   - Menggunakan **TimescaleDB** Hypertables untuk analisis getaran per shift dan per tanggal.
4. **Notifikasi WhatsApp Real-time (Baileys)**
   - Mengirimkan pesan darurat saat getaran atau suhu menembus batas kritis.
5. **Integrasi Lintas-Layanan (Cross-Service Webhook)**
   - Mengirim payload peringatan ke **Backend Preventive (Port 4002)** saat terdeteksi getaran abnormal untuk otomatisasi tiket Work Order.

## Port & Endpoints (Port 4001)
- `GET /api/devices`: Data sensor live terkini beserta raw payload ISO
- `GET /api/telemetry`: Riwayat telemetri time-series
- `GET /api/telemetry/shifts`: Riwayat terkelompok per shift kerja
- `GET /api/setpoints` & `POST /api/setpoints`: Kalibrasi batas ambang batas getaran
- `GET /api/wa/status` & `GET /api/wa/qr`: Status dan scan QR bot WhatsApp
- `POST /api/wa/send-test`: Uji kirim notifikasi WhatsApp

## Menjalankan Layanan
```bash
# 1. Install dependencies
npm install

# 2. Setup database schema
npx prisma generate

# 3. Jalankan server dev
npm run start:dev
```

## Cara Push ke Git Repo Baru Anda
```bash
git init
git add .
git commit -m "feat: initial commit mtm backend predictive with iso 10816-8 and webhook"
git branch -M main
git remote add origin <URL_REPO_PREDICTIVE_ANDA>
git push -u origin main
```
