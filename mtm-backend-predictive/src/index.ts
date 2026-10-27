import express from "express";
import cors from "cors";
import path from "path";
import { config } from "./config";
import { MqttSubscriberService } from "./subscriber";

const app = express();
app.use(cors());
app.use(express.json());

// Root Service Info
app.get("/", (req, res) => {
  res.json({
    service: "MTM Predictive Maintenance (PdM) Service",
    version: "2.5.0",
    status: "online",
    port: config.server.port,
    machineTarget: "Jingduan Forging Press 630T",
    standards: "ISO 10816-8 / ISO 20816-8",
  });
});

// Inisialisasi Service MQTT
const subscriberService = new MqttSubscriberService();
subscriberService.start();

// ==========================================
// REST API ENDPOINTS UNTUK FRONTEND
// ==========================================

// 1. Status Kesehatan & Konektivitas
app.get("/api/health", async (req, res) => {
  const timescaleConnected = subscriberService.timescaleManager
    ? subscriberService.timescaleManager.isConnected()
    : false;
  const isHypertable = subscriberService.timescaleManager
    ? subscriberService.timescaleManager.isHypertable()
    : false;
  const monitoringConnected = subscriberService.monitoringManager
    ? subscriberService.monitoringManager.isConnected()
    : false;

  const mqttDetails = subscriberService.getMqttDetails();

  res.json({
    status: "ok",
    timestamp: new Date().toISOString(),
    connections: {
      mqtt: mqttDetails,
      timescale: {
        enabled: config.timescale.enabled,
        connected: timescaleConnected,
        database: config.timescale.database,
        host: `${config.timescale.host}:${config.timescale.port}`,
        hypertable: isHypertable,
      },
      monitoring: {
        enabled: config.monitoring.enabled,
        connected: monitoringConnected,
        database: config.monitoring.database,
        host: `${config.monitoring.host}:${config.monitoring.port}`,
      },
    },
  });
});

// Helper: Pastikan hanya data Mesin Jingduan Forging yang diloloskan
const isJingduan = (id: string = "") => {
  const s = String(id || "").toLowerCase();
  return (
    s.includes("jingduan") ||
    s.includes("eccentric") ||
    s.includes("bearing") ||
    s.includes("link-temperature") ||
    s.includes("vibration")
  );
};

// 2. Data Terbaru per Device (Live Cache + Fallback Database) - EKSKLUSIF JINGDUAN
app.get("/api/devices", async (req, res) => {
  // Ambil data real-time live dari memori subscriber MQTT
  const liveDevices = subscriberService.getLiveDevices().filter((d) => isJingduan(d.device_id));

  if (liveDevices.length > 0) {
    return res.json(liveDevices);
  }

  // Jika live cache masih kosong (misal baru start), baca dari Monitoring DB
  if (subscriberService.monitoringManager && subscriberService.monitoringManager.isConnected()) {
    try {
      const dbDevices = await subscriberService.monitoringManager.getLatestPerDevice();
      const filteredDb = (dbDevices || []).filter((d: any) => isJingduan(d.device_id));
      if (filteredDb.length > 0) return res.json(filteredDb);
    } catch {}
  }

  // Fallback ke Timescale jika belum ada data di monitoring
  if (subscriberService.timescaleManager && subscriberService.timescaleManager.isConnected()) {
    try {
      const fallback = await subscriberService.timescaleManager.getRecentTelemetry(20);
      const filteredFallback = (fallback || []).filter((d: any) => isJingduan(d.device_id));
      if (filteredFallback.length > 0) return res.json(filteredFallback);
    } catch {}
  }

  res.json([]);
});

// 3. Detail Lengkap Koneksi & Ingestion MQTT
app.get("/api/mqtt-info", (req, res) => {
  res.json(subscriberService.getMqttDetails());
});

// 4. Riwayat Telemetri dari PostgreSQL Timescale - EKSKLUSIF JINGDUAN
app.get("/api/telemetry", async (req, res) => {
  const limit = Math.min(parseInt((req.query.limit as string) || "50", 10), 1000);
  const deviceId = req.query.device as string | undefined;
  const shift = req.query.shift as string | undefined;
  const date = req.query.date as string | undefined;

  if (subscriberService.timescaleManager && subscriberService.timescaleManager.isConnected()) {
    try {
      const data = await subscriberService.timescaleManager.getRecentTelemetry(limit, deviceId, shift, date);
      const filtered = (data || []).filter((r: any) => isJingduan(r.device_id));
      if (filtered.length > 0) return res.json(filtered);
    } catch {}
  }

  // Fallback 1: Ambil dari riwayat in-memory telemetry subscriber MQTT
  const inMemory = subscriberService.getRecentTelemetryHistory(limit, deviceId, date).filter((r) => isJingduan(r.device_id));
  if (inMemory.length > 0) {
    return res.json(inMemory);
  }

  // Fallback 2: Jika DB offline/belum sinkron, tampilkan snapshot perangkat live di tabel riwayat
  const live = subscriberService.getLiveDevices().filter((d) => isJingduan(d.device_id));
  const fallbackRows = live.map((d, idx) => ({
    id: idx + 1,
    device_id: d.device_id,
    temperature: d.temperature,
    humidity: d.humidity,
    pressure: d.pressure,
    status: d.status,
    raw_payload: d.raw_payload,
    created_at: d.recorded_at,
  }));
  res.json(fallbackRows);
});

// 4B. Riwayat Telemetri Terkelompok Per Shift - Tidak Hilang Saat Pergantian Shift
app.get("/api/telemetry/shifts", async (req, res) => {
  const date = (req.query.date as string) || undefined;
  if (subscriberService.timescaleManager && subscriberService.timescaleManager.isConnected()) {
    try {
      const data = await subscriberService.timescaleManager.getShiftTelemetry(date);
      if (data) return res.json(data);
    } catch (err: any) {
      console.error("[API] getShiftTelemetry error:", err.message);
    }
  }

  // Fallback ke in-memory subscriber buffers
  const fallback = subscriberService.getShiftTelemetry(date);
  res.json(fallback);
});

// 5. Log Real-time Ingestion - EKSKLUSIF JINGDUAN
app.get("/api/logs", (req, res) => {
  const logs = subscriberService.getRecentLogs().filter((l) => isJingduan(l.deviceId) || isJingduan(l.topic));
  res.json(logs);
});

// 6. Statistik Database (Khusus Mesin Jingduan Forging)
app.get("/api/stats", async (req, res) => {
  let totalRows = 0;
  let activeDevices = subscriberService.getLiveDevices().filter((d) => isJingduan(d.device_id)).length;
  let totalAlerts = 0;

  if (subscriberService.timescaleManager) {
    const tStats = await subscriberService.timescaleManager.getStats();
    totalRows = tStats.totalRows;
    if (tStats.activeDevices > activeDevices && tStats.activeDevices <= 6) {
      activeDevices = tStats.activeDevices;
    }
  }

  if (subscriberService.monitoringManager) {
    const mStats = await subscriberService.monitoringManager.getStats();
    totalAlerts = mStats.totalAlerts;
  }

  res.json({
    totalRows,
    activeDevices,
    totalAlerts,
    totalMqttMessages: subscriberService.getMqttDetails().totalMessagesReceived,
  });
});

// 7. Konfigurasi Batas Warning (Set Point) Sensor
app.get("/api/setpoints", (req, res) => {
  res.json(subscriberService.getSetPoints());
});

app.post("/api/setpoints", (req, res) => {
  try {
    const updated = subscriberService.updateSetPoints(req.body);
    res.json({
      success: true,
      message: "Batas set point berhasil disimpan dan diterapkan",
      setpoints: updated,
    });
  } catch (err: any) {
    res.status(500).json({ error: "Gagal menyimpan set point", details: err?.message });
  }
});

// ==========================================
// ENDPOINT INTEGRASI WHATSAPP NOTIFICATION
// ==========================================

// 1. Cek Status WhatsApp
app.get("/api/wa/status", (req, res) => {
  if (!subscriberService.waService) {
    return res.status(503).json({ error: "WhatsApp service belum diinisialisasi" });
  }
  res.json(subscriberService.waService.getStatus());
});

// 2. Tampilkan Halaman QR Code untuk Scan di HP / Browser
app.get("/api/wa/qr", (req, res) => {
  if (!subscriberService.waService) {
    return res.status(503).send("WhatsApp service belum aktif");
  }
  const status = subscriberService.waService.getStatus();

  if (status.connected) {
    return res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <title>WhatsApp MTM - Terhubung</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <style>
          body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; display: flex; align-items: center; justify-content: center; height: 100vh; margin: 0; background: #0f172a; color: #f8fafc; }
          .card { background: #1e293b; padding: 2rem; border-radius: 1rem; text-align: center; border: 1px solid #334155; max-width: 400px; }
          .badge { background: #10b981; color: white; padding: 6px 12px; border-radius: 9999px; font-weight: bold; font-size: 0.85rem; display: inline-block; margin-bottom: 1rem; }
          .btn { background: #3b82f6; color: white; padding: 8px 16px; border-radius: 6px; text-decoration: none; display: inline-block; margin-top: 1rem; }
        </style>
      </head>
      <body>
        <div class="card">
          <div class="badge">TERHUBUNG</div>
          <h2>WhatsApp Terhubung</h2>
          <p>Akun: <b>${status.userPhone || "Aktif"}</b></p>
          <p style="color: #94a3b8; font-size: 0.9rem;">Target Notifikasi: <b>${status.targetPhone || "Belum diisi di .env"}</b></p>
          <a class="btn" href="/">Buka Dashboard</a>
        </div>
      </body>
      </html>
    `);
  }

  if (status.qrDataUrl) {
    return res.send(`
      <!DOCTYPE html>
      <html>
      <head>
        <title>Scan QR WhatsApp - MTM Maintenance</title>
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <meta http-equiv="refresh" content="5">
        <style>
          body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; display: flex; align-items: center; justify-content: center; min-height: 100vh; margin: 0; background: #0f172a; color: #f8fafc; }
          .card { background: #1e293b; padding: 2rem; border-radius: 1rem; text-align: center; border: 1px solid #334155; max-width: 420px; box-shadow: 0 10px 25px rgba(0,0,0,0.5); }
          .qr-img { background: white; padding: 12px; border-radius: 12px; margin: 1.5rem 0; width: 260px; height: 260px; }
          p { color: #94a3b8; font-size: 0.9rem; line-height: 1.5; margin: 0.5rem 0; }
          .note { font-size: 0.8rem; color: #64748b; margin-top: 1rem; }
        </style>
      </head>
      <body>
        <div class="card">
          <h2 style="margin: 0 0 0.5rem 0;">Tautkan WhatsApp</h2>
          <p>Buka WhatsApp di HP &gt; <b>Perangkat Tertaut</b> &gt; <b>Tautkan Perangkat</b>, lalu scan kode QR di bawah ini:</p>
          <img class="qr-img" src="${status.qrDataUrl}" alt="WhatsApp QR Code" />
          <p>Halaman ini akan me-refresh otomatis setiap 5 detik.</p>
          <div class="note">PT Menara Terus Makmur - Astra Otoparts Group</div>
        </div>
      </body>
      </html>
    `);
  }

  res.send(`
    <!DOCTYPE html>
    <html>
    <head>
      <title>Menyiapkan QR Code...</title>
      <meta http-equiv="refresh" content="3">
      <style>body { font-family: sans-serif; display: flex; justify-content: center; align-items: center; height: 100vh; background: #0f172a; color: white; }</style>
    </head>
    <body>
      <div style="text-align: center;">
        <p>Sedang menghubungkan ke WhatsApp & menyiapkan QR Code...</p>
        <p style="color: #64748b; font-size: 0.85rem;">Halaman akan me-refresh dalam 3 detik</p>
      </div>
    </body>
    </html>
  `);
});

// 3. Tes Kirim Notifikasi Manual (Bisa ke target spesifik atau broadcast ke semua di .env)
app.post("/api/wa/send-test", async (req, res) => {
  if (!subscriberService.waService) {
    return res.status(503).json({ error: "WhatsApp service belum aktif" });
  }

  const specificTarget = req.body.target;
  const testMessage =
    `🔔 *[TEST NOTIFIKASI] WHATSAPP MTM BERHASIL!*\n` +
    `🏭 *PT Menara Terus Makmur - Maintenance Apps*\n` +
    `🕒 *Waktu*: ${new Date().toLocaleString("id-ID", { timeZone: "Asia/Jakarta" })} WIB\n` +
    `Status: Sistem notifikasi darurat & early warning aktif dengan sukses!`;

  if (specificTarget) {
    const success = await subscriberService.waService.sendMessage(specificTarget, testMessage);
    return res.json({
      success,
      target: specificTarget,
      message: success ? "Pesan tes berhasil dikirim!" : "Gagal kirim pesan. Cek koneksi WA.",
    });
  }

  const success = await subscriberService.waService.broadcastMessage(testMessage);
  res.json({
    success,
    targets: subscriberService.waService.getTargetList(),
    message: success ? "Pesan tes broadcast berhasil dikirim ke semua target!" : "Gagal kirim pesan.",
  });
});

// 4. Lihat Daftar Grup WA yang Diikuti oleh Bot (Membantu mendapatkan Group ID)
app.get("/api/wa/groups", async (req, res) => {
  if (!subscriberService.waService) {
    return res.status(503).json({ error: "WhatsApp service belum aktif" });
  }
  const groups = await subscriberService.waService.getParticipatingGroups();
  res.json({
    total: groups.length,
    groups,
    hint: "Salin ID grup di atas (contoh: 120363xxx@g.us) ke WA_TARGET_PHONE di file .env",
  });
});

// Fallback 404
app.use((req, res) => {
  res.status(404).json({ error: "Endpoint tidak ditemukan di MTM Predictive Service" });
});

// Jalankan Server HTTP
const server = app.listen(config.server.port, () => {
  console.log(`[Server] Dashboard & API berjalan pada http://localhost:${config.server.port}`);
});

// Tangani shutdown graceful
const shutdown = async () => {
  console.log("\n[Shutdown] Mematikan server...");
  server.close();
  await subscriberService.stop();
  process.exit(0);
};

// Global error handlers untuk menjaga kestabilan server
process.on("uncaughtException", (err: any) => {
  if (err?.message?.includes("verifyMAC") || err?.stack?.includes("verifyMAC")) {
    console.warn("[WhatsApp/libsignal] Warning MAC verification failed for session key, safely handled.");
    return;
  }
  console.error("[Uncaught Exception]", err);
});

process.on("unhandledRejection", (reason: any) => {
  console.warn("[Unhandled Rejection]", reason?.message || reason);
});

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
