import dotenv from "dotenv";
import path from "path";
import fs from "fs";

// Muat konfigurasi dari file .env di folder root repository dan folder backend
const rootEnv = path.resolve(__dirname, "../../.env");
const backendEnv = path.resolve(__dirname, "../.env");

if (fs.existsSync(rootEnv)) {
  dotenv.config({ path: rootEnv });
}
if (fs.existsSync(backendEnv)) {
  dotenv.config({ path: backendEnv });
}
dotenv.config();

// Sanitasi topic MQTT agar wildcard '#' tidak hilang jika terpotong dotenv
let rawTopic = (process.env.MQTT_TOPIC || "").trim();
if (!rawTopic || rawTopic === "mtm/jingduanforging" || rawTopic === "mtm/jingduanforging/") {
  rawTopic = "mtm/jingduanforging/#";
} else if (!rawTopic.includes("#") && !rawTopic.includes("+")) {
  rawTopic = rawTopic.endsWith("/") ? `${rawTopic}#` : `${rawTopic}/#`;
}

export const config = {
  server: {
    port: parseInt(process.env.API_PORT || process.env.PORT || "4001", 10),
  },
  preventiveWebhookUrl: process.env.PREVENTIVE_WEBHOOK_URL || "http://localhost:4002/api/internal/alert-wo",
  mqtt: {
    brokerHost: process.env.MQTT_BROKER_HOST || "localhost",
    brokerPort: parseInt(process.env.MQTT_BROKER_PORT || "1883", 10),
    topic: rawTopic,
    clientId: process.env.MQTT_CLIENT_ID || "mqtt_ts_subscriber",
    username: process.env.MQTT_USERNAME || "",
    password: process.env.MQTT_PASSWORD || "",
    keepalive: parseInt(process.env.MQTT_KEEPALIVE || "60", 10),
  },
  // 1. PostgreSQL untuk Timescale (Time-series Telemetry Storage)
  timescale: {
    enabled: (process.env.ENABLE_TIMESCALE || "true").toLowerCase() === "true",
    host: process.env.TIMESCALE_HOST || process.env.POSTGRES_HOST || "localhost",
    port: parseInt(process.env.TIMESCALE_PORT || process.env.POSTGRES_PORT || "5432", 10),
    database: process.env.TIMESCALE_DB || "timescale_db",
    user: process.env.TIMESCALE_USER || process.env.POSTGRES_USER || "postgres",
    password: process.env.TIMESCALE_PASSWORD || process.env.POSTGRES_PASSWORD || "postgres",
  },
  // 2. PostgreSQL untuk Monitoring (Device Health, Ingestion Audit & Status)
  monitoring: {
    enabled: (process.env.ENABLE_MONITORING || "true").toLowerCase() === "true",
    host: process.env.MONITORING_HOST || process.env.POSTGRES_HOST || "localhost",
    port: parseInt(process.env.MONITORING_PORT || process.env.POSTGRES_PORT || "5432", 10),
    database: process.env.MONITORING_DB || "monitoring_db",
    user: process.env.MONITORING_USER || process.env.POSTGRES_USER || "postgres",
    password: process.env.MONITORING_PASSWORD || process.env.POSTGRES_PASSWORD || "postgres",
  },
  // 3. Konfigurasi WhatsApp Notification (Baileys)
  whatsapp: {
    enabled: (process.env.ENABLE_WA_NOTIF || "true").toLowerCase() === "true",
    targetPhone: process.env.WA_TARGET_PHONE || "", // Misal: '628123456789' atau Group ID 'xxx@g.us'
    mqttTimeoutSeconds: parseInt(process.env.MQTT_DATA_TIMEOUT_SECONDS || "180", 10), // Default: 3 menit tidak ada data
  },
};
