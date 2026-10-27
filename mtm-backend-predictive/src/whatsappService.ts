import makeWASocket, {
  DisconnectReason,
  useMultiFileAuthState,
  WASocket,
} from "@whiskeysockets/baileys";
import { Boom } from "@hapi/boom";
import pino from "pino";
import path from "path";
import fs from "fs";
import qrcode from "qrcode";
import qrcodeTerminal from "qrcode-terminal";
import { config } from "./config";

export interface WaStatusResponse {
  enabled: boolean;
  connected: boolean;
  userPhone: string | null;
  hasQr: boolean;
  qrDataUrl: string | null;
  targetPhone: string;
  mqttTimeoutSeconds: number;
}

export class WhatsAppNotificationService {
  private sock: WASocket | null = null;
  private isConnected = false;
  private latestQrText: string | null = null;
  private latestQrDataUrl: string | null = null;
  private userJid: string | null = null;
  private authFolder: string;

  // State Machine untuk memastikan HANYA 1 NOTIFIKASI per kejadian
  private deviceAlarmStates: Map<string, "NORMAL" | "WARNING" | "CRITICAL"> = new Map();
  private mqttConnectionState: "ONLINE" | "OFFLINE" = "ONLINE";
  private watchdogInterval: NodeJS.Timeout | null = null;

  constructor() {
    this.authFolder = path.resolve(__dirname, "../wa_auth_session");
    if (!fs.existsSync(this.authFolder)) {
      fs.mkdirSync(this.authFolder, { recursive: true });
    }
  }

  public async init(): Promise<void> {
    if (!config.whatsapp.enabled) {
      console.log("[WhatsApp] Fitur notifikasi WA dinonaktifkan di konfigurasi.");
      return;
    }

    try {
      const { state, saveCreds } = await useMultiFileAuthState(this.authFolder);

      this.sock = makeWASocket({
        auth: state,
        printQRInTerminal: false,
        logger: pino({ level: "silent" }), // Heningkan log internal socket agar konsol bersih
        browser: ["MTM Maintenance System", "Chrome", "1.0.0"],
      });

      this.sock.ev.on("creds.update", saveCreds);

      this.sock.ev.on("connection.update", async (update) => {
        const { connection, lastDisconnect, qr } = update;

        if (qr) {
          this.latestQrText = qr;
          this.isConnected = false;
          try {
            this.latestQrDataUrl = await qrcode.toDataURL(qr);
          } catch {
            this.latestQrDataUrl = null;
          }

          console.log("\n============================================================");
          console.log(" [WhatsApp] SCAN QR CODE DI BAWAH INI DENGAN WHATSAPP ANDA:");
          console.log(" Buka WhatsApp di HP > Perangkat Tertaut > Tautkan Perangkat");
          console.log(" Atau akses via browser di: http://localhost:4000/api/wa/qr");
          console.log("============================================================\n");
          qrcodeTerminal.generate(qr, { small: true });
        }

        if (connection === "close") {
          this.isConnected = false;
          this.latestQrText = null;
          this.latestQrDataUrl = null;

          const statusCode = (lastDisconnect?.error as Boom)?.output?.statusCode;
          const shouldReconnect = statusCode !== DisconnectReason.loggedOut;

          console.log(
            `[WhatsApp] Koneksi terputus (status: ${statusCode}). Mencoba rekoneksi: ${shouldReconnect}`
          );

          if (shouldReconnect) {
            setTimeout(() => this.init(), 5000);
          } else {
            console.log("[WhatsApp] Sesi telah logout. Silakan restart & scan QR kembali.");
          }
        } else if (connection === "open") {
          this.isConnected = true;
          this.latestQrText = null;
          this.latestQrDataUrl = null;
          this.userJid = this.sock?.user?.id || null;

          console.log("============================================================");
          console.log(` [WhatsApp] TERHUBUNG SUKSES! Akun: ${this.userJid}`);
          console.log(` [WhatsApp] Target Notif: ${config.whatsapp.targetPhone || "Belum diatur di .env"}`);
          console.log("============================================================");
        }
      });
    } catch (err: any) {
      console.error("[WhatsApp] Error saat inisialisasi:", err?.message || err);
    }
  }

  // Format nomor penerima (mendukung format '08xxx', '628xxx', atau group ID 'xxx@g.us')
  private formatJid(target: string): string {
    let clean = target.trim().replace(/[^0-9@a-zA-Z._-]/g, "");
    if (clean.endsWith("@g.us") || clean.endsWith("@s.whatsapp.net")) {
      return clean;
    }
    if (clean.startsWith("0")) {
      clean = "62" + clean.slice(1);
    }
    return `${clean}@s.whatsapp.net`;
  }

  // Ambil daftar target (bisa multiple nomor atau grup dipisahkan tanda koma)
  public getTargetList(): string[] {
    const raw = process.env.WA_TARGET_PHONE || config.whatsapp.targetPhone || "";
    return raw
      .split(",")
      .map((t) => t.trim())
      .filter((t) => t.length > 0);
  }

  // Pengiriman pesan dasar
  public async sendMessage(target: string, text: string): Promise<boolean> {
    if (!this.isConnected || !this.sock) {
      console.warn("[WhatsApp] Gagal kirim notifikasi: WhatsApp belum terhubung / belum login.");
      return false;
    }

    const jid = this.formatJid(target);
    try {
      await this.sock.sendMessage(jid, { text });
      console.log(`[WhatsApp] Notifikasi terkirim ke ${jid}`);
      return true;
    } catch (err: any) {
      console.error(`[WhatsApp] Gagal kirim pesan ke ${jid}:`, err?.message || err);
      return false;
    }
  }

  // Broadcast notifikasi ke seluruh target terdaftar (nomor pribadi & grup)
  public async broadcastMessage(text: string): Promise<boolean> {
    const targets = this.getTargetList();
    if (targets.length === 0) {
      console.warn("[WhatsApp] Target nomor/group WA belum diisi di WA_TARGET_PHONE pada .env!");
      return false;
    }

    let allSuccess = true;
    for (const target of targets) {
      const ok = await this.sendMessage(target, text);
      if (!ok) allSuccess = false;
      if (targets.length > 1) {
        await new Promise((r) => setTimeout(r, 600)); // jeda 600ms antar pengiriman
      }
    }
    return allSuccess;
  }

  // Mengambil daftar semua Grup WA yang diikuti oleh bot beserta ID-nya
  public async getParticipatingGroups(): Promise<Array<{ id: string; subject: string; participantsCount: number }>> {
    if (!this.isConnected || !this.sock) {
      return [];
    }
    try {
      const groups = await this.sock.groupFetchAllParticipating();
      return Object.values(groups).map((g) => ({
        id: g.id,
        subject: g.subject,
        participantsCount: g.participants?.length || 0,
      }));
    } catch (err: any) {
      console.error("[WhatsApp] Gagal mengambil daftar grup:", err?.message || err);
      return [];
    }
  }

  // =========================================================================
  // DETEKSI 1: SENSOR MENYENTUH AMBANG WARNING (HANYA KIRIM 1 KALI PER SIKLUS)
  // =========================================================================
  public async handleDeviceMetricAlert(
    deviceId: string,
    currentStatus: string,
    details: {
      temperature?: number | null;
      pressure?: number | null;
      metricName?: string;
    }
  ): Promise<void> {
    const normStatus = (currentStatus || "NORMAL").toUpperCase();
    const prevStatus = this.deviceAlarmStates.get(deviceId) || "NORMAL";

    // Jika status turun kembali ke NORMAL, reset state agar siap untuk peringatan berikutnya
    if (normStatus === "NORMAL") {
      if (prevStatus !== "NORMAL") {
        this.deviceAlarmStates.set(deviceId, "NORMAL");
        console.log(`[WhatsApp State] Device '${deviceId}' pulih ke status NORMAL. State alarm di-reset.`);
      }
      return;
    }

    // Jika status berubah ke WARNING atau CRITICAL dan status sebelumnya adalah NORMAL:
    // HANYA TERKIRIM 1 KALI KARENA SETELAH DIKIRIM, STATE DIUBAH!
    if ((normStatus === "WARNING" || normStatus === "CRITICAL") && prevStatus === "NORMAL") {
      this.deviceAlarmStates.set(deviceId, normStatus as "WARNING" | "CRITICAL");

      const target = config.whatsapp.targetPhone;
      if (!target) {
        console.warn("[WhatsApp] Target nomor/group WA belum diisi di WA_TARGET_PHONE pada .env!");
        return;
      }

      const icon = normStatus === "CRITICAL" ? "🚨" : "⚠️";
      const timeStr = new Date().toLocaleString("id-ID", { timeZone: "Asia/Jakarta" });

      let metricInfo = "";
      if (details.temperature !== null && details.temperature !== undefined) {
        metricInfo += `\n🌡️ *Suhu*: ${details.temperature} °C`;
      }
      if (details.pressure !== null && details.pressure !== undefined) {
        metricInfo += `\n📈 *Vibrasi / Tekanan*: ${details.pressure}`;
      }

      const message =
        `${icon} *[EARLY WARNING] SENSOR MELEWATI THRESHOLD!*` +
        `\n🏭 *PT Menara Terus Makmur - Maintenance Alert*` +
        `\n` +
        `\n📌 *Mesin*: JINGDUAN FORGING 01` +
        `\n🔧 *Komponen*: ${deviceId}` +
        `\n📊 *Status*: *${normStatus}*` +
        metricInfo +
        `\n🕒 *Waktu Terdeteksi*: ${timeStr} WIB` +
        `\n` +
        `\n⚠️ *Peringatan Dini*: Parameter telah menyentuh batas ${normStatus}. Harap lakukan inspeksi pelumasan & kelancaran komponen!` +
        `\nℹ️ _(Notifikasi ini hanya dikirim 1x saat transisi status)._`;

      console.log(`[WhatsApp] Mengirim 1x Peringatan ${normStatus} untuk komponen '${deviceId}'...`);
      await this.broadcastMessage(message);
    }
  }

  // =========================================================================
  // DETEKSI 2: DATA MQTT TIDAK MASUK / STALL (HANYA KIRIM 1 KALI PER KEJADIAN)
  // =========================================================================
  public startWatchdog(getLastMessageTimestamp: () => Date | null): void {
    const timeoutSec = config.whatsapp.mqttTimeoutSeconds || 180;
    console.log(`[WhatsApp Watchdog] Aktif. Toleransi data MQTT terhenti: ${timeoutSec} detik (${Math.round(timeoutSec / 60)} menit).`);

    // Periksa setiap 30 detik
    this.watchdogInterval = setInterval(async () => {
      const lastTime = getLastMessageTimestamp();
      const now = Date.now();
      const targets = this.getTargetList();

      if (targets.length === 0) return;

      const diffSec = lastTime ? Math.floor((now - lastTime.getTime()) / 1000) : 999999;

      // Kondisi data tidak masuk (melebihi batas toleransi)
      if (diffSec > timeoutSec) {
        // HANYA kirim jika sebelumnya berstatus ONLINE (Menjamin HANYA 1x NOTIFIKASI)
        if (this.mqttConnectionState === "ONLINE") {
          this.mqttConnectionState = "OFFLINE";

          const lastSeenStr = lastTime
            ? lastTime.toLocaleString("id-ID", { timeZone: "Asia/Jakarta" }) + " WIB"
            : "Belum pernah menerima data sejak server aktif";

          const message =
            `🚨 *[ALERT] KONEKSI DATA MQTT TERPUTUS!*` +
            `\n🏭 *PT Menara Terus Makmur - Maintenance Alert*` +
            `\n` +
            `\n📌 *Mesin*: JINGDUAN FORGING 01` +
            `\n⚠️ *Status*: *DATA TIDAK MASUK*` +
            `\n⏱️ *Durasi Terputus*: > ${Math.round(diffSec / 60)} menit (${diffSec} detik)` +
            `\n🕒 *Data Terakhir Diterima*: ${lastSeenStr}` +
            `\n` +
            `\n🛠️ *Tindakan Diperlukan*:` +
            `\n1. Periksa koneksi IoT Gateway / Node-RED / WiFi di area mesin.` +
            `\n2. Pastikan PLC Jingduan & transmisi MQTT broker aktif.` +
            `\nℹ️ _(Pemberitahuan ini hanya dikirim 1x)._`;

          console.log(`[WhatsApp Watchdog] Data MQTT macet (> ${timeoutSec}s). Mengirim 1x Alert ke semua target...`);
          await this.broadcastMessage(message);
        }
      } else {
        // Jika data masuk kembali dan sebelumnya sempat OFFLINE, kirim 1x notif pemulihan
        if (this.mqttConnectionState === "OFFLINE") {
          this.mqttConnectionState = "ONLINE";
          const recoverTimeStr = new Date().toLocaleString("id-ID", { timeZone: "Asia/Jakarta" }) + " WIB";

          const recoverMessage =
            `✅ *[RESTORED] KONEKSI DATA MQTT KEMBALI NORMAL*` +
            `\n🏭 *PT Menara Terus Makmur - Maintenance Alert*` +
            `\n` +
            `\n📌 *Mesin*: JINGDUAN FORGING 01` +
            `\n🕒 *Waktu Pemulihan*: ${recoverTimeStr}` +
            `\n📊 *Status*: Aliran data telemetri telah masuk kembali normal.`;

          console.log("[WhatsApp Watchdog] Data MQTT kembali masuk! Mengirim 1x info pemulihan ke semua target...");
          await this.broadcastMessage(recoverMessage);
        }
      }
    }, 30000);
  }

  public getStatus(): WaStatusResponse {
    return {
      enabled: config.whatsapp.enabled,
      connected: this.isConnected,
      userPhone: this.userJid,
      hasQr: !!this.latestQrText,
      qrDataUrl: this.latestQrDataUrl,
      targetPhone: this.getTargetList().join(", "),
      mqttTimeoutSeconds: config.whatsapp.mqttTimeoutSeconds,
    };
  }

  public stop(): void {
    if (this.watchdogInterval) {
      clearInterval(this.watchdogInterval);
    }
  }
}
