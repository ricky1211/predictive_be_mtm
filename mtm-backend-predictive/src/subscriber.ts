import mqtt, { MqttClient } from "mqtt";
import fs from "fs";
import path from "path";
import { config } from "./config";
import { TimescaleManager, TelemetryData } from "./dbTimescale";
import { MonitoringManager } from "./dbMonitoring";
import { WhatsAppNotificationService } from "./whatsappService";
import { sendPreventiveAlertWebhook } from "./internalWebhook";

export interface SetPointConfig {
  bushing: number;
  bearing: number;
  link: number;
  vibration: number;
  toastAlert?: boolean;
  audioAlert?: boolean;
}

export interface LogItem {
  id: string;
  timestamp: string;
  topic: string;
  deviceId: string;
  temperature: number | null;
  humidity: number | null;
  pressure: number | null;
  status: string;
  savedTimescale: boolean;
  savedMonitoring: boolean;
  rawPayload: Record<string, any>;
}

export interface LiveDeviceItem {
  device_id: string;
  temperature: number | null;
  humidity: number | null;
  pressure: number | null;
  status: string;
  recorded_at: string;
  last_seen_seconds_ago?: number;
  total_messages: number;
  topic: string;
  raw_payload?: Record<string, any>;
}

export class MqttSubscriberService {
  private client: MqttClient | null = null;
  public timescaleManager: TimescaleManager | null = null;
  public monitoringManager: MonitoringManager | null = null;
  public waService: WhatsAppNotificationService;
  private isConnectedToBroker = false;
  private recentLogs: LogItem[] = [];
  private maxLogs = 50;

  private liveDevices: Map<string, LiveDeviceItem> = new Map();
  private recentTelemetryHistory: TelemetryData[] = [];
  private maxTelemetryHistory = 1000;
  private shiftBuffers: {
    shift1: TelemetryData[];
    shift2: TelemetryData[];
    shift3: TelemetryData[];
  } = {
    shift1: [],
    shift2: [],
    shift3: [],
  };
  private maxShiftBuffer = 500;
  private totalMessagesReceived = 0;
  private lastMessageTimestamp: Date | null = null;

  private setpoints: SetPointConfig = {
    bushing: 70.0,
    bearing: 70.0,
    link: 70.0,
    vibration: 0.180,
    toastAlert: true,
    audioAlert: true,
  };
  private setpointsFile = path.resolve(__dirname, "../setpoints.json");

  constructor() {
    this.waService = new WhatsAppNotificationService();
    this.loadSetpointsFromFile();

    if (config.timescale.enabled) {
      console.log("[App] Mengaktifkan PostgreSQL Timescale...");
      this.timescaleManager = new TimescaleManager();
    }

    if (config.monitoring.enabled) {
      console.log("[App] Mengaktifkan PostgreSQL Monitoring...");
      this.monitoringManager = new MonitoringManager();
    }
  }

  private loadSetpointsFromFile(): void {
    try {
      if (fs.existsSync(this.setpointsFile)) {
        const raw = fs.readFileSync(this.setpointsFile, "utf-8");
        this.setpoints = { ...this.setpoints, ...JSON.parse(raw) };
        console.log("[SetPoints] Berhasil memuat batas set point tersimpan:", this.setpoints);
      }
    } catch (err: any) {
      console.warn("[SetPoints] Gagal membaca setpoints.json:", err.message);
    }
  }

  public getSetPoints(): SetPointConfig {
    return { ...this.setpoints };
  }

  public updateSetPoints(newConfig: Partial<SetPointConfig>): SetPointConfig {
    this.setpoints = {
      ...this.setpoints,
      ...newConfig,
    };
    try {
      fs.writeFileSync(this.setpointsFile, JSON.stringify(this.setpoints, null, 2), "utf-8");
      console.log("[SetPoints] Konfigurasi set point berhasil disimpan ke file:", this.setpoints);
    } catch (err: any) {
      console.warn("[SetPoints] Gagal menyimpan setpoints.json:", err.message);
    }
    return { ...this.setpoints };
  }

  public isMqttConnected(): boolean {
    return this.isConnectedToBroker;
  }

  public getRecentLogs(): LogItem[] {
    return this.recentLogs;
  }

  public getLiveDevices(): LiveDeviceItem[] {
    const now = Date.now();
    return Array.from(this.liveDevices.values()).map(dev => {
      const recTime = new Date(dev.recorded_at).getTime();
      const diffSec = Math.max(0, Math.floor((now - recTime) / 1000));
      return {
        ...dev,
        last_seen_seconds_ago: diffSec,
      };
    });
  }

  public getRecentTelemetryHistory(limit: number = 50, deviceId?: string, targetDate?: string): TelemetryData[] {
    let list = this.recentTelemetryHistory;
    if (deviceId) {
      list = list.filter(t => t.device_id.toLowerCase().includes(deviceId.toLowerCase()));
    }
    if (targetDate) {
      list = list.filter(t => {
        const d = new Date(t.recorded_at || Date.now()).toLocaleDateString("en-CA", { timeZone: "Asia/Jakarta" });
        return d === targetDate;
      });
    }
    return list.slice(0, limit);
  }

  public getShiftTelemetry(targetDate?: string): any {
    const dateStr = targetDate || new Date().toLocaleDateString("en-CA", { timeZone: "Asia/Jakarta" });
    const nowInJakarta = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Jakarta" }));
    const mins = nowInJakarta.getHours() * 60 + nowInJakarta.getMinutes();
    let activeShift: "shift1" | "shift2" | "shift3" = "shift1";
    if (mins >= 0 && mins < 460) activeShift = "shift1";
    else if (mins >= 450 && mins < 970) activeShift = "shift2";
    else activeShift = "shift3";

    const buildShiftObj = (code: string, title: string, timeLabel: string, items: TelemetryData[]) => {
      const normalCount = items.filter(d => (d.status || "").toUpperCase() === "NORMAL").length;
      const warningCount = items.filter(d => (d.status || "").toUpperCase() === "WARNING").length;
      const criticalCount = items.filter(d => (d.status || "").toUpperCase() === "CRITICAL").length;
      const totalCount = items.length;
      const compliance = totalCount > 0 ? Math.round((normalCount / totalCount) * 100) : 100;
      return {
        code,
        title,
        timeLabel,
        totalCount,
        normalCount,
        warningCount,
        criticalCount,
        compliance,
        latestRec: items[0]?.recorded_at || null,
        records: items.slice(0, 100)
      };
    };

    return {
      date: dateStr,
      activeShift,
      shifts: {
        shift1: buildShiftObj("SHIFT-1", "Shift 1", "00:00 - 07:40", this.shiftBuffers.shift1),
        shift2: buildShiftObj("SHIFT-2", "Shift 2", "07:30 - 16:10", this.shiftBuffers.shift2),
        shift3: buildShiftObj("SHIFT-3", "Shift 3", "16:10 - 00:00", this.shiftBuffers.shift3)
      }
    };
  }

  public getMqttDetails() {
    return {
      connected: this.isConnectedToBroker,
      broker: `${config.mqtt.brokerHost}:${config.mqtt.brokerPort}`,
      brokerHost: config.mqtt.brokerHost,
      brokerPort: config.mqtt.brokerPort,
      topicPattern: config.mqtt.topic,
      clientId: config.mqtt.clientId,
      totalMessagesReceived: this.totalMessagesReceived,
      lastMessageTime: this.lastMessageTimestamp ? this.lastMessageTimestamp.toISOString() : null,
      activeDevicesCount: this.liveDevices.size,
      deviceList: Array.from(this.liveDevices.keys()),
    };
  }

  public start(): void {
    // Inisialisasi WhatsApp Service & Watchdog (Deteksi jika data terputus)
    this.waService.init();
    this.waService.startWatchdog(() => this.lastMessageTimestamp);

    const brokerUrl = `mqtt://${config.mqtt.brokerHost}:${config.mqtt.brokerPort}`;
    console.log(`[MQTT] Menghubungkan ke broker: ${brokerUrl}`);

    this.client = mqtt.connect(brokerUrl, {
      clientId: config.mqtt.clientId,
      username: config.mqtt.username || undefined,
      password: config.mqtt.password || undefined,
      keepalive: config.mqtt.keepalive,
      reconnectPeriod: 3000,
    });

    this.client.on("connect", () => {
      this.isConnectedToBroker = true;
      const targetTopic = config.mqtt.topic || "mtm/jingduanforging/#";
      console.log(`[MQTT] Terhubung ke Broker! Khusus Mesin Jingduan Forging: '${targetTopic}'`);

      this.client?.subscribe(targetTopic, (err) => {
        if (err) {
          console.error(`[MQTT] Gagal subscribe ke '${targetTopic}':`, err);
        } else {
          console.log(`[MQTT] Berhasil subscribe ke '${targetTopic}'`);
        }
      });
    });

    this.client.on("reconnect", () => {
      console.log("[MQTT] Mencoba rekoneksi ke broker...");
    });

    this.client.on("offline", () => {
      this.isConnectedToBroker = false;
    });

    this.client.on("error", (err) => {
      this.isConnectedToBroker = false;
      console.error("[MQTT Error]:", err.message);
    });

    this.client.on("message", async (topic: string, message: Buffer) => {
      await this.handleMessage(topic, message.toString("utf-8"));
    });
  }

  private async handleMessage(topic: string, rawStr: string): Promise<void> {
    // Filter eksklusif: HANYA proses data dari Mesin Jingduan Forging
    if (!topic.toLowerCase().includes("jingduanforging")) {
      return;
    }

    let payload: Record<string, any>;
    try {
      payload = JSON.parse(rawStr);
    } catch {
      console.warn(`[Peringatan] Payload bukan JSON valid: ${rawStr}`);
      return;
    }

    // Ekstrak nama perangkat/komponen dari topik atau payload
    const parts = topic.split("/").filter(Boolean);
    let deviceId = "";

    if (parts.length >= 3 && parts[0].toLowerCase() === "mtm") {
      // Format MTM: mtm / jingduanforging / <nama-komponen>
      const component = parts.slice(2).join("/");
      deviceId = component;
    } else if (payload.device_id || payload.deviceId) {
      deviceId = String(payload.device_id || payload.deviceId);
    } else if (parts.length >= 2) {
      deviceId = parts[1];
    } else {
      deviceId = topic || "unknown_device";
    }

    // Ekstrak waktu recordedAt
    let recordedAt: Date = new Date();
    if (payload.date && payload.time) {
      // Format: "2026-10-13" dan "11:33:51"
      const dateStr = `${payload.date}T${payload.time}`;
      const parsedDate = new Date(dateStr);
      if (!isNaN(parsedDate.getTime())) {
        recordedAt = parsedDate;
      }
    } else if (payload.timestamp) {
      const parsedDate = new Date(payload.timestamp);
      if (!isNaN(parsedDate.getTime())) {
        recordedAt = parsedDate;
      }
    }

    // Ekstrak metrik nilai (PV / Process Value, Vibration, Suhu, dll)
    let temperature: number | null = null;
    let humidity: number | null = null;
    let pressure: number | null = null;
    let status: string = payload.status || "NORMAL";

    if (payload.data && typeof payload.data === "object") {
      // Format Industri / PLC MTM (misal {"PV": 49, "DP": 0})
      if (typeof payload.data.PV === "number") {
        temperature = payload.data.PV;
      } else if (typeof payload.data.Temp_C === "number") {
        temperature = payload.data.Temp_C;
      } else if (typeof payload.data.PV === "string" && !isNaN(parseFloat(payload.data.PV))) {
        temperature = parseFloat(payload.data.PV);
      } else if (typeof payload.data.temperature === "number") {
        temperature = payload.data.temperature;
      }

      // Format Getaran / Akselerasi MTM (misal {"Z_RMS_Vel_in": 0.05})
      if (typeof payload.data.Z_RMS_Vel_in === "number") {
        pressure = payload.data.Z_RMS_Vel_in;
      } else if (typeof payload.data.vibration === "number") {
        pressure = payload.data.vibration;
      } else if (typeof payload.data.pressure === "number") {
        pressure = payload.data.pressure;
      }

      if (typeof payload.data.humidity === "number") {
        humidity = payload.data.humidity;
      }
    } else {
      if (typeof payload.temperature === "number") temperature = payload.temperature;
      if (typeof payload.humidity === "number") humidity = payload.humidity;
      if (typeof payload.pressure === "number") pressure = payload.pressure;
    }

    // Evaluasi status sensor secara dinamis terhadap batas set point aktif
    const devLow = deviceId.toLowerCase();
    if (devLow.includes("vibration") || (pressure !== null && pressure < 2.0 && devLow.includes("vib"))) {
      const vibVal = pressure ?? 0;
      const vibWarn = this.setpoints.vibration || 0.180;
      const vibCrit = 0.280; // Standar Mesin Jingduan Forging ISO 10816-8: > 7.1 mm/s (0.280 in/s) Danger
      status = vibVal >= vibCrit ? "CRITICAL" : vibVal >= vibWarn ? "WARNING" : "NORMAL";
    } else if (temperature !== null) {
      let tempWarn = this.setpoints.bushing || 70.0;
      if (devLow.includes("link")) {
        tempWarn = this.setpoints.link || 70.0;
      } else if (devLow.includes("bearing")) {
        tempWarn = this.setpoints.bearing || 70.0;
      } else if (devLow.includes("bush") || devLow.includes("eccentric")) {
        tempWarn = this.setpoints.bushing || 70.0;
      }
      const tempCrit = tempWarn + 5;
      status = temperature >= tempCrit ? "CRITICAL" : temperature >= tempWarn ? "WARNING" : "NORMAL";
    }

    this.totalMessagesReceived++;
    this.lastMessageTimestamp = new Date();

    const telemetry: TelemetryData = {
      device_id: deviceId,
      temperature,
      humidity,
      pressure,
      status,
      recorded_at: recordedAt,
      raw_payload: payload,
    };

    // Update in-memory live cache seketika
    const existing = this.liveDevices.get(deviceId);
    this.liveDevices.set(deviceId, {
      device_id: deviceId,
      temperature: telemetry.temperature ?? null,
      humidity: telemetry.humidity ?? null,
      pressure: telemetry.pressure ?? null,
      status: telemetry.status || "NORMAL",
      recorded_at: recordedAt.toISOString(),
      total_messages: (existing?.total_messages || 0) + 1,
      topic,
      raw_payload: payload,
    });

    // Simpan ke in-memory telemetry buffer untuk fallback API & streaming
    this.recentTelemetryHistory.unshift(telemetry);
    if (this.recentTelemetryHistory.length > this.maxTelemetryHistory) {
      this.recentTelemetryHistory.pop();
    }

    // Simpan ke shift-partitioned buffer agar riwayat per shift tidak hilang saat pergantian shift
    try {
      const recDate = new Date(telemetry.recorded_at || Date.now());
      const nowInJakarta = new Date(recDate.toLocaleString("en-US", { timeZone: "Asia/Jakarta" }));
      const mins = nowInJakarta.getHours() * 60 + nowInJakarta.getMinutes();
      let shiftKey: "shift1" | "shift2" | "shift3" = "shift1";
      if (mins >= 0 && mins < 460) shiftKey = "shift1";
      else if (mins >= 450 && mins < 970) shiftKey = "shift2";
      else shiftKey = "shift3";

      this.shiftBuffers[shiftKey].unshift(telemetry);
      if (this.shiftBuffers[shiftKey].length > this.maxShiftBuffer) {
        this.shiftBuffers[shiftKey].pop();
      }
    } catch {}

    // 1. Simpan time-series ke PostgreSQL Timescale
    // 2. Simpan status pemantauan ke PostgreSQL Monitoring
    const [savedTimescale, savedMonitoring] = await Promise.all([
      this.timescaleManager ? this.timescaleManager.insertTelemetry(telemetry) : Promise.resolve(false),
      this.monitoringManager ? this.monitoringManager.updateDeviceStatus(telemetry) : Promise.resolve(false),
    ]);

    // 3. Evaluasi Notifikasi WhatsApp: Hanya kirim 1x jika menyentuh ambang Warning/Critical
    if (this.waService) {
      this.waService.handleDeviceMetricAlert(deviceId, telemetry.status || "NORMAL", {
        temperature: telemetry.temperature,
        pressure: telemetry.pressure,
      }).catch((err) => {
        console.error("[WhatsApp] Error saat proses alert:", err);
      });
    }

    // 4. Integrasi Lintas-Layanan: Kirim sinyal alert ke Backend Preventive untuk otomatisasi tiket Work Order
    if (telemetry.status === "WARNING" || telemetry.status === "CRITICAL") {
      const metricVal = telemetry.pressure != null ? telemetry.pressure : (telemetry.temperature || 0);
      const isVib = deviceId.toLowerCase().includes("vibration") || deviceId.toLowerCase().includes("vib");
      sendPreventiveAlertWebhook({
        deviceId,
        metricName: isVib ? "Vibration Velocity" : "Bearing/Link Temperature",
        value: metricVal,
        threshold: isVib ? (this.setpoints.vibration || 0.180) : 70.0,
        severity: telemetry.status as "WARNING" | "CRITICAL",
        machineId: "JINGDUAN-FORGING-630T",
        message: `Terdeteksi anomali ${isVib ? "getaran" : "suhu"} (${metricVal} ${isVib ? "in/s" : "°C"}) pada mesin Jingduan (Status: ${telemetry.status}).`,
        timestamp: new Date().toISOString(),
      }).catch(() => {});
    }

    const logEntry: LogItem = {
      id: `${Date.now()}-${Math.floor(Math.random() * 1000)}`,
      timestamp: new Date().toLocaleTimeString("id-ID"),
      topic,
      deviceId,
      temperature: telemetry.temperature ?? null,
      humidity: telemetry.humidity ?? null,
      pressure: telemetry.pressure ?? null,
      status: telemetry.status || "NORMAL",
      savedTimescale,
      savedMonitoring,
      rawPayload: payload,
    };

    this.recentLogs.unshift(logEntry);
    if (this.recentLogs.length > this.maxLogs) {
      this.recentLogs.pop();
    }

    const t = telemetry.temperature !== null ? `${telemetry.temperature}°C` : "N/A";
    const h = telemetry.humidity !== null ? `${telemetry.humidity}%` : "N/A";
    const p = telemetry.pressure !== null ? `${telemetry.pressure}hPa` : "N/A";
    console.log(
      `[Ingest #${this.totalMessagesReceived}] ${deviceId} | T:${t} H:${h} P:${p} | Timescale:${savedTimescale ? "✓" : "✗"} Monitoring:${savedMonitoring ? "✓" : "✗"}`
    );
  }

  public async stop(): Promise<void> {
    if (this.waService) {
      this.waService.stop();
    }
    if (this.client) {
      this.client.end();
    }
    if (this.timescaleManager) {
      await this.timescaleManager.close();
    }
    if (this.monitoringManager) {
      await this.monitoringManager.close();
    }
  }
}
