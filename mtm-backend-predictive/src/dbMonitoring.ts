import { Pool, PoolClient } from "pg";
import { config } from "./config";
import { TelemetryData } from "./dbTimescale";

export class MonitoringManager {
  private pool: Pool | null = null;
  private connected = false;

  constructor() {
    this.initPool();
  }

  private async ensureDatabaseExists(): Promise<void> {
    try {
      const adminPool = new Pool({
        host: config.monitoring.host,
        port: config.monitoring.port,
        database: "postgres",
        user: config.monitoring.user,
        password: config.monitoring.password,
        connectionTimeoutMillis: 4000,
      });

      const client = await adminPool.connect();
      try {
        const check = await client.query(
          "SELECT 1 FROM pg_database WHERE datname = $1",
          [config.monitoring.database]
        );
        if (check.rowCount === 0) {
          console.log(`[Monitoring DB] Membuat database '${config.monitoring.database}'...`);
          await client.query(`CREATE DATABASE "${config.monitoring.database}"`);
        }
      } finally {
        client.release();
        await adminPool.end();
      }
    } catch {
      // Abaikan jika tidak punya akses create db
    }
  }

  private async initPool(): Promise<void> {
    try {
      await this.ensureDatabaseExists();

      this.pool = new Pool({
        host: config.monitoring.host,
        port: config.monitoring.port,
        database: config.monitoring.database,
        user: config.monitoring.user,
        password: config.monitoring.password,
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 5000,
      });

      this.pool.on("error", (err) => {
        console.error("[Monitoring DB Pool Error]:", err.message);
        this.connected = false;
      });

      await this.initSchema();
    } catch (error: any) {
      console.error("[Monitoring DB] Gagal inisialisasi pool:", error.message);
      this.connected = false;
    }
  }

  private async initSchema(): Promise<void> {
    if (!this.pool) return;
    let client: PoolClient | null = null;
    try {
      client = await this.pool.connect();

      // 1. Tabel status pemantauan perangkat real-time
      await client.query(`
        CREATE TABLE IF NOT EXISTS device_monitoring (
            device_id VARCHAR(100) PRIMARY KEY,
            last_seen TIMESTAMPTZ NOT NULL,
            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            pressure DOUBLE PRECISION,
            status VARCHAR(50) DEFAULT 'NORMAL',
            total_messages BIGINT DEFAULT 1,
            updated_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );

        -- 2. Tabel audit log / event pemantauan sistem
        CREATE TABLE IF NOT EXISTS system_monitoring_logs (
            id BIGSERIAL PRIMARY KEY,
            device_id VARCHAR(100),
            event_type VARCHAR(50) DEFAULT 'TELEMETRY_INGEST',
            message TEXT,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP
        );
      `);

      this.connected = true;
      console.log("[Monitoring DB] Tabel 'device_monitoring' & 'system_monitoring_logs' siap digunakan.");
    } catch (err: any) {
      console.error("[Monitoring DB] Gagal inisialisasi schema:", err.message);
      this.connected = false;
    } finally {
      client?.release();
    }
  }

  public isConnected(): boolean {
    return this.connected;
  }

  public async updateDeviceStatus(data: TelemetryData): Promise<boolean> {
    if (!this.pool) {
      await this.initPool();
      if (!this.pool) return false;
    }

    const query = `
      INSERT INTO device_monitoring 
        (device_id, last_seen, temperature, humidity, pressure, status, total_messages, updated_at)
      VALUES 
        ($1, $2, $3, $4, $5, $6, 1, CURRENT_TIMESTAMP)
      ON CONFLICT (device_id) DO UPDATE SET
        last_seen = EXCLUDED.last_seen,
        temperature = EXCLUDED.temperature,
        humidity = EXCLUDED.humidity,
        pressure = EXCLUDED.pressure,
        status = EXCLUDED.status,
        total_messages = device_monitoring.total_messages + 1,
        updated_at = CURRENT_TIMESTAMP;
    `;

    const values = [
      data.device_id,
      data.recorded_at ?? new Date(),
      data.temperature ?? null,
      data.humidity ?? null,
      data.pressure ?? null,
      data.status ?? "NORMAL",
    ];

    try {
      await this.pool.query(query, values);
      this.connected = true;
      return true;
    } catch (error: any) {
      console.error("[Monitoring DB] Update device status error:", error.message);
      return false;
    }
  }

  public async getLatestPerDevice(): Promise<any[]> {
    if (!this.pool) return [];
    try {
      const res = await this.pool.query(`
        SELECT 
          device_id,
          temperature,
          humidity,
          pressure,
          status,
          last_seen AS recorded_at,
          total_messages
        FROM device_monitoring
        ORDER BY last_seen DESC
      `);
      this.connected = true;
      return res.rows;
    } catch (err: any) {
      console.error("[Monitoring DB] Query devices error:", err.message);
      return [];
    }
  }

  public async logEvent(deviceId: string, eventType: string, message: string): Promise<void> {
    if (!this.pool) return;
    try {
      await this.pool.query(
        "INSERT INTO system_monitoring_logs (device_id, event_type, message) VALUES ($1, $2, $3)",
        [deviceId, eventType, message]
      );
    } catch {
      // Abaikan kegagalan logging sekunder
    }
  }

  public async getStats(): Promise<{ activeDevices: number; totalAlerts: number }> {
    if (!this.pool) return { activeDevices: 0, totalAlerts: 0 };
    try {
      const res = await this.pool.query(`
        SELECT 
          COUNT(*)::int AS active_devices,
          COUNT(CASE WHEN status != 'NORMAL' THEN 1 END)::int AS total_alerts
        FROM device_monitoring
      `);
      this.connected = true;
      return {
        activeDevices: res.rows[0]?.active_devices || 0,
        totalAlerts: res.rows[0]?.total_alerts || 0,
      };
    } catch {
      return { activeDevices: 0, totalAlerts: 0 };
    }
  }

  public async close(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
      this.connected = false;
      console.log("[Monitoring DB] Pool connection ditutup.");
    }
  }
}
