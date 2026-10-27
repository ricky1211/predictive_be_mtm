import { Pool, PoolClient } from "pg";
import { config } from "./config";

export interface TelemetryData {
  device_id: string;
  temperature?: number | null;
  humidity?: number | null;
  pressure?: number | null;
  status?: string | null;
  recorded_at?: Date | null;
  raw_payload?: Record<string, unknown> | null;
}

export class TimescaleManager {
  private pool: Pool | null = null;
  private connected = false;
  private isHypertableEnabled = false;

  constructor() {
    this.initPool();
  }

  private async ensureDatabaseExists(): Promise<void> {
    try {
      const adminPool = new Pool({
        host: config.timescale.host,
        port: config.timescale.port,
        database: "postgres",
        user: config.timescale.user,
        password: config.timescale.password,
        connectionTimeoutMillis: 4000,
      });

      const client = await adminPool.connect();
      try {
        const check = await client.query(
          "SELECT 1 FROM pg_database WHERE datname = $1",
          [config.timescale.database]
        );
        if (check.rowCount === 0) {
          console.log(`[Timescale] Membuat database '${config.timescale.database}'...`);
          await client.query(`CREATE DATABASE "${config.timescale.database}"`);
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
        host: config.timescale.host,
        port: config.timescale.port,
        database: config.timescale.database,
        user: config.timescale.user,
        password: config.timescale.password,
        max: 10,
        idleTimeoutMillis: 30000,
        connectionTimeoutMillis: 5000,
      });

      this.pool.on("error", (err) => {
        console.error("[Timescale Pool Error]:", err.message);
        this.connected = false;
      });

      await this.initSchema();
    } catch (error: any) {
      console.error("[Timescale] Gagal inisialisasi pool:", error.message);
      this.connected = false;
    }
  }

  private async initSchema(): Promise<void> {
    if (!this.pool) return;
    let client: PoolClient | null = null;
    try {
      client = await this.pool.connect();

      // Buat tabel iot_telemetry untuk time-series data
      await client.query(`
        CREATE TABLE IF NOT EXISTS iot_telemetry (
            id BIGSERIAL,
            device_id VARCHAR(100) NOT NULL,
            temperature DOUBLE PRECISION,
            humidity DOUBLE PRECISION,
            pressure DOUBLE PRECISION,
            status VARCHAR(50),
            raw_payload JSONB,
            recorded_at TIMESTAMPTZ NOT NULL,
            created_at TIMESTAMPTZ DEFAULT CURRENT_TIMESTAMP,
            PRIMARY KEY (id, recorded_at)
        );
        CREATE INDEX IF NOT EXISTS idx_timescale_device_time 
        ON iot_telemetry (device_id, recorded_at DESC);
      `);

      // Coba aktifkan ekstensi timescaledb & buat hypertable jika didukung
      try {
        await client.query(`CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;`);
        await client.query(`
          SELECT create_hypertable('iot_telemetry', 'recorded_at', if_not_exists => TRUE);
        `);
        this.isHypertableEnabled = true;
        console.log("[Timescale] Hypertable TimescaleDB 'iot_telemetry' aktif!");
      } catch {
        this.isHypertableEnabled = false;
        console.log("[Timescale] Standard PostgreSQL time-series table 'iot_telemetry' aktif.");
      }

      this.connected = true;
    } catch (err: any) {
      console.error("[Timescale] Gagal inisialisasi schema:", err.message);
      this.connected = false;
    } finally {
      client?.release();
    }
  }

  public isConnected(): boolean {
    return this.connected;
  }

  public isHypertable(): boolean {
    return this.isHypertableEnabled;
  }

  public async insertTelemetry(data: TelemetryData): Promise<boolean> {
    if (!this.pool) {
      await this.initPool();
      if (!this.pool) return false;
    }

    const query = `
      INSERT INTO iot_telemetry 
      (device_id, temperature, humidity, pressure, status, recorded_at, raw_payload)
      VALUES ($1, $2, $3, $4, $5, $6, $7)
    `;

    const values = [
      data.device_id,
      data.temperature ?? null,
      data.humidity ?? null,
      data.pressure ?? null,
      data.status ?? "NORMAL",
      data.recorded_at ?? new Date(),
      data.raw_payload ? JSON.stringify(data.raw_payload) : null,
    ];

    try {
      await this.pool.query(query, values);
      this.connected = true;
      return true;
    } catch (error: any) {
      console.error("[Timescale] Insert error:", error.message);
      return false;
    }
  }

  public async getRecentTelemetry(limit = 20, deviceId?: string, shift?: string, targetDate?: string): Promise<any[]> {
    if (!this.pool) return [];
    try {
      let query = `
        SELECT id, device_id, temperature, humidity, pressure, status, recorded_at, created_at
        FROM iot_telemetry
        WHERE 1=1
      `;
      const values: any[] = [];
      let paramIdx = 1;

      if (deviceId) {
        query += ` AND device_id = $${paramIdx++}`;
        values.push(deviceId);
      }

      if (targetDate) {
        query += ` AND (recorded_at AT TIME ZONE 'Asia/Jakarta')::date = $${paramIdx++}::date`;
        values.push(targetDate);
      }

      if (shift) {
        if (shift === "1" || shift === "shift1") {
          query += ` AND (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) < 460`;
        } else if (shift === "2" || shift === "shift2") {
          query += ` AND (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) >= 450 AND (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) < 970`;
        } else if (shift === "3" || shift === "shift3") {
          query += ` AND (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) >= 970`;
        }
      }

      query += ` ORDER BY recorded_at DESC LIMIT $${paramIdx++}`;
      values.push(limit);

      const res = await this.pool.query(query, values);
      this.connected = true;
      return res.rows;
    } catch (err: any) {
      console.error("[Timescale] Query recent error:", err.message);
      return [];
    }
  }

  public async getShiftTelemetry(targetDate?: string): Promise<any> {
    if (!this.pool) return null;
    try {
      const dateStr = targetDate || new Date().toLocaleDateString("en-CA", { timeZone: "Asia/Jakarta" });

      // Determine active shift in Asia/Jakarta
      const nowInJakarta = new Date(new Date().toLocaleString("en-US", { timeZone: "Asia/Jakarta" }));
      const mins = nowInJakarta.getHours() * 60 + nowInJakarta.getMinutes();
      let activeShift: "shift1" | "shift2" | "shift3" = "shift1";
      if (mins >= 0 && mins < 460) activeShift = "shift1";
      else if (mins >= 450 && mins < 970) activeShift = "shift2";
      else activeShift = "shift3";

      // 1. Summary stats per shift for targetDate
      const summaryRes = await this.pool.query(`
        SELECT 
          CASE 
            WHEN (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) < 460 THEN 'shift1'
            WHEN (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) < 970 THEN 'shift2'
            ELSE 'shift3'
          END AS shift,
          COUNT(*)::int AS total_count,
          COUNT(CASE WHEN UPPER(status) = 'NORMAL' THEN 1 END)::int AS normal_count,
          COUNT(CASE WHEN UPPER(status) = 'WARNING' THEN 1 END)::int AS warning_count,
          COUNT(CASE WHEN UPPER(status) = 'CRITICAL' THEN 1 END)::int AS critical_count,
          MAX(recorded_at) as latest_rec
        FROM iot_telemetry
        WHERE (recorded_at AT TIME ZONE 'Asia/Jakarta')::date = $1::date
        GROUP BY shift
      `, [dateStr]);

      const statsMap: Record<string, any> = {
        shift1: { totalCount: 0, normalCount: 0, warningCount: 0, criticalCount: 0, latestRec: null },
        shift2: { totalCount: 0, normalCount: 0, warningCount: 0, criticalCount: 0, latestRec: null },
        shift3: { totalCount: 0, normalCount: 0, warningCount: 0, criticalCount: 0, latestRec: null }
      };

      for (const row of summaryRes.rows) {
        if (statsMap[row.shift]) {
          statsMap[row.shift] = {
            totalCount: row.total_count,
            normalCount: row.normal_count,
            warningCount: row.warning_count,
            criticalCount: row.critical_count,
            latestRec: row.latest_rec
          };
        }
      }

      // 2. Fetch up to 100 recent records per shift
      const recordsRes = await this.pool.query(`
        WITH ranked AS (
          SELECT 
            id, device_id, temperature, humidity, pressure, status, recorded_at, created_at,
            CASE 
              WHEN (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) < 460 THEN 'shift1'
              WHEN (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) < 970 THEN 'shift2'
              ELSE 'shift3'
            END AS shift,
            ROW_NUMBER() OVER (
              PARTITION BY 
                CASE 
                  WHEN (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) < 460 THEN 'shift1'
                  WHEN (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) < 970 THEN 'shift2'
                  ELSE 'shift3'
                END
              ORDER BY recorded_at DESC
            ) as rn
          FROM iot_telemetry
          WHERE (recorded_at AT TIME ZONE 'Asia/Jakarta')::date = $1::date
        )
        SELECT id, device_id, temperature, humidity, pressure, status, recorded_at, created_at, shift
        FROM ranked
        WHERE rn <= 100
        ORDER BY recorded_at DESC
      `, [dateStr]);

      const recordsMap: Record<string, any[]> = {
        shift1: [],
        shift2: [],
        shift3: []
      };

      for (const r of recordsRes.rows) {
        if (recordsMap[r.shift]) {
          recordsMap[r.shift].push(r);
        }
      }

      const calcCompliance = (normal: number, total: number) => {
        if (total === 0) return 100;
        return Math.round((normal / total) * 100);
      };

      // 3. Vibration mode & stats per shift
      const vibShiftRes = await this.pool.query(`
        SELECT 
          CASE 
            WHEN (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) < 460 THEN 'shift1'
            WHEN (EXTRACT(HOUR FROM recorded_at AT TIME ZONE 'Asia/Jakarta') * 60 + EXTRACT(MINUTE FROM recorded_at AT TIME ZONE 'Asia/Jakarta')) < 970 THEN 'shift2'
            ELSE 'shift3'
          END AS shift,
          MODE() WITHIN GROUP (ORDER BY ROUND(COALESCE(pressure, temperature, 0)::numeric, 3)) as mode_val,
          ROUND(AVG(COALESCE(pressure, temperature, 0))::numeric, 4) as avg_val,
          COUNT(*)::int as total_samples
        FROM iot_telemetry
        WHERE device_id = 'vibration'
          AND (recorded_at AT TIME ZONE 'Asia/Jakarta')::date = $1::date
        GROUP BY shift
      `, [dateStr]);

      const vibShiftMap: Record<string, any> = {
        shift1: { mode_val: null, avg_val: null, total_samples: 0 },
        shift2: { mode_val: null, avg_val: null, total_samples: 0 },
        shift3: { mode_val: null, avg_val: null, total_samples: 0 }
      };

      for (const r of vibShiftRes.rows) {
        if (vibShiftMap[r.shift]) {
          vibShiftMap[r.shift] = {
            mode_val: r.mode_val !== null ? Number(r.mode_val).toFixed(4) : null,
            avg_val: r.avg_val !== null ? Number(r.avg_val).toFixed(4) : null,
            total_samples: r.total_samples || 0
          };
        }
      }

      // 4. Overall vibration stats & distribution
      const vibOverallRes = await this.pool.query(`
        SELECT 
          MODE() WITHIN GROUP (ORDER BY ROUND(COALESCE(pressure, temperature, 0)::numeric, 3)) as overall_mode,
          ROUND(AVG(COALESCE(pressure, temperature, 0))::numeric, 4) as overall_avg,
          ROUND(MIN(COALESCE(pressure, temperature, 0))::numeric, 4) as min_val,
          ROUND(MAX(COALESCE(pressure, temperature, 0))::numeric, 4) as max_val,
          COUNT(*)::int as total_samples
        FROM iot_telemetry
        WHERE device_id = 'vibration'
          AND (recorded_at AT TIME ZONE 'Asia/Jakarta')::date = $1::date
      `, [dateStr]);

      const vibOverall = vibOverallRes.rows[0] || {};

      // 5. Vibration frequency brackets
      const vibDistRes = await this.pool.query(`
        SELECT 
          CASE 
            WHEN COALESCE(pressure, temperature, 0) < 0.03 THEN '< 0.030 in/s'
            WHEN COALESCE(pressure, temperature, 0) < 0.06 THEN '0.030 - 0.060 in/s'
            WHEN COALESCE(pressure, temperature, 0) < 0.08 THEN '0.060 - 0.080 in/s'
            ELSE '≥ 0.080 in/s'
          END AS range_key,
          CASE 
            WHEN COALESCE(pressure, temperature, 0) < 0.03 THEN 'Rendah / Tenang'
            WHEN COALESCE(pressure, temperature, 0) < 0.06 THEN 'Normal Operasi'
            WHEN COALESCE(pressure, temperature, 0) < 0.08 THEN 'Mendekati Batas'
            ELSE 'Tinggi / Waspada'
          END AS label,
          CASE 
            WHEN COALESCE(pressure, temperature, 0) < 0.03 THEN 'low'
            WHEN COALESCE(pressure, temperature, 0) < 0.06 THEN 'normal'
            WHEN COALESCE(pressure, temperature, 0) < 0.08 THEN 'warning'
            ELSE 'critical'
          END AS level,
          COUNT(*)::int AS count
        FROM iot_telemetry
        WHERE device_id = 'vibration'
          AND (recorded_at AT TIME ZONE 'Asia/Jakarta')::date = $1::date
        GROUP BY range_key, label, level
        ORDER BY count DESC
      `, [dateStr]);

      const totalVibSamples = vibOverall.total_samples || 0;
      let dominantBracket = null;
      let maxDistCount = 0;
      const distribution = (vibDistRes.rows || []).map(row => {
        const pct = totalVibSamples > 0 ? Math.round((row.count / totalVibSamples) * 100) : 0;
        if (row.count > maxDistCount) {
          maxDistCount = row.count;
          dominantBracket = `${row.range_key} (${row.label})`;
        }
        return {
          rangeKey: row.range_key,
          label: row.label,
          level: row.level,
          count: row.count,
          percentage: pct
        };
      });

      this.connected = true;
      return {
        date: dateStr,
        activeShift,
        shifts: {
          shift1: {
            title: "Shift 1",
            code: "SHIFT-1",
            timeLabel: "00:00 - 07:40",
            totalCount: statsMap.shift1.totalCount,
            normalCount: statsMap.shift1.normalCount,
            warningCount: statsMap.shift1.warningCount,
            criticalCount: statsMap.shift1.criticalCount,
            compliance: calcCompliance(statsMap.shift1.normalCount, statsMap.shift1.totalCount),
            latestRec: statsMap.shift1.latestRec,
            records: recordsMap.shift1,
            vibrationMode: vibShiftMap.shift1.mode_val,
            vibrationAvg: vibShiftMap.shift1.avg_val,
            vibrationSamples: vibShiftMap.shift1.total_samples
          },
          shift2: {
            title: "Shift 2",
            code: "SHIFT-2",
            timeLabel: "07:30 - 16:10",
            totalCount: statsMap.shift2.totalCount,
            normalCount: statsMap.shift2.normalCount,
            warningCount: statsMap.shift2.warningCount,
            criticalCount: statsMap.shift2.criticalCount,
            compliance: calcCompliance(statsMap.shift2.normalCount, statsMap.shift2.totalCount),
            latestRec: statsMap.shift2.latestRec,
            records: recordsMap.shift2,
            vibrationMode: vibShiftMap.shift2.mode_val,
            vibrationAvg: vibShiftMap.shift2.avg_val,
            vibrationSamples: vibShiftMap.shift2.total_samples
          },
          shift3: {
            title: "Shift 3",
            code: "SHIFT-3",
            timeLabel: "16:10 - 00:00",
            totalCount: statsMap.shift3.totalCount,
            normalCount: statsMap.shift3.normalCount,
            warningCount: statsMap.shift3.warningCount,
            criticalCount: statsMap.shift3.criticalCount,
            compliance: calcCompliance(statsMap.shift3.normalCount, statsMap.shift3.totalCount),
            latestRec: statsMap.shift3.latestRec,
            records: recordsMap.shift3,
            vibrationMode: vibShiftMap.shift3.mode_val,
            vibrationAvg: vibShiftMap.shift3.avg_val,
            vibrationSamples: vibShiftMap.shift3.total_samples
          }
        },
        vibrationStats: {
          overallMode: vibOverall.overall_mode !== null && vibOverall.overall_mode !== undefined ? Number(vibOverall.overall_mode).toFixed(4) : "--",
          overallAvg: vibOverall.overall_avg !== null && vibOverall.overall_avg !== undefined ? Number(vibOverall.overall_avg).toFixed(4) : "--",
          minVal: vibOverall.min_val !== null && vibOverall.min_val !== undefined ? Number(vibOverall.min_val).toFixed(4) : "--",
          maxVal: vibOverall.max_val !== null && vibOverall.max_val !== undefined ? Number(vibOverall.max_val).toFixed(4) : "--",
          totalSamples: totalVibSamples,
          dominantRange: dominantBracket || "--",
          distribution
        }
      };
    } catch (err: any) {
      console.error("[Timescale] getShiftTelemetry error:", err.message);
      return null;
    }
  }

  public async getStats(): Promise<{ totalRows: number; activeDevices: number }> {
    if (!this.pool) return { totalRows: 0, activeDevices: 0 };
    try {
      const res = await this.pool.query(`
        SELECT 
          COUNT(*)::int AS total_rows,
          COUNT(DISTINCT device_id)::int AS active_devices
        FROM iot_telemetry
      `);
      this.connected = true;
      return {
        totalRows: res.rows[0]?.total_rows || 0,
        activeDevices: res.rows[0]?.active_devices || 0,
      };
    } catch {
      return { totalRows: 0, activeDevices: 0 };
    }
  }

  public async close(): Promise<void> {
    if (this.pool) {
      await this.pool.end();
      this.connected = false;
      console.log("[Timescale] Pool connection ditutup.");
    }
  }
}
