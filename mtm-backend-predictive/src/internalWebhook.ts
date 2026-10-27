import http from "http";
import https from "https";
import { config } from "./config";

export interface AlertPayload {
  deviceId: string;
  metricName: string;
  value: number;
  threshold: number;
  severity: "WARNING" | "CRITICAL";
  machineId?: string;
  message: string;
  timestamp: string;
}

/**
 * Mengirimkan notifikasi alert otomatis dari Backend Predictive ke Backend Preventive
 * agar dapat memicu otomatisasi pembuatan Work Order (WO) perbaikan mesin.
 */
export async function sendPreventiveAlertWebhook(alert: AlertPayload): Promise<boolean> {
  const webhookUrl = (config as any).preventiveWebhookUrl;
  if (!webhookUrl) return false;

  return new Promise((resolve) => {
    try {
      const url = new URL(webhookUrl);
      const isHttps = url.protocol === "https:";
      const lib = isHttps ? https : http;

      const data = JSON.stringify(alert);

      const req = lib.request(
        {
          hostname: url.hostname,
          port: url.port || (isHttps ? 443 : 80),
          path: url.pathname,
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            "Content-Length": Buffer.byteLength(data),
          },
          timeout: 4000,
        },
        (res) => {
          resolve(res.statusCode === 200 || res.statusCode === 201);
        }
      );

      req.on("error", () => {
        // Fail-safe jika backend preventive sedang offline
        resolve(false);
      });

      req.on("timeout", () => {
        req.destroy();
        resolve(false);
      });

      req.write(data);
      req.end();
    } catch {
      resolve(false);
    }
  });
}
