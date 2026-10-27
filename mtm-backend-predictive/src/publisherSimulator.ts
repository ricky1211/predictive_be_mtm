import mqtt from "mqtt";
import { config } from "./config";

function getRandom(min: number, max: number): number {
  return parseFloat((Math.random() * (max - min) + min).toFixed(2));
}

function formatDate(d: Date): string {
  const yyyy = d.getFullYear();
  const mm = String(d.getMonth() + 1).padStart(2, "0");
  const dd = String(d.getDate()).padStart(2, "0");
  return `${yyyy}-${mm}-${dd}`;
}

function formatTime(d: Date): string {
  const hh = String(d.getHours()).padStart(2, "0");
  const mm = String(d.getMinutes()).padStart(2, "0");
  const ss = String(d.getSeconds()).padStart(2, "0");
  return `${hh}:${mm}:${ss}`;
}

function main(): void {
  console.log("============================================================");
  console.log("    MTM JINGDUAN FORGING - MQTT SENSOR SIMULATOR (TS)");
  console.log("============================================================");

  const brokerUrl = `mqtt://${config.mqtt.brokerHost}:${config.mqtt.brokerPort}`;
  const client = mqtt.connect(brokerUrl, {
    clientId: "iot_simulator_ts",
    username: config.mqtt.username || undefined,
    password: config.mqtt.password || undefined,
  });

  const mtmSensors = [
    {
      subtopic: "link-temperature",
      deviceId: "0",
      getData: () => ({ PV: getRandom(30, 45), DP: 0 }),
    },
    {
      subtopic: "right-eccentric-bush",
      deviceId: "3",
      getData: () => ({ PV: getRandom(45, 52), DP: 0 }),
    },
    {
      subtopic: "left-eccentric-bush",
      deviceId: "4",
      getData: () => ({ PV: getRandom(44, 50), DP: 0 }),
    },
    {
      subtopic: "right-transmission-bearing",
      deviceId: "5",
      getData: () => ({ PV: getRandom(42, 48), DP: 0 }),
    },
    {
      subtopic: "left-transmission-bearing",
      deviceId: "6",
      getData: () => ({ PV: getRandom(41, 47), DP: 0 }),
    },
    {
      subtopic: "vibration",
      deviceId: "1",
      getData: () => ({ Z_RMS_Vel_in: getRandom(0.01, 0.08) }),
    },
  ];

  client.on("connect", () => {
    console.log(`[Simulator] Terhubung ke ${brokerUrl}`);
    console.log("Mengirimkan data mtm/jingduanforging/... setiap 2 detik. Tekan Ctrl+C untuk berhenti.\n");

    let count = 1;
    setInterval(() => {
      const sensor = mtmSensors[Math.floor(Math.random() * mtmSensors.length)];
      const topic = `mtm/jingduanforging/${sensor.subtopic}`;
      const now = new Date();

      const payload = {
        date: formatDate(now),
        time: formatTime(now),
        deviceId: sensor.deviceId,
        data: sensor.getData(),
      };

      const msgStr = JSON.stringify(payload);
      client.publish(topic, msgStr, { qos: 1 });
      console.log(`[${count++}] Dipublish ke '${topic}': ${msgStr}`);
    }, 2000);
  });

  client.on("error", (err) => {
    console.error("[Simulator Error]:", err.message);
  });

  process.on("SIGINT", () => {
    console.log("\n[Simulator] Menghentikan simulasi...");
    client.end();
    process.exit(0);
  });
}

main();
