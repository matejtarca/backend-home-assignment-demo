import mqtt from "mqtt";
import amqplib from "amqplib";

// ── Configuration ────────────────────────────────────────────────────────────

const MQTT_URL = process.env.MQTT_URL ?? "mqtt://localhost:51883";
const AMQP_URL = process.env.AMQP_URL ?? "amqp://admin:admin@localhost:55672";
const QUEUE_NAME = "car_state";
const CAR_ID = 1;
const FLUSH_INTERVAL_MS = 5_000; // 5-second granularity

// ── Types ────────────────────────────────────────────────────────────────────

interface BatteryInfo {
  soc?: number; // 0-100
  capacity?: number; // Wh
}

interface CarState {
  latitude?: number;
  longitude?: number;
  speed?: number; // m/s from MQTT
  gear?: string; // "N" | "1"-"6"
  batteries: Record<number, BatteryInfo>;
}

interface CarStateMessage {
  carId: number;
  time: string; // ISO timestamp
  latitude: number | null;
  longitude: number | null;
  speed: number | null; // km/h
  gear: number | null; // 0-6
  stateOfCharge: number | null; // weighted average percentage
}

// ── Helpers ──────────────────────────────────────────────────────────────────

function gearToInt(gear: string | undefined): number | null {
  if (gear === undefined) return null;
  if (gear === "N") return 0;
  const n = parseInt(gear, 10);
  return isNaN(n) ? null : n;
}

function msToKmh(speed: number): number {
  return speed * 3.6;
}

/**
 * Compute weighted average SoC across batteries.
 * Only batteries that have BOTH soc and capacity contribute.
 */
function computeWeightedSoc(
  batteries: Record<number, BatteryInfo>
): number | null {
  let totalWeightedSoc = 0;
  let totalCapacity = 0;

  for (const battery of Object.values(batteries)) {
    if (battery.soc !== undefined && battery.capacity !== undefined) {
      totalWeightedSoc += battery.soc * battery.capacity;
      totalCapacity += battery.capacity;
    }
  }

  if (totalCapacity === 0) return null;
  return Math.round(totalWeightedSoc / totalCapacity);
}

/**
 * Round a Date down to the nearest 5-second boundary.
 */
function floorTo5Seconds(date: Date): Date {
  const ms = date.getTime();
  return new Date(ms - (ms % FLUSH_INTERVAL_MS));
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  // Connect to RabbitMQ
  const amqpConn = await amqplib.connect(AMQP_URL);
  const channel = await amqpConn.createChannel();
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  console.log(`[collector] Connected to RabbitMQ, queue "${QUEUE_NAME}" ready`);

  // Per-car state (we only track car 1, but the structure is generic)
  const carStates: Record<number, CarState> = {};

  // Known battery capacities — the README says we can treat them as constants
  // once read. We store them so partial SoC updates still compute correctly.
  const batteryCapacities: Record<string, number> = {}; // "carId-batteryIndex" -> capacity

  function getOrCreateState(carId: number): CarState {
    if (!carStates[carId]) {
      carStates[carId] = { batteries: {} };
    }
    return carStates[carId];
  }

  // Connect to MQTT
  const mqttClient = mqtt.connect(MQTT_URL);

  mqttClient.on("connect", () => {
    console.log("[collector] Connected to MQTT broker");
    mqttClient.subscribe(`car/${CAR_ID}/#`, (err) => {
      if (err) {
        console.error("[collector] MQTT subscribe error:", err);
      } else {
        console.log(`[collector] Subscribed to car/${CAR_ID}/#`);
      }
    });
  });

  mqttClient.on("message", (topic: string, payload: Buffer) => {
    const value = payload.toString();
    const parts = topic.split("/");
    // Expected patterns:
    //   car/<carId>/location/latitude
    //   car/<carId>/location/longitude
    //   car/<carId>/speed
    //   car/<carId>/gear
    //   car/<carId>/battery/<batteryIndex>/soc
    //   car/<carId>/battery/<batteryIndex>/capacity

    if (parts[0] !== "car") return;
    const carId = parseInt(parts[1], 10);
    if (carId !== CAR_ID) return;

    const state = getOrCreateState(carId);

    if (parts[2] === "location" && parts[3] === "latitude") {
      state.latitude = parseFloat(value);
    } else if (parts[2] === "location" && parts[3] === "longitude") {
      state.longitude = parseFloat(value);
    } else if (parts[2] === "speed") {
      state.speed = parseFloat(value);
    } else if (parts[2] === "gear") {
      state.gear = value.trim();
    } else if (parts[2] === "battery") {
      const batteryIndex = parseInt(parts[3], 10);
      if (!state.batteries[batteryIndex]) {
        state.batteries[batteryIndex] = {};
      }
      if (parts[4] === "soc") {
        state.batteries[batteryIndex].soc = parseFloat(value);
      } else if (parts[4] === "capacity") {
        const cap = parseFloat(value);
        state.batteries[batteryIndex].capacity = cap;
        // Cache capacity as a constant so it persists even if the topic stops
        batteryCapacities[`${carId}-${batteryIndex}`] = cap;
      }
    }
  });

  // ── Periodic flush ───────────────────────────────────────────────────────

  // We align the first flush to the next 5-second wall-clock boundary.
  const now = Date.now();
  const msUntilNextBoundary = FLUSH_INTERVAL_MS - (now % FLUSH_INTERVAL_MS);

  setTimeout(() => {
    flush(); // First aligned flush
    setInterval(flush, FLUSH_INTERVAL_MS);
  }, msUntilNextBoundary);

  function flush() {
    const timestamp = floorTo5Seconds(new Date());

    for (const [carIdStr, state] of Object.entries(carStates)) {
      const carId = parseInt(carIdStr, 10);

      // Make sure cached capacities are applied (they're constant)
      for (const [key, cap] of Object.entries(batteryCapacities)) {
        const [cId, bIdx] = key.split("-").map(Number);
        if (cId === carId) {
          if (!state.batteries[bIdx]) state.batteries[bIdx] = {};
          if (state.batteries[bIdx].capacity === undefined) {
            state.batteries[bIdx].capacity = cap;
          }
        }
      }

      const message: CarStateMessage = {
        carId,
        time: timestamp.toISOString(),
        latitude: state.latitude ?? null,
        longitude: state.longitude ?? null,
        speed: state.speed !== undefined ? msToKmh(state.speed) : null,
        gear: gearToInt(state.gear),
        stateOfCharge: computeWeightedSoc(state.batteries),
      };

      channel.sendToQueue(QUEUE_NAME, Buffer.from(JSON.stringify(message)), {
        persistent: true,
      });

      console.log(
        `[collector] Published state for car ${carId} @ ${message.time} ` +
          `| lat=${message.latitude} lng=${message.longitude} ` +
          `speed=${message.speed?.toFixed(1)} gear=${message.gear} ` +
          `soc=${message.stateOfCharge}`
      );
    }
  }

  // ── Graceful shutdown ────────────────────────────────────────────────────

  async function shutdown() {
    console.log("[collector] Shutting down…");
    mqttClient.end();
    await channel.close();
    await amqpConn.close();
    process.exit(0);
  }

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("[collector] Fatal error:", err);
  process.exit(1);
});
