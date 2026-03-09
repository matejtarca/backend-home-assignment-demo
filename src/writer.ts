import amqplib from "amqplib";
import { Client } from "pg";

// ── Configuration ────────────────────────────────────────────────────────────

const AMQP_URL = process.env.AMQP_URL ?? "amqp://admin:admin@localhost:55672";
const QUEUE_NAME = "car_state";

const PG_CONFIG = {
  host: process.env.PG_HOST ?? "localhost",
  port: parseInt(process.env.PG_PORT ?? "55432", 10),
  user: process.env.PG_USER ?? "postgres",
  password: process.env.PG_PASSWORD ?? "postgres",
  database: process.env.PG_DATABASE ?? "postgres",
};

// ── Types ────────────────────────────────────────────────────────────────────

interface CarStateMessage {
  carId: number;
  time: string;
  latitude: number | null;
  longitude: number | null;
  speed: number | null;
  gear: number | null;
  stateOfCharge: number | null;
}

// ── Main ─────────────────────────────────────────────────────────────────────

async function main() {
  // Connect to Postgres
  const pgClient = new Client(PG_CONFIG);
  await pgClient.connect();
  console.log("[writer] Connected to Postgres");

  // Ensure a unique constraint exists for upserts to work
  await pgClient.query(
    `CREATE UNIQUE INDEX IF NOT EXISTS idx_car_state_car_id_time
     ON car_state (car_id, time)`
  );
  console.log("[writer] Ensured unique index on (car_id, time)");

  // Connect to RabbitMQ
  const amqpConn = await amqplib.connect(AMQP_URL);
  const channel = await amqpConn.createChannel();
  await channel.assertQueue(QUEUE_NAME, { durable: true });
  // Process one message at a time to maintain ordering and avoid conflicts
  await channel.prefetch(1);
  console.log(`[writer] Connected to RabbitMQ, consuming from "${QUEUE_NAME}"`);

  channel.consume(
    QUEUE_NAME,
    async (msg) => {
      if (!msg) return;

      try {
        const data: CarStateMessage = JSON.parse(msg.content.toString());

        // UPSERT: if a row for this car_id + time already exists, update it
        // with COALESCE so we only overwrite NULLs or update with new values.
        await pgClient.query(
          `INSERT INTO car_state (car_id, time, state_of_charge, latitude, longitude, gear, speed)
           VALUES ($1, $2, $3, $4, $5, $6, $7)
           ON CONFLICT (car_id, time) DO UPDATE SET
             state_of_charge = COALESCE(EXCLUDED.state_of_charge, car_state.state_of_charge),
             latitude        = COALESCE(EXCLUDED.latitude, car_state.latitude),
             longitude       = COALESCE(EXCLUDED.longitude, car_state.longitude),
             gear            = COALESCE(EXCLUDED.gear, car_state.gear),
             speed           = COALESCE(EXCLUDED.speed, car_state.speed)`,
          [
            data.carId,
            data.time,
            data.stateOfCharge,
            data.latitude,
            data.longitude,
            data.gear,
            data.speed,
          ]
        );

        console.log(
          `[writer] Upserted car ${data.carId} @ ${data.time} ` +
            `| soc=${data.stateOfCharge} lat=${data.latitude} lng=${data.longitude} ` +
            `gear=${data.gear} speed=${data.speed?.toFixed(1)}`
        );

        channel.ack(msg);
      } catch (err) {
        console.error("[writer] Error processing message:", err);
        // Negative-ack and requeue so we don't lose data
        channel.nack(msg, false, true);
      }
    },
    { noAck: false }
  );

  // ── Graceful shutdown ────────────────────────────────────────────────────

  async function shutdown() {
    console.log("[writer] Shutting down…");
    await channel.close();
    await amqpConn.close();
    await pgClient.end();
    process.exit(0);
  }

  process.on("SIGINT", shutdown);
  process.on("SIGTERM", shutdown);
}

main().catch((err) => {
  console.error("[writer] Fatal error:", err);
  process.exit(1);
});
