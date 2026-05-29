/**
 * Sample / smoke test for the scheduler.
 *
 * Spins up THREE scheduler instances that all poll the same Mongo collection,
 * so they compete for jobs. Each is given a different `processEvery` so they
 * tick out of phase and the winner of any given job varies between them. Job
 * claiming is atomic (claimDueJob does a findOneAndUpdate on the lock), so a
 * job is only ever run by one worker at a time.
 *
 * Run with:   npm run dev
 * Needs a reachable MongoDB. Override the connection string with MONGO_URL,
 * e.g. MONGO_URL="mongodb://localhost:27017/scheduler_test" npm run dev
 */
import { Scheduler, MongoStore } from "./src/index.js";

const MONGO_URL = "mongodb://localhost:27017/scheduler_test";
const COLLECTION = "jobs";

// ***********************************************
// Build a scheduler tagged with a short label, its own store/connection, and
// the full template set + event wiring. The tag prefixes every log line so we
// can see which worker won each job.
// ***********************************************
function makeScheduler(tag: string, processEvery: number): Scheduler {
  const store = new MongoStore({ url: MONGO_URL, collection: COLLECTION });
  const scheduler = new Scheduler(store, {
    concurrency: 5,
    processEvery,
    lockMs: 10_000,
    drainTimeoutMs: 5_000,
  });

  // ***********************************************
  // Wire up every event so we can watch the lifecycle
  // ***********************************************
  //   scheduler.on("scheduler:start", () =>
  //     console.log(`[${tag}] start (processEvery=${processEvery}ms)`),
  //   );
  //   scheduler.on("scheduler:stop", () => console.log(`[${tag}] stop`));
  //   scheduler.on("scheduler:drain", (_w, count) =>
  //     console.log(`[${tag}] drain — ${count} job(s) finished during shutdown`),
  //   );
  //   scheduler.on("scheduler:error", (_w, err) =>
  //     console.error(`[${tag}] scheduler:error ${err.message}`),
  //   );

  scheduler.on("job:locked", (workerId, job) =>
    console.log(`[${tag}] locked ${job.template} (${job.id}) by ${workerId}`),
  );
  //   scheduler.on("job:attempt", (_w, job, attempt) =>
  //     console.log(`[${tag}] attempt #${attempt} ${job.template} (${job.id})`),
  //   );
  //   scheduler.on("job:complete", (_w, job) =>
  //     console.log(`[${tag}] complete ${job.template} runCount=${job.runCount}`),
  //   );
  //   scheduler.on("job:fail", (_w, job, err, attempt) =>
  //     console.warn(
  //       `[${tag}] fail ${job.template} attempt ${attempt}: ${(err as Error).message}`,
  //     ),
  //   );
  //   scheduler.on("job:exhausted", (_w, job, err) =>
  //     console.error(
  //       `[${tag}] exhausted ${job.template}: ${(err as Error).message} — giving up`,
  //     ),
  //   );
  //   scheduler.on("job:abort", (_w, job, reason) =>
  //     console.warn(`[${tag}] abort ${job.template}: ${reason}`),
  //   );

  // ***********************************************
  // Templates: the actual work each job runs. Every worker must know how to
  // run every template, since any worker might win any job.
  // ***********************************************
  scheduler.addTemplate("greet", async (data) => {
    console.log(
      `  [${tag}] -> hello, ${(data as { name?: string })?.name ?? "world"}!`,
    );
  });

  scheduler.addTemplate("heartbeat", async () => {
    console.log(`  [${tag}] -> tick @ ${new Date().toISOString()}`);
  });

  // Fails ~60% of the time so retry/backoff is observable.
  scheduler.addTemplate("flaky", async () => {
    if (Math.random() < 0.6) {
      throw new Error("random transient failure");
    }
    console.log(`  [${tag}] -> flaky job succeeded`);
  });

  // Long-running job: takes 30s. The heartbeat renews the lock (every
  // ~lockMs/3) so the lock stays held for the full duration.
  scheduler.addTemplate("longRunning", async () => {
    console.log(`  [${tag}] -> long job started @ ${new Date().toISOString()}`);
    await new Promise((resolve) => setTimeout(resolve, 30_000));
    console.log(
      `  [${tag}] -> long job finished @ ${new Date().toISOString()}`,
    );
  });

  return scheduler;
}

// ***********************************************
// Three competing workers, polling out of phase.
// ***********************************************
const schedulers = [
  makeScheduler("S1", 250),
  makeScheduler("S2", 500),
  makeScheduler("S3", 1000),
];

async function main() {
  await Promise.all(schedulers.map((s) => s.start()));

  // Seed jobs once. Stable IDs + upsert means jobs (and their lifetime
  // counters) survive a restart, and seeding from every worker is idempotent —
  // so we just use the first scheduler. runCount/failCount/createdAt live in
  // addJob's setOnInsert block: written once on insert, preserved on upsert.
  const seeder = schedulers[0];

  // One-shot job, runs immediately.
  await seeder.addJob({
    id: "greet",
    template: "greet",
    data: { name: "Alex" },
    upsert: true,
  });

  // 100 recurring heartbeat jobs (every 3s) — different workers will win
  // different jobs/ticks. Jitter spreads their first run across the interval
  // so they don't all fire at once.
  for (let i = 0; i < 100; i++) {
    await seeder.addJob({
      id: `heartbeat-${i}`,
      template: "heartbeat",
      repeat: 3_000,
      jitter: true,
      upsert: true,
    });
  }

  // One-shot job that will likely fail and retry with backoff.
  await seeder.addJob({ id: "flaky", template: "flaky", upsert: true });

  // One-shot job that takes 30s to complete.
  await seeder.addJob({
    id: "longRunning",
    template: "longRunning",
    upsert: true,
  });

  console.log("3 schedulers running and competing. Press Ctrl+C to stop.\n");
}

// ***********************************************
// Graceful shutdown — stop all workers.
// ***********************************************
let shuttingDown = false;
async function shutdown(signal: string) {
  if (shuttingDown) return;
  shuttingDown = true;
  console.log(`\nReceived ${signal}, shutting down...`);
  try {
    await Promise.allSettled(schedulers.map((s) => s.stop()));
  } catch (err) {
    console.error("Error during shutdown:", err);
  } finally {
    process.exit(0);
  }
}

process.on("SIGINT", () => void shutdown("SIGINT"));
process.on("SIGTERM", () => void shutdown("SIGTERM"));

main().catch((err) => {
  console.error("Fatal:", err);
  process.exit(1);
});
