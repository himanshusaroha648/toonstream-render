import express from "express";
import cron from "node-cron";
import { start as runSyncScript } from "./toonstream-supabase-sync.js";

const app = express();
const PORT = process.env.PORT || 5000;

let syncStatus = {
  isRunning: false,
  lastRunTime: null,
  lastRunSuccess: null,
  totalRuns: 0,
  successfulRuns: 0,
  failedRuns: 0,
  nextRunTime: null,
};

app.get("/", (req, res) => {
  res.json({
    status: "alive",
    service: "Toonstream Netlify Sync Server",
    uptime: process.uptime(),
    syncStatus,
    timestamp: new Date().toISOString(),
  });
});

app.get("/sync", async (req, res) => {
  if (syncStatus.isRunning) {
    return res.json({
      status: "already_running",
      message: "Sync is already in progress",
      syncStatus,
    });
  }

  res.json({
    status: "triggered",
    message: "Sync started manually",
  });

  runSync();
});

app.get("/status", (req, res) => {
  res.json({
    syncStatus,
    proxyEnabled: process.env.USE_PROXY === "true",
    pollInterval: process.env.POLL_INTERVAL_MS || "600000",
    syncIntervalMinutes: 10,
    timestamp: new Date().toISOString(),
  });
});

async function runSync() {
  if (syncStatus.isRunning) {
    console.log("⏭️  Sync already running, skipping...");
    return;
  }

  syncStatus.isRunning = true;
  syncStatus.lastRunTime = new Date().toISOString();
  syncStatus.totalRuns++;

  console.log(`\n${"=".repeat(60)}`);
  console.log(`🚀 Starting sync run #${syncStatus.totalRuns}`);
  console.log(`⏰ Time: ${syncStatus.lastRunTime}`);
  console.log(`${"=".repeat(60)}\n`);

  try {
    await runSyncScript();

    syncStatus.lastRunSuccess = true;
    syncStatus.successfulRuns++;
    console.log("\n✅ Sync completed successfully\n");
  } catch (error) {
    syncStatus.lastRunSuccess = false;
    syncStatus.failedRuns++;
    console.error(`\n❌ Sync failed: ${error.message}\n`);
    if (error.stack) {
      console.error(error.stack);
    }
  } finally {
    syncStatus.isRunning = false;
  }
}

const cronExpression = process.env.CRON_SCHEDULE || "*/10 * * * *";
cron.schedule(cronExpression, () => {
  console.log("\n⏰ Scheduled sync triggered");
  runSync();
});

function updateNextRunTime() {
  const now = new Date();
  const next = new Date(now.getTime() + 600000);
  syncStatus.nextRunTime = next.toISOString();
}

setInterval(updateNextRunTime, 600000);
updateNextRunTime();

app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n${"=".repeat(60)}`);
  console.log(`🚀 Toonstream Netlify Sync Server Started`);
  console.log(`${"=".repeat(60)}`);
  console.log(`📡 Server running on port ${PORT}`);
  console.log(`⏰ Sync schedule: Every 10 minutes`);
  console.log(`🔐 Proxy enabled: ${process.env.USE_PROXY === "true" ? "Yes" : "No"}`);
  console.log(`🌐 Health check: http://localhost:${PORT}/`);
  console.log(`📊 Status: http://localhost:${PORT}/status`);
  console.log(`🔄 Manual trigger: http://localhost:${PORT}/sync`);
  console.log(`${"=".repeat(60)}\n`);

  console.log("🎬 Running initial sync...\n");
  runSync();
});

process.on("SIGTERM", () => {
  console.log("\n⚠️  SIGTERM received, shutting down gracefully...");
  process.exit(0);
});

process.on("SIGINT", () => {
  console.log("\n⚠️  SIGINT received, shutting down gracefully...");
  process.exit(0);
});
