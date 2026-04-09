import "dotenv/config";
import { spawn } from "child_process";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { NewMessage } from "telegram/events/index.js";

const apiId = Number(process.env.TELEGRAM_API_ID || process.env.API_ID);
const apiHash = process.env.TELEGRAM_API_HASH || process.env.API_HASH;
const sessionString =
  process.env.TELEGRAM_SESSION || process.env.telegram_Session || "";
const targetChatId = process.env.TELEGRAM_CHAT_ID || "-1003404540307";
const localPort = Number(process.env.PORT || 5000);
const syncTriggerUrl =
  process.env.SYNC_TRIGGER_URL || `http://127.0.0.1:${localPort}/sync`;
const pollIntervalMs = Number(process.env.TELEGRAM_POLL_INTERVAL_MS || 15000);
const reconnectBackoffMs = Number(process.env.TELEGRAM_RECONNECT_BACKOFF_MS || 3000);

if (!apiId || !apiHash) {
  console.error(
    "❌ Missing TELEGRAM_API_ID/API_ID or TELEGRAM_API_HASH/API_HASH in .env",
  );
  process.exit(1);
}

if (!sessionString) {
  console.error(
    "❌ Missing TELEGRAM_SESSION or telegram_Session in .env (session string required)",
  );
  process.exit(1);
}

const client = new TelegramClient(new StringSession(sessionString), apiId, apiHash, {
  connectionRetries: 5,
});

let latestMessageId = 0;
let syncRunning = false;
let pendingMessage = null;
const handledMessageIds = new Set();
let pollTimer = null;
let reconnectInProgress = false;
let lastReconnectAt = 0;

function isNotConnectedError(err) {
  const msg = String(err?.message || err || "").toLowerCase();
  return msg.includes("not connected") || msg.includes("connection closed");
}

async function reconnectClient(reason = "unknown") {
  const now = Date.now();
  if (reconnectInProgress) return;
  if (now - lastReconnectAt < reconnectBackoffMs) return;

  reconnectInProgress = true;
  lastReconnectAt = now;

  try {
    console.warn(`⚠️ Telegram disconnected. Reconnecting (${reason})...`);
    try {
      await client.disconnect();
    } catch {
      // ignore if already disconnected
    }
    await client.connect();
    console.log("✅ Telegram reconnected");
    await showLatestMessageFromTarget();
  } catch (err) {
    console.error("❌ Telegram reconnect failed:", err?.message || err);
  } finally {
    reconnectInProgress = false;
  }
}
function runDirectSyncScript() {
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, ["toonstream-supabase-sync.js"], {
      cwd: process.cwd(),
      stdio: "inherit",
    });

    child.on("close", (code) => {
      if (code === 0) return resolve();
      reject(new Error(`Sync run exited with code ${code}`));
    });

    child.on("error", (err) => {
      reject(err);
    });
  });
}

async function triggerSyncViaServer() {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(), 20000);

  try {
    const response = await fetch(syncTriggerUrl, {
      method: "GET",
      signal: controller.signal,
      headers: { Accept: "application/json" },
    });

    let payload = null;
    try {
      payload = await response.json();
    } catch {
      payload = null;
    }

    if (!response.ok) {
      throw new Error(`sync endpoint error: HTTP ${response.status}`);
    }

    if (payload?.status === "already_running") {
      console.log("⏳ Server sync already running. Trigger acknowledged.");
      return;
    }

    console.log("✅ Server sync trigger accepted.");
  } finally {
    clearTimeout(timeout);
  }
}

async function runSyncScript(triggerMessage) {
  const messageId = triggerMessage?.id;
  if (!messageId) {
    console.log("⏭️ Invalid message received. Sync not triggered.");
    return;
  }

  if (handledMessageIds.has(messageId)) {
    console.log(`⏭️ Message ${messageId} already handled. Skipping duplicate.`);
    return;
  }

  if (syncRunning) {
    pendingMessage = triggerMessage;
    console.log(
      `⏳ Sync already running. Queued message ${messageId} for next run.`,
    );
    return;
  }

  syncRunning = true;
  handledMessageIds.add(messageId);
  if (handledMessageIds.size > 2000) {
    const oldest = handledMessageIds.values().next().value;
    handledMessageIds.delete(oldest);
  }

  console.log(
    `🚀 Triggering sync for message ${messageId} via ${syncTriggerUrl}`,
  );

  try {
    await triggerSyncViaServer();
  } catch (err) {
    console.warn(
      "⚠️ Could not trigger /sync endpoint, falling back to direct sync script:",
      err?.message || err,
    );
    try {
      await runDirectSyncScript();
      console.log("✅ Direct sync run completed successfully.");
    } catch (directErr) {
      console.error("❌ Direct sync run failed:", directErr?.message || directErr);
    }
  } finally {
    syncRunning = false;
    if (pendingMessage) {
      const nextMessage = pendingMessage;
      pendingMessage = null;
      console.log(`🔁 Processing queued message ${nextMessage.id}...`);
      await runSyncScript(nextMessage);
    }
  }
}

function sameChat(inputId, configuredId) {
  if (!inputId || !configuredId) return false;

  const left = String(inputId).replace(/^-100/, "").replace(/^-/, "");
  const right = String(configuredId).replace(/^-100/, "").replace(/^-/, "");

  return left === right;
}

function printMessage(prefix, message) {
  const msgText = message?.message?.trim() || "[non-text message]";
  const msgDate = message?.date
    ? new Date(message.date * 1000).toLocaleString("en-IN")
    : "unknown-date";

  console.log("\n------------------------------");
  console.log(`${prefix}`);
  console.log(`Message ID: ${message?.id}`);
  console.log(`Date: ${msgDate}`);
  console.log(`Text: ${msgText}`);
  console.log("------------------------------\n");
}

function processIncomingMessage(message, source = "event") {
  if (!message) return;
  if (message.id <= latestMessageId) return;

  latestMessageId = message.id;
  printMessage(`🆕 New message received in ${targetChatId} [${source}]`, message);

  console.log("🎯 New channel message detected. Triggering sync...");
  void runSyncScript(message);
}

async function showLatestMessageFromTarget() {
  const entity = await client.getEntity(targetChatId);
  const messages = await client.getMessages(entity, { limit: 1 });

  if (!messages || messages.length === 0) {
    console.log(`ℹ️ No messages found in chat ${targetChatId}`);
    return;
  }

  const latest = messages[0];
  latestMessageId = latest.id || 0;
  printMessage(`📌 Latest existing message in ${targetChatId}`, latest);
}

async function pollLatestMessage() {
  try {
    const entity = await client.getEntity(targetChatId);
    const messages = await client.getMessages(entity, { limit: 1 });
    if (!messages || messages.length === 0) return;

    processIncomingMessage(messages[0], "poll");
  } catch (err) {
    console.warn("⚠️ Poll check failed:", err?.message || err);
    if (isNotConnectedError(err)) {
      await reconnectClient("poll");
    }
  }
}

async function startListener() {
  await client.connect();

  console.log("✅ Telegram connected using saved session");
  await showLatestMessageFromTarget();

  client.addEventHandler(
    (event) => {
      const incomingChatId = event?.chatId;
      const message = event?.message;

      if (!message) return;
      if (!sameChat(incomingChatId, targetChatId)) return;
      processIncomingMessage(message, "event");
    },
    new NewMessage({}),
  );

  pollTimer = setInterval(() => {
    void pollLatestMessage();
  }, pollIntervalMs);

  console.log(`🛰️ Poll fallback enabled every ${Math.round(pollIntervalMs / 1000)}s`);
  console.log(`👂 Listening for new messages in chat ${targetChatId}...`);
}

startListener().catch((error) => {
  console.error("❌ Listener failed:", error?.message || error);
  process.exit(1);
});

process.on("uncaughtException", (err) => {
  if (isNotConnectedError(err)) {
    console.warn("⚠️ Transient Telegram connection exception handled.");
    void reconnectClient("uncaughtException");
    return;
  }
  console.error("❌ Uncaught exception:", err);
  process.exit(1);
});

process.on("unhandledRejection", (reason) => {
  if (isNotConnectedError(reason)) {
    console.warn("⚠️ Transient Telegram rejection handled.");
    void reconnectClient("unhandledRejection");
    return;
  }
  console.error("❌ Unhandled rejection:", reason);
});

process.once("SIGINT", async () => {
  console.log("\nStopping listener...");
  if (pollTimer) {
    clearInterval(pollTimer);
    pollTimer = null;
  }
  await client.disconnect();
  process.exit(0);
});
