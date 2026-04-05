import "dotenv/config";
import { spawn } from "child_process";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";
import { NewMessage } from "telegram/events/index.js";

const apiId = Number(process.env.TELEGRAM_API_ID || process.env.API_ID);
const apiHash = process.env.TELEGRAM_API_HASH || process.env.API_HASH;
const sessionString =
  process.env.TELEGRAM_SESSION || process.env.telegram_Session || "";
const targetChatId = process.env.TELEGRAM_CHAT_ID || "-1003358753323";

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

function runSyncScript(triggerMessage) {
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
    `🚀 Triggering sync for message ${messageId}: toonstream-supabase-sync.js`,
  );

  const child = spawn(process.execPath, ["toonstream-supabase-sync.js"], {
    cwd: process.cwd(),
    stdio: "inherit",
  });

  child.on("close", (code) => {
    syncRunning = false;
    if (code === 0) {
      console.log("✅ Sync run completed successfully.");
    } else {
      console.error(`❌ Sync run exited with code ${code}`);
    }

    if (pendingMessage) {
      const nextMessage = pendingMessage;
      pendingMessage = null;
      console.log(`🔁 Processing queued message ${nextMessage.id}...`);
      runSyncScript(nextMessage);
    }
  });

  child.on("error", (err) => {
    syncRunning = false;
    console.error("❌ Failed to start sync script:", err?.message || err);

    if (pendingMessage) {
      const nextMessage = pendingMessage;
      pendingMessage = null;
      console.log(`🔁 Retrying with queued message ${nextMessage.id}...`);
      runSyncScript(nextMessage);
    }
  });
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
      if (message.id <= latestMessageId) return;

      latestMessageId = message.id;
      printMessage(`🆕 New message received in ${targetChatId}`, message);

      console.log("🎯 New channel message detected. Triggering sync...");
      runSyncScript(message);
    },
    new NewMessage({}),
  );

  console.log(`👂 Listening for new messages in chat ${targetChatId}...`);
}

startListener().catch((error) => {
  console.error("❌ Listener failed:", error?.message || error);
  process.exit(1);
});

process.once("SIGINT", async () => {
  console.log("\nStopping listener...");
  await client.disconnect();
  process.exit(0);
});
