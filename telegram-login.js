import "dotenv/config";
import fs from "fs";
import path from "path";
import input from "input";
import { TelegramClient } from "telegram";
import { StringSession } from "telegram/sessions/index.js";

const apiId = Number(process.env.TELEGRAM_API_ID || process.env.API_ID);
const apiHash = process.env.TELEGRAM_API_HASH || process.env.API_HASH;
const envPhone = process.env.TELEGRAM_PHONE || process.env.MOBILE_NUMBER;

if (!apiId || !apiHash) {
  console.error(
    "❌ Missing TELEGRAM_API_ID/API_ID or TELEGRAM_API_HASH/API_HASH in environment.",
  );
  process.exit(1);
}

const sessionDir = path.join(process.cwd(), "bin");
const sessionFile = path.join(sessionDir, "telegram_session.txt");

if (!fs.existsSync(sessionDir)) {
  fs.mkdirSync(sessionDir, { recursive: true });
}

let savedSession = "";
if (fs.existsSync(sessionFile)) {
  savedSession = fs.readFileSync(sessionFile, "utf8").trim();
}

const stringSession = new StringSession(savedSession);
const client = new TelegramClient(stringSession, apiId, apiHash, {
  connectionRetries: 5,
});

async function loginTelegram() {
  await client.start({
    phoneNumber: async () => {
      if (envPhone) return envPhone;
      return input.text("Enter mobile number with country code (e.g. +91xxxx): ");
    },
    password: async () => input.text("Enter 2FA password (if enabled): "),
    phoneCode: async () => input.text("Enter OTP code from Telegram: "),
    onError: (err) => console.error("Login error:", err?.message || err),
  });

  const session = client.session.save();
  fs.writeFileSync(sessionFile, session, "utf8");

  const me = await client.getMe();
  console.log("✅ Telegram login successful");
  console.log(`👤 User: ${me?.username || me?.firstName || me?.id}`);
  console.log(`💾 Session saved: ${sessionFile}`);
}

loginTelegram()
  .catch((err) => {
    console.error("❌ Telegram login failed:", err?.message || err);
    process.exit(1);
  })
  .finally(async () => {
    await client.disconnect();
  });
