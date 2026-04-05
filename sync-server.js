import express from "express";
import cron from "node-cron";
import path from "path";
import util from "util";
import { spawn } from "child_process";
import { start as runSyncScript, fetchFullSeries } from "./toonstream-supabase-sync.js";

const app = express();
const PORT = process.env.PORT || 5000;
const AUTO_SYNC_ON_START = process.env.AUTO_SYNC_ON_START === "true";
const ENABLE_CRON_SYNC = process.env.ENABLE_CRON_SYNC === "true";
const ENABLE_TELEGRAM_TRIGGER =
  process.env.ENABLE_TELEGRAM_TRIGGER !== "false";

let telegramListenerProcess = null;

const LOG_BUFFER_LIMIT = Number(process.env.LOG_BUFFER_LIMIT || 2000);
const logBuffer = [];
const logClients = new Set();
let nextLogId = 1;

const originalConsole = {
  log: console.log.bind(console),
  info: console.info.bind(console),
  warn: console.warn.bind(console),
  error: console.error.bind(console),
};

function stringifyLogArg(value) {
  if (typeof value === "string") return value;
  try {
    return util.inspect(value, { depth: 4, breakLength: 120, colors: false });
  } catch {
    return String(value);
  }
}

function appendLog(level, args) {
  const message = args.map(stringifyLogArg).join(" ");
  const entry = {
    id: nextLogId++,
    level,
    message,
    timestamp: new Date().toISOString(),
  };

  logBuffer.push(entry);
  if (logBuffer.length > LOG_BUFFER_LIMIT) {
    logBuffer.shift();
  }

  const payload = `data: ${JSON.stringify(entry)}\n\n`;
  for (const client of logClients) {
    try {
      client.write(payload);
    } catch {
      logClients.delete(client);
    }
  }
}

console.log = (...args) => {
  appendLog("log", args);
  originalConsole.log(...args);
};

console.info = (...args) => {
  appendLog("info", args);
  originalConsole.info(...args);
};

console.warn = (...args) => {
  appendLog("warn", args);
  originalConsole.warn(...args);
};

console.error = (...args) => {
  appendLog("error", args);
  originalConsole.error(...args);
};

let syncStatus = {
  isRunning: false,
  lastRunTime: null,
  lastRunSuccess: null,
  totalRuns: 0,
  successfulRuns: 0,
  failedRuns: 0,
  nextRunTime: null,
};

import { TMDBService } from "./services/tmdb/tmdb-service.js";
import { createClient } from "@supabase/supabase-js";

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY
);

const supabase2 = createClient(
  process.env.SUPABASE_URL2,
  process.env.SUPABASE_SERVICE_ROLE_KEY2
);

const tmdb = new TMDBService(process.env.TMDB_API_KEY);

app.use(express.json());
app.use(express.static("public"));
app.use("/admin", express.static("admin"));
app.use("/toonstream", express.static("admin/toonstream"));

app.get("/logs", (req, res) => {
  res.sendFile(path.join(process.cwd(), "admin", "logs.html"));
});

app.get("/api/logs/recent", (req, res) => {
  const limit = Math.min(Math.max(Number(req.query.limit || 300), 1), 2000);
  const beforeId = Number(req.query.beforeId || 0);

  const source =
    beforeId > 0 ? logBuffer.filter((entry) => entry.id < beforeId) : logBuffer;

  const logs = source.slice(-limit);
  const oldestBufferedId = logBuffer[0]?.id || null;
  const newestBufferedId = logBuffer[logBuffer.length - 1]?.id || null;
  const oldestReturnedId = logs[0]?.id || null;

  res.json({
    logs,
    total: logBuffer.length,
    limit,
    beforeId: beforeId || null,
    hasMore:
      oldestBufferedId !== null &&
      oldestReturnedId !== null &&
      oldestReturnedId > oldestBufferedId,
    oldestBufferedId,
    newestBufferedId,
    oldestReturnedId,
    timestamp: new Date().toISOString(),
  });
});

app.get("/api/logs/stream", (req, res) => {
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  logClients.add(res);
  res.write(`data: ${JSON.stringify({ type: "connected", timestamp: new Date().toISOString() })}\n\n`);

  const keepAlive = setInterval(() => {
    try {
      res.write(`: ping ${Date.now()}\n\n`);
    } catch {
      clearInterval(keepAlive);
      logClients.delete(res);
    }
  }, 30000);

  req.on("close", () => {
    clearInterval(keepAlive);
    logClients.delete(res);
  });
});

// Dashboard API
app.get("/api/series", async (req, res) => {
  const query = req.query.q;
  let supabaseQuery = supabase.from("series").select("*");
  
  if (query) {
    supabaseQuery = supabaseQuery.ilike("title", `%${query}%`);
  } else {
    // If no search query, return empty or limit to avoid heavy load
    return res.json([]);
  }

  const { data, error } = await supabaseQuery.order("updated_at", { ascending: false }).limit(20);
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/api/series/add", async (req, res) => {
  const { tmdbId, type, manualData } = req.body;
  let seriesData = manualData;

  if (tmdbId) {
    seriesData = await tmdb.getDetails(tmdbId, type);
  }

  if (!seriesData) return res.status(400).json({ error: "Failed to get series data" });

  // Generate a URL-friendly slug from the title
  const slug = seriesData.title
    .toLowerCase()
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .trim();

  // Determine target table based on type
  const isMovie = type === "movie";
  const targetTable = isMovie ? "movies" : "series";

  const payload = {
    slug,
    tmdb_id: seriesData.tmdb_id,
    title: seriesData.title,
    description: seriesData.description,
    poster: seriesData.poster,
    banner_image: seriesData.backdrop,
    rating: seriesData.rating,
    genres: seriesData.genres,
    release_date: seriesData.release_date,
    year: seriesData.year,
    updated_at: new Date().toISOString()
  };

  if (isMovie) {
    payload.runtime = seriesData.runtime || null;
  } else {
    payload.total_seasons = seriesData.total_seasons;
    payload.total_episodes = seriesData.total_episodes;
    payload.random_key = Math.random().toString(36).substring(2, 15);
  }

  const { data, error } = await supabase.from(targetTable).upsert(payload, { onConflict: 'slug' }).select().single();

  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/api/series/refetch", async (req, res) => {
  const { id, title, type } = req.body;
  const isMovie = type === "movie";
  const targetTable = isMovie ? "movies" : "series";
  
  const results = await tmdb.search(title, type || "tv");
  if (!results.length) return res.status(404).json({ error: "No results found on TMDB" });
  
  const details = await tmdb.getDetails(results[0].id, type || "tv");
  
  const payload = {
    poster: details.poster,
    banner_image: details.backdrop,
    description: details.description,
    rating: details.rating,
    genres: details.genres,
    release_date: details.release_date,
    year: details.year,
    tmdb_id: details.tmdb_id,
    updated_at: new Date().toISOString()
  };

  if (isMovie) {
    payload.runtime = details.runtime || null;
  } else {
    payload.total_seasons = details.total_seasons;
    payload.total_episodes = details.total_episodes;
    payload.random_key = Math.random().toString(36).substring(2, 15);
  }

  const { error } = await supabase.from(targetTable).update(payload).eq("id", id);

  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true, details });
});

app.post("/api/series/rename", async (req, res) => {
  const { id, newName, type } = req.body;
  const targetTable = type === "movie" ? "movies" : "series";
  const { error } = await supabase.from(targetTable).update({ title: newName }).eq("id", id);
  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.post("/api/tmdb/search", async (req, res) => {
  const { query, type } = req.body;
  const results = await tmdb.search(query, type || "tv");
  res.json(results);
});

app.get("/api/episodes", async (req, res) => {
  const { slug, season } = req.query;
  if (!slug) return res.status(400).json({ error: "Series slug is required" });
  
  let query = supabase.from("episodes").select("*").eq("series_slug", slug);
  if (season) query = query.eq("season", season);
  
  const { data, error } = await query.order("episode", { ascending: true });
  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/api/episodes/refetch-images", async (req, res) => {
  const { slug, tmdbId, title, episodes } = req.body;
  if (!episodes || !episodes.length) return res.status(400).json({ error: "Missing required data" });

  const results = [];
  for (const ep of episodes) {
    let imageUrl = null;
    
    // 1. Try TMDB
    if (tmdbId) {
      imageUrl = await tmdb.fetchEpisodeImage(tmdbId, ep.season, ep.episode);
    }
    
    // 2. Fallback to TVDB if TMDB fails
    if (!imageUrl && title) {
      console.log(`ℹ️ TMDB image missing for S${ep.season}E${ep.episode}, trying TVDB...`);
      imageUrl = await tmdb.fetchTVDBEpisodeImage(title, ep.season, ep.episode);
    }

    if (imageUrl) {
      const { error } = await supabase.from("episodes").update({
        thumbnail: imageUrl,
        episode_card_thumbnail: imageUrl,
        episode_list_thumbnail: imageUrl
      }).eq("series_slug", slug).eq("season", ep.season).eq("episode", ep.episode);
      
      if (!error) results.push({ season: ep.season, episode: ep.episode, success: true, source: imageUrl.includes('tmdb') ? 'TMDB' : 'TVDB' });
    }
  }
  res.json({ success: true, results });
});
app.post("/api/episodes/bulk-add", async (req, res) => {
  const { seriesSlug, season, episodes } = req.body;
  const episodesToInsert = episodes.map(ep => ({
    series_slug: seriesSlug,
    season: parseInt(season),
    episode: parseInt(ep.number),
    title: ep.title,
    servers: [{ name: "Main", url: ep.url }]
  }));

  const { error } = await supabase.from("episodes").upsert(episodesToInsert, {
    onConflict: 'series_slug,season,episode'
  });

  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true });
});

app.post("/api/episodes/add-single", async (req, res) => {
  const { seriesSlug, tmdbId, season, episode, serversRaw } = req.body;
  if (!seriesSlug || !season || !episode) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  // Fetch title from TMDB if tmdbId is available
  let title = `Episode ${episode}`;
  if (tmdbId) {
    const tmdbTitle = await tmdb.fetchEpisodeTitle(tmdbId, season, episode);
    if (tmdbTitle) title = tmdbTitle;
  }

  // Parse server links into the requested format
  const serverLines = (serversRaw || "").split('\n').map(s => s.trim()).filter(Boolean);
  const servers = serverLines.map((url, index) => ({
    option: index + 1,
    real_video: url
  }));

  const { error } = await supabase.from("episodes").upsert({
    series_slug: seriesSlug,
    season: parseInt(season),
    episode: parseInt(episode),
    title,
    servers: servers
  }, {
    onConflict: 'series_slug,season,episode'
  });

  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true, title });
});

app.post("/api/episodes/add-special", async (req, res) => {
  const { seriesSlug, tmdbId, season, episode, serversRaw } = req.body;
  if (!seriesSlug || !season || !episode || !serversRaw) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  try {
    // 1. Convert URL to m3u8 if it's a Rumble link
    // Input: https://rumble.com/embed/v72sgjg/?pub=4oi67i
    // Output: https://rumble.com/hls-vod/72sgjg/playlist.m3u8
    let m3u8_url = null;
    let video_id_source = null;
    
    const rumbleMatch = serversRaw.match(/rumble\.com\/embed\/v([a-zA-Z0-9]+)/);
    if (rumbleMatch) {
      video_id_source = rumbleMatch[1];
      m3u8_url = `https://rumble.com/hls-vod/${video_id_source}/playlist.m3u8`;
    }

    // 2. Generate unique video_id for Supabase 2
    const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
    let randomStr = "";
    for (let i = 0; i < 20; i++) {
      randomStr += charset.charAt(Math.floor(Math.random() * charset.length));
    }
    const video_id = "grdgrdg" + randomStr;

    // 3. Save to Supabase 2 (public.iframe)
    // Updated to only use columns that exist: iframe_url, m3u8_url, video_id, name_title
    const { error: error2 } = await supabase2.from("iframe").insert({
      iframe_url: serversRaw.trim(),
      m3u8_url: m3u8_url,
      name_title: "Lastanime",
      video_id: video_id
    });

    if (error2) throw new Error("Supabase 2 error: " + error2.message);

    // 4. Fetch title from TMDB
    let title = `Episode ${episode}`;
    if (tmdbId) {
      const tmdbTitle = await tmdb.fetchEpisodeTitle(tmdbId, season, episode);
      if (tmdbTitle) title = tmdbTitle;
    }

    // 5. Save to Supabase 1 with modified URL
    // Format: https://stream.lastanime.in/v/video_id
    const finalUrl = `https://stream.lastanime.in/v/${video_id}`;
    const servers = [{
      option: 1,
      real_video: finalUrl
    }];

    const { error: error1 } = await supabase.from("episodes").upsert({
      series_slug: seriesSlug,
      season: parseInt(season),
      episode: parseInt(episode),
      title,
      servers: servers
    }, {
      onConflict: 'series_slug,season,episode'
    });

    if (error1) throw new Error("Supabase 1 error: " + error1.message);

    res.json({ success: true, title, video_id });
  } catch (err) {
    console.error("Special add error:", err);
    res.status(500).json({ error: err.message });
  }
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
if (ENABLE_CRON_SYNC) {
  cron.schedule(cronExpression, () => {
    console.log("\n⏰ Scheduled sync triggered");
    runSync();
  });
} else {
  console.log("ℹ️ Scheduled sync is disabled (ENABLE_CRON_SYNC=false)");
}

function updateNextRunTime() {
  const now = new Date();
  const next = new Date(now.getTime() + 600000);
  syncStatus.nextRunTime = next.toISOString();
}

function startTelegramListener() {
  if (!ENABLE_TELEGRAM_TRIGGER) {
    console.log("ℹ️ Telegram trigger listener is disabled (ENABLE_TELEGRAM_TRIGGER=false)");
    return;
  }

  if (telegramListenerProcess) {
    console.log("ℹ️ Telegram listener already running");
    return;
  }

  telegramListenerProcess = spawn(process.execPath, ["telegram-chat-listener.js"], {
    cwd: process.cwd(),
    stdio: "inherit",
  });

  console.log("📨 Telegram listener started (telegram-chat-listener.js)");

  telegramListenerProcess.on("close", (code) => {
    console.log(`⚠️ Telegram listener exited with code ${code}`);
    telegramListenerProcess = null;
  });

  telegramListenerProcess.on("error", (err) => {
    console.error("❌ Failed to start telegram listener:", err?.message || err);
    telegramListenerProcess = null;
  });
}

function stopTelegramListener() {
  if (!telegramListenerProcess) return;
  try {
    telegramListenerProcess.kill("SIGINT");
  } catch (err) {
    console.warn("⚠️ Failed to stop telegram listener cleanly:", err?.message || err);
  }
}

setInterval(updateNextRunTime, 600000);
updateNextRunTime();

// ── Full Series Fetch (SSE streaming progress) ──────────────────
app.post("/api/fetch-full-series", async (req, res) => {
  const { seriesUrl } = req.body;
  if (!seriesUrl) return res.status(400).json({ error: "seriesUrl is required" });

  // Use SSE to stream progress
  res.setHeader("Content-Type", "text/event-stream");
  res.setHeader("Cache-Control", "no-cache");
  res.setHeader("Connection", "keep-alive");
  res.flushHeaders();

  const send = (msg) => {
    res.write(`data: ${JSON.stringify({ log: msg })}\n\n`);
  };

  try {
    const result = await fetchFullSeries(seriesUrl, send);
    res.write(`data: ${JSON.stringify({ done: true, ...result })}\n\n`);
  } catch (err) {
    res.write(`data: ${JSON.stringify({ done: true, success: false, error: err.message })}\n\n`);
  } finally {
    res.end();
  }
});

app.get("/api/tmdb/details", async (req, res) => {
  const { id, type } = req.query;
  const details = await tmdb.getDetails(id, type || "tv");
  const seasons = await tmdb.getSeasons(id, type || "tv");
  res.json({ ...details, seasons });
});

app.get("/api/tmdb/episodes", async (req, res) => {
  const { id, season } = req.query;
  const episodes = await tmdb.getSeasonEpisodes(id, season);
  res.json(episodes);
});

app.post("/api/episodes/add-external", async (req, res) => {
  const { tmdbId, title, season, episodes, subDub } = req.body;
  if (!tmdbId || !season || !episodes) {
    return res.status(400).json({ error: "Missing required fields" });
  }

  const seriesSlug = title
    .toLowerCase()
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .trim();

  const results = [];
  for (const epNum of episodes) {
    try {
      // 1. Generate unique video_id for Supabase 2
      const charset = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
      let randomStr = "";
      for (let i = 0; i < 20; i++) {
        randomStr += charset.charAt(Math.floor(Math.random() * charset.length));
      }
      const video_id = "vDN" + randomStr;

      // 2. Build the vidnest URL based on sub/dub/hindi
      // https://vidnest.fun/tv/[TMDB_ID]/[SEASON]/[EPISODE]
      const iframe_url = `https://vidnest.fun/tv/${tmdbId}/${season}/${epNum}`;
      
      // 3. Save to Supabase 2 (public.iframe)
      const { error: error2 } = await supabase2.from("iframe").insert({
        iframe_url: iframe_url,
        name_title: "Vidnest",
        video_id: video_id
      });

      if (error2) throw error2;

      // 4. Fetch title from TMDB
      const epTitle = await tmdb.fetchEpisodeTitle(tmdbId, season, epNum);

      // 5. Save to Supabase 1
      const finalUrl = `https://stream.lastanime.in/v/${video_id}`;
      const { error: error1 } = await supabase.from("episodes").upsert({
        series_slug: seriesSlug,
        season: parseInt(season),
        episode: parseInt(epNum),
        title: epTitle,
        servers: [{ option: 1, real_video: finalUrl }]
      }, {
        onConflict: 'series_slug,season,episode'
      });

      if (error1) throw error1;
      results.push({ episode: epNum, success: true });
    } catch (err) {
      results.push({ episode: epNum, success: false, error: err.message });
    }
  }

  res.json({ success: true, results });
});

app.listen(PORT, "0.0.0.0", () => {
  console.log(`\n${"=".repeat(60)}`);
  console.log(`🚀 Toonstream Netlify Sync Server Started`);
  console.log(`${"=".repeat(60)}`);
  console.log(`📡 Server running on port ${PORT}`);
  console.log(`⏰ Sync schedule: ${ENABLE_CRON_SYNC ? `Enabled (${cronExpression})` : "Disabled"}`);
  console.log(`🔐 Proxy enabled: ${process.env.USE_PROXY === "true" ? "Yes" : "No"}`);
  console.log(`🌐 Health check: http://localhost:${PORT}/`);
  console.log(`📊 Status: http://localhost:${PORT}/status`);
  console.log(`🔄 Manual trigger: http://localhost:${PORT}/sync`);
  console.log(`${"=".repeat(60)}\n`);

  if (AUTO_SYNC_ON_START) {
    console.log("🎬 Running initial sync...\n");
    runSync();
  } else {
    console.log("🎬 Initial sync is disabled (AUTO_SYNC_ON_START=false)\n");
  }

  startTelegramListener();
});

process.on("SIGTERM", () => {
  console.log("\n⚠️  SIGTERM received, shutting down gracefully...");
  stopTelegramListener();
  process.exit(0);
});

process.on("SIGINT", () => {
  console.log("\n⚠️  SIGINT received, shutting down gracefully...");
  stopTelegramListener();
  process.exit(0);
});
