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

import { TMDBService } from "./services/tmdb/tmdb-service.js";
import { createClient } from "@supabase/supabase-js";
import "dotenv/config";

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

  const { data, error } = await supabase.from("series").upsert({
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
    total_seasons: seriesData.total_seasons,
    total_episodes: seriesData.total_episodes,
    random_key: Math.random().toString(36).substring(2, 15),
    updated_at: new Date().toISOString()
  }).select().single();

  if (error) return res.status(500).json({ error: error.message });
  res.json(data);
});

app.post("/api/series/refetch", async (req, res) => {
  const { id, title, type } = req.body;
  const results = await tmdb.search(title, type || "tv");
  if (!results.length) return res.status(404).json({ error: "No results found on TMDB" });
  
  const details = await tmdb.getDetails(results[0].id, type || "tv");
  const { error } = await supabase.from("series").update({
    poster: details.poster,
    banner_image: details.backdrop,
    description: details.description,
    rating: details.rating,
    genres: details.genres,
    release_date: details.release_date,
    year: details.year,
    total_seasons: details.total_seasons,
    total_episodes: details.total_episodes,
    tmdb_id: details.tmdb_id,
    random_key: Math.random().toString(36).substring(2, 15),
    updated_at: new Date().toISOString()
  }).eq("id", id);

  if (error) return res.status(500).json({ error: error.message });
  res.json({ success: true, details });
});

app.post("/api/series/rename", async (req, res) => {
  const { id, newName } = req.body;
  const { error } = await supabase.from("series").update({ title: newName }).eq("id", id);
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
