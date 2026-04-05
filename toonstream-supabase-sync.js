import "dotenv/config";
import axios from "axios";
import * as cheerio from "cheerio";
import { createClient } from "@supabase/supabase-js";
import ProxyManager from "./proxy-manager.js";
import fs from "fs";
import path from "path";
import { randomBytes } from "crypto";

const REQUIRED_ENV = [
  "SUPABASE_URL",
  "SUPABASE_SERVICE_ROLE_KEY",
  "TMDB_API_KEY",
];

const missing = REQUIRED_ENV.filter((key) => !process.env[key]);
if (missing.length) {
  console.error(`❌ Missing environment variables: ${missing.join(", ")}`);
  process.exit(1);
}

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

const CONFIG = {
  homeUrl: process.env.TOONSTREAM_HOME_URL || "https://toonstream.dad/home/",
  episodeBaseUrl: process.env.TOONSTREAM_EPISODE_BASE_URL || "https://toonstream.live/",
  pollIntervalMs: Number(process.env.POLL_INTERVAL_MS || 60_000),
  requestTimeout: 30_000,
  maxRetries: 3,
  maxParallelSeriesFetch: Number(process.env.MAX_PARALLEL_SERIES || 4),
  embedMaxDepth: Number(process.env.EMBED_MAX_DEPTH || 3),
  toonstreamCookies: process.env.TOONSTREAM_COOKIES?.trim() || null,
  ajaxUrl:
    process.env.TOONSTREAM_AJAX_URL ||
    "https://toonstream.dad/home/wp-admin/admin-ajax.php",
};

const defaultFallbacks = [`${CONFIG.homeUrl}home/`, `${CONFIG.homeUrl}page/1/`];

const envFallbacks = (process.env.TOONSTREAM_HOME_FALLBACKS || "")
  .split(",")
  .map((u) => u.trim())
  .filter(Boolean);

CONFIG.homepageCandidates = Array.from(
  new Set([CONFIG.homeUrl, ...envFallbacks, ...defaultFallbacks]),
);

const USER_AGENTS = [
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
  "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
  "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
  "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
];

const TMDB_BASE_URL = "https://api.themoviedb.org/3";
const TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/original";

const seriesCache = new Map();

const TVDB_API_KEY = process.env.TVDB_API_KEY;
const TVDB_BASE_URL = "https://api4.thetvdb.com/v4";
let tvdbToken = null;

async function getTVDBToken() {
  if (tvdbToken) return tvdbToken;
  try {
    const res = await axios.post(
      `${TVDB_BASE_URL}/login`,
      {
        apikey: TVDB_API_KEY,
      },
      {
        headers: { "Content-Type": "application/json" },
      },
    );
    tvdbToken = res.data?.data?.token;
    return tvdbToken;
  } catch (err) {
    console.warn(`   ⚠️ TVDB: Login failed: ${err.message}`);
    return null;
  }
}

async function searchTVDBSeries(title) {
  const token = await getTVDBToken();
  if (!token) return null;
  try {
    const res = await axios.get(
      `${TVDB_BASE_URL}/search?query=${encodeURIComponent(title)}&type=series`,
      {
        headers: { Authorization: `Bearer ${token}` },
      },
    );
    return res.data?.data?.[0]?.tvdb_id || null;
  } catch (err) {
    console.warn(
      `   ⚠️ TVDB: Series search failed for "${title}": ${err.message}`,
    );
    return null;
  }
}

async function fetchTVDBEpisodeImage(tvdbId, seasonNum, episodeNum) {
  const token = await getTVDBToken();
  if (!token || !tvdbId) return null;
  try {
    const res = await axios.get(
      `${TVDB_BASE_URL}/series/${tvdbId}/episodes/default?page=0`,
      {
        headers: { Authorization: `Bearer ${token}` },
      },
    );

    const episodes = res.data?.data?.episodes || [];
    const ep = episodes.find(
      (e) => e.seasonNumber === seasonNum && e.number === episodeNum,
    );

    if (ep && ep.image) {
      return ep.image.startsWith("http")
        ? ep.image
        : `https://artworks.thetvdb.com${ep.image}`;
    }
    return null;
  } catch (err) {
    console.warn(
      `   ⚠️ TVDB: Episode image fetch failed for ID ${tvdbId} S${seasonNum}E${episodeNum}: ${err.message}`,
    );
    return null;
  }
}

const TOONSTREAM_HOST = (() => {
  try {
    return new URL(CONFIG.homeUrl).hostname.replace(/^www\./, "");
  } catch {
    return null;
  }
})();

const TOONSTREAM_ORIGIN = (() => {
  try {
    return new URL(CONFIG.homeUrl).origin;
  } catch {
    return CONFIG.homeUrl;
  }
})();

const TOONSTREAM_EPISODE_ORIGIN = (() => {
  try {
    return new URL(CONFIG.episodeBaseUrl).origin;
  } catch {
    return "https://toonstream.live";
  }
})();

const TOONSTREAM_EPISODE_HOST = (() => {
  try {
    return new URL(TOONSTREAM_EPISODE_ORIGIN).hostname.replace(/^www\./, "");
  } catch {
    return "toonstream.live";
  }
})();

const CACHE_DIR = path.join(process.cwd(), "bin");
const SERIES_CACHE_FILE = path.join(CACHE_DIR, "series_cache.json");
const EPISODE_CACHE_FILE = path.join(CACHE_DIR, "episode_cache.json");

if (!fs.existsSync(CACHE_DIR)) {
  fs.mkdirSync(CACHE_DIR, { recursive: true });
}

function loadCache(filePath) {
  if (fs.existsSync(filePath)) {
    try {
      return JSON.parse(fs.readFileSync(filePath, "utf-8"));
    } catch (err) {
      console.warn(`⚠️ Failed to load cache from ${filePath}: ${err.message}`);
    }
  }
  return {};
}

function saveCache(filePath, data) {
  try {
    fs.writeFileSync(filePath, JSON.stringify(data, null, 2));
  } catch (err) {
    console.warn(`⚠️ Failed to save cache to ${filePath}: ${err.message}`);
  }
}

const localSeriesCache = loadCache(SERIES_CACHE_FILE);
const localEpisodeCache = loadCache(EPISODE_CACHE_FILE);
const processedEpisodes = new Set();
const proxyManager = new ProxyManager();

const completedSeries = new Set();

const stats = {
  newEpisodes: 0,
  updatedEpisodes: 0,
  failedEpisodes: 0,
  skippedEpisodes: 0,
  totalServers: 0,
  seriesProcessed: new Set(),
};

function makeEpisodeKey(slug, season, episode) {
  return `${slug}::${season}x${episode}`;
}

function makeSeasonEpisodeKey(season, episode) {
  return `${season}x${episode}`;
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function generateRandomKey(length = 22) {
  const chars = "abcdefghijklmnopqrstuvwxyz0123456789";
  let out = "";
  while (out.length < length) {
    const bytes = randomBytes(length);
    for (const value of bytes) {
      out += chars[value % chars.length];
      if (out.length >= length) break;
    }
  }
  return out;
}

function getUA() {
  return USER_AGENTS[Math.floor(Math.random() * USER_AGENTS.length)];
}

function cleanSlug(name) {
  if (!name) return "item";
  let cleaned = name.toLowerCase();
  if (
    cleaned.includes("naruto shippūden") ||
    cleaned.includes("naruto shippuden") ||
    cleaned.includes("naruto-shippuden") ||
    cleaned.includes("naruto-shippden")
  ) {
    return "naruto-shippden";
  }
  if (/^naruto-shipp[u]?den(-\d+x\d+)?$/i.test(cleaned)) {
    return "naruto-shippden";
  }
  return cleaned
    .replace(/['"]/g, "")
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function decodeHtmlEntities(str) {
  if (!str) return str;
  return str
    .replace(/&#038;/g, "&")
    .replace(/&#38;/g, "&")
    .replace(/&amp;/g, "&")
    .replace(/&#039;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&#034;/g, '"')
    .replace(/&#34;/g, '"')
    .replace(/&quot;/g, '"')
    .replace(/&lt;/g, "<")
    .replace(/&gt;/g, ">");
}

function normalizeUrl(rawUrl, base = CONFIG.homeUrl) {
  if (!rawUrl || /^javascript:/i.test(rawUrl)) return null;
  try {
    const decodedUrl = decodeHtmlEntities(rawUrl);
    return new URL(decodedUrl, base).href;
  } catch {
    return null;
  }
}

function forceToEpisodeDomain(rawUrl, base = TOONSTREAM_EPISODE_ORIGIN) {
  const normalized = normalizeUrl(rawUrl, base);
  if (!normalized) return null;

  try {
    const urlObj = new URL(normalized);
    const host = urlObj.hostname.replace(/^www\./, "");
    if (!host.includes("toonstream")) return normalized;

    const targetOrigin = new URL(TOONSTREAM_EPISODE_ORIGIN);
    urlObj.protocol = targetOrigin.protocol;
    urlObj.hostname = targetOrigin.hostname;
    urlObj.port = targetOrigin.port;

    return urlObj.href;
  } catch {
    return normalized;
  }
}

function isToonstreamUrl(url) {
  if (!url) return false;
  try {
    const hostname = new URL(url).hostname.replace(/^www\./, "");
    return hostname === TOONSTREAM_HOST || hostname === TOONSTREAM_EPISODE_HOST;
  } catch {
    return false;
  }
}

function extractSeriesSlugFromUrl(seriesUrl) {
  try {
    const u = new URL(seriesUrl);
    const parts = u.pathname.split("/").filter(Boolean);
    const slug = parts.pop() || null;
    return cleanSlug(slug);
  } catch {
    return null;
  }
}

function deriveSeriesUrlFromEpisode(episodeUrl) {
  try {
    const u = new URL(episodeUrl);
    const parts = u.pathname.split("/").filter(Boolean);
    const episodeSlug = parts[1] || parts[parts.length - 1] || "";
    if (!episodeSlug) return null;
    const baseSlug = episodeSlug.replace(/-\d+x\d+$/i, "") || episodeSlug;
    const normalizedSlug = cleanSlug(baseSlug);
    return `${TOONSTREAM_EPISODE_ORIGIN}/series/${normalizedSlug}/`;
  } catch {
    return null;
  }
}

function buildSeriesUrlFromSlug(seriesSlug) {
  if (!seriesSlug) return null;
  const urlSlug =
    seriesSlug === "naruto-shippden" ? "naruto-shippuden" : seriesSlug;
  return `${TOONSTREAM_EPISODE_ORIGIN}/series/${urlSlug}/`;
}

function buildEpisodeUrl(seriesSlug, season, episode) {
  if (!seriesSlug) return null;
  const urlSlug =
    seriesSlug === "naruto-shippden" ? "naruto-shippuden" : seriesSlug;
  return `${TOONSTREAM_EPISODE_ORIGIN}/episode/${urlSlug}-${season}x${episode}/`;
}

function isValidEpisodeUrl(url) {
  if (!url || typeof url !== "string") return false;
  if (!url.includes("/episode/")) return false;
  const episodePart = url.split("/episode/")[1];
  if (!episodePart || episodePart === "" || episodePart === "/") return false;
  if (url.endsWith("/episode/") || url.endsWith("/episode")) return false;
  return true;
}

function buildRequestHeaders(url, options = {}) {
  const headers = {
    "User-Agent": getUA(),
    Accept: "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    "Upgrade-Insecure-Requests": "1",
  };
  if (options.referer) headers.Referer = options.referer;
  if (options.headers) Object.assign(headers, options.headers);
  if (isToonstreamUrl(url)) {
    if (!headers.Referer) headers.Referer = CONFIG.homeUrl;
    const reqOrigin = (() => {
      try {
        return new URL(url).origin;
      } catch {
        return TOONSTREAM_ORIGIN;
      }
    })();
    headers.Origin = reqOrigin;
    headers["Sec-Fetch-Dest"] = "document";
    headers["Sec-Fetch-Mode"] = "navigate";
    headers["Sec-Fetch-Site"] = "same-origin";
    headers["Sec-Fetch-User"] = "?1";
    if (CONFIG.toonstreamCookies) headers.Cookie = CONFIG.toonstreamCookies;
  }
  return headers;
}

async function fetchHtmlWithRetry(
  url,
  retries = CONFIG.maxRetries,
  options = {},
) {
  let lastErr = null;
  let currentProxy = null;
  for (let attempt = 1; attempt <= retries; attempt++) {
    try {
      currentProxy = proxyManager.getNextProxy();
      const proxyAgent = proxyManager.getProxyAgent(currentProxy);
      const config = {
        timeout: options.timeout || CONFIG.requestTimeout,
        headers: buildRequestHeaders(url, options),
        responseType: "text",
        maxRedirects: 5,
        decompress: true,
        validateStatus: (status) => status >= 200 && status < 400,
      };
      if (proxyAgent) {
        config.httpAgent = proxyAgent;
        config.httpsAgent = proxyAgent;
      }
      const res = await axios.get(url, config);
      return String(res.data || "");
    } catch (err) {
      lastErr = err;
      if (
        currentProxy &&
        (err.code === "ECONNREFUSED" || err.code === "ETIMEDOUT")
      ) {
        proxyManager.markProxyAsFailed(currentProxy);
      }
      const status = err.response?.status;
      if (status)
        console.warn(
          `  ⚠️ Request failed (${status}) for ${url} (attempt ${attempt}/${retries})`,
        );
      await delay(500 * attempt);
    }
  }
  throw new Error(
    `Failed to fetch ${url}: ${lastErr?.message || "unknown error"}`,
  );
}

function cleanTitleForTMDB(title) {
  if (!title) return title;
  let cleaned = title;
  if (cleaned.toLowerCase().includes("bleach")) return "Bleach";
  if (cleaned.toLowerCase().includes("jujutsu kaisen")) return "Jujutsu Kaisen";
  cleaned = cleaned.replace(/(\w+)1[-\/]2/gi, "$1 1/2");
  cleaned = cleaned.replace(/(\w+)-1[-\/]2/gi, "$1 1/2");
  cleaned = cleaned.replace(/-/g, " ");
  cleaned = cleaned.replace(/[:\-–—]+\s*Season\s*\d+/gi, "");
  cleaned = cleaned.replace(/\s*Season\s*\d+/gi, "");
  cleaned = cleaned.replace(/\s*S\d+E?\d*/gi, "");
  cleaned = cleaned.replace(/\s*\d+x\d+/gi, "");
  cleaned = cleaned.replace(/\s*\[[^\]]*\]/gi, " ");
  cleaned = cleaned.replace(
    /\s*(Dub|Sub|Dubbed|Subbed|English|Japanese|Hindi|Hindi Dub|Eng|Jap)\s*/gi,
    " ",
  );
  cleaned = cleaned.replace(/\s*(1080p|720p|480p|HD|4K)\s*/gi, " ");
  cleaned = cleaned.replace(/\([^)]*\)/g, "");
  cleaned = cleaned.replace(/\[[^\]]*\]/g, "");
  cleaned = cleaned.replace(/\s+/g, " ").trim();
  return cleaned;
}

function extractSeriesNameFromSlug(slug) {
  if (!slug) return null;
  let name = slug.replace(/-\d+x\d+$/i, "");
  name = name.replace(/(\w+)1-2$/i, "$1 1/2");
  name = name.replace(/-/g, " ");
  name = name.replace(/\b\w/g, (c) => c.toUpperCase());
  return name.trim();
}

async function searchTMDB(title, type = "tv") {
  const apiKey = process.env.TMDB_API_KEY;
  if (!apiKey) return null;
  const cleanedTitle = cleanTitleForTMDB(title);
  const searchQueries = [cleanedTitle];
  if (cleanedTitle !== title) searchQueries.push(title);
  const withoutSuffix = cleanedTitle
    .replace(/\s*(the animation|the series|movie|ova|special)$/i, "")
    .trim();
  if (withoutSuffix && withoutSuffix !== cleanedTitle)
    searchQueries.push(withoutSuffix);
  for (const query of searchQueries) {
    const url = `${TMDB_BASE_URL}/search/${type}?api_key=${apiKey}&query=${encodeURIComponent(query)}&language=en-US`;
    try {
      const res = await fetch(url);
      if (!res.ok) continue;
      const json = await res.json();
      if (json.results && json.results.length > 0) return json.results[0].id;
    } catch (err) {}
  }
  return null;
}

async function fetchTMDBDetails(tmdbId, type = "tv") {
  const apiKey = process.env.TMDB_API_KEY;
  if (!apiKey || !tmdbId) return null;
  const url = `${TMDB_BASE_URL}/${type}/${tmdbId}?api_key=${apiKey}&language=en-US&append_to_response=images`;
  const res = await fetch(url);
  if (!res.ok) return null;
  const data = await res.json();
  const posters = [];
  if (data.poster_path) posters.push(`${TMDB_IMAGE_BASE}${data.poster_path}`);
  data.images?.posters?.slice(0, 5).forEach((img) => {
    const src = `${TMDB_IMAGE_BASE}${img.file_path}`;
    if (!posters.includes(src)) posters.push(src);
  });
  const backdrops = [];
  if (data.backdrop_path)
    backdrops.push(`${TMDB_IMAGE_BASE}${data.backdrop_path}`);
  data.images?.backdrops?.slice(0, 5).forEach((img) => {
    const src = `${TMDB_IMAGE_BASE}${img.file_path}`;
    if (!backdrops.includes(src)) backdrops.push(src);
  });
  return {
    tmdb_id: data.id,
    title: data.name || data.title || null,
    description: data.overview || null,
    rating: data.vote_average ? parseFloat(data.vote_average.toFixed(2)) : null,
    popularity: data.popularity ? parseFloat(data.popularity.toFixed(3)) : null,
    status: data.status || null,
    genres: data.genres?.map((g) => g.name) || [],
    studios: data.production_companies?.map((c) => c.name) || [],
    release_date: data.first_air_date || data.release_date || null,
    total_seasons: data.number_of_seasons || null,
    total_episodes: data.number_of_episodes || null,
    runtime: data.runtime || null,
    posters,
    backdrops,
    poster: posters[0] || null,
    banner_image: backdrops[0] || null,
  };
}

async function getTMDBData(title, isMovie = false) {
  const type = isMovie ? "movie" : "tv";
  const tmdbId = await searchTMDB(title, type);
  if (!tmdbId) return null;
  return await fetchTMDBDetails(tmdbId, type);
}

async function fetchTMDBEpisodeImage(tmdbId, seasonNum, episodeNum) {
  const apiKey = process.env.TMDB_API_KEY;
  if (!apiKey || !tmdbId) return null;
  const url = `${TMDB_BASE_URL}/tv/${tmdbId}/season/${seasonNum}/episode/${episodeNum}?api_key=${apiKey}&language=en-US`;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const response = await axios.get(url, { timeout: 10000 });
      const data = response.data;
      if (data && data.still_path) return `${TMDB_IMAGE_BASE}${data.still_path}`;
      if (attempt === 3) return null;
    } catch (err) {
      if (attempt === 3) {
        try {
          const res = await fetch(url);
          if (!res.ok) return null;
          const data = await res.json();
          if (data?.still_path) return `${TMDB_IMAGE_BASE}${data.still_path}`;
        } catch {
          return null;
        }
      }
      await delay(350 * attempt);
    }
  }
  return null;
}

async function fetchTMDBSeasonEpisodes(tmdbId, seasonNum) {
  const apiKey = process.env.TMDB_API_KEY;
  if (!apiKey || !tmdbId) return {};
  const url = `${TMDB_BASE_URL}/tv/${tmdbId}/season/${seasonNum}?api_key=${apiKey}&language=en-US`;

  for (let attempt = 1; attempt <= 3; attempt++) {
    try {
      const response = await axios.get(url, { timeout: 15000 });
      const data = response.data;
      const episodeImages = {};
      if (data && data.episodes && Array.isArray(data.episodes)) {
        for (const ep of data.episodes) {
          if (ep.still_path)
            episodeImages[ep.episode_number] =
              `${TMDB_IMAGE_BASE}${ep.still_path}`;
        }
      }
      return episodeImages;
    } catch (err) {
      if (attempt === 3) {
        try {
          const res = await fetch(url);
          if (!res.ok) return {};
          const data = await res.json();
          const episodeImages = {};
          if (Array.isArray(data?.episodes)) {
            for (const ep of data.episodes) {
              if (ep.still_path)
                episodeImages[ep.episode_number] =
                  `${TMDB_IMAGE_BASE}${ep.still_path}`;
            }
          }
          return episodeImages;
        } catch {
          return {};
        }
      }
      await delay(350 * attempt);
    }
  }
  return {};
}

async function fetchTMDBTitleById(tmdbId) {
  const apiKey = process.env.TMDB_API_KEY;
  if (!apiKey || !tmdbId) return null;
  try {
    const url = `${TMDB_BASE_URL}/tv/${tmdbId}?api_key=${apiKey}&language=en-US`;
    const response = await axios.get(url, { timeout: 10000 });
    const data = response.data;
    return data?.name || data?.original_name || data?.title || null;
  } catch {
    return null;
  }
}

const tmdbEpisodeImageCache = new Map();

function extractEpisodeCards(html) {
  const $ = cheerio.load(html);
  const cards = [];
  const seen = new Set();
  $("article.episodes, article.post").each((_, el) => {
    const article = $(el);
    const anchor = article
      .find('a[href*="/episode/"], a.lnk-blk[href*="/episode/"]')
      .first();
    if (!anchor.length) return;
    const url = normalizeUrl(anchor.attr("href"));
    if (!url || seen.has(url)) return;
    seen.add(url);
    const titleEl = article.find(".entry-title, h2").first();
    const title = titleEl.text().trim() || anchor.attr("title") || "";
    const img = article.find("figure img, .post-thumbnail img, img").first();
    let thumb = null;
    if (img.length) {
      thumb =
        img.attr("data-src") || img.attr("src") || img.attr("data-lazy-src");
      if (thumb && thumb.startsWith("//")) thumb = "https:" + thumb;
      else if (thumb) thumb = normalizeUrl(thumb);
    }
    cards.push({ url, title, thumb });
  });
  return cards;
}

function extractSeriesEpisodeLinks(seriesHtml, seriesUrl) {
  const $ = cheerio.load(seriesHtml);
  const links = [];
  const seen = new Set();
  const addLink = (anchor) => {
    const node = $(anchor);
    const url = normalizeUrl(node.attr("href"), seriesUrl);
    if (!url || !url.includes("/episode/")) return;
    const code = parseEpisodeCode(url);
    if (!code || seen.has(`${code.season}x${code.episode}`)) return;
    seen.add(`${code.season}x${code.episode}`);
    links.push({
      url,
      season: code.season,
      episode: code.episode,
      title: node.attr("title") || node.text().trim(),
    });
  };
  $('a[href*="/episode/"]').each((_, el) => addLink(el));
  return links;
}

function parseEpisodeCode(url) {
  try {
    const u = new URL(url);
    const parts = u.pathname.split("/").filter(Boolean);
    const slug = parts[parts.length - 1];
    const match = slug.match(/-(\d+)x(\d+)(?:\/?|#.*)?$/i);
    if (match)
      return {
        season: parseInt(match[1], 10),
        episode: parseInt(match[2], 10),
      };
    return null;
  } catch {
    return null;
  }
}

function extractPostId(html) {
  const patterns = [
    // Toonstream season selector: <a data-post="1914"
    /class=["'][^"']*sel-temp[^"']*"[^>]*>\s*<a[^>]+data-post=["'](\d+)["']/i,
    /class=["'](?:postid-|wp-post-id-)(\d+)["']/i,
    /["']postid["']\s*:\s*(\d+)/i,
    /var\s+post_id\s*=\s*(\d+)/i,
    /data-post=["'](\d+)["']/i,
    /"post"\s*:\s*"(\d+)"/i,
  ];
  for (const pat of patterns) {
    const m = html.match(pat);
    if (m) return m[1];
  }
  return null;
}

function extractNonce(html) {
  const patterns = [
    /["']nonce["']\s*:\s*["']([a-f0-9]+)["']/i,
    /nonce\s*=\s*["']([a-f0-9]+)["']/i,
    /var\s+nonce\s*=\s*["']([a-f0-9]+)["']/i,
    /["']doo_nonce["']\s*:\s*["']([a-f0-9]+)["']/i,
    /["']ajax_nonce["']\s*:\s*["']([a-f0-9]+)["']/i,
    /_wpnonce["']\s*:\s*["']([a-f0-9]+)["']/i,
  ];
  for (const pat of patterns) {
    const m = html.match(pat);
    if (m) return m[1];
  }
  return null;
}

function getAjaxUrl(pageUrl) {
  try {
    const u = new URL(pageUrl || CONFIG.homeUrl);
    return `${u.origin}/wp-admin/admin-ajax.php`;
  } catch {
    return CONFIG.ajaxUrl;
  }
}

function getHomeAjaxUrl(pageUrl, pageHtml) {
  const explicitSeasonApi = process.env.TOONSTREAM_SEASON_API_URL;
  if (explicitSeasonApi) return explicitSeasonApi;

  if (pageHtml) {
    const seasonApiMatch = pageHtml.match(
      /(https?:\/\/[^"'\s]+\/fetch_episodes\.php)/i,
    );
    if (seasonApiMatch?.[1]) return seasonApiMatch[1];
  }

  return `${TOONSTREAM_EPISODE_ORIGIN}/fetch_episodes.php`;
}

function extractEpisodesFromSeasonApiResponse(data, season, pageUrl) {
  const episodes = [];
  const seen = new Set();

  const pushEpisode = (rawUrl, fallbackTitle = "") => {
    const url = forceToEpisodeDomain(
      rawUrl,
      pageUrl || TOONSTREAM_EPISODE_ORIGIN,
    );
    if (!url || !url.includes("/episode/")) return;

    const code = parseEpisodeCode(url);
    if (!code?.episode) return;

    const key = `${code.season}x${code.episode}`;
    if (seen.has(key)) return;
    seen.add(key);

    episodes.push({
      season: code.season || parseInt(season, 10),
      episode: code.episode,
      url,
      title: fallbackTitle,
    });
  };

  if (typeof data === "string") {
    const $ = cheerio.load(data);

    $("article a[href*='/episode/'], a[href*='/episode/']").each((_, el) => {
      const anchor = $(el);
      const href = anchor.attr("href");
      const title = anchor.attr("title") || anchor.text().trim() || "";
      pushEpisode(href, title);
    });

    if (episodes.length > 0) return episodes;

    for (const match of data.matchAll(/https?:\/\/[^"'\s]*\/episode\/[^"'\s]+/gi)) {
      pushEpisode(match[0]);
    }

    return episodes;
  }

  if (Array.isArray(data)) {
    data.forEach((item) => {
      pushEpisode(item?.url || item?.link || item?.episode_url, item?.title || "");
    });
    return episodes;
  }

  if (data && typeof data === "object") {
    const candidates = [
      data.html,
      data.data,
      data.episodes,
      data.results,
      data.items,
    ];

    for (const candidate of candidates) {
      const partial = extractEpisodesFromSeasonApiResponse(
        candidate,
        season,
        pageUrl,
      );
      partial.forEach((ep) => {
        const key = `${ep.season}x${ep.episode}`;
        if (!seen.has(key)) {
          seen.add(key);
          episodes.push(ep);
        }
      });
    }
  }

  return episodes;
}

async function fetchEpisodeDataFromAPI(
  postId,
  season,
  nonce,
  pageUrl,
  pageHtml,
) {
  if (!postId || !season) return [];
  const referer = pageUrl || CONFIG.homeUrl;

  // Extract AJAX URL from page HTML first (handles cross-domain like toonstream.one → toonstream.dad)
  const ajaxUrl = getHomeAjaxUrl(pageUrl, pageHtml);
  console.log(
    `         🌐 Season ${season} API: GET ${ajaxUrl}?post=${postId}&season=${season}`,
  );

  try {
    const res = await axios.get(ajaxUrl, {
      params: {
        post: postId,
        season,
      },
      headers: {
        Referer: referer,
        "User-Agent": getUA(),
        Accept: "application/json,text/html,*/*",
      },
      timeout: 15000,
    });

    const episodes = extractEpisodesFromSeasonApiResponse(
      res.data,
      season,
      pageUrl,
    );
    if (episodes.length > 0) {
      console.log(
        `         ✓ Season ${season}: ${episodes.length} episodes parsed from fetch_episodes API`,
      );
    }
    return episodes;
  } catch (err) {
    console.warn(
      `         ⚠️ Season API error (post=${postId}, season=${season}): ${err.message}`,
    );
    return [];
  }
}

function extractSeriesMeta(html, url) {
  const $ = cheerio.load(html);
  const title = $(".data h1, .entry-title").first().text().trim();
  const description = $(".wp-content p, .description p").first().text().trim();
  const poster = normalizeUrl($(".poster img").first().attr("src"), url);
  const genres = [];
  $(".sgeneros a").each((_, el) => genres.push($(el).text().trim()));
  const rating = parseFloat($(".dt_rating_vbc").text()) || null;
  const yearMatch = title.match(/\((\d{4})\)/);
  const year = yearMatch ? parseInt(yearMatch[1], 10) : null;
  return { title, description, poster, genres, rating, year };
}

function extractEpisodeMeta(html) {
  const $ = cheerio.load(html);
  const title = $(".data h1").text().trim();
  const description = $(".wp-content p").text().trim();
  const thumbnail = normalizeUrl($(".player_nav img").first().attr("src"));
  return { title, description, thumbnail };
}

function extractEmbedUrlFromResponse(data) {
  // 1. Direct embed_url field
  if (data?.embed_url && data.embed_url.startsWith("http"))
    return data.embed_url;

  const html = data?.html || (typeof data === "string" ? data : "");
  if (!html) return null;

  // 2. src inside <iframe>
  const iframeSrc = html.match(/<iframe[^>]+src=["']([^"']+)["']/i);
  if (iframeSrc?.[1]) return iframeSrc[1];

  // 3. src anywhere
  const anySrc = html.match(/src=["']([^"']+)["']/i);
  if (anySrc?.[1] && anySrc[1].startsWith("http")) return anySrc[1];

  // 4. url or file field in JSON-like string
  const urlMatch = html.match(/"(?:url|file|src|source)"\s*:\s*"([^"]+)"/i);
  if (urlMatch?.[1]) return urlMatch[1];

  return null;
}

function isToonstream(url) {
  try {
    const host = new URL(url).hostname;
    return host.includes("toonstream") || host.includes("trembed");
  } catch {
    return false;
  }
}

async function resolveTrembedUrl(trembedUrl, episodeUrl) {
  // Fetch the trembed page and extract the real video URL inside it
  try {
    const pageOrigin = (() => {
      try {
        return new URL(episodeUrl || trembedUrl).origin;
      } catch {
        return "";
      }
    })();
    const res = await axios.get(trembedUrl, {
      headers: buildRequestHeaders(trembedUrl, {
        referer: episodeUrl || pageOrigin,
      }),
      timeout: 15000,
      maxRedirects: 5,
      validateStatus: (status) => status >= 200 && status < 400,
    });
    const pageHtml = res.data || "";
    const $ = cheerio.load(pageHtml);

    // Find any iframe that is NOT a toonstream domain
    let found = null;
    $("iframe[src], iframe[data-src]").each((_, el) => {
      const src = $(el).attr("src") || $(el).attr("data-src");
      if (!src || src.startsWith("about:") || src.startsWith("javascript:"))
        return;
      const cleaned = src.replace(/&#038;/g, "&").replace(/&amp;/g, "&");
      if (!isToonstream(cleaned) && !found) {
        found = cleaned;
      }
    });
    if (found) return found;

    // Fallback: check script/inline for any external video URL
    const srcMatch = pageHtml.match(
      /(?:src|file|source)\s*[:=]\s*["']((https?:\/\/(?!(?:[^/]*\.)?toonstream)[^"'\s]+))["']/i,
    );
    if (srcMatch) return srcMatch[1];

    return null;
  } catch (err) {
    console.warn(
      `            ⚠️ resolveTrembedUrl failed for ${trembedUrl}: ${err.message}`,
    );
    return null;
  }
}

async function extractEmbeds(html, episodeUrl) {
  const embeds = [];
  const seen = new Set();

  try {
    const $ = cheerio.load(html);

    $("a.myservers[data-src], a[data-src]").each((index, el) => {
      const rawSrc = $(el).attr("data-src");
      const resolved = normalizeUrl(rawSrc, episodeUrl);
      if (!resolved || seen.has(resolved)) return;

      const label = $(el).text().trim();
      seen.add(resolved);
      embeds.push({
        option: embeds.length + 1,
        real_video: resolved,
        label: label || `Server ${index + 1}`,
      });
    });

    if (embeds.length > 0) {
      console.log(`            🔌 Found ${embeds.length} server URL(s) from data-src`);
      return embeds;
    }

    const pageOrigin = (() => {
      try {
        return new URL(episodeUrl).origin;
      } catch {
        return TOONSTREAM_EPISODE_ORIGIN;
      }
    })();

    // ── Step 1: Collect all trembed URLs from the episode page HTML ──────────
    // Pattern: <iframe data-src="https://toonstream.dad/home/?trembed=2&trid=9021&trtype=2"
    const trembedUrls = [];
    const trembedSeenKeys = new Set();

    // Match all data-src / src that contain trembed or trid
    const rawMatches = [
      ...html.matchAll(
        /(?:data-src|src)=["']([^"']*(?:trembed|trid)[^"']*)["']/gi,
      ),
    ];
    for (const m of rawMatches) {
      const rawUrl = m[1].replace(/&#038;/g, "&").replace(/&amp;/g, "&");
      const url = forceToEpisodeDomain(rawUrl, TOONSTREAM_EPISODE_ORIGIN);
      if (!url) continue;
      if (!trembedSeenKeys.has(url)) {
        trembedSeenKeys.add(url);
        trembedUrls.push(url);
      }
    }

    // Also build trembed URLs from trid pattern in HTML (trembed index = 0..N)
    if (trembedUrls.length === 0) {
      const pairs = new Map();
      for (const m of [
        ...html.matchAll(
          /trid=(\d+)[^"'\s&]*(?:&(?:amp;)?|&amp;)?trtype=(\d+)/gi,
        ),
      ]) {
        const key = `${m[1]}_${m[2]}`;
        if (!pairs.has(key)) pairs.set(key, { trid: m[1], trtype: m[2] });
      }
      for (const m of [
        ...html.matchAll(
          /trtype=(\d+)[^"'\s&]*(?:&(?:amp;)?|&amp;)?trid=(\d+)/gi,
        ),
      ]) {
        const key = `${m[2]}_${m[1]}`;
        if (!pairs.has(key)) pairs.set(key, { trid: m[2], trtype: m[1] });
      }
      let idx = 0;
      for (const { trid, trtype } of pairs.values()) {
        const url = `${TOONSTREAM_EPISODE_ORIGIN}/?trembed=${idx}&trid=${trid}&trtype=${trtype}`;
        trembedUrls.push(url);
        idx++;
      }
    }

    console.log(`            🔌 Found ${trembedUrls.length} trembed URL(s)`);

    // ── Step 2: Fetch each trembed URL and extract the real video URL ────────
    for (let i = 0; i < trembedUrls.length; i++) {
      const trembedUrl = trembedUrls[i];
      console.log(`            🔍 Resolving Server ${i + 1}: ${trembedUrl}`);
      const realUrl = await resolveTrembedUrl(trembedUrl, episodeUrl);
      if (realUrl && !seen.has(realUrl)) {
        seen.add(realUrl);
        embeds.push({ option: i + 1, real_video: realUrl });
        console.log(
          `            ✓ Server ${i + 1} resolved: ${realUrl.substring(0, 80)}`,
        );
      } else if (!realUrl) {
        console.warn(`            ⚠️ Could not resolve Server ${i + 1}`);
      }
      await delay(300);
    }
  } catch (err) {
    console.warn(`            ⚠️ extractEmbeds error: ${err.message}`);
  }

  if (embeds.length === 0) {
    console.warn(`            ⚠️ No servers found at ${episodeUrl}`);
  }

  return embeds;
}

async function getExistingEpisodeSet(seriesSlug) {
  const { data, error } = await supabase
    .from("episodes")
    .select("season, episode")
    .eq("series_slug", seriesSlug);
  if (error) throw new Error(`Supabase check failed: ${error.message}`);
  const set = new Set();
  data?.forEach((row) =>
    set.add(makeSeasonEpisodeKey(row.season, row.episode)),
  );
  return set;
}

async function syncEpisodeByUrl(url, options = {}) {
  try {
    const key = makeEpisodeKey(
      options.seriesSlug ||
        extractSeriesSlugFromUrl(deriveSeriesUrlFromEpisode(url)),
      options.code?.season || 1,
      options.code?.episode || 1,
    );
    if (!options.force && localEpisodeCache[key]) return;
    const { seriesCtx, code, episodePayload } = await buildEpisodeRecord(
      url,
      options,
    );

    // Build upsert payload — always update updated_at so it appears in "latest" views
    const now = new Date().toISOString();
    const savePayload = {
      ...episodePayload,
      series_slug: seriesCtx.slug,
      season: code.season,
      episode: code.episode,
      updated_at: now,
    };

    if (savePayload.thumbnail) {
      const normalizedImage = savePayload.thumbnail;
      savePayload.episode_main_poster =
        savePayload.episode_main_poster || normalizedImage;
      savePayload.episode_card_thumbnail =
        savePayload.episode_card_thumbnail || normalizedImage;
      savePayload.episode_list_thumbnail =
        savePayload.episode_list_thumbnail || normalizedImage;
      savePayload.video_player_thumbnail =
        savePayload.video_player_thumbnail || normalizedImage;
    }

    // Never overwrite existing servers with an empty array
    if (!episodePayload.servers || episodePayload.servers.length === 0) {
      delete savePayload.servers;
      console.log(`         ⚠️ No servers extracted — keeping existing servers in DB`);
    } else {
      console.log(`         💾 Saving ${episodePayload.servers.length} server(s) to DB`);
    }

    const { error } = await supabase
      .from("episodes")
      .upsert(savePayload, { onConflict: "series_slug,season,episode" });
    if (error) throw error;

    console.log(`         ✅ DB saved: S${code.season}E${code.episode} [${seriesCtx.slug}] at ${now}`);

    if (savePayload.thumbnail) {
      const imageValue = savePayload.thumbnail;
      const { error: imgErr } = await supabase
        .from("episodes")
        .update({
          episode_main_poster: imageValue,
          episode_card_thumbnail: imageValue,
          episode_list_thumbnail: imageValue,
          video_player_thumbnail: imageValue,
        })
        .eq("series_slug", seriesCtx.slug)
        .eq("season", code.season)
        .eq("episode", code.episode);
      if (imgErr) {
        console.warn(
          `         ⚠️ Episode image normalize failed: ${imgErr.message}`,
        );
      }
    }

    // Also save to latest_episodes table so new/updated episodes appear in latest feeds
    const { error: latestErr } = await supabase
      .from("latest_episodes")
      .upsert(
        {
          series_slug: seriesCtx.slug,
          series_title: seriesCtx.title || seriesCtx.slug,
          season: code.season,
          episode: code.episode,
          episode_title: episodePayload.title || `Episode ${code.episode}`,
          thumbnail: episodePayload.thumbnail || null,
          updated_at: now,
        },
        { onConflict: "series_slug,season,episode" },
      );
    if (latestErr) {
      console.warn(`         ⚠️ latest_episodes save failed: ${latestErr.message}`);
    } else {
      console.log(`         📋 latest_episodes updated: S${code.season}E${code.episode} [${seriesCtx.slug}]`);
    }

    // Update series random_key so frontend cache is invalidated for this series
    const newRandomKey = generateRandomKey();
    const { error: rkErr } = await supabase
      .from("series")
      .update({ random_key: newRandomKey, updated_at: now })
      .eq("slug", seriesCtx.slug);
    if (rkErr) {
      console.warn(`         ⚠️ series random_key update failed: ${rkErr.message}`);
    } else {
      console.log(`         🔑 series random_key updated: [${seriesCtx.slug}] → ${newRandomKey}`);
    }

    localEpisodeCache[key] = {
      ...episodePayload,
      updated_at: now,
    };
    saveCache(EPISODE_CACHE_FILE, localEpisodeCache);
    stats.newEpisodes++;
  } catch (err) {
    stats.failedEpisodes++;
    console.error(`         ❌ Save failed: ${err.message}`);
    throw err;
  }
}

async function buildEpisodeRecord(episodeUrl, hints = {}) {
  const episodeHtml = await fetchHtmlWithRetry(episodeUrl, CONFIG.maxRetries, {
    referer: hints.seriesUrl || CONFIG.homeUrl,
  });
  const derivedSeriesUrl =
    hints.seriesUrl || deriveSeriesUrlFromEpisode(episodeUrl);
  const seriesCtx = await resolveSeriesContext(
    derivedSeriesUrl,
    hints.seriesTitle,
  );
  const meta = extractEpisodeMeta(episodeHtml);
  const code = hints.code ||
    parseEpisodeCode(episodeUrl) || { season: 1, episode: 1 };
  const embeds = await extractEmbeds(episodeHtml, episodeUrl);
  let tmdbEpisodeImage = null;
  let tmdbTitleFromId = null;
  let imageSource = "none";
  if (seriesCtx.tmdb_id && process.env.TMDB_API_KEY) {
    const cacheKey = `${seriesCtx.tmdb_id}-${code.season}`;
    if (!tmdbEpisodeImageCache.has(cacheKey)) {
      const seasonImages = await fetchTMDBSeasonEpisodes(
        seriesCtx.tmdb_id,
        code.season,
      );
      if (Object.keys(seasonImages).length > 0) {
        tmdbEpisodeImageCache.set(cacheKey, seasonImages);
      }
    }
    const cachedImages = tmdbEpisodeImageCache.get(cacheKey) || {};
    tmdbEpisodeImage = cachedImages[code.episode];
    if (!tmdbEpisodeImage) {
      tmdbEpisodeImage = await fetchTMDBEpisodeImage(
        seriesCtx.tmdb_id,
        code.season,
        code.episode,
      );
      if (tmdbEpisodeImage) {
        cachedImages[code.episode] = tmdbEpisodeImage;
        tmdbEpisodeImageCache.set(cacheKey, cachedImages);
      }
    }
    if (tmdbEpisodeImage) imageSource = `tmdb:${seriesCtx.tmdb_id}`;
  }

  if (!tmdbEpisodeImage) {
    if (seriesCtx.tmdb_id) {
      tmdbTitleFromId = await fetchTMDBTitleById(seriesCtx.tmdb_id);
      if (tmdbTitleFromId) {
        console.log(
          `         🔎 TMDB image missing; trying TVDB with TMDB title: ${tmdbTitleFromId}`,
        );
      }
    }

    const tvdbSearchTitle = tmdbTitleFromId || seriesCtx.title || hints.seriesTitle;
    const tvdbId = tvdbSearchTitle ? await searchTVDBSeries(tvdbSearchTitle) : null;

    if (!tvdbId && seriesCtx.title && tvdbSearchTitle !== seriesCtx.title) {
      console.log(`         🔎 TVDB retry with series title: ${seriesCtx.title}`);
      const retryTvdbId = await searchTVDBSeries(seriesCtx.title);
      if (retryTvdbId)
        tmdbEpisodeImage = await fetchTVDBEpisodeImage(
          retryTvdbId,
          code.season,
          code.episode,
        );
    }

    if (tvdbId && !tmdbEpisodeImage)
      tmdbEpisodeImage = await fetchTVDBEpisodeImage(
        tvdbId,
        code.season,
        code.episode,
      );
    if (tmdbEpisodeImage && imageSource === "none") imageSource = "tvdb";
  }
  const bestImage =
    tmdbEpisodeImage ||
    seriesCtx.poster ||
    seriesCtx.banner_image ||
    null;
  if (!tmdbEpisodeImage && bestImage) imageSource = "series-fallback";
  if (bestImage) {
    console.log(
      `         🖼️ Episode image source: ${imageSource} (S${code.season}E${code.episode})`,
    );
  } else {
    console.log(`         ⚠️ Episode image source: none (S${code.season}E${code.episode})`);
  }
  const episodePayload = {
    title: meta.title || hints.card?.title || `Episode ${code.episode}`,
    thumbnail: bestImage,
    episode_main_poster: bestImage,
    episode_card_thumbnail: bestImage,
    episode_list_thumbnail: bestImage,
    video_player_thumbnail: bestImage,
    servers: embeds,
  };
  return { seriesCtx, code, episodePayload };
}

async function resolveSeriesContext(seriesUrl, fallbackTitle) {
  const rawSlug = extractSeriesSlugFromUrl(seriesUrl);
  if (!rawSlug) throw new Error(`Could not extract slug from ${seriesUrl}`);
  const isMovieUrl =
    seriesUrl.includes("/movie/") || seriesUrl.includes("/watch/");
  const finalSlug = cleanSlug(rawSlug);

  // Memory cache hit
  if (seriesCache.has(finalSlug)) {
    const cached = seriesCache.get(finalSlug);
    if (!cached.isMovie) {
      if (cached.tmdb_id) return cached; // Use series cache only when tmdb_id is present
      seriesCache.delete(finalSlug); // stale/incomplete cache, refresh from DB
    }
    if (isMovieUrl) return cached;      // URL is movie — use movie cache
    // Was cached as movie but URL is series — clear cache and re-resolve
    seriesCache.delete(finalSlug);
  }

  if (localSeriesCache[finalSlug] && !localSeriesCache[finalSlug].isMovie) {
    const cached = localSeriesCache[finalSlug];
    if (cached.tmdb_id) {
      const ctx = {
        ...cached,
        url: seriesUrl,
        sourceSlug: rawSlug,
        isMovie: false,
      };
      seriesCache.set(finalSlug, ctx);
      return ctx;
    }
    delete localSeriesCache[finalSlug];
    saveCache(SERIES_CACHE_FILE, localSeriesCache);
  }

  // Check series table first
  let { data: seriesData } = await supabase
    .from("series")
    .select("*")
    .eq("slug", finalSlug)
    .maybeSingle();

  if (seriesData) {
    if (!seriesData.random_key) {
      const missingKey = generateRandomKey();
      await supabase
        .from("series")
        .update({ random_key: missingKey, updated_at: new Date().toISOString() })
        .eq("slug", finalSlug);
      seriesData.random_key = missingKey;
      console.log(`   🔑 Added missing random_key: [${finalSlug}] → ${missingKey}`);
    }

    const ctx = { ...seriesData, url: seriesUrl, sourceSlug: rawSlug, isMovie: false };
    seriesCache.set(finalSlug, ctx);
    localSeriesCache[finalSlug] = { ...seriesData, isMovie: false };
    saveCache(SERIES_CACHE_FILE, localSeriesCache);
    return ctx;
  }

  // Not in series table — check movies table
  if (!seriesData) {
    let { data: movieData } = await supabase
      .from("movies")
      .select("*")
      .eq("slug", finalSlug)
      .maybeSingle();
    if (movieData) {
      if (isMovieUrl) {
        // Genuine movie URL — use movie record
        seriesData = movieData;
        isMovie = true;
        const ctx = { ...seriesData, url: seriesUrl, sourceSlug: rawSlug, isMovie: true };
        seriesCache.set(finalSlug, ctx);
        localSeriesCache[finalSlug] = { ...seriesData, isMovie: true };
        saveCache(SERIES_CACHE_FILE, localSeriesCache);
        return ctx;
      } else {
        // Was wrongly saved as movie — ignore and re-create in series table below
        console.log(`   🔁 "${finalSlug}" was in movies table but URL is /series/ — re-creating in series table`);
      }
    }
  }
  const seriesHtml = await fetchHtmlWithRetry(seriesUrl);
  const meta = extractSeriesMeta(seriesHtml, seriesUrl);
  const titleForTmdb =
    fallbackTitle || meta.title || extractSeriesNameFromSlug(rawSlug);
  // Only use URL-based or explicit type detection — never match on raw HTML text
  // (HTML always contains words like "Movie" or "duration" for unrelated content)
  const isActuallyMovie =
    isMovieUrl ||
    (Array.isArray(meta.genres) && meta.genres.some(g => g.toLowerCase() === "movie")) ||
    meta.type === "movie";
  const tmdbData = await getTMDBData(titleForTmdb, isActuallyMovie);
  const payload = {
    slug: finalSlug,
    title: tmdbData?.title || titleForTmdb,
    description: tmdbData?.description || meta.description,
    poster: tmdbData?.poster || meta.poster,
    banner_image: tmdbData?.banner_image || null,
    genres: tmdbData?.genres?.length ? tmdbData.genres : meta.genres,
    tmdb_id: tmdbData?.tmdb_id || null,
    rating: tmdbData?.rating || meta.rating || null,
    release_date: tmdbData?.release_date || null,
    year:
      meta.year ||
      (tmdbData?.release_date
        ? parseInt(tmdbData.release_date.split("-")[0], 10)
        : null),
    updated_at: new Date().toISOString(),
  };
  const targetTable = isActuallyMovie ? "movies" : "series";
  if (isActuallyMovie) payload.runtime = tmdbData?.runtime || null;
  else {
    payload.total_seasons = tmdbData?.total_seasons || 1;
    payload.total_episodes = tmdbData?.total_episodes || null;
    payload.random_key = generateRandomKey();
  }
  const { error: seriesUpsertError } = await supabase
    .from(targetTable)
    .upsert(payload, { onConflict: "slug" });

  if (seriesUpsertError) {
    console.error(`   ❌ Series upsert failed for "${finalSlug}": ${seriesUpsertError.message}`);
    // Still return a ctx so caller can decide, but mark it as unsaved
    const ctx = { ...payload, url: seriesUrl, sourceSlug: rawSlug, isMovie: isActuallyMovie, _unsaved: true };
    throw new Error(`Series save failed: ${seriesUpsertError.message}`);
  }

  console.log(`   ✅ Series saved to DB: ${payload.title} [${finalSlug}]`);

  const ctx = {
    ...payload,
    url: seriesUrl,
    sourceSlug: rawSlug,
    isMovie: isActuallyMovie,
  };
  seriesCache.set(finalSlug, ctx);
  localSeriesCache[finalSlug] = { ...payload, isMovie: isActuallyMovie };
  saveCache(SERIES_CACHE_FILE, localSeriesCache);
  return ctx;
}

function extractSeasonNumbers(html) {
  const $ = cheerio.load(html);
  const seasons = new Set();
  $("[data-season], option[value]").each((_, el) => {
    const s = $(el).attr("data-season") || $(el).attr("value");
    if (s && !isNaN(s)) seasons.add(parseInt(s, 10));
  });
  if (seasons.size === 0) {
    $(".aa-cnt .se-c").each((_, el) => {
      const match = $(el)
        .find(".se-t")
        .text()
        .match(/season\s+(\d+)/i);
      if (match) seasons.add(parseInt(match[1], 10));
    });
  }
  if (seasons.size === 0) seasons.add(1);
  return Array.from(seasons).sort((a, b) => a - b);
}

async function ensureSeriesComplete(seriesCtx, triggeringEpisode = null) {
  try {
    const seriesUrl = seriesCtx.url || buildSeriesUrlFromSlug(seriesCtx.slug);
    console.log(`      🔍 Fetching series data: ${seriesUrl}`);

    const html = await fetchHtmlWithRetry(seriesUrl);
    let postId = extractPostId(html);
    const nonce = extractNonce(html);
    let seasons = extractSeasonNumbers(html);

    // Fallback: Extract episodes from HTML if API might fail or to have a backup
    let htmlEpisodeLinks = extractSeriesEpisodeLinks(html, seriesUrl);

    if ((!postId || htmlEpisodeLinks.length === 0) && triggeringEpisode?.url) {
      const triggerUrl = forceToEpisodeDomain(triggeringEpisode.url, seriesUrl);
      if (triggerUrl) {
        console.log(
          `      🔁 Enriching series metadata from trigger episode page: ${triggerUrl}`,
        );
        try {
          const triggerHtml = await fetchHtmlWithRetry(triggerUrl, CONFIG.maxRetries, {
            referer: seriesUrl,
          });

          const triggerPostId = extractPostId(triggerHtml);
          if (!postId && triggerPostId) {
            postId = triggerPostId;
            console.log(`      ✅ postId resolved from trigger page: ${postId}`);
          }

          const triggerSeasons = extractSeasonNumbers(triggerHtml);
          if (triggerSeasons.length > 0) {
            const combined = new Set([...(seasons || []), ...triggerSeasons]);
            seasons = Array.from(combined).sort((a, b) => a - b);
          }

          const triggerEpisodeLinks = extractSeriesEpisodeLinks(
            triggerHtml,
            seriesUrl,
          );
          if (triggerEpisodeLinks.length > 0) {
            const seen = new Set(
              htmlEpisodeLinks.map((ep) => `${ep.season}x${ep.episode}`),
            );
            for (const ep of triggerEpisodeLinks) {
              const key = `${ep.season}x${ep.episode}`;
              if (seen.has(key)) continue;
              seen.add(key);
              htmlEpisodeLinks.push(ep);
            }
          }
        } catch (err) {
          console.warn(
            `      ⚠️ Trigger-page metadata fallback failed: ${err.message}`,
          );
        }
      }
    }

    if (
      triggeringEpisode?.season &&
      !seasons.includes(Number(triggeringEpisode.season))
    ) {
      seasons.push(Number(triggeringEpisode.season));
      seasons.sort((a, b) => a - b);
    }

    if (!postId) {
      console.warn(
        `      ⚠️ postId not found for ${seriesCtx.slug}; season API may fail for all seasons`,
      );
    }

    const htmlUrlMap = new Map();
    htmlEpisodeLinks.forEach((ep) => {
      const key = `${ep.season}x${ep.episode}`;
      htmlUrlMap.set(key, ep);
    });

    console.log(
      `      🔍 Found ${seasons.length} season(s) for ${seriesCtx.title}`,
    );

    const allEpisodeLinks = [];
    for (const season of seasons) {
      console.log(`         • Fetching Season ${season}...`);
      const episodeData = await fetchEpisodeDataFromAPI(
        postId,
        season,
        nonce,
        seriesUrl,
        html,
      );

      if (episodeData.length === 0) {
        console.log(
          `         ⚠️ No episode data from API for Season ${season}. Trying HTML fallback...`,
        );
        // Fallback to HTML links for this specific season
        const seasonHtmlLinks = htmlEpisodeLinks.filter(
          (link) => link.season === season,
        );
        if (seasonHtmlLinks.length > 0) {
          console.log(
            `         ✓ Found ${seasonHtmlLinks.length} episodes via HTML fallback`,
          );
          seasonHtmlLinks.forEach((ep) =>
            allEpisodeLinks.push({
              ...ep,
              url:
                ep.url ||
                buildEpisodeUrl(seriesCtx.slug, ep.season, ep.episode),
            }),
          );
        } else {
          console.log(
            `         ❌ No episodes found for Season ${season} in HTML either.`,
          );
        }
      } else {
        console.log(`         ✓ Found ${episodeData.length} episodes via API`);
        episodeData.forEach((ep) =>
          allEpisodeLinks.push({
            ...ep,
            url:
              ep.url || buildEpisodeUrl(seriesCtx.slug, ep.season, ep.episode),
          }),
        );
      }
    }

    // Final fallback: if allEpisodeLinks is still empty but we have htmlEpisodeLinks, use them all
    if (allEpisodeLinks.length === 0 && htmlEpisodeLinks.length > 0) {
      console.log(
        `      ⚠️ API returned nothing for all seasons. Using ${htmlEpisodeLinks.length} episodes found in HTML.`,
      );
      htmlEpisodeLinks.forEach((ep) =>
        allEpisodeLinks.push({
          ...ep,
          url: ep.url || buildEpisodeUrl(seriesCtx.slug, ep.season, ep.episode),
        }),
      );
    }

    const { data: existingData } = await supabase
      .from("episodes")
      .select("season, episode")
      .eq("series_slug", seriesCtx.slug);
    const existingEpisodes = new Set();
    const supabaseSeasonMap = new Map();
    existingData?.forEach((ep) => {
      existingEpisodes.add(makeSeasonEpisodeKey(ep.season, ep.episode));
      supabaseSeasonMap.set(
        ep.season,
        (supabaseSeasonMap.get(ep.season) || 0) + 1,
      );
    });

    const toonstreamSeasonMap = new Map();
    allEpisodeLinks.forEach((ep) =>
      toonstreamSeasonMap.set(
        ep.season,
        (toonstreamSeasonMap.get(ep.season) || 0) + 1,
      ),
    );

    console.log(`\n      📊 SEASONAL COMPARISON:`);
    const seasonsToBackfill = new Set();
    for (const [season, tsCount] of toonstreamSeasonMap) {
      const sbCount = supabaseSeasonMap.get(season) || 0;
      console.log(`         • Season ${season}: TS=${tsCount}, DB=${sbCount}`);
      if (tsCount > sbCount) {
        console.log(
          `         🔄 BACKFILL NEEDED for Season ${season} (${tsCount - sbCount} missing)`,
        );
        seasonsToBackfill.add(season);
      }
    }

    let processCount = 0;
    let checkedCount = 0;
    let skippedCount = 0;
    let foundTriggerInSeries = false;
    for (const ep of allEpisodeLinks) {
      checkedCount++;
      const key = makeSeasonEpisodeKey(ep.season, ep.episode);
      const existsInDb = existingEpisodes.has(key);
      const isTriggering =
        triggeringEpisode &&
        ep.season === triggeringEpisode.season &&
        ep.episode === triggeringEpisode.episode;
      const shouldBackfill = seasonsToBackfill.has(ep.season);

      if (isTriggering) {
        foundTriggerInSeries = true;
      }

      if (isTriggering || !existsInDb) {
        processCount++;
        let reason = "missing-in-db";
        if (isTriggering && existsInDb) reason = "trigger-latest-refetch";
        else if (isTriggering && !existsInDb) reason = "trigger-new-episode";

        console.log(
          `      🔎 CHECK S${ep.season}E${ep.episode} -> SYNC (${reason})`,
        );

        if (isTriggering && existingEpisodes.has(key)) {
          console.log(
            `      🔄 Re-fetching latest episode: S${ep.season}E${ep.episode} (even though it exists)`,
          );
        } else {
          console.log(`      📺 Syncing: S${ep.season}E${ep.episode}`);
        }
        const syncUrl = isTriggering
          ? forceToEpisodeDomain(triggeringEpisode?.url || ep.url, seriesUrl)
          : ep.url;

        await syncEpisodeByUrl(syncUrl, {
          seriesUrl,
          seriesTitle: seriesCtx.title,
          seriesSlug: seriesCtx.slug,
          force: true,
          code: { season: ep.season, episode: ep.episode },
        });
      } else {
        skippedCount++;
        const skipReason = shouldBackfill
          ? "already-in-db-backfill-skip"
          : "already-in-db";
        console.log(
          `      🔎 CHECK S${ep.season}E${ep.episode} -> SKIP (${skipReason})`,
        );
      }
    }

    if (triggeringEpisode && !foundTriggerInSeries) {
      console.warn(
        `      ⚠️ Trigger episode S${triggeringEpisode.season}E${triggeringEpisode.episode} not found in fetched episode list`,
      );

      const fallbackTriggerUrl = forceToEpisodeDomain(
        triggeringEpisode.url,
        seriesUrl,
      );
      if (fallbackTriggerUrl) {
        console.log(
          `      🔁 Fallback trigger sync via URL: ${fallbackTriggerUrl}`,
        );
        await syncEpisodeByUrl(fallbackTriggerUrl, {
          seriesUrl,
          seriesTitle: seriesCtx.title,
          seriesSlug: seriesCtx.slug,
          force: true,
          code: {
            season: triggeringEpisode.season,
            episode: triggeringEpisode.episode,
          },
        });
        processCount++;
      }
    }

    console.log(
      `      📌 Episode check summary: checked=${checkedCount}, synced=${processCount}, skipped=${skippedCount}`,
    );

    if (processCount === 0) {
      console.log(
        `      ✅ All episodes already synced for ${seriesCtx.title}`,
      );
    }
  } catch (err) {
    console.error(`   ⚠️ ensureSeriesComplete failed: ${err.message}`);
  }
}

async function updateSeriesFromLatestEpisodes(latestSeriesMap) {
  if (latestSeriesMap.size === 0) {
    console.log(`\n   ℹ️  No series to update from latest episodes`);
    return;
  }

  console.log(
    `\n🔄 Smart sync: Checking ${latestSeriesMap.size} series with new episodes...`,
  );

  for (const [slug, triggeringEpisode] of latestSeriesMap) {
    try {
      const { data: seriesData } = await supabase
        .from("series")
        .select("title")
        .eq("slug", slug)
        .maybeSingle();
      const seriesTitle = seriesData?.title || slug;
      console.log(
        `\n   📺 Processing: ${seriesTitle} (triggered by S${triggeringEpisode.season}E${triggeringEpisode.episode})`,
      );
      if (triggeringEpisode?.sourceUrl || triggeringEpisode?.url) {
        console.log(
          `   🔗 Trigger episode URL: ${triggeringEpisode.sourceUrl || triggeringEpisode.url}`,
        );
      }
      if (
        triggeringEpisode?.sourceUrl &&
        triggeringEpisode?.url &&
        triggeringEpisode.sourceUrl !== triggeringEpisode.url
      ) {
        console.log(`   🔁 Trigger URL converted: ${triggeringEpisode.url}`);
      }

      await ensureSeriesComplete(
        { slug, title: seriesTitle, url: buildSeriesUrlFromSlug(slug) },
        triggeringEpisode,
      );
    } catch (err) {
      console.error(`   ❌ Failed to process ${slug}: ${err.message}`);
    }
  }
}

async function main() {
  console.log(`\n${"=".repeat(60)}`);
  console.log(`🚀 Toonstream -> Supabase sync started`);
  console.log(`📡 Fetching latest episodes from Toonstream...`);

  const html = await fetchHtmlWithRetry(CONFIG.homeUrl);
  const cards = extractEpisodeCards(html);
  console.log(`🔍 Found ${cards.length} candidate episodes`);

  const latestSeriesMap = new Map();
  for (const card of cards) {
    const code = parseEpisodeCode(card.url);
    if (!code) {
      console.log(`   ⏭️ Skipping invalid URL: ${card.url}`);
      stats.skippedEpisodes++;
      continue;
    }
    const sourceEpisodeUrl = normalizeUrl(card.url, CONFIG.homeUrl);
    const liveEpisodeUrl = forceToEpisodeDomain(sourceEpisodeUrl, CONFIG.homeUrl);
    const seriesUrl = deriveSeriesUrlFromEpisode(liveEpisodeUrl || card.url);
    const slug = extractSeriesSlugFromUrl(seriesUrl);
    if (slug) {
      if (!latestSeriesMap.has(slug)) {
        latestSeriesMap.set(slug, {
          season: code.season,
          episode: code.episode,
          sourceUrl: sourceEpisodeUrl,
          url: liveEpisodeUrl,
        });
        if (sourceEpisodeUrl && liveEpisodeUrl && sourceEpisodeUrl !== liveEpisodeUrl) {
          console.log(
            `   🔁 Trigger URL mapped: ${sourceEpisodeUrl} -> ${liveEpisodeUrl}`,
          );
        }
      }
    }
  }

  await updateSeriesFromLatestEpisodes(latestSeriesMap);

  console.log(`\n${"=".repeat(60)}`);
  console.log("📊 SYNC SUMMARY");
  console.log("=".repeat(60));
  console.log(`\n✅ Success:`);
  console.log(`   • New Episodes Added: ${stats.newEpisodes}`);
  console.log(`   • Failed Episodes: ${stats.failedEpisodes}`);
  console.log(`   • Skipped: ${stats.skippedEpisodes}`);
  console.log(`\n✅ Sync completed successfully`);
  console.log("=".repeat(60) + "\n");
}

export async function start() {
  await main();
}

export async function fetchFullSeries(seriesUrl, onProgress) {
  try {
    const log = (msg) => {
      console.log(msg);
      if (onProgress) onProgress(msg);
    };

    log(`🔍 Fetching series: ${seriesUrl}`);
    const html = await fetchHtmlWithRetry(seriesUrl);
    const postId = extractPostId(html);
    const nonce = extractNonce(html);
    const seasons = extractSeasonNumbers(html);
    const htmlEpisodeLinks = extractSeriesEpisodeLinks(html, seriesUrl);
    const meta = extractSeriesMeta(html, seriesUrl);
    const rawSlug = extractSeriesSlugFromUrl(seriesUrl);
    const seriesSlug = cleanSlug(rawSlug);

    log(`📺 Title: ${meta.title || seriesSlug}`);
    log(`🗂️ Seasons found: ${seasons.join(", ")}`);
    log(`🔗 Episodes in HTML: ${htmlEpisodeLinks.length}`);

    const allEpisodeLinks = [];
    for (const season of seasons) {
      log(`   ↳ Season ${season}: calling API...`);
      const apiEps = await fetchEpisodeDataFromAPI(
        postId,
        season,
        nonce,
        seriesUrl,
        html,
      );
      if (apiEps.length > 0) {
        log(`   ✓ Season ${season}: ${apiEps.length} episodes from API`);
        apiEps.forEach((ep) =>
          allEpisodeLinks.push({
            ...ep,
            url: ep.url || buildEpisodeUrl(seriesSlug, ep.season, ep.episode),
          }),
        );
      } else {
        const htmlSeason = htmlEpisodeLinks.filter((e) => e.season === season);
        log(
          `   ⚠️ Season ${season}: API empty, HTML fallback: ${htmlSeason.length} episodes`,
        );
        htmlSeason.forEach((ep) => allEpisodeLinks.push(ep));
      }
      await delay(300);
    }

    if (allEpisodeLinks.length === 0 && htmlEpisodeLinks.length > 0) {
      log(`⚠️ Using all HTML episodes as final fallback`);
      htmlEpisodeLinks.forEach((ep) => allEpisodeLinks.push(ep));
    }

    log(`\n📋 Total episodes to sync: ${allEpisodeLinks.length}`);

    // Resolve series context (creates/updates DB record)
    const seriesCtx = await resolveSeriesContext(seriesUrl, meta.title);
    log(`✅ Series saved: ${seriesCtx.title} (slug: ${seriesCtx.slug})`);

    let done = 0,
      failed = 0;
    for (const ep of allEpisodeLinks) {
      try {
        log(
          `   [${done + 1}/${allEpisodeLinks.length}] Syncing S${ep.season}E${ep.episode}...`,
        );
        await syncEpisodeByUrl(ep.url, {
          seriesUrl,
          seriesTitle: seriesCtx.title,
          seriesSlug: seriesCtx.slug,
          force: true,
          code: { season: ep.season, episode: ep.episode },
        });
        done++;
        log(`   ✅ S${ep.season}E${ep.episode} done`);
      } catch (err) {
        failed++;
        log(`   ❌ S${ep.season}E${ep.episode} failed: ${err.message}`);
      }
      await delay(400);
    }

    log(`\n🎉 Finished! ${done} synced, ${failed} failed`);
    return {
      success: true,
      total: allEpisodeLinks.length,
      done,
      failed,
      title: seriesCtx.title,
    };
  } catch (err) {
    console.error(`fetchFullSeries error: ${err.message}`);
    throw err;
  }
}

if (process.argv[1]?.includes("toonstream-supabase-sync.js")) {
  start().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}