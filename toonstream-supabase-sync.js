import "dotenv/config";
import axios from "axios";
import * as cheerio from "cheerio";
import { createClient } from "@supabase/supabase-js";
import ProxyManager from "./proxy-manager.js";
import fs from "fs";
import path from "path";

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
  process.env.SUPL,
  process.env.SUPABASE_SERVICE,
);

const CONFIG = {
  homeUrl: process.env.TOONSTREAM_HOME_URL || "https://toonstream.com/home/",
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

function isToonstreamUrl(url) {
  if (!url || !TOONSTREAM_HOST) return false;
  try {
    const hostname = new URL(url).hostname.replace(/^www\./, "");
    return hostname === TOONSTREAM_HOST;
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
    return `${TOONSTREAM_ORIGIN}/series/${normalizedSlug}/`;
  } catch {
    return null;
  }
}

function buildSeriesUrlFromSlug(seriesSlug) {
  if (!seriesSlug) return null;
  const urlSlug =
    seriesSlug === "naruto-shippden" ? "naruto-shippuden" : seriesSlug;
  return `${TOONSTREAM_ORIGIN}/series/${urlSlug}/`;
}

function buildEpisodeUrl(seriesSlug, season, episode) {
  if (!seriesSlug) return null;
  const urlSlug =
    seriesSlug === "naruto-shippden" ? "naruto-shippuden" : seriesSlug;
  return `${TOONSTREAM_ORIGIN}/episode/${urlSlug}-${season}x${episode}/`;
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
    headers.Origin = TOONSTREAM_ORIGIN;
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
  try {
    const url = `${TMDB_BASE_URL}/tv/${tmdbId}/season/${seasonNum}/episode/${episodeNum}?api_key=${apiKey}&language=en-US`;
    const response = await axios.get(url, { timeout: 10000 });
    const data = response.data;
    if (data && data.still_path) return `${TMDB_IMAGE_BASE}${data.still_path}`;
    return null;
  } catch (err) {
    return null;
  }
}

async function fetchTMDBSeasonEpisodes(tmdbId, seasonNum) {
  const apiKey = process.env.TMDB_API_KEY;
  if (!apiKey || !tmdbId) return {};
  try {
    const url = `${TMDB_BASE_URL}/tv/${tmdbId}/season/${seasonNum}?api_key=${apiKey}&language=en-US`;
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
    return {};
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
    const match = slug.match(/-(\d+)x(\d+)\/?$/);
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
  // First: try to extract ajax_url from the page's inline JS (most reliable)
  if (pageHtml) {
    const m =
      pageHtml.match(/['""]?ajax_url['""]?\s*[:=]\s*['""]([^'""]+)['""]/) ||
      pageHtml.match(/admin-ajax\.php['""]?/i);
    if (m && m[1] && m[1].startsWith("http")) {
      return m[1].trim();
    }
    // Direct regex for full URL
    const fullMatch = pageHtml.match(
      /(https?:\/\/[^/]+\/(?:home\/)?wp-admin\/admin-ajax\.php)/i,
    );
    if (fullMatch) return fullMatch[1];
  }
  // Fallback: use page URL's domain with /home/ prefix
  try {
    const u = new URL(pageUrl || CONFIG.homeUrl);
    return `${u.origin}/home/wp-admin/admin-ajax.php`;
  } catch {
    return CONFIG.ajaxUrl;
  }
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
    `         🌐 Season ${season} API: POST ${ajaxUrl} [post=${postId}]`,
  );

  try {
    const params = new URLSearchParams();
    params.append("action", "action_select_season");
    params.append("season", season);
    params.append("post", postId);
    if (nonce) params.append("nonce", nonce);

    const res = await axios.post(ajaxUrl, params, {
      headers: {
        "Content-Type": "application/x-www-form-urlencoded",
        Referer: referer,
        "X-Requested-With": "XMLHttpRequest",
        "User-Agent": getUA(),
      },
      timeout: 15000,
    });

    const responseHtml =
      typeof res.data === "string"
        ? res.data
        : res.data?.html || JSON.stringify(res.data);
    const $ = cheerio.load(responseHtml);
    const episodes = [];

    // Episode articles from season API response
    // URL may contain /home/ prefix e.g. toonstream.dad/home/episode/slug-2x1/
    $("article").each((_, el) => {
      const art = $(el);
      const anchor = art.find('a[href*="/episode/"]').first();
      const href = anchor.attr("href");
      if (!href) return;

      // Parse season/episode from URL (handles /home/ prefix too)
      const code = parseEpisodeCode(href);
      let epNum = code?.episode;
      let snNum = code?.season || parseInt(season);

      // Also try num-epi span like "2x1"
      if (!epNum) {
        const numText = art.find(".num-epi").text().trim(); // e.g. "2x1"
        const m = numText.match(/(\d+)x(\d+)/i);
        if (m) {
          snNum = parseInt(m[1]);
          epNum = parseInt(m[2]);
        }
      }

      if (epNum) {
        episodes.push({
          season: snNum,
          episode: epNum,
          url: href,
          title: art.find(".entry-title, .title").text().trim(),
        });
      }
    });
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
    const pageOrigin = (() => {
      try {
        return new URL(episodeUrl).origin;
      } catch {
        return TOONSTREAM_ORIGIN;
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
      const url = m[1].replace(/&#038;/g, "&").replace(/&amp;/g, "&");
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
        const url = `${pageOrigin}/home/?trembed=${idx}&trid=${trid}&trtype=${trtype}`;
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
    const newRandomKey = Math.random().toString(36).substring(2, 15) +
      Math.random().toString(36).substring(2, 15);
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
  if (seriesCtx.tmdb_id && process.env.TMDB_API_KEY) {
    const cacheKey = `${seriesCtx.tmdb_id}-${code.season}`;
    if (!tmdbEpisodeImageCache.has(cacheKey)) {
      const seasonImages = await fetchTMDBSeasonEpisodes(
        seriesCtx.tmdb_id,
        code.season,
      );
      tmdbEpisodeImageCache.set(cacheKey, seasonImages);
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
  }
  if (!tmdbEpisodeImage) {
    const tvdbId = await searchTVDBSeries(seriesCtx.title);
    if (tvdbId)
      tmdbEpisodeImage = await fetchTVDBEpisodeImage(
        tvdbId,
        code.season,
        code.episode,
      );
  }
  const bestImage = tmdbEpisodeImage || seriesCtx.tmdb_poster || null;
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
    if (!cached.isMovie) return cached; // Only use if it's confirmed a series
    if (isMovieUrl) return cached;      // URL is movie — use movie cache
    // Was cached as movie but URL is series — clear cache and re-resolve
    seriesCache.delete(finalSlug);
  }

  if (localSeriesCache[finalSlug] && !localSeriesCache[finalSlug].isMovie) {
    const cached = localSeriesCache[finalSlug];
    const ctx = {
      ...cached,
      url: seriesUrl,
      sourceSlug: rawSlug,
      isMovie: false,
    };
    seriesCache.set(finalSlug, ctx);
    return ctx;
  }

  // Check series table first
  let { data: seriesData } = await supabase
    .from("series")
    .select("*")
    .eq("slug", finalSlug)
    .maybeSingle();

  if (seriesData) {
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
    payload.random_key = Math.random().toString(36).substring(2, 15);
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
    const postId = extractPostId(html);
    const nonce = extractNonce(html);
    const seasons = extractSeasonNumbers(html);

    // Fallback: Extract episodes from HTML if API might fail or to have a backup
    const htmlEpisodeLinks = extractSeriesEpisodeLinks(html, seriesUrl);
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
    for (const ep of allEpisodeLinks) {
      const key = makeSeasonEpisodeKey(ep.season, ep.episode);
      const isTriggering =
        triggeringEpisode &&
        ep.season === triggeringEpisode.season &&
        ep.episode === triggeringEpisode.episode;
      const shouldBackfill = seasonsToBackfill.has(ep.season);

      if (!existingEpisodes.has(key) || isTriggering || shouldBackfill) {
        processCount++;
        if (isTriggering && existingEpisodes.has(key)) {
          console.log(
            `      🔄 Re-fetching latest episode: S${ep.season}E${ep.episode} (even though it exists)`,
          );
        } else {
          console.log(`      📺 Syncing: S${ep.season}E${ep.episode}`);
        }
        await syncEpisodeByUrl(ep.url, {
          seriesUrl,
          seriesTitle: seriesCtx.title,
          seriesSlug: seriesCtx.slug,
          force: true,
          code: { season: ep.season, episode: ep.episode },
        });
      }
    }

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
    const seriesUrl = deriveSeriesUrlFromEpisode(card.url);
    const slug = extractSeriesSlugFromUrl(seriesUrl);
    if (slug) {
      if (!latestSeriesMap.has(slug)) {
        latestSeriesMap.set(slug, code);
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
