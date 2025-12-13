import "dotenv/config";
import axios from "axios";
import * as cheerio from "cheerio";
import { createClient } from "@supabase/supabase-js";
import ProxyManager from "./proxy-manager.js";

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
  homeUrl: process.env.TOONSTREAM_HOME_URL || "https://toonstream.one/home/",
  pollIntervalMs: Number(process.env.POLL_INTERVAL_MS || 60_000),
  requestTimeout: 30_000,
  maxRetries: 3,
  maxParallelSeriesFetch: Number(process.env.MAX_PARALLEL_SERIES || 4),
  embedMaxDepth: Number(process.env.EMBED_MAX_DEPTH || 3),
  toonstreamCookies: process.env.TOONSTREAM_COOKIES?.trim() || null,
  ajaxUrl:
    process.env.TOONSTREAM_AJAX_URL ||
    "https://toonstream.one/wp-admin/admin-ajax.php",
};

const defaultFallbacks = [
  `${CONFIG.homeUrl}home/`,
  `${CONFIG.homeUrl}page/1/`,
];

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

const seriesCache = new Map();
const processedEpisodes = new Set();
const proxyManager = new ProxyManager();

// Track which series are completely synced
const completedSeries = new Set();

// Statistics tracking
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
  return (name || "item")
    .toLowerCase()
    .replace(/['"]/g, "")
    .replace(/[^\w\s-]/g, "")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-+|-+$/g, "");
}

function decodeHtmlEntities(str) {
  if (!str) return str;
  return str
    .replace(/&#038;/g, '&')
    .replace(/&#38;/g, '&')
    .replace(/&amp;/g, '&')
    .replace(/&#039;/g, "'")
    .replace(/&#39;/g, "'")
    .replace(/&apos;/g, "'")
    .replace(/&#034;/g, '"')
    .replace(/&#34;/g, '"')
    .replace(/&quot;/g, '"')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>');
}

function normalizeUrl(rawUrl, base = CONFIG.homeUrl) {
  if (!rawUrl || /^javascript:/i.test(rawUrl)) return null;
  try {
    // Decode HTML entities before parsing URL
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
    return parts.pop() || null;
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
    return `${CONFIG.homeUrl}series/${baseSlug}/`;
  } catch {
    return null;
  }
}

function buildSeriesUrlFromSlug(seriesSlug) {
  if (!seriesSlug) return null;
  return `${CONFIG.homeUrl}series/${seriesSlug}/`;
}

function buildEpisodeUrl(seriesSlug, season, episode) {
  if (!seriesSlug) return null;
  return `${CONFIG.homeUrl}episode/${seriesSlug}-${season}x${episode}/`;
}

function buildRequestHeaders(url, options = {}) {
  const headers = {
    "User-Agent": getUA(),
    Accept:
      "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    Pragma: "no-cache",
    "Upgrade-Insecure-Requests": "1",
  };

  if (options.referer) {
    headers.Referer = options.referer;
  }

  if (options.headers) {
    Object.assign(headers, options.headers);
  }

  if (isToonstreamUrl(url)) {
    if (!headers.Referer) {
      headers.Referer = CONFIG.homeUrl;
    }

    headers.Origin = TOONSTREAM_ORIGIN;
    headers["Sec-Fetch-Dest"] = "document";
    headers["Sec-Fetch-Mode"] = "navigate";
    headers["Sec-Fetch-Site"] = "same-origin";
    headers["Sec-Fetch-User"] = "?1";

    if (CONFIG.toonstreamCookies) {
      headers.Cookie = CONFIG.toonstreamCookies;
    }
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
      // Get proxy for this attempt
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
      
      // Add proxy agent if available
      if (proxyAgent) {
        config.httpAgent = proxyAgent;
        config.httpsAgent = proxyAgent;
      }
      
      const res = await axios.get(url, config);
      return String(res.data || "");
    } catch (err) {
      lastErr = err;
      
      // Mark proxy as failed if we're using one
      if (
        currentProxy &&
        (err.code === "ECONNREFUSED" || err.code === "ETIMEDOUT")
      ) {
        proxyManager.markProxyAsFailed(currentProxy);
      }

      const status = err.response?.status;
      if (status) {
        console.warn(
          `  ⚠️ Request failed (${status}) for ${url} (attempt ${attempt}/${retries})`,
        );
      }
      
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
  
  // Handle special anime naming patterns
  // ranma1-2 or ranma-1-2 -> ranma 1/2
  cleaned = cleaned.replace(/(\w+)1[-\/]2/gi, '$1 1/2');
  cleaned = cleaned.replace(/(\w+)-1[-\/]2/gi, '$1 1/2');
  
  // Convert dashes to spaces for slug-like titles (spy-x-family -> spy x family)
  // But preserve special patterns like "1/2"
  cleaned = cleaned.replace(/-/g, ' ');
  
  // Remove season/episode info
  cleaned = cleaned.replace(/[:\-–—]+\s*Season\s*\d+/gi, '');
  cleaned = cleaned.replace(/\s*Season\s*\d+/gi, '');
  cleaned = cleaned.replace(/\s*S\d+E?\d*/gi, '');
  cleaned = cleaned.replace(/\s*\d+x\d+/gi, '');
  
  // Remove language/quality tags
  cleaned = cleaned.replace(/\s*(Dub|Sub|Dubbed|Subbed|English|Japanese|Hindi|Hindi Dub)\s*/gi, ' ');
  cleaned = cleaned.replace(/\s*(1080p|720p|480p|HD|4K)\s*/gi, ' ');
  
  // Remove brackets and their contents
  cleaned = cleaned.replace(/\([^)]*\)/g, '');
  cleaned = cleaned.replace(/\[[^\]]*\]/g, '');
  
  // Clean up multiple spaces
  cleaned = cleaned.replace(/\s+/g, ' ').trim();
  
  return cleaned;
}

function extractSeriesNameFromSlug(slug) {
  if (!slug) return null;
  
  // Remove episode code pattern (e.g., -3x1, -1x5)
  let name = slug.replace(/-\d+x\d+$/i, '');
  
  // Handle special patterns
  // ranma1-2 -> ranma 1/2
  name = name.replace(/(\w+)1-2$/i, '$1 1/2');
  
  // Convert dashes to spaces
  name = name.replace(/-/g, ' ');
  
  // Capitalize words
  name = name.replace(/\b\w/g, c => c.toUpperCase());
  
  return name.trim();
}

async function searchTMDB(title, type = "tv") {
  const apiKey = process.env.TMDB_API_KEY;
  if (!apiKey) {
    console.log(`   ⚠️ TMDB API key not found in environment`);
    return null;
  }
  
  // Check if API key looks valid (should be 32 hex chars)
  if (apiKey.length < 20) {
    console.log(`   ⚠️ TMDB API key appears invalid (too short)`);
    return null;
  }
  
  const cleanedTitle = cleanTitleForTMDB(title);
  console.log(`   🔎 TMDB: Cleaned title "${title}" -> "${cleanedTitle}"`);
  
  // Try multiple search strategies
  const searchQueries = [cleanedTitle];
  
  // Add variations if the cleaned title is different
  if (cleanedTitle !== title) {
    searchQueries.push(title);
  }
  
  // For anime, try removing common suffixes
  const withoutSuffix = cleanedTitle
    .replace(/\s*(the animation|the series|movie|ova|special)$/i, '')
    .trim();
  if (withoutSuffix && withoutSuffix !== cleanedTitle) {
    searchQueries.push(withoutSuffix);
  }
  
  for (const query of searchQueries) {
    const url = `${TMDB_BASE_URL}/search/${type}?api_key=${apiKey}&query=${encodeURIComponent(query)}&language=en-US`;
    
    try {
      const res = await fetch(url);
      if (!res.ok) {
        console.log(`   ⚠️ TMDB API error: ${res.status} ${res.statusText}`);
        if (res.status === 401) {
          console.log(`   ⚠️ TMDB API key is invalid or expired. Please update TMDB_API_KEY.`);
        }
        return null;
      }
      const json = await res.json();
      
      if (json.results && json.results.length > 0) {
        console.log(`   ✓ TMDB: Found ${json.results.length} results for "${query}", using: "${json.results[0].name || json.results[0].title}"`);
        return json.results[0].id;
      }
      
      console.log(`   ℹ️ TMDB: No results for "${query}"`);
    } catch (err) {
      console.log(`   ⚠️ TMDB search error: ${err.message}`);
    }
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
  console.log(`   🎬 TMDB: Searching for "${title}" (${type})...`);
  const tmdbId = await searchTMDB(title, type);
  if (!tmdbId) {
    console.log(`   ⚠️ TMDB: No results found for "${title}"`);
    return null;
  }
  console.log(`   ✓ TMDB: Found ID ${tmdbId} for "${title}"`);
  const details = await fetchTMDBDetails(tmdbId, type);
  if (details) {
    console.log(`   ✓ TMDB: Fetched details - Rating: ${details.rating}, Genres: ${details.genres?.slice(0, 3).join(', ')}`);
  }
  return details;
}

function extractEpisodeCards(html) {
  const $ = cheerio.load(html);
  const cards = [];
  const seen = new Set();
  
  $('article.episodes, article.post').each((_, el) => {
    const article = $(el);
    const anchor = article.find('a[href*="/episode/"], a.lnk-blk[href*="/episode/"]').first();
    if (!anchor.length) return;
    
    const url = normalizeUrl(anchor.attr("href"));
    if (!url || seen.has(url)) return;
    seen.add(url);
    
    const titleEl = article.find('.entry-title, h2').first();
    const title = titleEl.text().trim() || anchor.attr("title") || "";
    
    const img = article.find('figure img, .post-thumbnail img, img').first();
    let thumb = null;
    if (img.length) {
      thumb = img.attr("data-src") || img.attr("src") || img.attr("data-lazy-src");
      if (thumb && thumb.startsWith('//')) {
        thumb = 'https:' + thumb;
      } else if (thumb) {
        thumb = normalizeUrl(thumb);
      }
    }
    
    cards.push({ url, title, thumb });
  });
  
  $('a[href*="/episode/"], a[href*="/watch/"]').each((_, el) => {
    const anchor = $(el);
    const url = normalizeUrl(anchor.attr("href"));
    if (!url || seen.has(url)) return;
    seen.add(url);
    const title = (anchor.attr("title") || anchor.text()).trim();
    const img = anchor.find("img").first();
    let thumb = null;
    if (img.length) {
      thumb = img.attr("data-src") || img.attr("src");
      if (thumb && thumb.startsWith('//')) {
        thumb = 'https:' + thumb;
      } else if (thumb) {
        thumb = normalizeUrl(thumb);
      }
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
    if (!code) return;
    const key = `${code.season}x${code.episode}`;
    if (seen.has(key)) return;
    seen.add(key);

    const text = node.text().trim();
    const img = node.find("img").first();
    const thumb = img.length
      ? normalizeUrl(img.attr("data-src") || img.attr("src"), seriesUrl)
      : null;

    links.push({
      url,
      season: code.season,
      episode: code.episode,
      title: text,
      thumb,
    });
  };

  $('a[href*="/episode/"]').each((_, el) => addLink(el));
  return links.sort((a, b) => {
    if (a.season === b.season) return a.episode - b.episode;
    return a.season - b.season;
  });
}

function parseEpisodeCode(url) {
  const match = url.match(/(\d+)x(\d+)/i);
  if (!match) return null;
  return {
    season: parseInt(match[1], 10),
    episode: parseInt(match[2], 10),
  };
}

async function extractRealVideoUrl(intermediateUrl, options = {}) {
  const visited = new Set();
  const MAX_DEPTH = CONFIG.embedMaxDepth + 2; // Increase depth for better resolution

  const needsFollow = (url) => {
    if (!url) return false;
    if (url.includes("trembed")) return true;
    if (url.includes("trid=")) return true;
    if (url.includes("trtype=")) return true;
    if (isToonstreamUrl(url)) return true;
    // Follow embed/player URLs
    if (url.includes("/embed/") || url.includes("/player/") || url.includes("/e/")) return true;
    return false;
  };

  const isVideoUrl = (url) => {
    if (!url) return false;
    // Check for common video file extensions
    const videoExtensions = ['.mp4', '.m3u8', '.webm', '.mkv', '.avi', '.mov', '.flv'];
    const lowerUrl = url.toLowerCase();
    for (const ext of videoExtensions) {
      if (lowerUrl.includes(ext)) return true;
    }
    // Check for common video streaming patterns
    if (url.includes('/video/') || url.includes('/stream/') || url.includes('/hls/')) return true;
    if (url.includes('googlevideo.com') || url.includes('googleusercontent.com')) return true;
    if (url.includes('streamtape') || url.includes('filemoon') || url.includes('voe.sx')) return true;
    if (url.includes('dood') || url.includes('mixdrop') || url.includes('streamlare')) return true;
    return false;
  };

  const resolve = async (url, depth = 0) => {
    if (!url) return null;
    if (visited.has(url)) return url;
    if (depth > MAX_DEPTH) return url;

    visited.add(url);
    console.log(`   🔍 [Depth ${depth}] Resolving: ${url.substring(0, 80)}...`);

    let html;
    try {
      html = await fetchHtmlWithRetry(url, CONFIG.maxRetries, {
        referer: options.referer || options.parent || CONFIG.homeUrl,
      });
    } catch (err) {
      console.warn(
        `  ⚠️ Failed to load embed ${url} (depth ${depth}): ${err.message}`,
      );
      // Return null if we can't fetch the URL - don't return the failed URL
      return null;
    }

    const $ = cheerio.load(html);

    const pickDirectVideo = () => {
      const videoTags = $("video, source");
      for (let i = 0; i < videoTags.length; i++) {
        const node = $(videoTags[i]);
        const src = node.attr("src") || node.attr("data-src");
        const normalized = normalizeUrl(src, url);
        if (normalized && !normalized.startsWith("blob:")) {
          console.log(`      ✓ Found video source: ${normalized.substring(0, 60)}...`);
          return normalized;
        }
      }
      return null;
    };

    const pickIframe = () => {
      const iframes = $("iframe");
      const iframeUrls = [];
      for (let i = 0; i < iframes.length; i++) {
        const iframe = $(iframes[i]);
        const raw =
          iframe.attr("src") ||
          iframe.attr("data-src") ||
          iframe.attr("data-lazy-src");
        const normalized = normalizeUrl(raw, url);
        if (normalized && normalized !== url && !visited.has(normalized)) {
          iframeUrls.push(normalized);
        }
      }
      // Return the first iframe that is not the current URL
      return iframeUrls[0] || null;
    };

    const pickFromScripts = () => {
      const scripts = $("script").toArray();
      const allCandidates = [];
      
      // More comprehensive patterns for video URLs
      const patterns = [
        // Direct video source patterns
        /["']?(?:src|file|source|url|video_url|stream_url)["']?\s*[:=]\s*["']([^"']+\.(?:mp4|m3u8|webm)[^"']*)/gi,
        // Embed/iframe patterns
        /(?:iframe|embed|player).*?src=["']([^"']+)["']/gi,
        // JSON-style patterns
        /"(?:url|file|src|source)":\s*"([^"]+)"/gi,
        /'(?:url|file|src|source)':\s*'([^']+)'/gi,
        // Player setup patterns
        /(?:player|jwplayer|videojs).*?["']?(?:file|src|source)["']?\s*[:=]\s*["']([^"']+)/gi,
        // Direct URL patterns (for .mp4, .m3u8, etc.)
        /https?:\/\/[^\s"'<>\]]+\.(?:mp4|m3u8|webm)/gi,
        // Generic HTTPS URLs in scripts
        /https?:\/\/[^\s"'<>\]]+/gi,
      ];

      for (const script of scripts) {
        const content = $(script).html() || "";
        for (const pattern of patterns) {
          pattern.lastIndex = 0; // Reset regex state
          const matches = content.matchAll(pattern);
          for (const match of matches) {
            const candidate = match[1] || match[0];
            if (candidate) {
              const normalized = normalizeUrl(candidate, url);
              if (normalized && normalized !== url && !visited.has(normalized)) {
                allCandidates.push(normalized);
              }
            }
          }
        }
      }

      // Prioritize video URLs
      const videoUrl = allCandidates.find(u => isVideoUrl(u));
      if (videoUrl) {
        console.log(`      ✓ Found video in script: ${videoUrl.substring(0, 60)}...`);
        return videoUrl;
      }

      // Otherwise return first embed/iframe URL for further processing
      const embedUrl = allCandidates.find(u => needsFollow(u) || u.includes('/embed/') || u.includes('/player/'));
      if (embedUrl) {
        return embedUrl;
      }

      return allCandidates[0] || null;
    };

    // Check for direct video first
    const directVideo = pickDirectVideo();
    if (directVideo && isVideoUrl(directVideo)) {
      return directVideo;
    }

    // Check iframes FIRST - prefer iframe over script URLs
    const iframeUrl = pickIframe();
    
    // If we have an iframe URL, follow it (this is likely the real video player)
    if (iframeUrl && depth < MAX_DEPTH) {
      const iframeNotToonstream = !iframeUrl.includes('toonstream.one') && 
                                  !iframeUrl.includes('trembed') && 
                                  !iframeUrl.includes('trid=') &&
                                  !iframeUrl.includes('trtype=');
      
      // Check if iframe points to a known video player domain
      const isVideoPlayer = iframeUrl.includes('play.') || 
                           iframeUrl.includes('player.') ||
                           iframeUrl.includes('/video/') ||
                           iframeUrl.includes('/embed/') ||
                           iframeUrl.includes('/e/') ||
                           iframeUrl.includes('/t/') ||
                           iframeUrl.includes('zephyrflick') ||
                           iframeUrl.includes('filemoon') ||
                           iframeUrl.includes('streamtape') ||
                           iframeUrl.includes('dood') ||
                           iframeUrl.includes('voe.sx') ||
                           iframeUrl.includes('mixdrop') ||
                           iframeUrl.includes('emturbovid') ||
                           iframeUrl.includes('turbovid') ||
                           iframeUrl.includes('vidmoly') ||
                           iframeUrl.includes('streamwish') ||
                           iframeUrl.includes('vidhide') ||
                           iframeUrl.includes('vidguard') ||
                           iframeUrl.includes('vidsrc') ||
                           iframeUrl.includes('embedsito') ||
                           iframeUrl.includes('upstream') ||
                           iframeUrl.includes('mp4upload') ||
                           iframeUrl.includes('okru') ||
                           iframeUrl.includes('sbplay') ||
                           iframeUrl.includes('streamsb') ||
                           iframeUrl.includes('vidcloud') ||
                           iframeUrl.includes('goload') ||
                           iframeUrl.includes('gogo');
      
      // If iframe URL is from external domain (not toonstream), return it directly
      if (iframeNotToonstream && (isVideoPlayer || isVideoUrl(iframeUrl))) {
        console.log(`      ✓ Found video player iframe: ${iframeUrl.substring(0, 60)}...`);
        return iframeUrl;
      }
      
      // If external domain but not recognized player, still return it (it's the real embed URL)
      if (iframeNotToonstream && iframeUrl.startsWith('http')) {
        console.log(`      ✓ Found external iframe: ${iframeUrl.substring(0, 60)}...`);
        return iframeUrl;
      }
      
      // Follow the iframe to find the actual video (only if it's still a toonstream URL)
      if (needsFollow(iframeUrl)) {
        return resolve(iframeUrl, depth + 1);
      }
    }

    // Check scripts for video URLs (only if no good iframe found)
    const scriptUrl = pickFromScripts();
    
    // Only use script URL if it's an actual video URL or needs to be followed
    if (scriptUrl) {
      if (isVideoUrl(scriptUrl)) {
        return scriptUrl;
      }
      // Don't follow random script URLs - they're often ads/trackers
      // Only follow if it looks like a video embed
      if (needsFollow(scriptUrl) && depth < MAX_DEPTH) {
        return resolve(scriptUrl, depth + 1);
      }
    }

    // If we have an iframe, follow it even if not a known player
    if (iframeUrl && depth < MAX_DEPTH) {
      return resolve(iframeUrl, depth + 1);
    }
    
    // Fallback to direct video or original URL
    return directVideo || url;
  };

  const result = await resolve(intermediateUrl, 0);
  console.log(`   📹 Final resolved URL: ${result ? result.substring(0, 60) + '...' : 'none'}`);
  return result;
}

function extractPostId(html) {
  const $ = cheerio.load(html);
  
  // Look for post ID in common WordPress locations
  const postIdPatterns = [
    // From data attributes
    () => $('[data-post], [data-post-id]').first().attr('data-post') || $('[data-post], [data-post-id]').first().attr('data-post-id'),
    // From input fields
    () => $('input[name="post"], input[name="post_id"]').first().val(),
    // From body class
    () => {
      const bodyClass = $('body').attr('class') || '';
      const match = bodyClass.match(/postid-(\d+)/);
      return match ? match[1] : null;
    },
    // From article tag
    () => $('article[id^="post-"]').first().attr('id')?.replace('post-', ''),
    // From script tags
    () => {
      const scripts = $('script').toArray();
      for (const script of scripts) {
        const content = $(script).html() || '';
        const patterns = [
          /"post_id"\s*:\s*"?(\d+)"?/,
          /'post_id'\s*:\s*'?(\d+)'?/,
          /post[_-]?id\s*=\s*['"]?(\d+)['"]?/i,
        ];
        
        for (const pattern of patterns) {
          const match = content.match(pattern);
          if (match && match[1]) {
            return match[1];
          }
        }
      }
      return null;
    },
  ];
  
  for (const extractFn of postIdPatterns) {
    const postId = extractFn();
    if (postId) {
      return postId;
    }
  }
  
  return null;
}

function extractNonce(html) {
  const $ = cheerio.load(html);
  
  const noncePatterns = [
    () => $('input[name="_wpnonce"]').first().val(),
    () => $('input[name="nonce"]').first().val(),
    () => $('[data-nonce]').first().attr('data-nonce'),
    () => {
      const scripts = $('script').toArray();
      for (const script of scripts) {
        const content = $(script).html() || '';
        const patterns = [
          /["']nonce["']\s*:\s*["']([A-Za-z0-9_-]+)["']/,
          /["']_wpnonce["']\s*:\s*["']([A-Za-z0-9_-]+)["']/,
          /nonce\s*=\s*["']([A-Za-z0-9_-]+)["']/,
          /ajax_nonce\s*[=:]\s*["']([A-Za-z0-9_-]+)["']/,
          /security\s*[=:]\s*["']([A-Za-z0-9_-]+)["']/,
          /dooplay\s*=\s*\{[^}]*nonce\s*:\s*["']([A-Za-z0-9_-]+)["']/,
          /var\s+\w+\s*=\s*\{[^}]*["']nonce["']\s*:\s*["']([A-Za-z0-9_-]+)["']/,
        ];
        
        for (const pattern of patterns) {
          const match = content.match(pattern);
          if (match && match[1]) {
            return match[1];
          }
        }
      }
      return null;
    },
  ];
  
  for (const extractFn of noncePatterns) {
    const nonce = extractFn();
    if (nonce) {
      return nonce;
    }
  }
  
  return null;
}

async function fetchEpisodeDataFromAPI(postId, season, nonce = null) {
  if (!postId || !season) return null;
  
  try {
    const url = CONFIG.ajaxUrl;
    const params = new URLSearchParams({
      action: 'action_select_season',
      season: season.toString(),
      post: postId,
    });
    
    if (nonce) {
      params.append('nonce', nonce);
      params.append('_wpnonce', nonce);
    }
    
    const response = await axios.post(url, params, {
      timeout: CONFIG.requestTimeout,
      headers: {
        "User-Agent": getUA(),
        "Content-Type": "application/x-www-form-urlencoded",
        Referer: CONFIG.homeUrl,
        Accept:
          "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "X-Requested-With": "XMLHttpRequest",
        ...(CONFIG.toonstreamCookies
          ? { Cookie: CONFIG.toonstreamCookies }
          : {}),
      },
    });
    
    const html = String(response.data || '');
    if (!html) return null;
    
    const $ = cheerio.load(html);
    const episodes = [];
    
    // Parse episode list items
    $('li').each((_, el) => {
      const $li = $(el);
      const $article = $li.find('article.post, article.episodes');
      
      if ($article.length === 0) return;
      
      // Extract episode image
      const $img = $article.find('img, .post-thumbnail img');
      let imageUrl = null;
      if ($img.length > 0) {
        imageUrl = $img.attr('data-src') || $img.attr('src') || $img.attr('data-lazy-src');
        // Fix protocol-relative URLs
        if (imageUrl && imageUrl.startsWith('//')) {
          imageUrl = 'https:' + imageUrl;
        } else if (imageUrl) {
          imageUrl = normalizeUrl(imageUrl);
        }
      }
      
      // Extract episode URL
      const $link = $article.find('a.lnk-blk, a[href*="/episode/"]');
      const episodeUrl = normalizeUrl($link.attr('href'));
      
      // Extract episode code (e.g., "1x1")
      const episodeCode = $article.find('.num-epi, .entry-header span').first().text().trim();
      const codeMatch = episodeCode.match(/(\d+)x(\d+)/);
      
      // Extract title
      const title = $article.find('.entry-title, h2').first().text().trim();
      
      if (episodeUrl && codeMatch) {
        episodes.push({
          url: episodeUrl,
          image: imageUrl,
          season: parseInt(codeMatch[1], 10),
          episode: parseInt(codeMatch[2], 10),
          title: title,
          code: episodeCode,
        });
      }
    });
    
    return episodes;
  } catch (err) {
    console.warn(`  ⚠️ Failed to fetch episode data from API: ${err.message}`);
    return null;
  }
}

async function extractEmbeds(html, episodeUrl) {
  const $ = cheerio.load(html);
  const serverOptions = [];
  const seen = new Set();
  
  // First, look for player options (dooplay_player_option) to build trembed URLs
  $('li.dooplay_player_option').each((_, el) => {
    const $el = $(el);
    const type = $el.attr('data-type');
    const post = $el.attr('data-post');
    const nume = $el.attr('data-nume');
    
    if (post && nume) {
      const trembedUrl = `https://toonstream.one/?trembed=${nume}&trid=${post}&trtype=${type || '2'}`;
      if (!seen.has(trembedUrl)) {
        seen.add(trembedUrl);
        serverOptions.push({
          name: `Server ${parseInt(nume) + 1}`,
          trembedUrl,
          option: parseInt(nume) + 1,
        });
      }
    }
  });
  
  // Also look for direct iframes that might already be external URLs
  $("iframe").each((_, el) => {
    const src =
      $(el).attr("src") ||
      $(el).attr("data-src") ||
      $(el).attr("data-lazy-src");
    const url = normalizeUrl(src);
    if (url && !seen.has(url)) {
      seen.add(url);
      // Check if this is already an external video URL (not toonstream)
      const isExternal = !url.includes('toonstream.one') && 
                         !url.includes('trembed') && 
                         !url.includes('trid=');
      serverOptions.push({
        name: `Server ${serverOptions.length + 1}`,
        trembedUrl: isExternal ? null : url,
        directUrl: isExternal ? url : null,
        option: serverOptions.length + 1,
      });
    }
  });
  
  // Now fetch real video URLs from trembed URLs
  const embeds = [];
  for (let i = 0; i < serverOptions.length; i++) {
    const server = serverOptions[i];
    
    // If we already have a direct external URL, use it
    if (server.directUrl) {
      console.log(`   ✓ Direct external URL: ${server.directUrl.substring(0, 60)}...`);
      embeds.push({ 
        name: server.name,
        url: server.directUrl,
        real_video: server.directUrl,
        type: 'iframe',
        option: server.option,
      });
      continue;
    }
    
    // Otherwise, resolve the trembed URL
    if (server.trembedUrl) {
      console.log(`   🔍 Resolving: ${server.trembedUrl.substring(0, 60)}...`);
      const realVideoUrl = await extractRealVideoUrl(server.trembedUrl, {
        referer: episodeUrl,
        parent: episodeUrl,
      });
      
      // Only add if we got a valid external URL (not the same as trembed URL)
      if (realVideoUrl && 
          realVideoUrl !== server.trembedUrl &&
          !realVideoUrl.includes('toonstream.one') &&
          !realVideoUrl.includes('trembed')) {
        console.log(`   ✓ Resolved to: ${realVideoUrl.substring(0, 60)}...`);
        embeds.push({ 
          name: server.name,
          url: realVideoUrl,
          real_video: realVideoUrl,
          type: 'iframe',
          intermediate_url: server.trembedUrl,
          option: server.option,
        });
      } else {
        console.warn(`   ⚠️ Could not resolve: ${server.trembedUrl.substring(0, 60)}...`);
      }
      
      // Small delay to avoid overwhelming the server
      await delay(300);
    }
  }
  
  return embeds;
}

function extractEpisodeMeta(html) {
  const $ = cheerio.load(html);
  const title =
    $("h1.entry-title").first().text().trim() ||
    $('meta[property="og:title"]').attr("content") ||
    $("title").text().trim();

  const thumbnail =
    $('meta[property="og:image"]').attr("content") ||
    $("div.post-thumbnail img").attr("src") ||
    $("div.video-options img").attr("src") ||
    null;

  return {
    title: title?.replace(/\s+/g, " ").trim() || null,
    thumbnail: thumbnail ? normalizeUrl(thumbnail) : null,
    episode_main_poster: normalizeUrl(
      $("div.video-options img").attr("src") ||
        $("div.video-options img").attr("data-src") ||
        $("div.video-options img").attr("data-lazy-src"),
    ),
  };
}

function extractSeriesMeta(seriesHtml) {
  const $ = cheerio.load(seriesHtml);
  const title =
    $("h1.entry-title").first().text().trim() ||
    $('meta[property="og:title"]').attr("content") ||
    $("title").text().trim();
  const description =
    $('meta[property="og:description"]').attr("content") ||
    $("div.entry-content p").first().text().trim() ||
    "";
  const thumbnail =
    $('meta[property="og:image"]').attr("content") ||
    $("div.post-thumbnail img").attr("src") ||
    null;
  const genres = [];
  $('a[rel="tag"], .genres a').each((_, el) => {
    const name = $(el).text().trim();
    if (name && !genres.includes(name)) genres.push(name);
  });
  const yearMatch = $("span.year, .year").first().text().match(/\d{4}/);
  const year = yearMatch ? parseInt(yearMatch[0], 10) : null;
  return {
    title: title?.replace(/\s+/g, " ").trim() || null,
    description: description?.replace(/\s+/g, " ").trim() || null,
    poster: thumbnail ? normalizeUrl(thumbnail) : null,
    genres,
    year,
  };
}

async function resolveSeriesContext(seriesUrl, fallbackTitle) {
  if (seriesCache.has(seriesUrl)) return seriesCache.get(seriesUrl);
  const html = await fetchHtmlWithRetry(seriesUrl, CONFIG.maxRetries, {
    referer: CONFIG.homeUrl,
  });
  const meta = extractSeriesMeta(html);
  if (!meta.title && fallbackTitle) meta.title = fallbackTitle;
  if (!meta.title) meta.title = cleanSlug(seriesUrl).replace(/-/g, " ");
  const sourceSlug =
    extractSeriesSlugFromUrl(seriesUrl) || cleanSlug(meta.title);
  const slug = sourceSlug || cleanSlug(meta.title);

  // Extract clean series name from slug for TMDB search
  const slugBasedName = extractSeriesNameFromSlug(slug);
  
  let tmdbData = null;
  try {
    // First try with the cleaned slug-based name (better for anime)
    if (slugBasedName) {
      console.log(`   🔍 Trying TMDB search with slug name: "${slugBasedName}"`);
      tmdbData = await getTMDBData(slugBasedName);
    }
    
    // If no results, try with page title
    if (!tmdbData && meta.title && meta.title !== slugBasedName) {
      console.log(`   🔍 Trying TMDB search with page title: "${meta.title}"`);
      tmdbData = await getTMDBData(meta.title);
    }
  } catch (err) {
    console.warn(`TMDB lookup failed for ${meta.title}: ${err.message}`);
  }

  const payload = {
    slug,
    title: meta.title,
    description: tmdbData?.description || meta.description,
    poster: tmdbData?.poster || meta.poster,
    banner_image: tmdbData?.banner_image || null,
    cover_image_large: tmdbData?.poster || meta.poster,
    cover_image_extra_large: tmdbData?.poster || meta.poster,
    genres: tmdbData?.genres?.length ? tmdbData.genres : meta.genres,
    tmdb_id: tmdbData?.tmdb_id || null,
    rating: tmdbData?.rating || null,
    popularity: tmdbData?.popularity || null,
    status: tmdbData?.status || null,
    studios: tmdbData?.studios || [],
    release_date: tmdbData?.release_date || null,
    total_seasons: tmdbData?.total_seasons || 1,
    total_episodes: tmdbData?.total_episodes || null,
    posters: tmdbData?.posters || (meta.poster ? [meta.poster] : []),
    backdrops: tmdbData?.backdrops || [],
    year:
      meta.year ||
      (tmdbData?.release_date
        ? parseInt(tmdbData.release_date.split("-")[0], 10)
        : null),
  };

  console.log(`   💾 Supabase: Upserting series "${meta.title}" (slug: ${slug})...`);
  console.log(`      📊 TMDB data: ${tmdbData ? 'Yes' : 'No'}, Rating: ${tmdbData?.rating || 'N/A'}`);
  
  const { error, data } = await supabase
    .from("series")
    .upsert(payload, { onConflict: "slug" })
    .select();
    
  if (error) {
    console.log(`   ❌ Supabase series upsert FAILED: ${error.message}`);
    console.log(`      Error code: ${error.code}, Details: ${JSON.stringify(error.details || {})}`);
    throw new Error(`Supabase series upsert failed: ${error.message}`);
  }
  
  console.log(`   ✅ Supabase: Series upserted successfully`);

  const ctx = { ...payload, url: seriesUrl, sourceSlug: sourceSlug || slug };
  seriesCache.set(seriesUrl, ctx);
  return ctx;
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

async function episodeExistsInSupabase(seriesSlug, season, episode) {
  const { data, error } = await supabase
    .from("episodes")
    .select("id")
    .eq("series_slug", seriesSlug)
    .eq("season", season)
    .eq("episode", episode)
    .maybeSingle();

  if (error) {
    throw new Error(`Episode verification failed: ${error.message}`);
  }

  return Boolean(data);
}

async function checkEpisodeNeedsUpdate(seriesSlug, season, episode) {
  const { data, error } = await supabase
    .from("episodes")
    .select("servers, thumbnail, episode_main_poster")
    .eq("series_slug", seriesSlug)
    .eq("season", season)
    .eq("episode", episode)
    .maybeSingle();

  if (error) {
    throw new Error(`Episode check failed: ${error.message}`);
  }

  if (!data) return { exists: false, needsUpdate: true };

  const hasServers = data.servers && Array.isArray(data.servers) && data.servers.length > 0;
  const hasThumbnail = Boolean(data.thumbnail);
  const hasPoster = Boolean(data.episode_main_poster);

  return {
    exists: true,
    needsUpdate: !hasServers || !hasThumbnail || !hasPoster,
    missingServers: !hasServers,
    missingThumbnail: !hasThumbnail,
    missingPoster: !hasPoster,
  };
}

async function upsertEpisode(
  seriesSlug,
  seriesTitle,
  season,
  episode,
  episodePayload,
) {
  const basePayload = {
    series_slug: seriesSlug,
    season,
    episode,
    ...episodePayload,
  };
  
  console.log(`   💾 Supabase: Upserting episode ${seriesSlug} S${season}E${episode}...`);
  console.log(`      📊 Servers count: ${episodePayload.servers?.length || 0}`);
  
  if (episodePayload.servers && episodePayload.servers.length > 0) {
    console.log(`      🔗 First server URL: ${episodePayload.servers[0]?.url?.substring(0, 50)}...`);
  }
  
  const { error, data } = await supabase
    .from("episodes")
    .upsert(basePayload, { onConflict: "series_slug,season,episode" })
    .select();
    
  if (error) {
    console.log(`   ❌ Supabase episode upsert FAILED: ${error.message}`);
    console.log(`      Error code: ${error.code}, Details: ${JSON.stringify(error.details || {})}`);
    throw new Error(`Supabase episode upsert failed: ${error.message}`);
  }
  
  console.log(`   ✅ Supabase: Episode upserted successfully`);

  const latestPayload = {
    series_slug: seriesSlug,
    series_title: seriesTitle,
    season,
    episode,
    episode_title: episodePayload.title,
    thumbnail:
      episodePayload.episode_card_thumbnail ||
      episodePayload.episode_list_thumbnail ||
      episodePayload.thumbnail ||
      null,
    added_at: new Date().toISOString(),
  };

  const latest = await supabase
    .from("latest_episodes")
    .upsert(latestPayload, { onConflict: "series_slug,season,episode" })
    .select();
    
  if (latest.error) {
    console.log(`   ❌ Supabase latest_episodes upsert FAILED: ${latest.error.message}`);
    throw new Error(`Supabase latest upsert failed: ${latest.error.message}`);
  }
  
  console.log(`   ✅ Supabase: Latest episode record updated`);
}

async function extractSeriesUrlFromBreadcrumb(html) {
  const $ = cheerio.load(html);
  const breadcrumbs = $(
    'nav.breadcrumb a[href*="/series/"], .entry-meta a[href*="/series/"]',
  );
  const last = breadcrumbs.last();
  return normalizeUrl(last.attr("href"));
}

async function buildEpisodeRecord(episodeUrl, hints = {}) {
  const episodeHtml = await fetchHtmlWithRetry(episodeUrl, CONFIG.maxRetries, {
    referer: hints.seriesUrl || CONFIG.homeUrl,
  });
  const derivedSeriesUrl =
    (await extractSeriesUrlFromBreadcrumb(episodeHtml)) ||
    deriveSeriesUrlFromEpisode(episodeUrl) ||
    hints.seriesUrl ||
    episodeUrl.split("/episode/")[0];

  const fallbackTitle = hints.seriesTitle || hints.card?.title || null;
  const seriesCtx = await resolveSeriesContext(derivedSeriesUrl, fallbackTitle);
  const meta = extractEpisodeMeta(episodeHtml);
  const code = hints.code ||
    parseEpisodeCode(episodeUrl) || {
      season: 1,
      episode: Math.floor(Date.now() / 1000),
    };
  const embeds = await extractEmbeds(episodeHtml, episodeUrl);

  // Try to fetch episode image from API
  let apiEpisodeImage = null;
  try {
    const postId = extractPostId(episodeHtml);
    const nonce = extractNonce(episodeHtml);
    if (postId && code.season) {
      const episodeData = await fetchEpisodeDataFromAPI(postId, code.season, nonce);
      if (episodeData && episodeData.length > 0) {
        // Find the matching episode by season and episode number
        const matchingEpisode = episodeData.find(
          ep => ep.season === code.season && ep.episode === code.episode
        );
        if (matchingEpisode && matchingEpisode.image) {
          apiEpisodeImage = matchingEpisode.image;
        }
      }
    }
  } catch (err) {
    console.warn(`  ⚠️ Failed to fetch episode image from API: ${err.message}`);
  }

  const episodePayload = {
    title: meta.title || hints.card?.title || `Episode ${code.episode}`,
    thumbnail: apiEpisodeImage || meta.thumbnail || hints.card?.thumb || seriesCtx.poster,
    episode_main_poster: apiEpisodeImage || meta.episode_main_poster || seriesCtx.poster,
    episode_card_thumbnail: apiEpisodeImage || meta.thumbnail || hints.card?.thumb || null,
    episode_list_thumbnail: apiEpisodeImage || hints.card?.thumb || meta.thumbnail || null,
    video_player_thumbnail:
      apiEpisodeImage || meta.episode_main_poster || meta.thumbnail || hints.card?.thumb || null,
    servers: embeds,
  };

  if (!episodePayload.thumbnail) {
    episodePayload.thumbnail =
      seriesCtx.poster || seriesCtx.cover_image_large || null;
  }

  return { seriesCtx, code, episodePayload };
}

async function syncEpisodeByUrl(episodeUrl, options = {}) {
  let lastSeriesCtx = null;
  let lastCode = null;

  for (let attempt = 1; attempt <= 3; attempt++) {
    const { seriesCtx, code, episodePayload } = await buildEpisodeRecord(
      episodeUrl,
      options,
    );
    lastSeriesCtx = seriesCtx;
    lastCode = code;

    const key = makeEpisodeKey(seriesCtx.slug, code.season, code.episode);
    const seasonEpisodeKey = makeSeasonEpisodeKey(code.season, code.episode);

    // Check if episode needs update (missing servers or images)
    const updateCheck = await checkEpisodeNeedsUpdate(
      seriesCtx.slug,
      code.season,
      code.episode,
    );

    // When force is true, ALWAYS update regardless of processed status
    // This ensures old episodes get refreshed when series has new content
    if (options.force) {
      // Force update - skip all checks
    } else {
      // Normal mode - skip if already processed and complete
      if (processedEpisodes.has(key) && !updateCheck.needsUpdate) {
        return { ...seriesCtx, season: code.season, episode: code.episode };
      }
      if (options.existingEpisodes?.has(seasonEpisodeKey) && !updateCheck.needsUpdate) {
        return { ...seriesCtx, season: code.season, episode: code.episode };
      }
    }

    // Log what we're updating
    // If force is true, always treat as update if episode exists
    const isUpdate = options.force ? updateCheck.exists : (updateCheck.exists && updateCheck.needsUpdate);
    
    if (isUpdate) {
      if (options.force && updateCheck.exists && !updateCheck.needsUpdate) {
        // Episode exists with complete data but we're forcing a refresh
        console.log(
          `   🔄 Force updating ${seriesCtx.title} S${code.season}E${code.episode} (refresh all data)`
        );
      } else if (updateCheck.needsUpdate) {
        // Episode exists but has missing data
        const missing = [];
        if (updateCheck.missingServers) missing.push("servers");
        if (updateCheck.missingThumbnail) missing.push("thumbnail");
        if (updateCheck.missingPoster) missing.push("poster");
        console.log(
          `   🔄 Updating ${seriesCtx.title} S${code.season}E${code.episode} (missing: ${missing.join(", ")})`
        );
      }
    }

    await upsertEpisode(
      seriesCtx.slug,
      seriesCtx.title,
      code.season,
      code.episode,
      episodePayload,
    );

    const persisted = await episodeExistsInSupabase(
      seriesCtx.slug,
      code.season,
      code.episode,
    );
    if (persisted) {
      processedEpisodes.add(key);
      options.existingEpisodes?.add(seasonEpisodeKey);

      // Update statistics
      const serversCount = episodePayload.servers?.length || 0;
      stats.totalServers += serversCount;
      stats.seriesProcessed.add(seriesCtx.slug);
      
      if (isUpdate) {
        stats.updatedEpisodes++;
      } else {
        stats.newEpisodes++;
      }

      const context = options.reason ? ` (${options.reason})` : "";
      const attemptInfo = attempt > 1 ? ` (attempt ${attempt})` : "";
      console.log(
        `✅ Synced ${seriesCtx.title} S${code.season}E${code.episode}${context}${attemptInfo} | Servers: ${serversCount}`,
      );
      // Return seriesCtx with season/episode info for smart sync tracking
      return { ...seriesCtx, season: code.season, episode: code.episode };
    }

    console.warn(
      `   ⚠️  Episode ${seriesCtx.title} S${code.season}E${code.episode} missing in Supabase after upsert, retrying...`,
    );
    await delay(500 * attempt);
  }

  console.error(
    `❌ Failed to persist ${lastSeriesCtx?.title || "episode"} S${lastCode?.season}E${lastCode?.episode} after retries`,
  );
  stats.failedEpisodes++;
  // Return with season/episode info even on failure for smart sync tracking
  return lastSeriesCtx ? { ...lastSeriesCtx, season: lastCode?.season, episode: lastCode?.episode } : null;
}

function extractSeasonNumbers(html) {
  const $ = cheerio.load(html);
  const seasons = new Set();
  
  // Look for season buttons/options
  $('[data-season], option[value]').each((_, el) => {
    const seasonAttr = $(el).attr('data-season') || $(el).attr('value');
    if (seasonAttr && !isNaN(seasonAttr)) {
      seasons.add(parseInt(seasonAttr, 10));
    }
  });
  
  // If no seasons found via data attributes, try text content
  if (seasons.size === 0) {
    $('.aa-cnt .se-c').each((_, el) => {
      const text = $(el).find('.se-t').text();
      const match = text.match(/season\s+(\d+)/i);
      if (match) {
        seasons.add(parseInt(match[1], 10));
      }
    });
  }
  
  // Default to season 1 if nothing found
  if (seasons.size === 0) {
    seasons.add(1);
  }
  
  return Array.from(seasons).sort((a, b) => a - b);
}

async function ensureSeriesComplete(seriesCtx, triggeringEpisode = null) {
  try {
    const html = await fetchHtmlWithRetry(seriesCtx.url, CONFIG.maxRetries, {
      referer: CONFIG.homeUrl,
    });
    
    // Extract post ID and nonce from series page
    const postId = extractPostId(html);
    const nonce = extractNonce(html);
    if (!postId) {
      console.warn(`   ⚠️  No post ID found for ${seriesCtx.title}, skipping`);
      return;
    }
    
    // Extract available seasons
    const seasons = extractSeasonNumbers(html);
    console.log(`      🔍 Found ${seasons.length} season(s) for ${seriesCtx.title}`);
    
    // Fetch all episodes from all seasons using WordPress AJAX API
    const allEpisodeLinks = [];
    for (const season of seasons) {
      const episodeData = await fetchEpisodeDataFromAPI(postId, season, nonce);
      if (episodeData && episodeData.length > 0) {
        console.log(`         • Season ${season}: ${episodeData.length} episode(s)`);
        allEpisodeLinks.push(...episodeData);
      }
      await delay(300); // Small delay between API calls
    }
    
    if (allEpisodeLinks.length === 0) {
      console.warn(`   ⚠️  No episodes found for ${seriesCtx.title}`);
      return;
    }

    // Get all existing episodes for this series
    const { data: existingData } = await supabase
      .from("episodes")
      .select("season, episode, servers, thumbnail, episode_main_poster")
      .eq("series_slug", seriesCtx.slug);

    const existingEpisodes = new Set();
    existingData?.forEach((ep) => {
      const key = makeSeasonEpisodeKey(ep.season, ep.episode);
      existingEpisodes.add(key);
    });

    // Find missing episodes (new episodes not in database)
    const missing = allEpisodeLinks.filter(
      (link) => !existingEpisodes.has(makeSeasonEpisodeKey(link.season, link.episode)),
    );

    if (missing.length === 0) {
      console.log(`      ✅ All episodes already synced for ${seriesCtx.title}`);
      return;
    }

    // SMART SYNC LOGIC:
    // Check if we should fetch only the new episode or all missing episodes
    let episodesToSync = [];
    
    if (triggeringEpisode) {
      // Check if previous episode exists
      const prevEpisode = triggeringEpisode.episode - 1;
      const prevKey = makeSeasonEpisodeKey(triggeringEpisode.season, prevEpisode);
      const prevExists = existingEpisodes.has(prevKey) || prevEpisode < 1;
      
      if (prevExists) {
        // Previous episode exists (or this is episode 1) - only sync the new episode
        const newEp = missing.find(
          (m) => m.season === triggeringEpisode.season && m.episode === triggeringEpisode.episode
        );
        if (newEp) {
          episodesToSync = [newEp];
          console.log(`      🎯 Smart sync: Previous episode S${triggeringEpisode.season}E${prevEpisode} exists, fetching only new episode`);
        }
      } else {
        // Previous episode doesn't exist - need to backfill all missing episodes
        episodesToSync = missing;
        console.log(`      📥 Smart sync: Previous episode S${triggeringEpisode.season}E${prevEpisode} missing, fetching ${missing.length} episodes`);
      }
    } else {
      // No triggering episode info - sync all missing
      episodesToSync = missing;
      console.log(`      📥 Full sync: Fetching ${missing.length} missing episodes`);
    }

    if (episodesToSync.length === 0) {
      console.log(`      ✅ No episodes to sync for ${seriesCtx.title}`);
      return;
    }

    console.log(
      `      ↪ ${seriesCtx.title}: Syncing ${episodesToSync.length} episode(s)`,
    );

    // Sync the selected episodes
    for (const link of episodesToSync) {
      await syncEpisodeByUrl(link.url, {
        force: true,
        code: { season: link.season, episode: link.episode },
        seriesUrl: seriesCtx.url,
        seriesTitle: seriesCtx.title,
        existingEpisodes,
        card: { title: link.title, thumb: link.image },
        reason: episodesToSync.length === 1 ? "smart-new" : "backfill",
      });
      await delay(750);
    }
  } catch (err) {
    console.error(`   ❌ Failed to process ${seriesCtx.title}: ${err.message}`);
    stats.failedEpisodes++;
  }
}

async function auditLatestEpisodes(
  limit = Number(process.env.LATEST_AUDIT_LIMIT || 25),
) {
  try {
    const { data, error } = await supabase
      .from("latest_episodes")
      .select(
        "series_slug, series_title, season, episode, episode_title, thumbnail",
      )
      .order("added_at", { ascending: false })
      .limit(limit);

    if (error) throw error;
    if (!data || data.length === 0) return;

    for (const entry of data) {
      const exists = await episodeExistsInSupabase(
        entry.series_slug,
        entry.season,
        entry.episode,
      );
      if (exists) continue;

      console.log(
        `   ↺ Restoring missing episode ${entry.series_title} S${entry.season}E${entry.episode} from latest feed`,
      );

      const episodeUrl = buildEpisodeUrl(
        entry.series_slug,
        entry.season,
        entry.episode,
      );
      const seriesUrl = buildSeriesUrlFromSlug(entry.series_slug);

      await syncEpisodeByUrl(episodeUrl, {
        force: true,
        seriesUrl,
        seriesTitle: entry.series_title,
        card: { title: entry.episode_title, thumb: entry.thumbnail },
        reason: "latest-audit",
      });
      await delay(500);
    }
  } catch (err) {
    console.error(`⚠️ Latest episodes audit failed: ${err.message}`);
  }
}

async function auditAndUpdateEmptyServers(
  limit = Number(process.env.EMPTY_SERVERS_AUDIT_LIMIT || 50),
) {
  try {
    console.log(`\n🔍 Checking for episodes with missing servers/images...`);
    
    const { data, error } = await supabase
      .from("episodes")
      .select("series_slug, season, episode, servers, thumbnail, episode_main_poster")
      .order("updated_at", { ascending: false })
      .limit(limit);

    if (error) throw error;
    if (!data || data.length === 0) return;

    let updatedCount = 0;
    let skippedCount = 0;
    
    for (const ep of data) {
      const hasServers = ep.servers && Array.isArray(ep.servers) && ep.servers.length > 0;
      const hasThumbnail = Boolean(ep.thumbnail);
      const hasPoster = Boolean(ep.episode_main_poster);

      if (!hasServers || !hasThumbnail || !hasPoster) {
        try {
          const episodeUrl = buildEpisodeUrl(ep.series_slug, ep.season, ep.episode);
          const seriesUrl = buildSeriesUrlFromSlug(ep.series_slug);

          await syncEpisodeByUrl(episodeUrl, {
            force: true,
            seriesUrl,
            reason: "update-missing-data",
          });
          updatedCount++;
          await delay(1000);
        } catch (err) {
          // Skip invalid/404 episodes gracefully
          if (err.message.includes("404") || err.message.includes("Failed to fetch")) {
            skippedCount++;
            stats.skippedEpisodes++;
          } else {
            console.warn(`   ⚠️ Failed to update ${ep.series_slug} S${ep.season}E${ep.episode}: ${err.message}`);
            stats.failedEpisodes++;
          }
        }
      }
    }

    if (updatedCount > 0) {
      console.log(`   ✅ Updated ${updatedCount} episodes with missing data`);
    } else {
      console.log(`   ✅ All recent episodes have complete data`);
    }
    
    if (skippedCount > 0) {
      console.log(`   ℹ️  Skipped ${skippedCount} invalid/deleted episodes`);
    }
  } catch (err) {
    console.error(`⚠️ Empty servers audit failed: ${err.message}`);
  }
}

async function fetchHomepageHtml() {
  let lastErr = null;
  for (const candidate of CONFIG.homepageCandidates) {
    try {
      const html = await fetchHtmlWithRetry(candidate, CONFIG.maxRetries, {
        referer: CONFIG.homeUrl,
      });
      if (candidate !== CONFIG.homeUrl) {
        console.log(`ℹ️ Using homepage fallback: ${candidate}`);
      }
      return html;
    } catch (err) {
      lastErr = err;
      console.warn(
        `⚠️ Failed to fetch homepage ${candidate}: ${err.message}`,
      );
      await delay(500);
    }
  }
  throw lastErr || new Error("All homepage candidates failed");
}

async function pollHomepage() {
  // Map of slug -> { season, episode } for smart sync
  const latestSeriesMap = new Map();
  try {
    const html = await fetchHomepageHtml();
    const cards = extractEpisodeCards(html);
    console.log(`🔍 Found ${cards.length} candidate episodes`);
    for (const card of cards) {
      const seriesCtx = await syncEpisodeByUrl(card.url, { card });
      if (seriesCtx && seriesCtx.slug) {
        // Store the triggering episode info for smart sync
        latestSeriesMap.set(seriesCtx.slug, {
          season: seriesCtx.season || 1,
          episode: seriesCtx.episode || 1,
        });
      }
      await delay(1000);
    }
  } catch (err) {
    console.error(`❌ Polling error: ${err.message}`);
  }
  return latestSeriesMap;
}

function printSummary() {
  console.log("\n" + "=".repeat(60));
  console.log("📊 SYNC SUMMARY");
  console.log("=".repeat(60));
  
  const totalEpisodes = stats.newEpisodes + stats.updatedEpisodes;
  const totalProcessed = totalEpisodes + stats.failedEpisodes + stats.skippedEpisodes;
  
  console.log(`\n✅ Success:`);
  console.log(`   • New Episodes Added: ${stats.newEpisodes}`);
  console.log(`   • Episodes Updated: ${stats.updatedEpisodes}`);
  console.log(`   • Total Servers Fetched: ${stats.totalServers}`);
  console.log(`   • Series Processed: ${stats.seriesProcessed.size}`);
  
  if (stats.failedEpisodes > 0 || stats.skippedEpisodes > 0) {
    console.log(`\n⚠️  Issues:`);
    if (stats.failedEpisodes > 0) {
      console.log(`   • Failed Episodes: ${stats.failedEpisodes}`);
    }
    if (stats.skippedEpisodes > 0) {
      console.log(`   • Skipped (Invalid/Deleted): ${stats.skippedEpisodes}`);
    }
  }
  
  console.log(`\n📈 Total Episodes Processed: ${totalProcessed}`);
  
  const successRate = totalProcessed > 0 
    ? ((totalEpisodes / totalProcessed) * 100).toFixed(1) 
    : 0;
  
  console.log(`   Success Rate: ${successRate}%`);
  
  console.log("\n" + "=".repeat(60));
  
  if (stats.failedEpisodes > 0) {
    console.log("⚠️  Status: Completed with some failures");
  } else {
    console.log("✅ Status: All operations completed successfully!");
  }
  
  console.log("💡 Run 'npm run sync' again to fetch new episodes");
  console.log("=".repeat(60) + "\n");
}

async function updateSeriesFromLatestEpisodes(latestSeriesMap) {
  try {
    if (!latestSeriesMap || latestSeriesMap.size === 0) {
      console.log(`\n   ℹ️  No series to update from latest episodes`);
      return;
    }
    
    console.log(`\n🔄 Smart sync: Checking series with new episodes...`);
    console.log(`   📚 Found ${latestSeriesMap.size} series with latest episodes\n`);
    
    // Process only the series from latestSeriesMap
    for (const [slug, triggeringEpisode] of latestSeriesMap) {
      try {
        // Get series title from database
        const { data: seriesData, error } = await supabase
          .from("series")
          .select("title")
          .eq("slug", slug)
          .maybeSingle();
        
        if (error) {
          console.warn(`   ⚠️ Failed to fetch series data for ${slug}: ${error.message}`);
          continue;
        }
        
        const seriesUrl = buildSeriesUrlFromSlug(slug);
        
        // Create a minimal series context
        const seriesCtx = {
          slug: slug,
          title: seriesData?.title || slug,
          url: seriesUrl,
        };
        
        console.log(`   📺 Processing: ${seriesCtx.title} (triggered by S${triggeringEpisode.season}E${triggeringEpisode.episode})`);
        
        // Smart sync: pass triggering episode to determine if backfill is needed
        await ensureSeriesComplete(seriesCtx, triggeringEpisode);
        
        // Small delay between series
        await delay(1500);
      } catch (err) {
        console.warn(`   ⚠️ Failed to process ${slug}: ${err.message}`);
        stats.failedEpisodes++;
      }
    }
    
    console.log(`\n✅ Finished smart sync for all series`);
  } catch (err) {
    console.error(`⚠️ Update series failed: ${err.message}`);
  }
}

export async function start() {
  console.log("🚀 Toonstream -> Supabase sync started");
  console.log("📡 Fetching latest episodes from Toonstream...\n");
  
  // Initialize proxy system
  await proxyManager.initialize();
  const proxyStats = proxyManager.getStats();
  if (proxyStats.enabled) {
    console.log(`🔐 Proxy Status: ${proxyStats.active}/${proxyStats.total} active\n`);
  }
  
  // Step 1: Fetch latest from homepage and track which series have new episodes
  const latestSeriesSlugs = await pollHomepage();
  
  // Step 2: Update ALL episodes (old + new) for series with recent activity
  await updateSeriesFromLatestEpisodes(latestSeriesSlugs);
  
  // Step 3: Audit and restore from latest_episodes
  await auditLatestEpisodes();
  
  // Step 4: Final check for any episodes with missing data
  await auditAndUpdateEmptyServers();
  
  printSummary();
}

// Run directly if this file is executed standalone (not imported)
// Check if this is the main module by seeing if it was run directly
const isMainModule = process.argv[1] && (
  process.argv[1].endsWith('toonstream-supabase-sync.js') ||
  process.argv[1].includes('toonstream-supabase-sync.js')
);

if (isMainModule) {
  start().catch((err) => {
    console.error("\n❌ Error occurred:", err.message);
    process.exit(1);
  });
}
