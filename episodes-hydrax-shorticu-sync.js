import "dotenv/config";
import axios from "axios";
import { createClient } from "@supabase/supabase-js";

const REQUIRED_ENV = ["SUPABASE_URL", "SUPABASE_SERVICE_ROLE_KEY"];
const missingEnv = REQUIRED_ENV.filter((key) => !process.env[key]);
if (missingEnv.length) {
  console.error(`Missing required environment variables: ${missingEnv.join(", ")}`);
  process.exit(1);
}

const HYDRAX_API_KEY =
  process.env.HYDRAX_API_KEY || "0024c518af651a1cf432a2825a3d0f09";
const BATCH_SIZE = Number(process.env.HYDRAX_SYNC_BATCH_SIZE || 200);
const REQUEST_TIMEOUT_MS = Number(process.env.HYDRAX_TIMEOUT_MS || 20000);
const REQUEST_DELAY_MS = Number(process.env.HYDRAX_REQUEST_DELAY_MS || 200);

const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_SERVICE_ROLE_KEY,
);

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

function extractSlugFromUrl(rawUrl) {
  if (!rawUrl || typeof rawUrl !== "string") return null;

  let parsed;
  try {
    parsed = new URL(rawUrl.trim());
  } catch {
    return null;
  }

  const host = parsed.hostname.toLowerCase().replace(/^www\./, "");

  if (host === "abysscdn.com") {
    const querySlug = parsed.searchParams.get("v");
    if (querySlug) return querySlug.trim();
    return null;
  }

  if (host === "short.icu") {
    const pathSlug = parsed.pathname.split("/").filter(Boolean)[0];
    if (pathSlug) return pathSlug.trim();
    return null;
  }

  return null;
}

function isSupportedUrl(rawUrl) {
  if (!rawUrl || typeof rawUrl !== "string") return false;

  try {
    const host = new URL(rawUrl.trim()).hostname.toLowerCase().replace(/^www\./, "");
    return host === "abysscdn.com" || host === "short.icu";
  } catch {
    return false;
  }
}

async function copySlugViaHydrax(oldSlug) {
  const apiUrl = `https://api.hydrax.net/${HYDRAX_API_KEY}/copy/${encodeURIComponent(oldSlug)}`;

  const response = await axios.get(apiUrl, {
    timeout: REQUEST_TIMEOUT_MS,
    headers: {
      Accept: "application/json",
      "User-Agent": "toonstream-hydrax-sync/1.0",
    },
  });

  const data = response?.data || {};
  if (!data.status || !data.slug) {
    throw new Error(`Hydrax returned invalid response for slug ${oldSlug}`);
  }

  return String(data.slug).trim();
}

function buildShortIcuUrl(slug) {
  return `https://short.icu/${slug}`;
}

async function processEpisodeRow(row) {
  const originalServers = Array.isArray(row.servers) ? row.servers : [];
  if (!originalServers.length) {
    return { updated: false, changedCount: 0 };
  }

  let changedCount = 0;
  const nextServers = [];

  for (const server of originalServers) {
    if (!server || typeof server !== "object") {
      nextServers.push(server);
      continue;
    }

    const currentUrl = server.real_video;
    if (!isSupportedUrl(currentUrl)) {
      nextServers.push(server);
      continue;
    }

    const oldSlug = extractSlugFromUrl(currentUrl);
    if (!oldSlug) {
      nextServers.push(server);
      continue;
    }

    try {
      const newSlug = await copySlugViaHydrax(oldSlug);
      const newUrl = buildShortIcuUrl(newSlug);

      nextServers.push({
        ...server,
        real_video: newUrl,
      });
      changedCount += 1;
      await sleep(REQUEST_DELAY_MS);
    } catch (error) {
      // If one server fails, keep original URL so we do not lose data.
      console.warn(
        `Hydrax copy failed for episode id=${row.id}, slug=${oldSlug}: ${error.message}`,
      );
      nextServers.push(server);
    }
  }

  if (!changedCount) {
    return { updated: false, changedCount: 0 };
  }

  const { error: updateError } = await supabase
    .from("episodes")
    .update({
      servers: nextServers,
      updated_at: new Date().toISOString(),
    })
    .eq("id", row.id);

  if (updateError) {
    throw new Error(updateError.message);
  }

  return { updated: true, changedCount };
}

async function run() {
  console.log("Starting Hydrax URL sync for episodes table...");

  let page = 0;
  let rowsProcessed = 0;
  let episodesUpdated = 0;
  let totalServersUpdated = 0;

  while (true) {
    const from = page * BATCH_SIZE;
    const to = from + BATCH_SIZE - 1;

    const { data: rows, error } = await supabase
      .from("episodes")
      .select("id, series_slug, season, episode, servers")
      .order("id", { ascending: true })
      .range(from, to);

    if (error) {
      throw new Error(`Failed to fetch episodes batch: ${error.message}`);
    }

    if (!rows || rows.length === 0) {
      break;
    }

    for (const row of rows) {
      rowsProcessed += 1;
      try {
        const result = await processEpisodeRow(row);
        if (result.updated) {
          episodesUpdated += 1;
          totalServersUpdated += result.changedCount;
          console.log(
            `Updated episode ${row.series_slug} S${row.season}E${row.episode} (id=${row.id}) with ${result.changedCount} server change(s)`,
          );
        }
      } catch (err) {
        console.error(
          `Failed to update episode id=${row.id} (${row.series_slug} S${row.season}E${row.episode}): ${err.message}`,
        );
      }
    }

    page += 1;
  }

  console.log("Hydrax sync complete.");
  console.log(`Episodes scanned: ${rowsProcessed}`);
  console.log(`Episodes updated: ${episodesUpdated}`);
  console.log(`Server URLs updated: ${totalServersUpdated}`);
}

run().catch((error) => {
  console.error(`Fatal error in Hydrax sync: ${error.message}`);
  process.exit(1);
});
