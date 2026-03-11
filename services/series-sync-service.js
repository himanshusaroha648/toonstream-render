async function resolveSeriesComplete(seriesCtx, triggeringEpisode = null) {
  try {
    if (!seriesCtx.slug && seriesCtx.url) {
      seriesCtx.slug = extractSeriesSlugFromUrl(seriesCtx.url);
    }
    if (!seriesCtx.slug) {
      console.warn(`   ⚠️ Cannot process series without slug: ${seriesCtx.title || seriesCtx.url}`);
      return;
    }
    
    const html = await fetchHtmlWithRetry(seriesCtx.url, CONFIG.maxRetries, {
      referer: CONFIG.homeUrl,
    });
    
    const postId = extractPostId(html);
    const nonce = extractNonce(html);
    
    const htmlEpisodeLinks = extractSeriesEpisodeLinks(html, seriesCtx.url);
    const htmlUrlMap = new Map();
    htmlEpisodeLinks.forEach(ep => {
      const key = `${ep.season}x${ep.episode}`;
      htmlUrlMap.set(key, ep);
    });
    
    const seasons = extractSeasonNumbers(html);
    console.log(`      🔍 Found ${seasons.length} season(s) for ${seriesCtx.title}`);
    
    const allEpisodeLinks = [];
    for (const season of seasons) {
      const episodeData = await fetchEpisodeDataFromAPI(postId, season, nonce);
      if (episodeData && episodeData.length > 0) {
        console.log(`         • Season ${season}: ${episodeData.length} episode(s)`);
        for (const ep of episodeData) {
          const builtUrl = buildEpisodeUrl(seriesCtx.slug, ep.season, ep.episode);
          let validUrl = builtUrl;
          if (!isValidEpisodeUrl(validUrl)) {
            const key = `${ep.season}x${ep.episode}`;
            const htmlEp = htmlUrlMap.get(key);
            if (htmlEp && htmlEp.url && isValidEpisodeUrl(htmlEp.url)) {
              validUrl = htmlEp.url;
            } else if (ep.url && isValidEpisodeUrl(ep.url)) {
              validUrl = ep.url;
            }
          }
          if (!isValidEpisodeUrl(validUrl)) continue;
          allEpisodeLinks.push({
            url: validUrl,
            season: ep.season,
            episode: ep.episode,
            title: ep.title,
            image: ep.image,
          });
        }
      }
      await delay(300);
    }
    
    if (allEpisodeLinks.length === 0 && htmlEpisodeLinks.length > 0) {
      for (const ep of htmlEpisodeLinks) {
        const builtUrl = buildEpisodeUrl(seriesCtx.slug, ep.season, ep.episode);
        let episodeUrl = builtUrl;
        if (!isValidEpisodeUrl(episodeUrl) && ep.url && isValidEpisodeUrl(ep.url)) {
          episodeUrl = ep.url;
        }
        if (!isValidEpisodeUrl(episodeUrl)) continue;
        allEpisodeLinks.push({
          url: episodeUrl,
          season: ep.season,
          episode: ep.episode,
          title: ep.title,
          image: ep.thumb,
        });
      }
    }
    
    if (allEpisodeLinks.length === 0) return;

    const { data: existingData } = await supabase
      .from("episodes")
      .select("season, episode")
      .eq("series_slug", seriesCtx.slug);

    const existingEpisodes = new Set();
    const supabaseSeasonMap = new Map();
    existingData?.forEach((ep) => {
      const key = makeSeasonEpisodeKey(ep.season, ep.episode);
      existingEpisodes.add(key);
      const count = supabaseSeasonMap.get(ep.season) || 0;
      supabaseSeasonMap.set(ep.season, count + 1);
    });

    const toonstreamSeasonMap = new Map();
    allEpisodeLinks.forEach((ep) => {
      const count = toonstreamSeasonMap.get(ep.season) || 0;
      toonstreamSeasonMap.set(ep.season, count + 1);
    });

    const seasonsToBackfill = new Set();
    for (const [season, tsCount] of toonstreamSeasonMap) {
      const sbCount = supabaseSeasonMap.get(season) || 0;
      if (tsCount > sbCount) {
        console.log(`      🔄 Smart Backfill: Season ${season} has ${tsCount} episodes on Toonstream but only ${sbCount} in Supabase.`);
        seasonsToBackfill.add(season);
      }
    }

    for (const ep of allEpisodeLinks) {
      const key = makeSeasonEpisodeKey(ep.season, ep.episode);
      const isNew = !existingEpisodes.has(key);
      const isTriggering = triggeringEpisode && ep.season === triggeringEpisode.season && ep.episode === triggeringEpisode.episode;
      const shouldBackfill = seasonsToBackfill.has(ep.season);

      if (isNew || isTriggering || shouldBackfill) {
        try {
          await syncEpisodeByUrl(ep.url, {
            seriesUrl: seriesCtx.url,
            seriesTitle: seriesCtx.title,
            force: shouldBackfill || isTriggering,
            code: { season: ep.season, episode: ep.episode },
            hints: { card: ep }
          });
        } catch (err) {
          console.warn(`         ⚠️ Failed to sync S${ep.season}E${ep.episode}: ${err.message}`);
        }
      }
    }
  } catch (err) {
    console.error(`   ⚠️ ensureSeriesComplete failed: ${err.message}`);
  }
}
