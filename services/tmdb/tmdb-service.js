import axios from "axios";

const TMDB_BASE_URL = "https://api.themoviedb.org/3";
const TMDB_IMAGE_BASE = "https://image.tmdb.org/t/p/original";

const TVDB_API_KEY = process.env.TVDB_API_KEY;
const TVDB_BASE_URL = "https://api4.thetvdb.com/v4";

export class TMDBService {
  constructor(apiKey) {
    this.apiKey = apiKey;
    this.tvdbToken = null;
  }

  async getTVDBToken() {
    if (this.tvdbToken) return this.tvdbToken;
    try {
      const res = await axios.post(`${TVDB_BASE_URL}/login`, {
        apikey: TVDB_API_KEY
      }, {
        headers: { 'Content-Type': 'application/json' }
      });
      this.tvdbToken = res.data?.data?.token;
      return this.tvdbToken;
    } catch (err) {
      console.warn(`⚠️ TVDB: Login failed: ${err.message}`);
      return null;
    }
  }

  async fetchTVDBEpisodeImage(title, seasonNum, episodeNum) {
    const token = await this.getTVDBToken();
    if (!token) return null;
    try {
      const searchRes = await axios.get(`${TVDB_BASE_URL}/search?query=${encodeURIComponent(title)}&type=series`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      const tvdbId = searchRes.data?.data?.[0]?.tvdb_id;
      if (!tvdbId) return null;

      const epRes = await axios.get(`${TVDB_BASE_URL}/series/${tvdbId}/episodes/default?page=0`, {
        headers: { 'Authorization': `Bearer ${token}` }
      });
      
      const episodes = epRes.data?.data?.episodes || [];
      const ep = episodes.find(e => e.seasonNumber == seasonNum && e.number == episodeNum);
      
      if (ep && ep.image) {
        return ep.image.startsWith('http') ? ep.image : `https://artworks.thetvdb.com${ep.image}`;
      }
      return null;
    } catch (err) {
      console.warn(`⚠️ TVDB: Episode image fetch failed for "${title}" S${seasonNum}E${episodeNum}: ${err.message}`);
      return null;
    }
  }

  async search(query, type = "tv") {
    try {
      const response = await axios.get(`${TMDB_BASE_URL}/search/${type}`, {
        params: {
          api_key: this.apiKey,
          query: query,
          language: "en-US"
        }
      });
      return response.data.results;
    } catch (error) {
      console.error("TMDB Search Error:", error.message);
      return [];
    }
  }

  async getDetails(tmdbId, type = "tv") {
    try {
      const response = await axios.get(`${TMDB_BASE_URL}/${type}/${tmdbId}`, {
        params: {
          api_key: this.apiKey,
          language: "en-US",
          append_to_response: "images,external_ids"
        }
      });
      const data = response.data;
      
      const releaseDate = data.first_air_date || data.release_date;
      const year = releaseDate ? new Date(releaseDate).getFullYear() : null;

      return {
        tmdb_id: data.id,
        title: data.name || data.title,
        description: data.overview,
        poster: data.poster_path ? `${TMDB_IMAGE_BASE}${data.poster_path}` : null,
        backdrop: data.backdrop_path ? `${TMDB_IMAGE_BASE}${data.backdrop_path}` : null,
        rating: data.vote_average,
        genres: data.genres?.map(g => g.name) || [],
        release_date: releaseDate,
        year: year,
        total_seasons: data.number_of_seasons,
        total_episodes: data.number_of_episodes
      };
    } catch (error) {
      console.error("TMDB Details Error:", error.message);
      return null;
    }
  }

  async fetchEpisodeTitle(tmdbId, seasonNum, episodeNum) {
    try {
      const response = await axios.get(`${TMDB_BASE_URL}/tv/${tmdbId}/season/${seasonNum}/episode/${episodeNum}`, {
        params: {
          api_key: this.apiKey,
          language: "en-US"
        }
      });
      return response.data.name || null;
    } catch (error) {
      console.error(`TMDB Episode Title Error (ID: ${tmdbId} S${seasonNum}E${episodeNum}):`, error.message);
      return null;
    }
  }

  async fetchEpisodeImage(tmdbId, seasonNum, episodeNum) {
    try {
      const response = await axios.get(`${TMDB_BASE_URL}/tv/${tmdbId}/season/${seasonNum}/episode/${episodeNum}`, {
        params: {
          api_key: this.apiKey,
          language: "en-US"
        }
      });
      const stillPath = response.data.still_path;
      return stillPath ? `${TMDB_IMAGE_BASE}${stillPath}` : null;
    } catch (error) {
      console.error(`TMDB Episode Image Error (ID: ${tmdbId} S${seasonNum}E${episodeNum}):`, error.message);
      return null;
    }
  }
}
