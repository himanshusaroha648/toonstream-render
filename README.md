# Toonstream Render Sync Server

Continuous sync server for Toonstream to Supabase - optimized for Render.com deployment.

## Features

- Automatic sync every 10 minutes
- TMDB integration for metadata (series poster, banner)
- TMDB episode images (fetches still images for each episode)
- **Smart Backfill**: Checks if old episodes exist in Supabase, if missing then fetches all
- Proxy support for scraping
- Real-time status monitoring

## Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/` | GET | Health check + sync status |
| `/status` | GET | Detailed sync status |
| `/sync` | GET | Trigger manual sync |

## Deploy to Render

### Option 1: One-Click Deploy
1. Fork this repo to your GitHub
2. Go to [Render Dashboard](https://dashboard.render.com)
3. Click "New" > "Blueprint"
4. Connect your GitHub repo
5. Render will use `render.yaml` for configuration

### Option 2: Manual Deploy
1. Go to [Render Dashboard](https://dashboard.render.com)
2. Click "New" > "Web Service"
3. Connect your GitHub repo
4. Configure:
   - **Build Command**: `npm install`
   - **Start Command**: `npm start`
   - **Plan**: Free (or Starter for better performance)

### Environment Variables
Set these in Render Dashboard:
- `SUPABASE_URL` - Your Supabase project URL
- `SUPABASE_SERVICE_ROLE_KEY` - Supabase service role key
- `TMDB_API_KEY` - TMDB API key (optional, for metadata)
- `USE_PROXY` - Set to "true" to enable proxy rotation
- `CRON_SCHEDULE` - Cron expression (default: "*/10 * * * *")

## Local Development

```bash
# Install dependencies
npm install

# Create .env file with your credentials
cp .env.example .env

# Run the server
npm start
```

## Sync Schedule

By default, syncs run every 10 minutes. Customize with `CRON_SCHEDULE` env var:
- `*/5 * * * *` - Every 5 minutes
- `*/15 * * * *` - Every 15 minutes
- `0 * * * *` - Every hour
