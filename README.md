# TrendTrack MCP Search CLI

Zero-dependency Python CLI for querying **TrendTrack** via its Streamable HTTP MCP server (`https://api.trendtrack.io/v1/mcp`, MCP `2026-07-28` stateless transport) and REST API (`https://api.trendtrack.io`).

## Setup

1. Store your TrendTrack API key (from TrendTrack **Settings → API**) outside the repository so it is never committed:
   ```bash
   printf '%s\n' 'YOUR_TRENDTRACK_API_KEY' | ./trendtrack auth set --stdin
   ```
   Or export `TRENDTRACK_API_KEY` in your shell / `.env` (gitignored).

2. Verify connectivity and list all 54 live MCP tools:
   ```bash
   ./trendtrack freshness
   ./trendtrack tools
   ```

## Usage

```bash
# Discover & Search via MCP
./trendtrack search-shops "sleep aids" --limit 5
./trendtrack search-products "collagen" --limit 5
./trendtrack search-landing-pages "sleep" --limit 5
./trendtrack winning-products "pet supplies"
./trendtrack product-radar "posture"
./trendtrack similar-shops "glossier.com"
./trendtrack creative-pack "skincare"

# Ad & Entity Search via MCP
./trendtrack lookup "gymshark.com"
./trendtrack search-ads "collagen gummies" --limit 10
./trendtrack search-tiktok "collagen gummies" --type ad --limit 10
./trendtrack search-google-ads "nike" --limit 10

# Competitor & Brandtracker Intelligence via MCP
./trendtrack brief "gymshark.com"
./trendtrack scan-ad "<meta_ad_url_or_id>"
./trendtrack radar
./trendtrack tracked
./trendtrack credits

# Call any of the 54 MCP tools directly
./trendtrack tools --schema search_shops
./trendtrack call search_shops --json '{"query":"collagen gummies","limit":5}'
```

## Tests

```bash
python3 -m unittest discover -s tests -v
```
