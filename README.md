# UK Financial Regulation MCP

MCP server for the UK Financial Conduct Authority (FCA) Handbook. Provides tools to query FCA rules, guidance, evidential provisions, directions, and enforcement actions.

Tool prefix: `gb_fin_`

## Tools

| Tool | Description |
|------|-------------|
| `gb_fin_search_regulations` | Full-text search across FCA Handbook provisions |
| `gb_fin_get_regulation` | Get a specific provision by sourcebook + reference (e.g., SYSC 3.2.1R) |
| `gb_fin_list_sourcebooks` | List all FCA sourcebooks with descriptions |
| `gb_fin_search_enforcement` | Search FCA enforcement actions (fines, bans, restrictions) |
| `gb_fin_check_currency` | Check if a provision reference is currently in force |
| `gb_fin_about` | Return server metadata and tool list |

## Provision Types

| Suffix | Meaning |
|--------|---------|
| R | Rule |
| G | Guidance |
| E | Evidential provision |
| D | Direction |

## Setup

### Prerequisites

- Node.js 20+
- A populated `data/fca.db` SQLite database (run `npm run ingest` after implementing the ingestion script)

### Build

```bash
npm install
npm run build
```

### Run (stdio)

```bash
node dist/src/index.js
```

### Run (HTTP server)

```bash
PORT=3000 node dist/src/http-server.js
```

The HTTP server exposes:
- `POST /mcp` — MCP Streamable HTTP endpoint
- `GET /health` — liveness probe

### Docker

```bash
docker build -t uk-financial-regulation-mcp .
docker run --rm -p 3000:3000 -e FCA_DB_PATH=/app/data/fca.db uk-financial-regulation-mcp
```

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `FCA_DB_PATH` | `data/fca.db` | Path to the SQLite database |
| `PORT` | `3000` | HTTP server port |

## Data Source

FCA Handbook: https://www.handbook.fca.org.uk/

Ingestion is handled by `scripts/ingest.ts` (not yet implemented — see TODO in that file).

## License

Apache-2.0 — Ansvar Systems AB
