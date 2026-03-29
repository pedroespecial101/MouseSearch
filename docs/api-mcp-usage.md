# API And MCP Usage

MouseSearch exposes two automation-facing interfaces intended for private use on trusted networks such as Tailscale:

- JSON API under `/api/v1/*`
- HTTP MCP server exposed by [mcp_server.py](/Users/petetreadaway/Projects/MouseSearch/mcp_server.py)

## JSON API

Base URL examples:

- local app: `http://127.0.0.1:5000/api/v1`
- OCI Tailscale app: `https://mousesearch.bearded-pomano.ts.net/api/v1`

Recommended workflow:

1. Call `/info` to discover the API surface and the intended agent workflow.
2. Call `/mam/status` and `/client/status` before doing any work.
3. Search with `/mam/search`.
4. Add a torrent with `/client/add` using the result's `download_link`.
5. If no hash is returned immediately, resolve it with `/client/resolve_mid`.
6. Monitor progress with `/client/info/<hash>` or `/client/info/batch`.

### Endpoints

- `GET /api/v1/info`
- `GET /api/v1/mam/status`
- `GET /api/v1/mam/user_data`
- `GET /api/v1/mam/autosuggest`
- `GET /api/v1/mam/search`
- `GET /api/v1/client/status`
- `GET /api/v1/client/categories`
- `POST /api/v1/client/add`
- `POST /api/v1/client/resolve_mid`
- `GET /api/v1/client/info/<hash>`
- `POST /api/v1/client/info/batch`

### Search Example

```bash
curl "https://mousesearch.bearded-pomano.ts.net/api/v1/mam/search?query=harry+potter&hide_downloaded=true"
```

### Add Example

```bash
curl -X POST "https://mousesearch.bearded-pomano.ts.net/api/v1/client/add" \
  -H 'Content-Type: application/json' \
  -d '{
    "torrent_url": "https://www.myanonamouse.net/tor/download.php/example",
    "title": "Example Title",
    "author": "Example Author",
    "id": "1234567",
    "category": "audiobooks",
    "download_link": "https://www.myanonamouse.net/tor/download.php/example"
  }'
```

### Search Parameters

`/api/v1/mam/search` accepts the same filter names used by the UI querystring. Common ones:

- `query`
- `hide_downloaded`
- `searchType`
- `search_scope`
- `main_cat`
- `category_ids`
- `flag_ids`
- `language_ids`
- `search_in_title`
- `search_in_author`
- `search_in_series`
- `search_in_narrator`
- `min_seeders`
- `max_seeders`

Boolean values should be sent as `true` or `false`.

## HTTP MCP

Base URL examples:

- local MCP server: `http://127.0.0.1:8765/mcp`
- OCI Tailscale MCP: `https://mousesearch.bearded-pomano.ts.net/mcp`

The server is designed to be agent-friendly and wraps the JSON API instead of reimplementing tracker or torrent-client behavior.

### Recommended Agent Flow

1. `server_info`
2. `mam_status`
3. `client_status`
4. `search_torrents`
5. `add_torrent`
6. `resolve_mid` if needed
7. `torrent_info` or `torrent_info_batch`

### Exposed MCP Tools

- `server_info`
- `mam_status`
- `mam_user_data`
- `client_status`
- `client_categories`
- `search_torrents`
- `add_torrent`
- `resolve_mid`
- `torrent_info`
- `torrent_info_batch`

### Python Client Example

```python
import asyncio
from fastmcp import Client

client = Client("https://mousesearch.bearded-pomano.ts.net/mcp")

async def main():
    async with client:
        info = await client.call_tool("server_info", {})
        print(info)

        results = await client.call_tool(
            "search_torrents",
            {
                "query": "harry potter",
                "filters": {"hide_downloaded": True}
            },
        )
        print(results)

asyncio.run(main())
```

## Deployment Notes

OCI-specific setup, Compose profile usage, and Tailscale Serve details live in [deploy/oci/README.md](/Users/petetreadaway/Projects/MouseSearch/deploy/oci/README.md).

The rollout and validation checklist for these interfaces lives in [docs/api-mcp-test-plan.md](/Users/petetreadaway/Projects/MouseSearch/docs/api-mcp-test-plan.md).
