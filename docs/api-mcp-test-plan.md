# API And MCP Test Plan

This plan is intended for the OCI deployment on `pedroserve02-a1`, but most checks also work locally.

## Scope

Validate:

- JSON API discovery and status endpoints
- JSON search behavior
- JSON add-to-client workflow
- HTTP MCP server availability and tool execution
- Tailscale exposure for both app and MCP endpoints

## Preconditions

- MouseSearch app stack is running
- Optional `mousesearch-mcp` profile is enabled
- MAM is configured and connected
- Torrent client is configured and connected
- Tailscale Serve is active for:
  - `https://mousesearch.bearded-pomano.ts.net`
  - `https://mousesearch.bearded-pomano.ts.net/mcp`

## Test Cases

### API-01 Discovery

Goal: confirm the API is available and self-describing.

Command:

```bash
curl -fsS https://mousesearch.bearded-pomano.ts.net/api/v1/info
```

Pass criteria:

- HTTP 200
- JSON includes `service`, `api_version`, and `agent_instructions`

### API-02 MAM Status

Goal: confirm MyAnonamouse connectivity.

Command:

```bash
curl -fsS https://mousesearch.bearded-pomano.ts.net/api/v1/mam/status
```

Pass criteria:

- HTTP 200
- JSON includes `"status": "connected"`

### API-03 Torrent Client Status

Goal: confirm torrent client connectivity.

Command:

```bash
curl -fsS https://mousesearch.bearded-pomano.ts.net/api/v1/client/status
```

Pass criteria:

- HTTP 200
- JSON indicates success/connected status

### API-04 Search

Goal: confirm search returns ranked JSON results.

Command:

```bash
curl -fsS "https://mousesearch.bearded-pomano.ts.net/api/v1/mam/search?query=harry+potter&hide_downloaded=true"
```

Pass criteria:

- HTTP 200
- JSON includes `ok: true`
- JSON includes `results`
- `results` is an array

### API-05 Add To Client

Goal: confirm programmatic add works end-to-end.

Method:

1. Run API-04 and capture one result with `download_link`, `title`, `author`, and `id`
2. POST that result to `/api/v1/client/add`

Command shape:

```bash
curl -fsS -X POST https://mousesearch.bearded-pomano.ts.net/api/v1/client/add \
  -H 'Content-Type: application/json' \
  -d '{ ... }'
```

Pass criteria:

- HTTP 200
- response includes success message
- response includes a hash, or MID resolution works via `/api/v1/client/resolve_mid`

### MCP-01 HTTP Endpoint Reachability

Goal: confirm the HTTP MCP endpoint is reachable over Tailscale.

Method:

- connect a FastMCP client to `https://mousesearch.bearded-pomano.ts.net/mcp`

Pass criteria:

- client connects successfully
- tool list can be retrieved

### MCP-02 Discovery Tool

Goal: confirm the MCP layer can return server instructions.

Method:

- call `server_info`

Pass criteria:

- tool call succeeds
- response payload includes API discovery info

### MCP-03 Search Tool

Goal: confirm MCP search works through the JSON API.

Method:

- call `search_torrents` with query `harry potter`

Pass criteria:

- tool call succeeds
- result payload includes ranked search results

### MCP-04 Add Tool

Goal: confirm MCP add-to-client works.

Method:

- use a result from `search_torrents`
- call `add_torrent`

Pass criteria:

- tool call succeeds
- result payload indicates add success

## Recording Results

For each rollout, record:

- test timestamp
- branch/commit deployed
- app URL used
- MCP URL used
- pass/fail for each test case
- any follow-up issues created in `bd`
