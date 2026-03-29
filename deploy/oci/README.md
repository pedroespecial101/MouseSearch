# MouseSearch OCI A1 Deployment

This bundle targets the OCI host `pedroserve02-A1` and follows the same conventions used by the existing OCI service stacks:

- source checkout at `/home/ubuntu/projects/MouseSearch`
- persistent bind mounts under `/opt/appdata/mousesearch`
- private access through a dedicated Tailscale sidecar
- no `/downloads` mount and no `AUTO_ORGANIZE_*` settings in the first rollout

Related docs:

- API and MCP usage: [docs/api-mcp-usage.md](/Users/petetreadaway/Projects/MouseSearch/docs/api-mcp-usage.md)
- API and MCP rollout tests: [docs/api-mcp-test-plan.md](/Users/petetreadaway/Projects/MouseSearch/docs/api-mcp-test-plan.md)

## Files

- `docker-compose.yml`: OCI stack using an on-host ARM64 build
- `mousesearch.env.example`: server-local environment template
- `serve.json`: Tailscale Serve config for `https://mousesearch.bearded-pomano.ts.net` and the optional MCP hostname `https://mousesearch-mcp.bearded-pomano.ts.net`
- `install-on-server.sh`: provisions `/opt/appdata/mousesearch` and installs the deployment files

## Provision The Host

SSH to the host:

```bash
ssh ubuntu@pedroserve02-a1
```

Clone or update the repo on the server:

```bash
mkdir -p ~/projects
cd ~/projects
git clone https://github.com/pedroespecial101/MouseSearch.git
cd MouseSearch
```

Install the deployment files:

```bash
sudo ./deploy/oci/install-on-server.sh
```

This creates:

- `/opt/appdata/mousesearch/docker-compose.yml`
- `/opt/appdata/mousesearch/serve.json`
- `/opt/appdata/mousesearch/.env`
- `/opt/appdata/mousesearch/data`
- `/opt/appdata/mousesearch/tailscale`

## Configure Server-Local Secrets

Edit `/opt/appdata/mousesearch/.env` and replace the placeholder values:

- `TS_AUTHKEY`
- `QUART_SECRET_KEY`
- `MAM_ID`
- `TORRENT_CLIENT_URL`
- `TORRENT_CLIENT_USERNAME`
- `TORRENT_CLIENT_PASSWORD`

Keep this file server-local and out of git.

Set `PUID` and `PGID` to the real host account you want owning the data files. On `pedroserve02-A1`, `ubuntu` is `1001:1001`.

For `TORRENT_CLIENT_URL`, prefer a Tailscale IP or another address resolvable from inside the container namespace, and omit any trailing slash. During deployment testing on this host, `http://100.85.214.86:18080` worked while `http://optiplex3070-1:18080/` did not.

If you want the optional HTTP MCP layer, leave `MOUSESEARCH_API_BASE_URL` at `http://127.0.0.1:5000/api/v1` unless you have a reason to change it. The MCP profile shares the same Tailscale namespace and proxies HTTP on port `8765`.

## Validate And Start

Render and validate the stack:

```bash
cd /opt/appdata/mousesearch
docker compose --env-file .env config
docker compose --env-file .env build
```

Start the stack:

```bash
docker compose --env-file .env up -d
```

Start the optional HTTP MCP profile:

```bash
docker compose --env-file .env --profile mcp up -d
```

## Runtime Validation

Check container state:

```bash
docker ps --filter name=ts-mousesearch --filter name=mousesearch
docker compose --env-file .env logs --tail 100
```

Validate the app inside the sidecar namespace:

```bash
docker exec ts-mousesearch curl -I -H 'Host: mousesearch.bearded-pomano.ts.net' http://127.0.0.1:5000
docker exec ts-mousesearch tailscale status
docker exec ts-mousesearch tailscale serve status
```

Then open the app from another Tailscale-connected device:

```text
https://mousesearch.bearded-pomano.ts.net
```

If the MCP profile is enabled, its Tailscale URL is:

```text
https://mousesearch-mcp.bearded-pomano.ts.net
```

## First-Run Application Scope

This first OCI rollout is intentionally narrow:

- use a remote torrent client
- do not enable `AUTO_ORGANIZE_ON_ADD`
- do not enable `AUTO_ORGANIZE_ON_SCHEDULE`
- do not add a `/downloads` mount

Once the UI loads, finish the runtime configuration through env and/or the Settings page, then smoke-test:

1. MAM connection
2. Torrent client connection
3. A basic search
4. One manual add-to-client action
