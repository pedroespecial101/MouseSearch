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
- `serve.json`: Tailscale Serve config for `https://mousesearch.bearded-pomano.ts.net` and the optional MCP path at `https://mousesearch.bearded-pomano.ts.net/mcp`
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

If you want the optional HTTP MCP layer, leave `MOUSESEARCH_API_BASE_URL` at `http://127.0.0.1:5000/api/v1` unless you have a reason to change it. The MCP profile shares the same Tailscale namespace and is exposed at `/mcp` on the main Tailscale hostname.

MouseSearch v1.0.0 adds optional Mousehole cookie sync, Hardcover enrichment, auto-task webhooks, haptics, and a personal Freeleech minimum-size gate. The OCI example keeps these inert by default: Mousehole is disabled, webhooks have no URL, and Hardcover has no token. Add secrets to the server-local `.env` only when you intend to enable the related feature.

## Rollback Backup Before Upgrade

Before upgrading an existing deployment, capture the current commit and server state:

```bash
cd /home/ubuntu/projects/MouseSearch
CURRENT_SHA="$(git rev-parse HEAD)"
BACKUP_DIR="/mnt/pedrostore01/backups/mousesearch/pre-v1.0.0-$(date +%Y%m%d%H%M%S)"
sudo install -d -m 0700 "$BACKUP_DIR"
printf '%s\n' "$CURRENT_SHA" | sudo tee "$BACKUP_DIR/git-sha.txt" >/dev/null
sudo cp -f /opt/appdata/mousesearch/docker-compose.yml "$BACKUP_DIR/docker-compose.yml"
sudo cp -f /opt/appdata/mousesearch/serve.json "$BACKUP_DIR/serve.json"
sudo cp -f /opt/appdata/mousesearch/.env "$BACKUP_DIR/env"
sudo cp -a /opt/appdata/mousesearch/data "$BACKUP_DIR/data"
```

To roll back, restore those files and rebuild from the recorded SHA:

```bash
cd /home/ubuntu/projects/MouseSearch
ROLLBACK_DIR="/mnt/pedrostore01/backups/mousesearch/pre-v1.0.0-YYYYMMDDHHMMSS"
git fetch origin
git checkout "$(sudo cat "$ROLLBACK_DIR/git-sha.txt")"
sudo cp -f "$ROLLBACK_DIR/docker-compose.yml" /opt/appdata/mousesearch/docker-compose.yml
sudo cp -f "$ROLLBACK_DIR/serve.json" /opt/appdata/mousesearch/serve.json
sudo cp -f "$ROLLBACK_DIR/env" /opt/appdata/mousesearch/.env
sudo rm -rf /opt/appdata/mousesearch/data
sudo cp -a "$ROLLBACK_DIR/data" /opt/appdata/mousesearch/data
cd /opt/appdata/mousesearch
docker compose --env-file .env up -d --build
```

Keep `pre-v1.0.0-*` backups for 14 stable days, then remove them after confirming the app, `/api/v1/info`, MAM status, and torrent-client status are healthy.

## Validate And Start

Render and validate the stack:

```bash
cd /opt/appdata/mousesearch
docker compose --env-file .env config
docker compose --env-file .env build
```

Start the stack:

```bash
docker compose --env-file .env up -d --build
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
https://mousesearch.bearded-pomano.ts.net/mcp
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
