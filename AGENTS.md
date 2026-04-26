# Agent Instructions

This project uses **bd** (beads) for issue tracking. Run `bd onboard` to get started.

## OCI Deployment Notes

- The target OCI host for this project is `pedroserve02-A1` (`arm64`, 4 OCPU, 24 GB RAM).
- Prefer Tailscale access: `ssh ubuntu@pedroserve02-a1`.
- Read global host guidance in [~/.codex/AGENTS.md](/Users/petetreadaway/.codex/AGENTS.md) and the detailed server docs in `/Users/petetreadaway/Projects/OCI-Server-Config/` before making deployment changes.
- The repo-owned OCI deployment bundle lives in `/Users/petetreadaway/Projects/MouseSearch/deploy/oci/`.
- On that host, source repos belong in `~/projects/` and persistent bind mounts belong in `/opt/appdata/<app-name>/`.
- For MouseSearch specifically, default to building from source on the OCI host unless you have confirmed a `linux/arm64` image tag. The upstream `sevenlayercookie/mousesearch:latest` image was observed as `amd64`-only during planning.
- Prefer bind mounts for:
  - app state under `/opt/appdata/mousesearch/data`
  - downloads/library paths under a single shared mount if auto-organize or hardlink mode is enabled
- The live unRAID Grimmory handoff for `MAM-QBTorrent` is documented in [docs/grimmory-bookdrop-handoff.md](/Users/petetreadaway/Projects/MouseSearch/docs/grimmory-bookdrop-handoff.md).
- For that handoff, qBittorrent's completion hook only writes request files; host-side scripts do the hardlinking. Do not try to hardlink from `/downloads` to `/bookdrop` inside the qBittorrent container, because those are separate Docker bind mounts and can fail with `Cross-device link`.
- The one-minute catchall reconciler is intentional and should stay enabled alongside the immediate request watcher.
- MouseSearch runs on OCI, but live `MAM-QBTorrent` announces from unRAID. Do not enable the OCI-side Dynamic IP Updater for that client; use `/mnt/user/appdata/MAM-QBTorrent/scripts/update_mam_dynamic_seedbox.sh` on unRAID so MAM follows qBittorrent's egress IP/ASN.
- If deploying publicly, pause and confirm the exposure plan first. This host already runs several services, and tailnet-only access is often preferred.
- Before claiming a deployment is done, verify with `docker compose logs`, `docker ps`, and a real HTTP check against the chosen endpoint.

## Quick Reference

```bash
bd ready              # Find available work
bd show <id>          # View issue details
bd update <id> --claim  # Claim work atomically
bd close <id>         # Complete work
bd dolt push          # Push beads data to remote
```

## Beads Dolt Remote Setup

- This repo uses the same GitHub repository for both Git and Beads/Dolt sync.
- Dolt 1.81.10+ can store Dolt data on a dedicated Git ref inside a normal Git remote, so a separate DoltHub repo is not required.
- One-time setup for a fresh clone:

```bash
bd dolt remote add origin "$(git remote get-url origin)"
bd vc commit -m "Configure Beads Dolt remote"
bd dolt push
```

- If `git remote get-url origin` returns an SSH URL, that is fine. HTTPS also works as long as your Git credentials already work non-interactively.
- The Git remote must already exist and have at least one branch before you add it as a Dolt remote.

## Non-Interactive Shell Commands

**ALWAYS use non-interactive flags** with file operations to avoid hanging on confirmation prompts.

Shell commands like `cp`, `mv`, and `rm` may be aliased to include `-i` (interactive) mode on some systems, causing the agent to hang indefinitely waiting for y/n input.

**Use these forms instead:**
```bash
# Force overwrite without prompting
cp -f source dest           # NOT: cp source dest
mv -f source dest           # NOT: mv source dest
rm -f file                  # NOT: rm file

# For recursive operations
rm -rf directory            # NOT: rm -r directory
cp -rf source dest          # NOT: cp -r source dest
```

**Other commands that may prompt:**
- `scp` - use `-o BatchMode=yes` for non-interactive
- `ssh` - use `-o BatchMode=yes` to fail instead of prompting
- `apt-get` - use `-y` flag
- `brew` - use `HOMEBREW_NO_AUTO_UPDATE=1` env var

<!-- BEGIN BEADS INTEGRATION v:1 profile:full hash:f65d5d33 -->
## Issue Tracking with bd (beads)

**IMPORTANT**: This project uses **bd (beads)** for ALL issue tracking. Do NOT use markdown TODOs, task lists, or other tracking methods.

### Why bd?

- Dependency-aware: Track blockers and relationships between issues
- Git-friendly: Dolt-powered version control with native sync
- Agent-optimized: JSON output, ready work detection, discovered-from links
- Prevents duplicate tracking systems and confusion

### Quick Start

**Check for ready work:**

```bash
bd ready --json
```

**Create new issues:**

```bash
bd create "Issue title" --description="Detailed context" -t bug|feature|task -p 0-4 --json
bd create "Issue title" --description="What this issue is about" -p 1 --deps discovered-from:bd-123 --json
```

**Claim and update:**

```bash
bd update <id> --claim --json
bd update bd-42 --priority 1 --json
```

**Complete work:**

```bash
bd close bd-42 --reason "Completed" --json
```

### Issue Types

- `bug` - Something broken
- `feature` - New functionality
- `task` - Work item (tests, docs, refactoring)
- `epic` - Large feature with subtasks
- `chore` - Maintenance (dependencies, tooling)

### Priorities

- `0` - Critical (security, data loss, broken builds)
- `1` - High (major features, important bugs)
- `2` - Medium (default, nice-to-have)
- `3` - Low (polish, optimization)
- `4` - Backlog (future ideas)

### Workflow for AI Agents

1. **Check ready work**: `bd ready` shows unblocked issues
2. **Claim your task atomically**: `bd update <id> --claim`
3. **Work on it**: Implement, test, document
4. **Discover new work?** Create linked issue:
   - `bd create "Found bug" --description="Details about what was found" -p 1 --deps discovered-from:<parent-id>`
5. **Complete**: `bd close <id> --reason "Done"`

### Quality
- Use `--acceptance` and `--design` fields when creating issues
- Use `--validate` to check description completeness

### Lifecycle
- `bd defer <id>` / `bd supersede <id>` for issue management
- `bd stale` / `bd orphans` / `bd lint` for hygiene
- `bd human <id>` to flag for human decisions
- `bd formula list` / `bd mol pour <name>` for structured workflows

### Auto-Sync

bd automatically syncs via Dolt:

- Each write auto-commits to Dolt history
- Use `bd dolt push`/`bd dolt pull` for remote sync
- No manual export/import needed!

### Important Rules

- ✅ Use bd for ALL task tracking
- ✅ Always use `--json` flag for programmatic use
- ✅ Link discovered work with `discovered-from` dependencies
- ✅ Check `bd ready` before asking "what should I work on?"
- ❌ Do NOT create markdown TODO lists
- ❌ Do NOT use external issue trackers
- ❌ Do NOT duplicate tracking systems

For more details, see README.md and docs/QUICKSTART.md.

## Session Completion

**When ending a work session**, you MUST complete ALL steps below. Work is NOT complete until `git push` succeeds.

**MANDATORY WORKFLOW:**

1. **File issues for remaining work** - Create issues for anything that needs follow-up
2. **Run quality gates** (if code changed) - Tests, linters, builds
3. **Update issue status** - Close finished work, update in-progress items
4. **PUSH TO REMOTE** - This is MANDATORY:
   ```bash
   git pull --rebase
   bd dolt push
   git push
   git status  # MUST show "up to date with origin"
   ```
5. **Clean up** - Clear stashes, prune remote branches
6. **Verify** - All changes committed AND pushed
7. **Hand off** - Provide context for next session

**CRITICAL RULES:**
- Work is NOT complete until `git push` succeeds
- NEVER stop before pushing - that leaves work stranded locally
- NEVER say "ready to push when you are" - YOU must push
- If push fails, resolve and retry until it succeeds

<!-- END BEADS INTEGRATION -->
