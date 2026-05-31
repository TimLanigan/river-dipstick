# River Dipstick — Agent Instructions

## Project Philosophy
**"Will it fish tomorrow?"**

A deliberately simple, low-maintenance river level dashboard for NW England fly anglers (primarily Lancashire / Ribble Valley area).

**Core principle**: Keep it simple so the maintainer can actually go fishing.

Real-world testing showed that complex ML predictions were less useful than clear visuals + human judgment ("Mk1 Eyeball"). The old dynamic G-Spot system and ML prediction code have been retired. We now rely on:
- Static "Good Fishing Bands" defined in `rules.json`
- Clean charts with rainfall overlay
- The "Extend Chart" toggle for 2-day eyeball forecasting
- Pressure trend data (working well with hourly smoothing + dual-axis)

The goal is a fast, reliable answer to "Is it worth going?" with minimal ongoing complexity.

## Key Files & Structure
- `riverdipstick_tasks.md` — **Living task list and source of truth**. Bugs, features, technical debt, and long-term goals live here.
- `app/dashboard.py` — Main Streamlit application (271 LOC)
- `app/get_readings.py` — 15-minute data collector (EA real-time + SEPA + pressure from Open-Meteo)
- `app/river_reference.py` — Station loading, source-to-sea sorting logic
- `app/data/rules.json` — Static good fishing bands per station
- `app/data/stations.csv` — Canonical station list (with lat/lon/rainfall_id)
- `docker-compose.yml` — Defines `db`, `dashboard`, and `collector` services
- `README.md` — High-level overview and history

## Development Workflow
- Primary way to run: `docker-compose up`
- Code changes in `app/` are live-mounted into the containers
- Collector runs in an infinite 15-minute loop inside its container
- Database is PostgreSQL 16 (health-checked)
- Useful utility: `app/utility/assess_db_status.py` (run inside the collector container or with correct DB connection)

## Instructions for This Agent
- **Task list discipline**: Before starting any significant work, review `riverdipstick_tasks.md`. When you discover bugs, complete work, or identify new goals (short or long term), **immediately update the task list** using precise edits. This file is the persistent memory of what needs doing.
- Use the internal `todo_write` tool for breaking down complex, multi-step work within a session.
- Prefer small, focused changes over large refactors unless explicitly requested.
- When making changes, briefly explain the "why", especially if it touches the simplicity/maintainability philosophy.
- Verify behavior where practical (check collector logs, inspect dashboard rendering, query the DB, run utilities).
- The project is intentionally small. Avoid adding new dependencies or significant complexity without strong justification.

## Known Technical Debt & Gotchas (as of June 2026)

**Still relevant:**
- Connection strings are duplicated between files and hardcoded to Docker service names (`wintermute-db` / `db`).
- Auto-refresh uses a blunt 60-second full `st.rerun()`.
- Several log files and old utility scripts reference retired ML / dynamic G-Spot systems (harmless but noisy).

**Recently resolved (May–June 2026):**
- Pressure Trend feature (dual-axis + hourly smoothing) — working well
- Good Fishing Band rendering — restored and functional
- Default Streamlit favicon — replaced with custom favicon support
- Ghostty + tmux clipboard — resolved with OSC52 + Ghostty settings (see section below)

## Long-term Direction
See `riverdipstick_tasks.md` (High Priority / Medium Priority / Backlog sections) and the Future Plans section of `README.md`.

Current focus: Test and polish interactions between all toggles (Pressure, Rain, Good Band, Extend Chart), then move into longer-term DX and infrastructure improvements (Ansible, home lab server).

## General Working Style
- Be direct and concise.
- Flag anything that would increase long-term maintenance burden.
- When in doubt, favor the simplest solution that delivers value to the angler at the riverbank.
- Keep documentation (especially the task list) in sync with reality.

## Deployment / Production Workflow

**Live server details (as of June 2026):**
- Hostname: `ubuntu`
- User: `root`
- Project path: `/opt/river-dipstick`
- Connection method (current): VSCode embedded terminal (found to be more reliable than plain Ghostty + tmux for clipboard)
- Git remote: `https://github.com/TimLanigan/river-dipstick.git`

**Standard deployment steps (manual) — observed May 2026:**
1. Connect to live server (currently using VSCode embedded terminal — found more reliable for clipboard than Ghostty + tmux)
2. `cd /opt/river-dipstick`
3. `git status` — expect "working tree clean" + "behind by X commits"
4. `git pull` (fast-forward when possible)
5. Restart services:
   - For pure code changes (most common case): `docker-compose restart dashboard` (or `docker-compose up -d dashboard`)
   - Full rebuild only needed for dependency or Dockerfile changes: `docker-compose up -d --build`
   - Collector can usually be restarted separately if needed: `docker-compose restart collector`
6. Verify:
   - `docker ps`
   - Hard refresh the live site (important after code changes)
   - Check https://riverdipstick.uk
   - Optionally check collector logs: `docker logs -f wintermute-collector`

**Important observation:**
- The `dashboard` service has `./app:/app` volume mounted, so most Python changes (including dashboard.py and get_readings.py) are picked up without a full image rebuild.
- A simple `docker compose restart dashboard` is often sufficient after `git pull`.

**Critical workflow note:**
- The project uses the modern Docker Compose plugin: `docker compose` (no hyphen), **not** the old standalone `docker-compose`.
- Always use `docker compose` on both dev and live servers.

**Notes for future Ansible migration:**
- ✅ Initial Ansible structure created under `ansible/` (inventory, playbooks, roles)
- ✅ Basic `playbooks/deploy.yml` that performs git operations + conditional `docker compose` restart/rebuild
- Use `ansible-playbook playbooks/deploy.yml -l live -e "version=main" --ask-vault-pass` from dev machine after `git push`
- Deployment is currently fully manual on live (being replaced).
- No automated health checks or rollback yet.
- Services are managed via `docker compose` (not systemd).
- Would benefit from:
  - Idempotent playbook for `git pull + restart` (in progress)
  - Separate handling for code changes vs image rebuilds (basic logic in v1 playbook)
  - Basic verification steps after deploy (containers healthy + site check)
  - Full server provisioning roles (Docker, users, firewall) in future iterations

See `ansible/README.md` for quick start and current status.

**Ghostty + Tmux Copy/Paste Setup (Confirmed Working — June 2026)**

The user uses **Ghostty** as their main terminal on macOS and runs tmux on remote Linux servers (dev + live). They are happy with Ghostty overall.

Native copy/paste was previously unreliable until the following setup was applied.

**Working configuration:**

**Ghostty (macOS side) — `~/.config/ghostty/config`:**
```ini
copy-on-select = clipboard
clipboard-read = allow
clipboard-write = allow
```

**tmux (remote server side) — `~/.tmux.conf`:**
```tmux
set -g allow-passthrough on
set -g set-clipboard on
```

**After making changes:**
- Fully restart Ghostty (changes don't apply to existing windows).
- Inside an existing tmux session: `tmux source-file ~/.tmux.conf`

This combination enables reliable OSC52 clipboard passthrough, which works very well with Ghostty. The user is happy with Ghostty overall and intends to keep using it.

**Tip for future machines:** These are the minimal settings needed for good copy-paste when using Ghostty + tmux over SSH.

**Notes:**
- Always pull on live only after changes have been pushed from dev.
- Prefer small, focused deploys when possible.
- Update this section in AGENTS.md after any significant changes to the deployment process.

---

## Handoff Note – June 2026

Homelab infrastructure, Ansible learning, and general homelab management work has moved to a separate repository and agent context:

**Homelab repo location on management node:** `~/homelab` (on the `homelab-mgmt` LXC)

All future work related to:
- Homelab architecture
- Ansible training course and automation
- Obsidian vault as a service
- Multi-server / Proxmox strategies

...should be discussed in the homelab agent context (by running `grok` from inside `~/homelab` on the management node).

This separation exists to prevent context pollution between the River Dipstick application work and broader homelab infrastructure work.

This file (River Dipstick AGENTS.md) should now stay focused on the application itself.

---

This file is loaded automatically into the system prompt for any session started inside the `/opt/river-dipstick` tree. Update it as the project evolves.