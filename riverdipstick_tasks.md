# River Dipstick Task List

**Living document.** This is the single source of truth for work on the project. Keep it updated as we discover issues, complete items, or identify new goals.

Last updated: June 2026

---

## High Priority (Current Focus)

**Recently completed (May–June 2026):**
- Pressure Trend feature (dual-axis + hourly smoothing)
- Main river level line mild smoothing (Altair transform_window)
- Good Fishing Bands restored
- Custom favicon + Ghostty + tmux clipboard fixes
- Successful deploy to LIVE

---

- Pressure Trend + Level Line Polish (remaining items)
  - Test full interaction between Pressure Trend + Extend Chart + Rain History toggles + Good Band
  - Evaluate current smoothing strength on noisy low-flow stations (e.g. Great Musgrave) vs moving periods
  - Final visual tweaks if needed when all overlays are active

- ~~Good Fishing Bands (Find G Spot)~~ ✅ — Fully working (June 2026)

- ~~Once pressure feature + good bands are stable: commit, deploy to LIVE, and update task list~~ ✅ (Successfully deployed to live June 2026)

---

## Development Environment & DX

- Create a proper local development experience (currently painful to run outside full Docker stack)
  - Add `docker-compose.dev.yml` or dev overrides for faster iteration
  - Provide a simple way to run the dashboard locally against the dev database
  - Make it easy to run the collector manually with verbose/debug output
  - Document the "local dev loop" clearly in README or a `DEVELOPMENT.md`

- Improve environment and secrets handling
  - Create `.env.example` with all required variables and sensible comments
  - Review and clean up current `.env` usage across services

- Better dependency management
  - Pin versions in `requirements.txt` (currently extremely loose)
  - Consider moving to `pyproject.toml` + pip-tools or uv for reproducibility

- Add a lightweight task runner (e.g. `justfile` or Makefile) for common operations:
  - `just up`, `just logs`, `just collector-once`, `just db-shell`, `just assess-db`, etc.

- Improve local database access and debugging tools

---

## Engineering Best Practices & Reliability

- Database schema management (critical)
  - Stop manually creating tables in production (see `pressure_forecasts`)
  - Introduce proper migrations (simple timestamped SQL files or Alembic)
  - Add schema version tracking

- Observability & operations
  - Improve structured logging (especially in collector)
  - Add basic health endpoints or status checks for dashboard + collector
  - Better log rotation / management for the persistent log volumes
  - Consider lightweight monitoring (e.g. simple uptime + error alerting)

- Code quality & structure
  - Dashboard.py is becoming a god file — consider extracting data access, chart builders, and components
  - Add basic linting + formatting (ruff + black or similar)
  - Introduce a minimal test harness (even if just smoke/integration tests against a test DB)
  - Reduce duplication of DB connection strings and init logic

- Deployment & release process
  - Document current DEV vs LIVE deployment workflow
  - Add a simple staging environment (ideally on the new home lab server)
  - Consider basic CI (even GitHub Actions lint + build check)

- Backup & recovery
  - Implement automated PostgreSQL backups (especially for the VPS)
  - Document restore procedures

---

## Infrastructure & Automation (Medium Term Strategic Goals)

- Home lab server project
  - Procure and install new dedicated dev/staging server in the home lab
  - Decide on hardware (Mini PC? Used enterprise? NUC-style?)
  - Set up Proxmox or plain Debian/Ubuntu base OS
  - Document the new server role vs the existing "wintermute" home dev server and the Fasthost VPS

- Ansible automation initiative
  - ✅ Started `ansible/` directory in the repo
  - ✅ Basic inventory + deploy playbook created (git pull + docker compose restart/rebuild)
  - Create playbooks/roles for:
    - Base server hardening + Docker installation
    - PostgreSQL setup (dev + prod patterns)
    - Deploying the full River Dipstick stack (with secrets management using ansible-vault)
    - Log rotation, backup jobs, and monitoring setup
  - Move away from manual Docker Compose management on the home server
  - Use Ansible for consistent environment setup between home lab and VPS
  - Document usage in AGENTS.md (in progress)

- Long-term infrastructure vision
  - Clarify the three environments: Home Lab Dev/Staging, Home "Wintermute" (current), and Fasthost LIVE
  - Define promotion pipeline (home lab → home dev → VPS)
  - Evaluate whether the current home dev server can be repurposed or retired

- Related side project
  - Mini PC integration into Proxmox cluster (already noted in backlog)

---

## Features & Enhancements

- Moon phase indicator (small, tasteful addition — medium priority)
- Add River Liddel improvements (SEPA data quality, better station ordering)
- Better source-to-sea station ordering logic across all rivers (general improvement)
- Simple About page enhancements (already has a basic one)
- Explore long-term ML predictions using historical pressure + level data (backlog / speculative)

---

## Technical Debt & Known Issues

- ~~"Find G Spot" toggle + `rules.json` had broken/missing rendering~~ ✅ (working on dev as of 30 May)
- ~~Default Streamlit favicon showing on bookmarks~~ ✅ (working in Chrome)
- Hardcoded Docker service names in connection strings (`wintermute-db` vs `db`)
- Crude 60-second full-page auto-refresh in Streamlit
- Multiple references in logs and old scripts to retired ML / dynamic G-Spot systems (cleanup)
- Collector runs a very basic `while true; sleep 900` loop with no proper process supervision
- No `.gitignore` protection for certain generated data files (station_coords_cache.json is already ignored)

---

## Process

- Keep this file as the single source of truth.
- Update it immediately when new bugs, ideas, or technical debt are discovered.
- Use the `todo_write` tool during sessions for fine-grained in-progress tracking.
- Before starting work on anything, re-read the relevant section of this file.

---

**Philosophy reminder**: Every change should be judged against "does this make the site more useful to an angler at the riverbank, or reduce long-term maintenance burden?" If it does neither, think twice.