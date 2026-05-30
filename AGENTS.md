# River Dipstick — Agent Instructions

## Project Philosophy
**"Will it fish tomorrow?"**

A deliberately simple, low-maintenance river level dashboard for NW England fly anglers (primarily Lancashire / Ribble Valley area).

**Core principle**: Keep it simple so the maintainer can actually go fishing.

Real-world testing showed that complex ML predictions were less useful than clear visuals + human judgment ("Mk1 Eyeball"). The old dynamic G-Spot system and ML prediction code have been retired. We now rely on:
- Static "Good Fishing Bands" defined in `rules.json`
- Clean charts with rainfall overlay
- The "Extend Chart" toggle for 2-day eyeball forecasting
- Pressure trend data (work in progress)

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

## Known Technical Debt & Gotchas (as of 30 May 2026)
- **Pressure Trend feature** (current high-priority work):
  - Data collection works and is writing to `pressure_forecasts`.
  - The table exists in the live DB but is **not** created inside `get_readings.py:init_db()`.
  - `dashboard.py:get_pressure_data()` has a column alias bug (`forecast_date as Date` then references `df['date']`).
  - Pressure values are concatenated onto the main level chart with no dual-axis handling (unlike rainfall).
- **"Find G Spot" / Good Fishing Bands**:
  - Toggle exists in the sidebar.
  - `rules.json` contains `good_min`/`good_max` for several stations.
  - README documents it as a shipped feature.
  - **No rendering logic currently exists** in `dashboard.py`.
- Connection strings are duplicated between files and hardcoded to Docker service names (`wintermute-db` / `db`).
- Auto-refresh uses a blunt 60-second full `st.rerun()`.
- Several log files and old utility scripts reference retired ML / G-Spot systems (harmless but noisy).

## Long-term Direction
See `riverdipstick_tasks.md` (High Priority / Medium Priority / Backlog sections) and the Future Plans section of `README.md`.

Current focus: Finish the Pressure Trend feature properly, test toggle interactions, then deploy.

## General Working Style
- Be direct and concise.
- Flag anything that would increase long-term maintenance burden.
- When in doubt, favor the simplest solution that delivers value to the angler at the riverbank.
- Keep documentation (especially the task list) in sync with reality.

---

This file is loaded automatically into the system prompt for any session started inside the `/opt/river-dipstick` tree. Update it as the project evolves.