# River Dipstick

**“Will it fish tomorrow?”** – The river dipstick for the North West UK.

A clean, fast, no-nonsense river level dashboard built by (and for) NW England fly anglers.

Live at: **[https://riverdipstick.uk](https://riverdipstick.uk)**

---

### Current Features

- Real-time river levels from Environment Agency gauges (updated every 15 mins)
- 7-day rainfall history overlay (extendable to 365)
- Clean tables + beautiful Altair graphs
- **New: Static "Good Fishing Band"** — light green horizontal target zone on graphs (based on `rules.json`)
- "Predict Level" toggle now extends graphs **2 days into the future** for Mk1 eyeballing
- Colour-coded table + source-to-sea sorting
- Mobile-friendly dark mode

---

### Philosophy

This site is deliberately simple. After months of testing, real-world experience showed that complex (ML) predictions weren't as useful as hoped. We now trust the **Mark 1 Eyeball** + clear visual targets.

The goal: Give fellow local anglers a fast, reliable tool to decide **"Is it worth going?"**

---

### Tech Stack

- **Frontend**: Streamlit (fast & Python-native)
- **Backend**: PostgreSQL
- **Data**: UK Environment Agency real-time API
- **Visualisation**: Altair
- **Deployment**: Docker + docker-compose (home dev server + Fasthost VPS)
- **Philosophy**: Keep it simple = more time for going fising.

---

### Project Status (May 2026)

- ML prediction system gracefully retired (code preserved)
- Old dynamic G-Spot system replaced with reliable static green fishing bands
- Collector now only runs essential data collection

---

### Future Plans

- Add River Liddel (SEPA data)
- Simple About page
- More rivers / user-requested features from Ribblesdale Angling Society and others


