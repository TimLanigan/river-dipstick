# River Dipstick  
**“Will it fish tomorrow?”** – The fly-fishing oracle for NW England  

Real-time river levels + rainfall + 24-hour Prophet ML predictions for the Eden, Ribble, Lune and Hodder.  
Built by one very tired angler at 7am after an all-nighter.

![Mega Chart](https://github.com/TimLanigan/river-dipstick/raw/main/screenshot-mega-chart.png?raw=true)  
*Blue = real EA data • Grey = past prediction accuracy • Violet = next 24 hrs • Light-blue bars = rain*

### The Legend

— Tim Lanigan, 2025

### Live Demo
https://riverdipstick.uk (or whatever domain you point at the VPS)

### Features
- Live 15-minute readings from Environment Agency gauges  
- 7-day rainfall overlay (nearest rain gauge per beat)  
- Prophet + rainfall regressor = 24-hour level forecast for every station  
- Colour-coded “go fishing” table (red / yellow / green via `rules.json`)  
- Source-to-sea sorting per river  
- “No recent data” warning badge  
- Raw-dog commit history (we keep it real)

### Stack
- Streamlit dashboard  
- Python 3.12 + Prophet + pandas + altair  
- PostgreSQL  
- Environment Agency flood-monitoring API  
- SuperCronhic'd data collection & prediction scripts  
- Docker happening soon

### Local Development (macOS / mini-PC / anywhere)
```bash
git clone https://github.com/TimLanigan/river-dipstick.git
cd river-dipstick
cp .env.example .env          # edit DB credentials
docker compose up --build -d
open http://localhost:8501

git pull
docker compose down && docker compose up --build -d
# cron already runs get_readings.py every 15 min & predict.py every hour

![Buy Me A Coffee](https://img.buymeacoffee.com/button-api/?text=Buy me a coffee&emoji=coffee&slug=riverdipstick&button_colour=FFDD00&font_colour=000000&font_family=Cookie&outline_colour=000000&coffee_colour=FFFFFF)

All catches and lies documented at → https://downstreamblog.uk

Data source: Environment Agency real-time flood-monitoring API
Built with blood, coffee, and a lot of help from Grok (xAI)
Commit history preserved for future generations