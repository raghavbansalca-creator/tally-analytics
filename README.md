# Seven Labs Vision — Tally Analytics Platform

AI-powered analytics dashboard for TallyPrime. Connect to any running TallyPrime instance and get instant financial insights — no add-ons, no TDL, no file exports needed.

## Features

- **Live Tally Sync** — Connects to TallyPrime's XML API, extracts all data in seconds
- **Interactive P&L & Balance Sheet** — Click any group to drill into ledgers and voucher entries
- **Business Analytics** — MoM sales/purchase trends, cash flow statement, future projections
- **Monthly MIS** — Startup-style columnar P&L with contribution margins, ratios, burn rate
- **GST Returns** — Auto-computed GSTR-1, GSTR-3B with monthly comparison and drill-down
- **Smart Chat** — Ask questions in natural language about your financial data
- **Drill-Down Everywhere** — Click any figure → invoices → voucher accounting entries
- **Indian Number Formatting** — All amounts in Lakhs (L) and Crores (Cr)

## Quick Start (Windows)

### Prerequisites
1. **Python 3.9+** — Download from [python.org](https://www.python.org/downloads/) (check "Add to PATH")
2. **TallyPrime** running with a company loaded
3. **Port 9000 enabled** — In Tally: F1 → Settings → Connectivity → Enable ODBC Server = Yes

### Installation
1. Download or clone this repository
2. Double-click **`install.bat`** — installs required packages
3. Double-click **`run.bat`** — opens the app in your browser
4. Go to **Setup** page → Enter Tally IP → Click **Sync All Data**
5. Navigate to Dashboard — your analytics are ready!

### Manual Installation
```bash
pip install -r requirements.txt
streamlit run app.py
```

## How It Works

```
TallyPrime (port 9000) ←→ tally_sync.py ←→ SQLite ←→ Streamlit Dashboard
```

The app connects to TallyPrime's built-in XML API on port 9000. No add-ons, no TDL files, no file exports. Just Tally running normally.

Data stays **100% local** on your machine. No cloud, no internet required after setup.

## Files

| File | Description |
|------|-------------|
| `app.py` | Main dashboard — P&L, Balance Sheet, drill-down |
| `tally_sync.py` | Live sync engine — XML API extractor |
| `analytics.py` | Business analytics computations |
| `gst_engine.py` | GST return calculations (GSTR-1, GSTR-3B) |
| `chat_engine.py` | Smart conversational query engine |
| `tally_reports.py` | SQL report templates |
| `pages/0_Setup.py` | Connection & sync setup UI |
| `pages/1_Business_Analytics.py` | Analytics dashboard |
| `pages/2_Monthly_MIS.py` | Monthly MIS reports |
| `pages/3_GST_Returns.py` | GST returns dashboard |
| `install.bat` | One-click Windows setup |
| `run.bat` | One-click Windows launch |

## Tech Stack

- **Python** + **Streamlit** — Interactive web dashboard
- **SQLite** — Local database (no server needed)
- **TallyPrime XML API** — Direct data extraction via port 9000

## Author

**Seven Labs Vision** — CA Raghav Bansal

---
*Built with Claude Code*
