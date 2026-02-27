# Python Playground — Claude Project Instructions

## Project Overview
This workspace contains Python automation scripts for logistics and supply chain operations.

## Folder Structure
- `AHC_PO/` — PO upload and container building scripts
- `Arrival Notice & DO/` — Arrival notice automation
- `ISF/` — ISF (Importer Security Filing) scripts
- `Landed Cost/` — Landed cost calculations
- `NetSuite/` — NetSuite API integration and CSV imports
- `StockIQ Related/` — StockIQ forecasting, MRP, and order schedule scripts
- `Warehouse Inbound/` — Inbound warehouse processing scripts

## Environment
- Python 3.12
- Virtual environment: `.venv/`
- Run scripts with: `python <script>.py`

## Key Integrations
- **Google APIs** — Gmail, Google Sheets, Google Drive (credentials stored locally, excluded from git)
- **NetSuite** — REST API and CSV import via RESTlet
- **StockIQ** — Data extraction and MRP automation
- **Tradlinx** — Shipping tracking data retrieval

## Git Rules
- Only `.py` files and config files are tracked
- `.venv`, `.xlsx`, `.json`, `.png`, `.pickle`, `.DS_Store` are excluded via `.gitignore`
- Main branch: `main`

## Code Style
- Keep scripts simple and focused on one task
- Use clear variable names relevant to logistics/supply chain context
- Credentials are never hardcoded — loaded from local `.json` files
