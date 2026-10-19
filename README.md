# PolyArb

PolyArb is a real-time quantitative prediction-market research platform focused on short-duration cryptocurrency markets.

## Current Features

- Automatic discovery of the active BTC 15-minute Up/Down market
- Polymarket US market-data integration
- BRTI price-to-beat retrieval
- Settlement-window timing and phase detection
- Defensive market rollover handling
- Full executable order-book retrieval
- Coherent best-bid / best-ask calculation from a single book snapshot
- Order-book depth inspection

## Status

PolyArb is currently in **research / read-only mode**.

It does not place real trades.

Development progression:

1. Market-data infrastructure
2. Real-time data recording
3. Statistical signal research
4. Paper trading and backtesting
5. Risk controls
6. Small-scale live validation

## Safety

Credentials, API keys, private trading configuration, and production strategy parameters are not stored in this public repository.

## Requirements

- Python 3.13
- polymarket-us 2.4.1

Install dependencies:

    python -m pip install -r requirements.txt

## Disclaimer

This project is for software engineering and quantitative research purposes. Prediction-market trading involves financial risk.
