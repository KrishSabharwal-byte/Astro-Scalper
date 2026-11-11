# 🌌 Astro Scalper

> An astro-driven scalping toolkit for **Futures**, **Indexes**, and **Commodities** — built for fast intraday decision-making. ⚡

![Python](https://img.shields.io/badge/Python-3.9%2B-blue?logo=python&logoColor=white)
![Status](https://img.shields.io/badge/Status-Active-brightgreen)
![Markets](https://img.shields.io/badge/Markets-Futures%20%7C%20Indexes%20%7C%20Commodities-orange)
![License](https://img.shields.io/badge/License-MIT-lightgrey)

---

## 📑 Table of Contents

- [✨ Overview](#-overview)
- [📈 Supported Markets](#-supported-markets)
- [🔮 How It Works](#-how-it-works)
- [🗂️ Repository Structure](#️-repository-structure)
- [🧰 Requirements](#-requirements)
- [⚙️ Installation](#️-installation)
- [🔑 Configuration](#-configuration)
- [▶️ Usage](#️-usage)
- [🛡️ Risk Management](#️-risk-management)
- [📊 Output & Logs](#-output--logs)
- [🧪 Backtesting & Paper Trading](#-backtesting--paper-trading)
- [🗺️ Roadmap](#️-roadmap)
- [🤝 Contributing](#-contributing)
- [⚠️ Disclaimer](#️-disclaimer)
- [👤 Author](#-author)

---

## ✨ Overview

**Astro Scalper** is a collection of scalping tools that combine **astrological timing/signal logic** with **intraday price action** to identify short-term trade opportunities.

This repository contains a dedicated scalper for each market segment:

| Segment | Scalper | Instruments |
|---|---|---|
| 📈 Futures | `future_indexes` | Stock & index futures |
| 🏦 Indexes | `index_scalper` | NIFTY, SENSEX (and more) |
| 🛢️ Commodities | `future` | Crude Oil, Gold, Silver, Natural Gas (and more) |

> 📝 **Note:** Rename the scalper names/files above to match the actual file names in your repo.

### 🌟 Key Features

- ⚡ **Multi-market support** – Futures, Indexes, and Commodities in one repository
- 🔮 **Astro-based signal engine** – time/planetary-driven bias combined with price confirmation
- 🎯 **Scalping-focused** – designed for quick entries and exits on lower timeframes
- 🛡️ **Built-in risk controls** – stop-loss, target, and position-size rules
- 🧪 **Paper-trading friendly** – test logic before risking real capital
- 🧩 **Modular design** – each scalper runs independently with its own config
- 🧾 **Trade logging** – every signal and trade can be saved for review

---

## 📈 Supported Markets

### 🏦 Indexes
- **NIFTY 50**
- **SENSEX**
- *(Add BANKNIFTY, FINNIFTY, etc. if supported)*

### 📉 Futures
- Index futures (NIFTY / SENSEX futures)
- Stock futures *(if supported)*

### 🛢️ Commodities
- Crude Oil
- Gold
- Silver
- Natural Gas
- *(Add others as applicable)*

---

## 🔮 How It Works

The scalpers follow a simple, repeatable pipeline:

```
┌────────────────┐   ┌──────────────────┐   ┌──────────────────┐   ┌──────────────┐
│  Market Data   │ → │  Astro Signal    │ → │  Price Action    │ → │  Entry/Exit  │
│  (live/hist)   │   │  Bias Engine     │   │  Confirmation    │   │  + Risk Mgmt │
└────────────────┘   └──────────────────┘   └──────────────────┘   └──────────────┘
```

1. 📡 **Data ingestion** – fetches live or historical candles for the chosen instrument.
2. 🔮 **Astro bias** – computes the astro-based timing/bias window for the session.
3. 📊 **Confirmation** – validates the bias with price action / indicators on a short timeframe.
4. 🎯 **Execution logic** – generates entry, stop-loss, and target levels.
5. 🛡️ **Risk management** – applies position sizing and exit rules.
6. 🧾 **Logging** – records signals, trades, and P&L.

> 🛠️ **Customize this section:** describe your exact astro logic (e.g., planetary hours, aspects, transits, Moon nakshatra, etc.) and the confirmation rules you use.

---

## 🗂️ Repository Structure

```
astro-scalper/
├── futures_scalper/          # 📉 Futures scalper
│   ├── main.py
│   ├── config.yaml
│   └── ...
├── index_scalper/            # 🏦 NIFTY / SENSEX scalper
│   ├── main.py
│   ├── config.yaml
│   └── ...
├── commodity_scalper/        # 🛢️ Commodities scalper
│   ├── main.py
│   ├── config.yaml
│   └── ...
├── common/                   # 🧩 Shared utilities (astro engine, data, risk)
│   ├── astro_engine.py
│   ├── data_feed.py
│   ├── risk_manager.py
│   └── logger.py
├── logs/                     # 🧾 Trade & signal logs
├── requirements.txt
├── .env.example
└── README.md
```

> 📝 Update this tree to mirror your real folders and file names.

---

## 🧰 Requirements

- 🐍 **Python 3.9+**
- 📦 `pip` (or `conda`)
- 🔌 A market data / broker API account (for live data and orders)
- 💻 Windows / macOS / Linux

Typical Python libraries:

```
pandas
numpy
requests
python-dotenv
pyyaml
# add your astro library here (e.g., pyswisseph / ephem / skyfield)
# add your broker SDK here
```

---

## ⚙️ Installation

```bash
# 1️⃣ Clone the repository
git clone https://github.com/<your-username>/astro-scalper.git
cd astro-scalper

# 2️⃣ Create a virtual environment
python -m venv venv

# 3️⃣ Activate it
# Windows
venv\Scripts\activate
# macOS / Linux
source venv/bin/activate

# 4️⃣ Install dependencies
pip install -r requirements.txt
```

---

## 🔑 Configuration

1. Copy the example environment file:

```bash
cp .env.example .env
```

2. Fill in your credentials:

```env
API_KEY=your_api_key
API_SECRET=your_api_secret
CLIENT_ID=your_client_id
```

3. Adjust each scalper's settings in its `config.yaml`:

```yaml
instrument: NIFTY
timeframe: 3m          # 1m / 3m / 5m
quantity: 1
stop_loss_points: 20
target_points: 40
trailing_sl: true
trading_window:
  start: "09:20"
  end: "15:15"
mode: paper            # paper | live
```

> 🔒 **Never commit your `.env` file or API keys.** Add `.env` to `.gitignore`.

---

## ▶️ Usage

Run the scalper for the market you want:

```bash
# 🏦 Index scalper (NIFTY / SENSEX)
python index_scalper/main.py --instrument NIFTY
python index_scalper/main.py --instrument SENSEX

# 📉 Futures scalper
python futures_scalper/main.py --instrument NIFTY_FUT

# 🛢️ Commodity scalper
python commodity_scalper/main.py --instrument CRUDEOIL
python commodity_scalper/main.py --instrument GOLD
```

### 🚩 Common Flags

| Flag | Description | Example |
|---|---|---|
| `--instrument` | Symbol to trade | `NIFTY` |
| `--timeframe` | Candle timeframe | `3m` |
| `--mode` | `paper` or `live` | `paper` |
| `--config` | Custom config path | `config.yaml` |

> 📝 Adjust commands and flags to match your actual CLI.

---

## 🛡️ Risk Management

Risk control is built into every scalper:

- 🛑 **Stop-loss** on every trade
- 🎯 **Fixed target / risk-reward ratio**
- 📉 **Trailing stop-loss** (optional)
- 🔢 **Position sizing** based on capital and risk per trade
- 🚫 **Max daily loss limit** to halt trading automatically
- ⏰ **Time-based exit** – square-off before market close

> 💡 Always start in **paper mode** and validate behavior before going live.

---

## 📊 Output & Logs

Each run saves:

- 🧾 Signal log (time, direction, price, reason)
- 💹 Trade log (entry, exit, P&L)
- 📈 End-of-day summary

Logs are stored in the `logs/` directory, e.g.:

```
logs/
├── index_scalper_2026-11-11.csv
├── futures_scalper_2026-11-11.csv
└── commodity_scalper_2026-11-11.csv
```

---

## 🧪 Backtesting & Paper Trading

- 🕰️ **Backtest** the strategy on historical data before deploying.
- 📝 **Paper trade** for multiple sessions to verify signal quality and execution.
- ✅ Move to live trading only after consistent, reviewed results.

---

## 🗺️ Roadmap

- [ ] 📊 Web dashboard for live signals and P&L
- [ ] 🔔 Telegram / WhatsApp alerts
- [ ] 🧠 Parameter optimization
- [ ] 📈 More instruments (BANKNIFTY, FINNIFTY, more commodities)
- [ ] 🧪 Built-in backtesting module
- [ ] 🐳 Docker support

---

## 🤝 Contributing

Contributions, ideas, and bug reports are welcome! 🙌

1. 🍴 Fork the repository
2. 🌿 Create a feature branch: `git checkout -b feature/my-feature`
3. 💾 Commit your changes: `git commit -m "Add my feature"`
4. 📤 Push: `git push origin feature/my-feature`
5. 🔁 Open a Pull Request

---

## ⚠️ Disclaimer

> **This project is for educational and research purposes only.**
>
> - 📉 Trading in futures, indexes, and commodities involves **substantial risk of loss**.
> - 🔮 Astrological signals are **not a guarantee** of market direction.
> - 🚫 Nothing in this repository is financial advice or a recommendation to buy or sell any instrument.
> - 💼 You are solely responsible for your trading decisions. Use at your own risk, and only risk capital you can afford to lose.

---

## 👤 Author

**Krish**

- 💼 GitHub: [@your-username](https://github.com/your-username)
- 🔗 LinkedIn: [your-profile](https://linkedin.com/in/your-profile)
- 📧 Email: your.email@example.com

---

⭐ If you find this project useful, consider giving it a star!
