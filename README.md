# Forex Trading Management Bot 🤖

A sophisticated Telegram bot for managing foreign exchange transactions, client balances, and generating detailed financial reports with multi-currency support.

## Features 🌟

- **Multi-Currency Transactions**
  - Buy/sell orders with custom exchange rates
  - Support for 3-4 letter currency codes (USD, EUR, MYR, USDT)
  - Automated balance tracking

- **Payment Processing**
  - Client payment registration (`/received`)
  - Company payment processing (`/paid`)
  - Payment cancellation functionality

- **Advanced Reporting**
  - Real-time P&L reports with Excel export
  - Client statements with image/Excel formats
  - Cash flow reports with visual summaries
  - Expense tracking with image reports

- **Risk Management**
  - Average cost calculation for major currencies
  - Debt tracking and balance adjustments
  - Transaction cancellation system

- **Database Management**
  - SQLite backend with migrations
  - Client data management
  - Balance history tracking

## Installation 🛠️

1. **Prerequisites**
   - Python 3.9+
   - PostgreSQL/SQLite
   - Telegram bot token from [@BotFather](https://t.me/BotFather)

2. **Setup**
   ```bash
   git clone https://github.com/yourusername/forex-trading-bot.git
   cd forex-trading-bot
   python -m venv venv
   source venv/bin/activate  # Linux/MacOS
   venv\Scripts\activate  # Windows

   pip install -r requirements.txt
