name: Daily Real Estate Alert

on:
  schedule:
    - cron: '0 23 * * *'
  workflow_dispatch:

jobs:
  run-script:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3
      - uses: actions/setup-python@v4
        with:
          python-version: '3.11'
      - name: Install dependencies
        run: pip install requests
      - name: Run alert script
        env:
          TELEGRAM_TOKEN: ${{ secrets.TELEGRAM_TOKEN }}
          TELEGRAM_CHAT_ID: ${{ secrets.TELEGRAM_CHAT_ID }}
          MOLIT_API_KEY: ${{ secrets.MOLIT_API_KEY }}
        run: python main.py
