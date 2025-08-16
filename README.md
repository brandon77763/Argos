# Argos Lead Finder

A simple, compliant Gradio app to discover potential leads from public web pages using dork-like queries on a search engine (DuckDuckGo by default), extract emails, phone numbers, and social links, and manage them in an editable table you can export/import.

Note: This project avoids scraping Google directly to respect their Terms of Service. You can optionally plug in a compliant search API later (e.g., Google Custom Search JSON API or SerpAPI) if you have credentials.

## Features

- Build dork-style search queries (per domain + keywords)
- Search via DuckDuckGo (default, no API key)
- Crawl result pages with polite timeouts and concurrency
- Extract:
  - Emails
  - Phone numbers
  - Social profiles (LinkedIn, Twitter/X, Facebook, Instagram, TikTok, YouTube, GitHub)
  - Source URL and page title
- Manage leads in an editable table (add/remove rows and columns)
- Import CSV and export CSV/JSON

## Quick start

### 1) Install deps

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

### 2) Run the app

```bash
python app.py
```

Open the URL printed by Gradio in your browser.

## Usage tips

- Enter one or more domains (comma-separated) and optional keywords. The app generates a set of dork-like queries.
- Click "Search & Extract" to search and extract leads. Results appear in the table.
- Edit cells inline. Use the controls to add/remove columns and rows. Import CSV to merge.

## Configuration

Create a `.env` file (optional):

```
# Example: tune crawl behavior
REQUEST_TIMEOUT=10
CONCURRENCY=8
USER_AGENT=ArgosLeadFinder/1.0 (+https://example.com)
``` 

To enable a commercial search API, you can extend later with specific providers and set your keys in `.env`.

## Legal and ethical considerations

- Only collect publicly available information with permission where required. Respect robots.txt and site terms.
- Do not scrape Google directly or attempt to bypass rate limits or captcha.
- Comply with privacy and anti-spam regulations (e.g., CAN-SPAM, GDPR). Obtain consent as needed.

## License

For internal/demo use. Review and adapt before production.
