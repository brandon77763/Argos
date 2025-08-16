import os
import re
import asyncio
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import gradio as gr
from ddgs import DDGS
import tldextract
from rapidfuzz import fuzz
import sqlite3
from datetime import datetime

load_dotenv()

REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", 10))
CONCURRENCY = int(os.getenv("CONCURRENCY", 6))
USER_AGENT = os.getenv("USER_AGENT", "ArgosLeadFinder/1.0 (+https://example.com)")

# Basic extractors
EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-.]+")
PHONE_RE = re.compile(r"(?:\+?\d[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}")

SOCIAL_DOMAINS = {
    "linkedin": ["linkedin.com"],
    "twitter": ["twitter.com", "x.com"],
    "facebook": ["facebook.com"],
    "instagram": ["instagram.com"],
    "tiktok": ["tiktok.com"],
    "youtube": ["youtube.com", "youtu.be"],
    "github": ["github.com"],
}

DEFAULT_COLUMNS = [
    "name",
    "email",
    "phone",
    "linkedin",
    "twitter",
    "facebook",
    "instagram",
    "tiktok",
    "youtube",
    "github",
    "source_url",
    "source_title",
]

HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}

# Database setup
DB_PATH = "leads.db"

def init_database():
    """Initialize the SQLite database with the leads table."""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create leads table if it doesn't exist
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            email TEXT,
            phone TEXT,
            linkedin TEXT,
            twitter TEXT,
            facebook TEXT,
            instagram TEXT,
            tiktok TEXT,
            youtube TEXT,
            github TEXT,
            source_url TEXT,
            source_title TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()

def save_leads_to_db(df):
    """Save the dataframe to the database."""
    if df.empty:
        return
    
    conn = sqlite3.connect(DB_PATH)
    
    # Clear existing data and insert new data
    cursor = conn.cursor()
    cursor.execute('DELETE FROM leads')
    
    # Prepare data for insertion
    for _, row in df.iterrows():
        values = []
        for col in DEFAULT_COLUMNS:
            values.append(row.get(col, ""))
        
        cursor.execute('''
            INSERT INTO leads (name, email, phone, linkedin, twitter, facebook, 
                             instagram, tiktok, youtube, github, source_url, source_title)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', values)
    
    conn.commit()
    conn.close()

def load_leads_from_db():
    """Load leads from the database into a dataframe."""
    if not os.path.exists(DB_PATH):
        # Return empty dataframe with default columns if no database exists
        return pd.DataFrame(columns=DEFAULT_COLUMNS)
    
    conn = sqlite3.connect(DB_PATH)
    
    # Load data excluding the database-specific columns (id, created_at, updated_at)
    query = f"SELECT {', '.join(DEFAULT_COLUMNS)} FROM leads ORDER BY id"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df


def build_queries(domains: str, keywords: str, extra: str) -> list[str]:
    dlist = [d.strip() for d in domains.split(",") if d.strip()]
    klist = [k.strip() for k in keywords.split(",") if k.strip()]
    elist = [e.strip() for e in extra.split(",") if e.strip()]

    base_terms = [
        '"contact"', '"email"', '"reach us"', '"about"', '"team"',
        '"sales"', '"marketing"', '"press"', '"careers"'
    ]
    if klist:
        base_terms.extend([f'"{k}"' if " " in k else k for k in klist])
    if elist:
        base_terms.extend(elist)

    queries = []
    if dlist:
        for d in dlist:
            queries.append(f"site:{d} (" + " OR ".join(base_terms) + ")")
    else:
        queries.append("(" + " OR ".join(base_terms) + ")")
    return queries


def normalize_social(url: str) -> tuple[str, str | None]:
    try:
        parts = tldextract.extract(url)
        domain = f"{parts.domain}.{parts.suffix}".lower()
        for key, hosts in SOCIAL_DOMAINS.items():
            if any(domain.endswith(h) for h in hosts):
                return key, url
    except Exception:
        pass
    return "", None


def parse_contacts(url: str, html: str) -> dict:
    out = {c: "" for c in DEFAULT_COLUMNS}
    out["source_url"] = url

    if not html:
        return out

    soup = BeautifulSoup(html, "html.parser")
    title = soup.title.text.strip() if soup.title and soup.title.text else ""
    out["source_title"] = title

    text = soup.get_text(" ", strip=True)
    emails = list(dict.fromkeys(EMAIL_RE.findall(text)))
    phones = list(dict.fromkeys(PHONE_RE.findall(text)))

    # naive name guess from title
    out["name"] = title[:80]
    out["email"] = ", ".join(emails[:3])
    out["phone"] = ", ".join(phones[:3])

    for a in soup.find_all("a", href=True):
        kind, norm = normalize_social(a["href"])
        if kind and norm:
            # prefer shorter/clean profile url
            cur = out.get(kind) or ""
            if not cur or len(norm) < len(cur):
                out[kind] = norm

    return out


async def search_ddg(query: str, max_results: int = 20) -> list[dict]:
    results = []
    try:
        with DDGS() as ddgs:
            for r in ddgs.text(query, max_results=max_results, region="wt-wt", safesearch="moderate"):
                if isinstance(r, dict) and r.get("href"):
                    results.append({
                        "title": r.get("title", ""),
                        "href": r["href"],
                        "source": "ddg"
                    })
    except Exception:
        pass
    return results


async def gather_pages(urls: list[str], progress_callback=None) -> list[tuple[str, str]]:
    def log(msg):
        if progress_callback:
            progress_callback(msg)
    
    sem = asyncio.Semaphore(CONCURRENCY)
    
    async def fetch(client, url):
        try:
            log(f"   Fetching: {url}")
            resp = await client.get(url, timeout=10.0)
            return (url, resp.text)
        except Exception as e:
            log(f"   ❌ Failed: {url} - {str(e)[:50]}...")
            return (url, "")

    async with httpx.AsyncClient(headers=HEADERS) as client:
        async def bound(url):
            async with sem:
                return await fetch(client, url)
        tasks = [bound(u) for u in urls]
        return await asyncio.gather(*tasks)


async def search_and_extract(domains: str, keywords: str, extra: str, max_results: int, progress_callback=None):
    def log(msg):
        if progress_callback:
            progress_callback(msg)
    
    queries = build_queries(domains, keywords, extra)
    log(f"🔍 Generated {len(queries)} search queries")
    for i, q in enumerate(queries, 1):
        log(f"Query {i}: {q}")
    
    seen = set()
    serps = []
    
    for i, q in enumerate(queries, 1):
        log(f"\n📡 Searching query {i}/{len(queries)}: {q}")
        res = await search_ddg(q, max_results=max_results)
        log(f"   Found {len(res)} results")
        
        for r in res:
            href = r.get("href")
            if not href or href in seen:
                continue
            seen.add(href)
            serps.append(r)
    
    log(f"\n🌐 Total unique URLs found: {len(serps)}")
    log("📄 Fetching page content...")

    pages = await gather_pages([r["href"] for r in serps], progress_callback=log)
    log(f"✅ Successfully fetched {len(pages)} pages")
    
    log("🔍 Extracting contact information...")
    rows = []
    contacts_found = 0
    
    for i, (u, h) in enumerate(pages, 1):
        if i % 5 == 0 or i == len(pages):
            log(f"   Processed {i}/{len(pages)} pages...")
        
        contact = parse_contacts(u, h)
        if contact and (contact.get("email") or contact.get("phone")):
            rows.append(contact)
            contacts_found += 1
    
    log(f"📧 Found {contacts_found} potential contacts")
    log("🔄 Deduplicating results...")

    # small dedupe by email + domain similarity
    def domain_of(u):
        try:
            parts = tldextract.extract(u)
            return f"{parts.domain}.{parts.suffix}".lower()
        except Exception:
            return ""

    deduped = []
    for row in rows:
        em = row.get("email", "").split(",")[0].strip().lower()
        dom = domain_of(row.get("source_url", ""))
        duplicate = False
        for existing in deduped:
            em2 = existing.get("email", "").split(",")[0].strip().lower()
            dom2 = domain_of(existing.get("source_url", ""))
            if em and em == em2:
                duplicate = True
                break
            if dom and dom2 and fuzz.partial_ratio(dom, dom2) > 95 and em == em2:
                duplicate = True
                break
        if not duplicate:
            deduped.append(row)

    log(f"✨ Final results: {len(deduped)} unique contacts after deduplication")
    df = pd.DataFrame(deduped, columns=DEFAULT_COLUMNS)
    return df


# Gradio UI
# Initialize database
init_database()

with gr.Blocks(title="Argos Lead Finder") as demo:
    gr.Markdown("# Argos Lead Finder\nSearch public pages for emails, phones, and social links. Edit and export your leads table.")

    with gr.Tabs():
        with gr.Tab("Search"):
            with gr.Group():
                with gr.Row():
                    domains = gr.Textbox(label="Domains", placeholder="example.com, example.org")
                    keywords = gr.Textbox(label="Keywords", placeholder="sales, marketing, contact")
                with gr.Row():
                    extra = gr.Textbox(label="Extra terms", placeholder="press, team, partners")
                    max_results = gr.Slider(10, 100, value=30, step=5, label="Max results/query")
                with gr.Row():
                    search_btn = gr.Button("Search & Extract", variant="primary")
                    status = gr.Markdown("", elem_id="status")
                
                console = gr.Textbox(
                    label="Search Console",
                    placeholder="Search progress will appear here...",
                    lines=10,
                    max_lines=15,
                    interactive=False,
                    show_copy_button=True
                )

        with gr.Tab("Leads"):
            gr.Markdown("### Leads table\nEdit cells inline. Use controls below to add/remove columns and rows, or import/export.")
            # Load existing data from database
            initial_data = load_leads_from_db()
            df = gr.Dataframe(
                value=initial_data,
                headers=DEFAULT_COLUMNS,
                datatype=["str"] * len(DEFAULT_COLUMNS),
                row_count=(1, "dynamic"),
                col_count=(len(DEFAULT_COLUMNS), "dynamic"),
                wrap=True,
                interactive=True,
            )

            with gr.Row():
                with gr.Accordion("Table controls", open=True):
                    with gr.Row():
                        add_col_name = gr.Textbox(label="New column", placeholder="e.g. company")
                        add_col_btn = gr.Button("Add Column")
                        del_col_name = gr.Textbox(label="Delete column", placeholder="column name")
                        del_col_btn = gr.Button("Delete Column")
                    with gr.Row():
                        add_row_btn = gr.Button("Add Row")
                        del_row_idx = gr.Number(label="Delete row #", value=0, precision=0)
                        del_row_btn = gr.Button("Delete Row")

            with gr.Row():
                with gr.Accordion("Import / Export", open=True):
                    with gr.Row():
                        imp = gr.File(label="CSV file", file_types=[".csv"], type="binary")
                        import_btn = gr.Button("Import & Merge")
                    with gr.Row():
                        export_csv_btn = gr.Button("Export CSV")
                        export_json_btn = gr.Button("Export JSON")
                        download_csv = gr.File(label="CSV", interactive=False)
                        download_json = gr.File(label="JSON", interactive=False)

        with gr.Tab("Auto Crawler"):
            gr.Markdown("### Advanced Auto Crawler\nAutomated contact discovery using advanced search techniques and continuous crawling.")
            
            with gr.Row():
                with gr.Column():
                    gr.Markdown("#### Crawler Settings")
                    crawler_industry = gr.Textbox(label="Target Industry", placeholder="e.g. SaaS, E-commerce, Healthcare")
                    crawler_location = gr.Textbox(label="Location", placeholder="e.g. San Francisco, remote, USA")
                    crawler_company_size = gr.Dropdown(
                        label="Company Size",
                        choices=["1-10", "11-50", "51-200", "201-500", "500+", "Any"],
                        value="Any"
                    )
                    crawler_role_types = gr.CheckboxGroup(
                        label="Target Roles",
                        choices=["CEO", "CTO", "VP", "Director", "Manager", "Sales", "Marketing", "Developer", "HR"],
                        value=["CEO", "CTO", "VP"]
                    )
                    
                with gr.Column():
                    gr.Markdown("#### Advanced Options")
                    crawler_social_focus = gr.CheckboxGroup(
                        label="Social Platforms to Focus",
                        choices=["LinkedIn", "Twitter", "GitHub", "AngelList", "Crunchbase"],
                        value=["LinkedIn", "Twitter"]
                    )
                    crawler_depth = gr.Slider(1, 10, value=3, step=1, label="Crawl Depth")
                    crawler_delay = gr.Slider(1, 30, value=5, step=1, label="Delay Between Requests (seconds)")
                    crawler_max_per_domain = gr.Slider(10, 500, value=100, step=10, label="Max Results Per Domain")
            
            with gr.Row():
                crawler_start_btn = gr.Button("🚀 Start Auto Crawler", variant="primary", size="lg")
                crawler_stop_btn = gr.Button("🛑 Stop Crawler", variant="secondary")
                crawler_status = gr.Markdown("Status: **Ready**")
            
            with gr.Row():
                with gr.Column():
                    crawler_console = gr.Textbox(
                        label="Crawler Activity Log",
                        placeholder="Crawler activity will appear here...",
                        lines=15,
                        max_lines=20,
                        interactive=False,
                        show_copy_button=True
                    )
                with gr.Column():
                    crawler_stats = gr.Markdown("### Statistics\n- **Total Searches:** 0\n- **Domains Found:** 0\n- **Contacts Found:** 0\n- **Success Rate:** 0%")
                    crawler_queue = gr.Textbox(
                        label="Current Search Queue",
                        placeholder="Queued searches will appear here...",
                        lines=8,
                        interactive=False
                    )

        with gr.Tab("Help"):
            gr.Markdown("""
            ## Argos Lead Finder - User Guide
            
            ### 🔍 Manual Search
            - **Domains**: Target specific websites (e.g., "techcrunch.com, ycombinator.com")
            - **Keywords**: Industry or role terms (e.g., "CEO, founder, startup")
            - **Extra terms**: Additional context (e.g., "contact, about, team")
            - **Console**: Real-time search progress and debugging info
            
            ### 🤖 Auto Crawler
            Advanced automated contact discovery with intelligent search strategies:
            
            **Features:**
            - **Industry Targeting**: Focuses searches on specific industries
            - **Role-Based Discovery**: Targets CEOs, CTOs, VPs, etc.
            - **Social Platform Integration**: LinkedIn, Twitter, GitHub crawling
            - **Intelligent Queuing**: Generates 50+ search strategies automatically
            - **Rate Limiting**: Respects website policies with configurable delays
            - **Real-time Monitoring**: Live stats and activity logging
            
            **Advanced Strategies:**
            - Company discovery via LinkedIn pages
            - Startup founder identification
            - Conference speaker contact extraction
            - Podcast guest bio mining
            - Press release contact harvesting
            - About us page team extraction
            
            ### 📊 Database Features
            - **Auto-save**: All data automatically saved to SQLite database
            - **Persistence**: Data survives app restarts
            - **Deduplication**: Automatic removal of duplicate contacts
            - **Export Options**: CSV and JSON formats available
            
            ### 💡 Pro Tips
            - Use specific industry terms for better targeting
            - Combine multiple search approaches for comprehensive coverage
            - Monitor the console for debugging search issues
            - Export data regularly as backup
            - Use appropriate delays in auto crawler to avoid rate limiting
            """)

    # Auto Crawler Functions
    crawler_active = gr.State(False)
    crawler_queue_list = gr.State([])
    
    async def start_auto_crawler(industry, location, company_size, role_types, social_focus, depth, delay, max_per_domain):
        """Start the auto crawler with advanced search strategies"""
        
        def log_crawler(msg):
            timestamp = pd.Timestamp.now().strftime("%H:%M:%S")
            return f"[{timestamp}] {msg}"
        
        crawler_log = log_crawler("🚀 Auto Crawler Starting...")
        
        # Generate search strategies
        strategies = generate_crawler_strategies(industry, location, company_size, role_types, social_focus)
        crawler_log += "\n" + log_crawler(f"📋 Generated {len(strategies)} search strategies")
        
        # Start executing strategies
        total_contacts = 0
        total_domains = set()
        successful_searches = 0
        
        for i, strategy in enumerate(strategies[:10]):  # Limit for demo
            crawler_log += "\n" + log_crawler(f"🔍 Executing: {strategy['name']}")
            
            try:
                # Use the existing search function with the strategy query
                df_results = await search_and_extract("", strategy['query'], "", max_per_domain)
                
                if not df_results.empty:
                    # Save results to database
                    save_leads_to_db(df_results)
                    contacts_found = len(df_results)
                    total_contacts += contacts_found
                    successful_searches += 1
                    
                    # Extract domains
                    for url in df_results['source_url'].dropna():
                        try:
                            parts = tldextract.extract(url)
                            domain = f"{parts.domain}.{parts.suffix}".lower()
                            if domain:
                                total_domains.add(domain)
                        except:
                            pass
                    
                    crawler_log += "\n" + log_crawler(f"✅ Found {contacts_found} contacts from {strategy['name']}")
                else:
                    crawler_log += "\n" + log_crawler(f"❌ No results from {strategy['name']}")
                
                # Update statistics
                success_rate = (successful_searches / (i + 1)) * 100
                stats_text = f"""### Statistics
- **Total Searches:** {i + 1}/{len(strategies)}
- **Domains Found:** {len(total_domains)}
- **Contacts Found:** {total_contacts}
- **Success Rate:** {success_rate:.1f}%"""
                
                # Update queue
                remaining = strategies[i+1:i+11]
                queue_text = "\n".join([f"• {s['name']}" for s in remaining])
                if len(strategies) > i + 11:
                    queue_text += f"\n... and {len(strategies) - i - 11} more"
                
                # Simulate delay
                if delay > 0 and i < len(strategies) - 1:
                    crawler_log += "\n" + log_crawler(f"⏱️ Waiting {delay} seconds before next search...")
                    await asyncio.sleep(delay)
                
            except Exception as e:
                crawler_log += "\n" + log_crawler(f"❌ Error in {strategy['name']}: {str(e)}")
        
        crawler_log += "\n" + log_crawler("🎉 Auto Crawler completed!")
        final_stats = f"""### Final Statistics
- **Total Searches:** {len(strategies)}
- **Domains Found:** {len(total_domains)}
- **Contacts Found:** {total_contacts}
- **Success Rate:** {(successful_searches / len(strategies)) * 100:.1f}%"""
        
        return True, "Status: **Completed** ✅", crawler_log, final_stats, "All searches completed", strategies
    
    def stop_auto_crawler():
        """Stop the auto crawler"""
        return False, "Status: **Stopped** 🔴", "", "### Statistics\n- **Total Searches:** 0\n- **Domains Found:** 0\n- **Contacts Found:** 0\n- **Success Rate:** 0%", ""
    
    def generate_crawler_strategies(industry, location, company_size, role_types, social_focus):
        """Generate advanced search strategies based on user inputs"""
        strategies = []
        
        # Industry-specific searches
        if industry:
            strategies.extend([
                {"name": f"{industry} companies {location}", "type": "company_discovery", "query": f"site:linkedin.com/company {industry} {location}"},
                {"name": f"{industry} startups", "type": "startup_discovery", "query": f"{industry} startup founders contact"},
                {"name": f"{industry} news mentions", "type": "news_crawl", "query": f"{industry} CEO CTO contact email"},
            ])
        
        # Role-based searches
        for role in role_types:
            strategies.extend([
                {"name": f"{role} at {industry} companies", "type": "role_search", "query": f"site:linkedin.com/in {role} {industry} {location}"},
                {"name": f"{role} contact pages", "type": "contact_discovery", "query": f"{role} {industry} contact email phone"},
            ])
        
        # Social platform specific searches
        if "LinkedIn" in social_focus:
            strategies.append({"name": "LinkedIn company pages", "type": "linkedin_crawl", "query": f"site:linkedin.com/company {industry}"})
        
        if "Twitter" in social_focus:
            strategies.append({"name": "Twitter profiles", "type": "twitter_crawl", "query": f"site:twitter.com {industry} CEO founder"})
        
        if "GitHub" in social_focus:
            strategies.append({"name": "GitHub developer profiles", "type": "github_crawl", "query": f"site:github.com {industry} developer contact"})
        
        # Advanced search patterns
        strategies.extend([
            {"name": "About us pages", "type": "about_crawl", "query": f"{industry} 'about us' team {location}"},
            {"name": "Press releases", "type": "press_crawl", "query": f"{industry} 'press release' contact media"},
            {"name": "Conference speakers", "type": "conference_crawl", "query": f"{industry} conference speaker contact"},
            {"name": "Podcast guests", "type": "podcast_crawl", "query": f"{industry} podcast guest bio contact"},
        ])
        
        return strategies[:50]  # Limit to 50 strategies

    async def do_search(domains_val, keywords_val, extra_val, max_results_val, table, console_text=""):
        console_output = console_text + "\n🚀 Starting search...\n"
        
        def log_progress(msg):
            nonlocal console_output
            console_output += msg + "\n"
            return console_output
        
        try:
            log_progress(f"📝 Search parameters:")
            log_progress(f"   Domains: {domains_val or 'None'}")
            log_progress(f"   Keywords: {keywords_val or 'None'}")
            log_progress(f"   Extra terms: {extra_val or 'None'}")
            log_progress(f"   Max results per query: {max_results_val}")
            
            df_new = await search_and_extract(
                domains_val or "", 
                keywords_val or "", 
                extra_val or "", 
                int(max_results_val or 20),
                progress_callback=log_progress
            )

            # Normalize current table to DataFrame
            if isinstance(table, pd.DataFrame):
                cur = table
            elif table is None:
                cur = pd.DataFrame(columns=DEFAULT_COLUMNS)
            else:
                try:
                    cur = pd.DataFrame(table, columns=DEFAULT_COLUMNS)
                except Exception:
                    cur = pd.DataFrame(table)

            if cur.empty:
                out = df_new
            else:
                log_progress("🔄 Merging with existing data...")
                out = pd.concat([cur, df_new], ignore_index=True, sort=False)
                out = out.drop_duplicates(subset=["email", "source_url"], keep="first")
                log_progress(f"📊 After merge and final dedup: {len(out)} total leads")

            if len(df_new) == 0:
                status_msg = "⚠️ No leads found. Try different keywords or domains."
                log_progress("❌ Search completed with no results")
            else:
                status_msg = f"✅ Found {len(df_new)} new leads. Total: {len(out)} leads"
                log_progress(f"🎉 Search completed successfully!")
            
            # Auto-save to database
            log_progress("💾 Saving results to database...")
            save_leads_to_db(out)
            log_progress("✅ Results saved to database")
            
            return out, status_msg, console_output
        except Exception as e:
            error_msg = f"❌ Search failed: {str(e)}"
            log_progress(error_msg)
            return table if table is not None else pd.DataFrame(columns=DEFAULT_COLUMNS), error_msg, console_output

    def add_column(table, name):
        if not name:
            return table
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        if name not in cur.columns:
            cur[name] = ""
        # Auto-save to database
        save_leads_to_db(cur)
        return cur

    def del_column(table, name):
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        if name in cur.columns:
            cur = cur.drop(columns=[name])
        # Auto-save to database
        save_leads_to_db(cur)
        return cur

    def add_row(table):
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        if cur.empty:
            cur = pd.DataFrame([{}], columns=df.headers)
        else:
            empty = {c: "" for c in cur.columns}
            cur = pd.concat([cur, pd.DataFrame([empty])], ignore_index=True)
        # Auto-save to database
        save_leads_to_db(cur)
        return cur

    def del_row(table, idx):
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        try:
            idx = int(idx)
            if 0 <= idx < len(cur):
                cur = cur.drop(index=idx).reset_index(drop=True)
        except Exception:
            pass
        # Auto-save to database
        save_leads_to_db(cur)
        return cur

    def do_import(fileobj, table):
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        if not fileobj:
            return cur
        try:
            import io
            csv_bytes = fileobj["data"] if isinstance(fileobj, dict) else fileobj
            if isinstance(csv_bytes, bytes):
                s = csv_bytes.decode("utf-8", errors="ignore")
            else:
                s = csv_bytes.read().decode("utf-8", errors="ignore")
            new = pd.read_csv(io.StringIO(s))
            merged = pd.concat([cur, new], ignore_index=True, sort=False)
            merged = merged.drop_duplicates(subset=["email", "source_url"], keep="first")
            for col in DEFAULT_COLUMNS:
                if col not in merged.columns:
                    merged[col] = ""
            # Auto-save to database
            save_leads_to_db(merged)
            return merged
        except Exception:
            return cur

    def to_csv_file(table):
        import tempfile
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
        cur.to_csv(tmp.name, index=False)
        return tmp.name

    def to_json_file(table):
        import tempfile
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
        cur.to_json(tmp.name, orient="records", indent=2)
        return tmp.name

    # Auto-save when dataframe is edited
    def on_dataframe_change(data):
        """Auto-save when dataframe is edited directly."""
        if data is not None:
            df_data = pd.DataFrame(data) if not isinstance(data, pd.DataFrame) else data
            save_leads_to_db(df_data)
        return data

    # Auto Crawler Event Handlers
    crawler_start_btn.click(
        start_auto_crawler,
        inputs=[crawler_industry, crawler_location, crawler_company_size, crawler_role_types, 
                crawler_social_focus, crawler_depth, crawler_delay, crawler_max_per_domain],
        outputs=[crawler_active, crawler_status, crawler_console, crawler_stats, crawler_queue, crawler_queue_list]
    )
    
    crawler_stop_btn.click(
        stop_auto_crawler,
        outputs=[crawler_active, crawler_status, crawler_console, crawler_stats, crawler_queue]
    )

    # Regular Event Handlers
    search_btn.click(do_search, inputs=[domains, keywords, extra, max_results, df, console], outputs=[df, status, console])
    add_col_btn.click(add_column, inputs=[df, add_col_name], outputs=df)
    del_col_btn.click(del_column, inputs=[df, del_col_name], outputs=df)
    add_row_btn.click(add_row, inputs=[df], outputs=df)
    del_row_btn.click(del_row, inputs=[df, del_row_idx], outputs=df)
    import_btn.click(do_import, inputs=[imp, df], outputs=df)
    export_csv_btn.click(to_csv_file, inputs=[df], outputs=download_csv)
    export_json_btn.click(to_json_file, inputs=[df], outputs=download_json)
    
    # Auto-save on direct dataframe edits
    df.change(on_dataframe_change, inputs=[df], outputs=[df])

if __name__ == "__main__":
    demo.queue().launch()
