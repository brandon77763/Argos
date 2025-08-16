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
    "craigslist": ["craigslist.org"],
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
    "craigslist",
    "source_url",
    "source_title",
]

HEADERS = {"User-Agent": USER_AGENT, "Accept-Language": "en-US,en;q=0.9"}

# Database setup
DB_PATH = "leads.db"

def init_database():
    """Initialize the SQLite database and create tables."""
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
            craigslist TEXT,
            source_url TEXT,
            source_title TEXT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Add craigslist column if it doesn't exist (for existing databases)
    try:
        cursor.execute('ALTER TABLE leads ADD COLUMN craigslist TEXT')
        print("Added craigslist column to existing database")
    except sqlite3.OperationalError:
        # Column already exists or other error, continue
        pass
    
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
                        choices=["LinkedIn", "Twitter", "GitHub", "AngelList", "Crunchbase", "Craigslist"],
                        value=["LinkedIn", "Twitter"]
                    )
                    crawler_auto_iterate = gr.Checkbox(label="Auto-Iterate (Find leads, then search for their contacts)", value=True)
                    crawler_continuous = gr.Checkbox(label="Continuous Mode (Never stop crawling)", value=False)
                    crawler_depth = gr.Slider(1, 10, value=3, step=1, label="Crawl Depth")
                    crawler_delay = gr.Slider(5, 120, value=30, step=5, label="Delay Between Searches (seconds)")
                    crawler_max_per_domain = gr.Slider(10, 500, value=100, step=10, label="Max Results Per Domain")
            
            with gr.Row():
                crawler_start_btn = gr.Button("🚀 Start Smart Crawler", variant="primary", size="lg")
                crawler_stop_btn = gr.Button("🛑 Stop Crawler", variant="secondary")
                crawler_status = gr.Markdown("Status: **Ready**")
                crawler_refresh_btn = gr.Button("🔄 Refresh Status", variant="secondary", size="sm")
            
            with gr.Row():
                with gr.Column():
                    crawler_console = gr.Textbox(
                        label="Live Crawler Activity",
                        placeholder="Smart crawler activity will appear here...",
                        lines=15,
                        max_lines=20,
                        interactive=False,
                        show_copy_button=True
                    )
                with gr.Column():
                    crawler_stats = gr.Markdown("### Live Statistics\n- **Status:** Idle\n- **Total Searches:** 0\n- **Domains Found:** 0\n- **Contacts Found:** 0\n- **Success Rate:** 0%\n- **Next Action:** Waiting to start")
                    crawler_queue = gr.Textbox(
                        label="Smart Search Queue",
                        placeholder="Intelligent search queue will appear here...",
                        lines=8,
                        interactive=False
                    )
                    crawler_insights = gr.Textbox(
                        label="Crawler Insights",
                        placeholder="AI insights and discoveries will appear here...",
                        lines=6,
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
            - **Social Platform Integration**: LinkedIn, Twitter, GitHub, Craigslist crawling
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
            - Craigslist business services and job postings
            - Local business owner discovery via Craigslist
            
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

    # Smart Auto Crawler Functions
    crawler_active = gr.State(False)
    crawler_queue_list = gr.State([])
    crawler_iteration_count = gr.State(0)
    
    async def smart_crawler_engine(industry, location, company_size, role_types, social_focus, auto_iterate, continuous, depth, delay, max_per_domain, current_df):
        """Intelligent self-iterating crawler that learns and adapts"""
        
        def log_crawler(msg):
            timestamp = pd.Timestamp.now().strftime("%H:%M:%S")
            return f"[{timestamp}] {msg}"
        
        def update_insights(insights):
            return insights
        
        # Initialize working state
        if isinstance(current_df, pd.DataFrame):
            working_df = current_df.copy()
        elif current_df is None:
            working_df = pd.DataFrame(columns=DEFAULT_COLUMNS)
        else:
            try:
                working_df = pd.DataFrame(current_df, columns=DEFAULT_COLUMNS)
            except Exception:
                working_df = pd.DataFrame(current_df)
        
        initial_count = len(working_df)
        iteration = 0
        total_new_contacts = 0
        total_domains = set()
        successful_searches = 0
        total_searches = 0
        
        crawler_log = log_crawler("🧠 Smart Crawler Engine Starting...")
        crawler_log += "\n" + log_crawler(f"📊 Starting with {initial_count} existing contacts")
        
        insights = "🤖 Initializing AI crawler...\n"
        
        while True:
            iteration += 1
            crawler_log += "\n" + log_crawler(f"🔄 Starting iteration #{iteration}")
            
            # Generate adaptive strategies based on current data
            if iteration == 1:
                # First iteration: use user inputs
                strategies = generate_crawler_strategies(industry, location, company_size, role_types, social_focus)
                insights += f"📋 Generated {len(strategies)} initial strategies\n"
            else:
                # Subsequent iterations: learn from existing data
                strategies = generate_adaptive_strategies(working_df, industry, location, social_focus, depth)
                insights += f"🎯 Generated {len(strategies)} adaptive strategies from existing {len(working_df)} contacts\n"
            
            crawler_log += "\n" + log_crawler(f"📋 Generated {len(strategies)} strategies for iteration #{iteration}")
            
            # Execute strategies for this iteration
            iteration_new_contacts = 0
            iteration_searches = 0
            
            for i, strategy in enumerate(strategies[:5]):  # Limit per iteration
                iteration_searches += 1
                total_searches += 1
                
                crawler_log += "\n" + log_crawler(f"🔍 [{iteration}.{i+1}] {strategy['name']}")
                
                # Update live stats
                stats_text = f"""### Live Statistics
- **Status:** Running (Iteration {iteration})
- **Total Searches:** {total_searches}
- **Domains Found:** {len(total_domains)}
- **Total Contacts:** {len(working_df)}
- **New This Iteration:** {iteration_new_contacts}
- **Success Rate:** {(successful_searches / max(total_searches, 1)) * 100:.1f}%
- **Next Action:** {strategy['name'][:50]}..."""
                
                queue_text = f"Current Iteration: {iteration}\n"
                queue_text += f"Strategy {i+1}/{len(strategies[:5])}: {strategy['name']}\n"
                queue_text += f"Remaining: {len(strategies[:5]) - i - 1} searches\n"
                if continuous:
                    queue_text += f"Mode: Continuous (will restart after completion)"
                else:
                    queue_text += f"Mode: Single run"
                
                # Yield live update
                yield (
                    True,  # crawler_active
                    "Status: **Running** 🟢",  # crawler_status
                    crawler_log,  # crawler_console
                    stats_text,  # crawler_stats
                    queue_text,  # crawler_queue
                    strategies,  # crawler_queue_list
                    working_df,  # updated dataframe
                    insights  # crawler_insights
                )
                
                try:
                    # Execute search
                    df_results = await search_and_extract("", strategy['query'], "", max_per_domain)
                    
                    if not df_results.empty:
                        # Merge results
                        before_merge = len(working_df)
                        working_df = pd.concat([working_df, df_results], ignore_index=True, sort=False)
                        working_df = working_df.drop_duplicates(subset=["email", "source_url"], keep="first")
                        after_merge = len(working_df)
                        
                        contacts_found = after_merge - before_merge
                        iteration_new_contacts += contacts_found
                        total_new_contacts += contacts_found
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
                        
                        crawler_log += "\n" + log_crawler(f"✅ Found {contacts_found} new contacts")
                        
                        # Update insights with discoveries
                        if contacts_found > 0:
                            insights += f"🎯 {strategy['type']}: Found {contacts_found} contacts\n"
                            
                            # Analyze what worked
                            if "linkedin" in strategy['query'].lower() and contacts_found > 3:
                                insights += "💡 LinkedIn searches highly effective\n"
                            if "startup" in strategy['query'].lower() and contacts_found > 2:
                                insights += "💡 Startup-focused queries working well\n"
                    else:
                        crawler_log += "\n" + log_crawler(f"❌ No results from {strategy['name']}")
                    
                    # Save progress after each search
                    save_leads_to_db(working_df)
                    
                    # Intelligent delay based on success rate
                    adaptive_delay = delay
                    if successful_searches / max(total_searches, 1) > 0.8:
                        adaptive_delay = max(delay // 2, 15)  # Speed up if very successful
                    elif successful_searches / max(total_searches, 1) < 0.3:
                        adaptive_delay = delay * 2  # Slow down if struggling
                    
                    crawler_log += "\n" + log_crawler(f"⏱️ Adaptive delay: {adaptive_delay}s (success rate: {(successful_searches / max(total_searches, 1)) * 100:.1f}%)")
                    await asyncio.sleep(adaptive_delay)
                    
                except Exception as e:
                    crawler_log += "\n" + log_crawler(f"❌ Error in {strategy['name']}: {str(e)}")
            
            # End of iteration summary
            crawler_log += "\n" + log_crawler(f"📊 Iteration #{iteration} complete: {iteration_new_contacts} new contacts")
            insights += f"📊 Iteration {iteration}: +{iteration_new_contacts} contacts\n"
            
            # Decide whether to continue
            if not continuous:
                break
            
            if auto_iterate and iteration_new_contacts == 0 and iteration > 3:
                crawler_log += "\n" + log_crawler("🛑 No new contacts found in recent iterations, stopping")
                insights += "🛑 Auto-stopped: No new discoveries\n"
                break
            
            if iteration >= 10:  # Safety limit
                crawler_log += "\n" + log_crawler("🛑 Reached maximum iterations (10), stopping")
                insights += "🛑 Reached iteration limit\n"
                break
            
            # Prepare for next iteration
            if continuous or (auto_iterate and iteration_new_contacts > 0):
                crawler_log += "\n" + log_crawler(f"🔄 Preparing iteration #{iteration + 1}...")
                insights += f"🔄 Planning iteration {iteration + 1}...\n"
                await asyncio.sleep(30)  # Brief pause between iterations
        
        # Final summary
        crawler_log += "\n" + log_crawler("🎉 Smart Crawler completed!")
        crawler_log += "\n" + log_crawler(f"📊 Final: {total_new_contacts} new contacts across {iteration} iterations")
        
        final_stats = f"""### Final Statistics
- **Status:** Completed
- **Total Iterations:** {iteration}
- **Total Searches:** {total_searches}
- **Domains Found:** {len(total_domains)}
- **New Contacts Found:** {total_new_contacts}
- **Total Contacts:** {len(working_df)}
- **Success Rate:** {(successful_searches / max(total_searches, 1)) * 100:.1f}%"""
        
        insights += f"🏆 Campaign completed: {total_new_contacts} total new contacts found\n"
        
        yield (
            False,  # crawler_active
            "Status: **Completed** ✅",  # crawler_status
            crawler_log,  # crawler_console
            final_stats,  # crawler_stats
            "All iterations completed",  # crawler_queue
            strategies,  # crawler_queue_list
            working_df,  # final dataframe
            insights  # final insights
        )
        """Start the auto crawler with live updates"""
        
        def log_crawler(msg):
            timestamp = pd.Timestamp.now().strftime("%H:%M:%S")
            return f"[{timestamp}] {msg}"
        
        # Initialize the working dataframe with current data
        if isinstance(current_df, pd.DataFrame):
            working_df = current_df.copy()
        elif current_df is None:
            working_df = pd.DataFrame(columns=DEFAULT_COLUMNS)
        else:
            try:
                working_df = pd.DataFrame(current_df, columns=DEFAULT_COLUMNS)
            except Exception:
                working_df = pd.DataFrame(current_df)
        
        initial_count = len(working_df)
        
        # Generate search strategies
        strategies = generate_crawler_strategies(industry, location, company_size, role_types, social_focus)
        
        # Initialize progress tracking
        crawler_log = log_crawler("🚀 Auto Crawler Starting...")
        crawler_log += "\n" + log_crawler(f"� Starting with {initial_count} existing contacts")
        crawler_log += "\n" + log_crawler(f"�📋 Generated {len(strategies)} search strategies")
        
        total_new_contacts = 0
        total_domains = set()
        successful_searches = 0
        
        # Yield initial state
        yield (
            True,  # crawler_active
            "Status: **Running** 🟢",  # crawler_status
            crawler_log,  # crawler_console
            f"### Statistics\n- **Total Searches:** 0/{len(strategies)}\n- **Domains Found:** 0\n- **New Contacts Found:** 0\n- **Total Contacts:** {len(working_df)}\n- **Success Rate:** 0%",  # crawler_stats
            "\n".join([f"• {s['name']}" for s in strategies[:10]]) + (f"\n... and {len(strategies) - 10} more" if len(strategies) > 10 else ""),  # crawler_queue
            strategies,  # crawler_queue_list
            working_df  # updated dataframe
        )
        
        for i, strategy in enumerate(strategies[:10]):  # Limit for demo
            crawler_log += "\n" + log_crawler(f"🔍 Executing: {strategy['name']}")
            
            # Yield progress update
            yield (
                True,
                "Status: **Running** 🟢",
                crawler_log,
                f"### Statistics\n- **Total Searches:** {i}/{len(strategies)}\n- **Domains Found:** {len(total_domains)}\n- **New Contacts Found:** {total_new_contacts}\n- **Total Contacts:** {len(working_df)}\n- **Success Rate:** {(successful_searches / max(i, 1)) * 100:.1f}%",
                "\n".join([f"• {s['name']}" for s in strategies[i:i+10]]) + (f"\n... and {len(strategies) - i - 10} more" if len(strategies) > i + 10 else ""),
                strategies,
                working_df
            )
            
            try:
                # Use the existing search function with the strategy query
                df_results = await search_and_extract("", strategy['query'], "", max_per_domain)
                
                if not df_results.empty:
                    # Merge with working dataframe
                    before_merge = len(working_df)
                    working_df = pd.concat([working_df, df_results], ignore_index=True, sort=False)
                    working_df = working_df.drop_duplicates(subset=["email", "source_url"], keep="first")
                    after_merge = len(working_df)
                    
                    contacts_found = after_merge - before_merge
                    total_new_contacts += contacts_found
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
                    
                    crawler_log += "\n" + log_crawler(f"✅ Found {contacts_found} new contacts from {strategy['name']}")
                else:
                    crawler_log += "\n" + log_crawler(f"❌ No results from {strategy['name']}")
                
                # Update statistics after each search
                success_rate = (successful_searches / (i + 1)) * 100
                stats_text = f"""### Statistics
- **Total Searches:** {i + 1}/{len(strategies)}
- **Domains Found:** {len(total_domains)}
- **New Contacts Found:** {total_new_contacts}
- **Total Contacts:** {len(working_df)}
- **Success Rate:** {success_rate:.1f}%"""
                
                # Update queue
                remaining = strategies[i+1:i+11]
                queue_text = "\n".join([f"• {s['name']}" for s in remaining])
                if len(strategies) > i + 11:
                    queue_text += f"\n... and {len(strategies) - i - 11} more"
                
                # Yield live update
                yield (
                    True,
                    "Status: **Running** 🟢",
                    crawler_log,
                    stats_text,
                    queue_text,
                    strategies,
                    working_df
                )
                
                # Save intermediate results to database
                save_leads_to_db(working_df)
                
                # Simulate delay
                if delay > 0 and i < len(strategies) - 1:
                    crawler_log += "\n" + log_crawler(f"⏱️ Waiting {delay} seconds before next search...")
                    yield (
                        True,
                        "Status: **Running** 🟢",
                        crawler_log,
                        stats_text,
                        queue_text,
                        strategies,
                        working_df
                    )
                    await asyncio.sleep(delay)
                
            except Exception as e:
                crawler_log += "\n" + log_crawler(f"❌ Error in {strategy['name']}: {str(e)}")
                yield (
                    True,
                    "Status: **Running** 🟢",
                    crawler_log,
                    stats_text,
                    queue_text,
                    strategies,
                    working_df
                )
        
        # Final update
        crawler_log += "\n" + log_crawler("🎉 Auto Crawler completed!")
        crawler_log += "\n" + log_crawler(f"💾 Updated database with {len(working_df)} total contacts")
        
        final_stats = f"""### Final Statistics
- **Total Searches:** {len(strategies)}
- **Domains Found:** {len(total_domains)}
- **New Contacts Found:** {total_new_contacts}
- **Total Contacts:** {len(working_df)}
- **Success Rate:** {(successful_searches / len(strategies)) * 100:.1f}%"""
        
        yield (
            False,  # crawler stops
            "Status: **Completed** ✅",
            crawler_log,
            final_stats,
            "All searches completed",
            strategies,
            working_df
        )
    
    async def start_auto_crawler_simple(industry, location, company_size, role_types, social_focus, depth, delay, max_per_domain, current_df):
        """Simple auto crawler with immediate updates"""
        
        def log_crawler(msg):
            timestamp = pd.Timestamp.now().strftime("%H:%M:%S")
            return f"[{timestamp}] {msg}"
        
        # Initialize the working dataframe with current data
        if isinstance(current_df, pd.DataFrame):
            working_df = current_df.copy()
        elif current_df is None:
            working_df = pd.DataFrame(columns=DEFAULT_COLUMNS)
        else:
            try:
                working_df = pd.DataFrame(current_df, columns=DEFAULT_COLUMNS)
            except Exception:
                working_df = pd.DataFrame(current_df)
        
        initial_count = len(working_df)
        
        # Generate search strategies
        strategies = generate_crawler_strategies(industry, location, company_size, role_types, social_focus)
        
        # Build initial log
        crawler_log = log_crawler("🚀 Auto Crawler Starting...")
        crawler_log += "\n" + log_crawler(f"📊 Starting with {initial_count} existing contacts")
        crawler_log += "\n" + log_crawler(f"📋 Generated {len(strategies)} search strategies")
        
        total_new_contacts = 0
        total_domains = set()
        successful_searches = 0
        iteration_count = 0
        max_iterations = 3  # Allow multiple learning cycles
        
        # Multi-iteration learning loop
        for iteration in range(max_iterations):
            iteration_count += 1
            crawler_log += "\n" + log_crawler(f"🔄 === ITERATION {iteration + 1} ===")
            
            # Generate adaptive strategies based on current data
            if iteration > 0:
                adaptive_strategies = generate_adaptive_strategies(working_df, industry, location)
                strategies.extend(adaptive_strategies)
                crawler_log += "\n" + log_crawler(f"🧠 Generated {len(adaptive_strategies)} adaptive strategies from existing data")
            
            current_strategies = strategies[iteration * 5:(iteration + 1) * 5]  # 5 strategies per iteration
            
            for i, strategy in enumerate(current_strategies):
                overall_index = iteration * 5 + i
                crawler_log += "\n" + log_crawler(f"🔍 [{iteration+1}.{i+1}] Executing: {strategy['name']}")
                
                try:
                    # Use the existing search function with the strategy query
                    df_results = await search_and_extract("", strategy['query'], "", max_per_domain, 
                                                          progress_callback=lambda msg: None)  # Disable internal logging for cleaner output
                    
                    if not df_results.empty:
                        # Merge with working dataframe
                        before_merge = len(working_df)
                        working_df = pd.concat([working_df, df_results], ignore_index=True, sort=False)
                        working_df = working_df.drop_duplicates(subset=["email", "source_url"], keep="first")
                        after_merge = len(working_df)
                        
                        contacts_found = after_merge - before_merge
                        total_new_contacts += contacts_found
                        successful_searches += 1
                        
                        # Extract domains for learning
                        new_domains = set()
                        for url in df_results['source_url'].dropna():
                            try:
                                parts = tldextract.extract(url)
                                domain = f"{parts.domain}.{parts.suffix}".lower()
                                if domain:
                                    total_domains.add(domain)
                                    new_domains.add(domain)
                            except:
                                pass
                        
                        crawler_log += "\n" + log_crawler(f"✅ Found {contacts_found} new contacts from {strategy['name']}")
                        if new_domains:
                            crawler_log += "\n" + log_crawler(f"📍 New domains discovered: {', '.join(list(new_domains)[:3])}")
                    else:
                        crawler_log += "\n" + log_crawler(f"❌ No results from {strategy['name']}")
                    
                    # Save intermediate results to database after each successful search
                    if not df_results.empty:
                        save_leads_to_db(working_df)
                        crawler_log += "\n" + log_crawler("💾 Auto-saved progress to database")
                    
                    # Smart delay based on success rate
                    current_success_rate = (successful_searches / max(overall_index + 1, 1)) * 100
                    adaptive_delay = max(1, delay - int(current_success_rate / 20))  # Reduce delay for successful crawls
                    
                    if adaptive_delay > 0 and i < len(current_strategies) - 1:
                        crawler_log += "\n" + log_crawler(f"⏱️ Smart delay: {adaptive_delay}s (success rate: {current_success_rate:.1f}%)")
                        await asyncio.sleep(adaptive_delay)
                    
                except Exception as e:
                    crawler_log += "\n" + log_crawler(f"❌ Error in {strategy['name']}: {str(e)}")
            
            # Check if we should continue iterating
            if iteration < max_iterations - 1:
                crawler_log += "\n" + log_crawler(f"📊 Iteration {iteration + 1} complete. Analyzing data for next iteration...")
                
                # Only continue if we're finding new contacts
                if total_new_contacts == 0 and iteration > 0:
                    crawler_log += "\n" + log_crawler("🛑 No new contacts found. Stopping iterations.")
                    break
        
        # Final update
        crawler_log += "\n" + log_crawler("🎉 Smart Crawler completed all iterations!")
        crawler_log += "\n" + log_crawler(f"💾 Final save: {len(working_df)} total contacts in database")
        
        final_stats = f"""### Final Statistics
- **Iterations Completed:** {iteration_count}
- **Total Searches:** {successful_searches}
- **Domains Found:** {len(total_domains)}
- **New Contacts Found:** {total_new_contacts}
- **Total Contacts:** {len(working_df)}
- **Success Rate:** {(successful_searches / max(len(strategies), 1)) * 100:.1f}%"""
        
        return (
            False,  # crawler stops
            "Status: **Completed** ✅",
            crawler_log,
            final_stats,
            "All searches completed",
            strategies,
            working_df
        )
    
    def stop_auto_crawler(current_df):
        """Stop the auto crawler"""
        return False, "Status: **Stopped** 🔴", "", "### Statistics\n- **Total Searches:** 0\n- **Domains Found:** 0\n- **Contacts Found:** 0\n- **Success Rate:** 0%", "", current_df, ""
    
    def generate_adaptive_strategies(existing_df, industry, location, social_focus, depth):
        """Generate new strategies based on existing contact data"""
        strategies = []
        
        if existing_df.empty:
            return generate_crawler_strategies(industry, location, [], [], social_focus)
        
        # Analyze existing data for patterns
        domains_found = []
        companies_found = []
        
        for url in existing_df['source_url'].dropna():
            try:
                parts = tldextract.extract(url)
                domain = f"{parts.domain}.{parts.suffix}".lower()
                if domain and domain not in ['linkedin.com', 'twitter.com', 'github.com']:
                    domains_found.append(domain)
            except:
                pass
        
        # Extract company names from titles and URLs
        for title in existing_df['source_title'].dropna():
            # Simple company name extraction
            words = title.split()
            for word in words:
                if len(word) > 3 and word.istitle():
                    companies_found.append(word)
        
        # Generate adaptive strategies
        unique_domains = list(set(domains_found))[:10]
        unique_companies = list(set(companies_found))[:10]
        
        # Search competitors/similar companies
        for company in unique_companies[:5]:
            strategies.extend([
                {"name": f"Competitors of {company}", "type": "competitor_search", "query": f"competitors {company} CEO founder contact"},
                {"name": f"Partners of {company}", "type": "partner_search", "query": f"partners {company} {industry} contact"},
                {"name": f"Clients of {company}", "type": "client_search", "query": f"clients customers {company} testimonial contact"},
            ])
        
        # Explore similar domains
        for domain in unique_domains[:5]:
            strategies.extend([
                {"name": f"Similar to {domain}", "type": "similar_domain", "query": f"similar {domain} {industry} company contact"},
                {"name": f"About pages like {domain}", "type": "about_pattern", "query": f"site:{domain} about team contact"},
            ])
        
        # Industry expansion based on found patterns
        if len(existing_df) > 20:  # Enough data to analyze
            strategies.extend([
                {"name": "Industry conference speakers", "type": "conference_expansion", "query": f"{industry} conference 2024 2025 speaker contact"},
                {"name": "Industry investors", "type": "investor_search", "query": f"{industry} investor VC partner contact"},
                {"name": "Industry advisors", "type": "advisor_search", "query": f"{industry} advisor consultant expert contact"},
            ])
        
        # Social expansion
        if "LinkedIn" in social_focus:
            strategies.append({"name": "LinkedIn company connections", "type": "linkedin_expansion", "query": f"site:linkedin.com/company {industry} {location}"})
        
        return strategies[:20]  # Limit adaptive strategies
    
    def generate_crawler_strategies(industry, location, company_size, role_types, social_focus):
        """Generate advanced search strategies based on user inputs"""
        strategies = []
        
        # Use fallback terms if no industry specified
        industry_terms = industry if industry else "business CEO founder contact"
        location_terms = location if location else "contact email phone"
        
        # Industry-specific searches
        strategies.extend([
            {"name": f"{industry_terms} companies {location_terms}", "type": "company_discovery", "query": f"site:linkedin.com/company {industry_terms} {location_terms}"},
            {"name": f"{industry_terms} startups", "type": "startup_discovery", "query": f"{industry_terms} startup founders contact"},
            {"name": f"{industry_terms} news mentions", "type": "news_crawl", "query": f"{industry_terms} CEO CTO contact email"},
        ])
        
        # Role-based searches
        if role_types:
            for role in role_types:
                strategies.extend([
                    {"name": f"{role} at {industry_terms} companies", "type": "role_search", "query": f"site:linkedin.com/in {role} {industry_terms} {location_terms}"},
                    {"name": f"{role} contact pages", "type": "contact_discovery", "query": f"{role} {industry_terms} contact email phone"},
                ])
        else:
            # Default role searches if none specified
            strategies.extend([
                {"name": "CEO contact pages", "type": "contact_discovery", "query": f"CEO {industry_terms} contact email phone"},
                {"name": "Founder contact pages", "type": "contact_discovery", "query": f"founder {industry_terms} contact email phone"},
            ])
        
        # Social platform specific searches
        if "LinkedIn" in social_focus:
            strategies.append({"name": "LinkedIn company pages", "type": "linkedin_crawl", "query": f"site:linkedin.com/company {industry_terms}"})
        
        if "Twitter" in social_focus:
            strategies.append({"name": "Twitter profiles", "type": "twitter_crawl", "query": f"site:twitter.com {industry_terms} CEO founder"})
        
        if "GitHub" in social_focus:
            strategies.append({"name": "GitHub developer profiles", "type": "github_crawl", "query": f"site:github.com {industry_terms} developer contact"})
        
        if "Craigslist" in social_focus:
            strategies.extend([
                {"name": "Craigslist services", "type": "craigslist_services", "query": f"site:craigslist.org {industry_terms} services contact"},
                {"name": "Craigslist jobs", "type": "craigslist_jobs", "query": f"site:craigslist.org {industry_terms} jobs hiring {location_terms}"},
                {"name": "Craigslist business services", "type": "craigslist_biz", "query": f"site:craigslist.org 'business services' {industry_terms} {location_terms}"},
                {"name": "Craigslist for sale by owner", "type": "craigslist_fsbo", "query": f"site:craigslist.org 'for sale' {industry_terms} business owner contact"},
            ])
        
        if "AngelList" in social_focus:
            strategies.append({"name": "AngelList startups", "type": "angellist_crawl", "query": f"site:angel.co {industry_terms} startup founder"})
        
        if "Crunchbase" in social_focus:
            strategies.append({"name": "Crunchbase companies", "type": "crunchbase_crawl", "query": f"site:crunchbase.com {industry_terms} company founder CEO"})
        
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

    # Smart Crawler Event Handlers
    crawler_start_btn.click(
        smart_crawler_engine,
        inputs=[crawler_industry, crawler_location, crawler_company_size, crawler_role_types, 
                crawler_social_focus, crawler_auto_iterate, crawler_continuous, crawler_depth, crawler_delay, crawler_max_per_domain, df],
        outputs=[crawler_active, crawler_status, crawler_console, crawler_stats, crawler_queue, crawler_queue_list, df, crawler_insights],
        show_progress=True
    )
    
    crawler_stop_btn.click(
        stop_auto_crawler,
        inputs=[df],
        outputs=[crawler_active, crawler_status, crawler_console, crawler_stats, crawler_queue, df, crawler_insights]
    )
    
    def refresh_crawler_status():
        """Refresh the crawler display with current database data"""
        current_data = load_leads_from_db()
        return current_data
    
    crawler_refresh_btn.click(
        refresh_crawler_status,
        outputs=[df]
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
