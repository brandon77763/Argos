import os
import re
import asyncio
import httpx
import pandas as pd
from bs4 import BeautifulSoup
from dotenv import load_dotenv
import gradio as gr
from duckduckgo_search import DDGS
import tldextract
from rapidfuzz import fuzz

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


async def fetch(client: httpx.AsyncClient, url: str) -> tuple[str, str]:
    try:
        r = await client.get(url, timeout=REQUEST_TIMEOUT, follow_redirects=True)
        if r.status_code >= 400:
            return url, ""
        return url, r.text
    except Exception:
        return url, ""


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


async def gather_pages(urls: list[str]) -> list[tuple[str, str]]:
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(headers=HEADERS) as client:
        async def bound(url):
            async with sem:
                return await fetch(client, url)
        tasks = [bound(u) for u in urls]
        return await asyncio.gather(*tasks)


async def search_and_extract(domains: str, keywords: str, extra: str, max_results: int):
    queries = build_queries(domains, keywords, extra)
    seen = set()
    serps = []
    for q in queries:
        res = await search_ddg(q, max_results=max_results)
        for r in res:
            href = r.get("href")
            if not href or href in seen:
                continue
            seen.add(href)
            serps.append(r)

    pages = await gather_pages([r["href"] for r in serps])
    rows = [parse_contacts(u, h) for (u, h) in pages]

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

    df = pd.DataFrame(deduped, columns=DEFAULT_COLUMNS)
    return df


# Gradio UI
with gr.Blocks(title="Argos Lead Finder") as demo:
    gr.Markdown("# Argos Lead Finder\nSearch public pages for emails, phones, and social links. Edit and export your leads table.")

    with gr.Tabs():
        with gr.Tab("Search"):
            with gr.Group():
                with gr.Row():
                    domains = gr.Textbox(label="Domains", placeholder="example.com, example.org", scale=2)
                    keywords = gr.Textbox(label="Keywords", placeholder="sales, marketing, contact", scale=2)
                with gr.Row():
                    extra = gr.Textbox(label="Extra terms", placeholder="press, team, partners", scale=3)
                    max_results = gr.Slider(10, 100, value=30, step=5, label="Max results/query", scale=1)
                with gr.Row():
                    search_btn = gr.Button("Search & Extract", variant="primary", scale=1)
                    status = gr.Markdown("", elem_id="status", scale=3)

        with gr.Tab("Leads"):
            gr.Markdown("### Leads table\nEdit cells inline. Use controls below to add/remove columns and rows, or import/export.")
            df = gr.Dataframe(
                headers=DEFAULT_COLUMNS,
                datatype=["str"] * len(DEFAULT_COLUMNS),
                row_count=(1, "dynamic"),
                col_count=(len(DEFAULT_COLUMNS), "dynamic"),
                wrap=True,
                interactive=True,
                height=420,
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
                        export_csv_btn = gr.DownloadButton("Export CSV", file_name="leads.csv")
                        export_json_btn = gr.DownloadButton("Export JSON", file_name="leads.json")

    async def do_search(domains_val, keywords_val, extra_val, max_results_val, table):
        status.update("Running search…")
        df_new = await search_and_extract(domains_val or "", keywords_val or "", extra_val or "", int(max_results_val or 20))

        # Normalize current table to DataFrame
        if isinstance(table, pd.DataFrame):
            cur = table
        elif table is None:
            cur = pd.DataFrame(columns=df.headers)
        else:
            try:
                cur = pd.DataFrame(table, columns=df.headers)
            except Exception:
                cur = pd.DataFrame(table)

        if cur.empty:
            out = df_new
        else:
            out = pd.concat([cur, df_new], ignore_index=True, sort=False)
            out = out.drop_duplicates(subset=["email", "source_url"], keep="first")

        status.update(f"Found {len(df_new)} new rows. Total: {len(out)}")
        return out

    def add_column(table, name):
        if not name:
            return table
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        if name not in cur.columns:
            cur[name] = ""
        return cur

    def del_column(table, name):
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        if name in cur.columns:
            cur = cur.drop(columns=[name])
        return cur

    def add_row(table):
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        if cur.empty:
            cur = pd.DataFrame([{}], columns=df.headers)
        else:
            empty = {c: "" for c in cur.columns}
            cur = pd.concat([cur, pd.DataFrame([empty])], ignore_index=True)
        return cur

    def del_row(table, idx):
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        try:
            idx = int(idx)
            if 0 <= idx < len(cur):
                cur = cur.drop(index=idx).reset_index(drop=True)
        except Exception:
            pass
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
            return merged
        except Exception:
            return cur

    def to_csv_bytes(table):
        import io
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        buff = io.StringIO()
        cur.to_csv(buff, index=False)
        return buff.getvalue()

    def to_json_bytes(table):
        import io
        cur = table if isinstance(table, pd.DataFrame) else pd.DataFrame(table or [], columns=df.headers)
        buff = io.StringIO()
        cur.to_json(buff, orient="records", indent=2)
        return buff.getvalue()

    search_btn.click(do_search, inputs=[domains, keywords, extra, max_results, df], outputs=df)
    add_col_btn.click(add_column, inputs=[df, add_col_name], outputs=df)
    del_col_btn.click(del_column, inputs=[df, del_col_name], outputs=df)
    add_row_btn.click(add_row, inputs=[df], outputs=df)
    del_row_btn.click(del_row, inputs=[df, del_row_idx], outputs=df)
    import_btn.click(do_import, inputs=[imp, df], outputs=df)
    export_csv_btn.click(to_csv_bytes, inputs=[df], outputs=export_csv_btn)
    export_json_btn.click(to_json_bytes, inputs=[df], outputs=export_json_btn)

if __name__ == "__main__":
    demo.queue().launch()
