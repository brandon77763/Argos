#!/usr/bin/env python3
"""
Craigslist Email Scanner
A focused tool for extracting emails from Craigslist posts
"""

import asyncio
import re
import sqlite3
import pandas as pd
import gradio as gr
import httpx
from bs4 import BeautifulSoup
from ddgs import DDGS
import tempfile
from datetime import datetime, timedelta
import tldextract
import subprocess
import time
import json
import random

# Configuration
DB_PATH = "craigslist_emails.db"
EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-.]+")
PHONE_RE = re.compile(r"(?:\+?\d[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}")

# Default columns for the results table
DEFAULT_COLUMNS = ["name", "email", "phone", "post_title", "location", "category", "url", "scan_date"]

# Continuous scanning control variables
continuous_running = False
continuous_thread = None

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
}

# VPN Management Functions
class VPNManager:
    def __init__(self):
        self.is_connected = False
        self.current_server = None
        self.is_authenticated = False
        self.available_countries = [
            "United_States", "Canada", "United_Kingdom", "Germany", "France", 
            "Netherlands", "Australia", "Japan", "Singapore", "Switzerland",
            "Sweden", "Norway", "Denmark", "Finland", "Italy", "Spain",
            "Poland", "Czech_Republic", "Austria", "Belgium", "Luxembourg"
        ]
    
    def login_with_token(self, token):
        """Login to NordVPN using access token"""
        if not token or len(token.strip()) < 20:
            return False, "Invalid token format. Token should be a long alphanumeric string."
        
        try:
            # First check if NordVPN CLI is installed
            if not self.check_nordvpn_installed():
                return False, "NordVPN CLI is not installed. Please install it first."
            
            # Logout first to clear any existing session
            subprocess.run(["nordvpn", "logout"], 
                         capture_output=True, text=True, timeout=10)
            
            # Login with token
            result = subprocess.run(
                ["nordvpn", "login", "--token", token.strip()],
                capture_output=True, text=True, timeout=30
            )
            
            if result.returncode == 0:
                self.is_authenticated = True
                return True, "Successfully authenticated with NordVPN!"
            else:
                error_msg = result.stderr.strip() if result.stderr else result.stdout.strip()
                return False, f"Login failed: {error_msg}"
                
        except subprocess.TimeoutExpired:
            return False, "Login timeout. Please try again."
        except FileNotFoundError:
            return False, "NordVPN CLI not found. Please install NordVPN CLI first."
        except Exception as e:
            return False, f"Login error: {str(e)}"
    
    def logout(self):
        """Logout from NordVPN"""
        try:
            result = subprocess.run(
                ["nordvpn", "logout"],
                capture_output=True, text=True, timeout=15
            )
            
            if result.returncode == 0:
                self.is_authenticated = False
                self.current_server = None
                self.is_connected = False
                return True, "Successfully logged out from NordVPN"
            else:
                return False, f"Logout failed: {result.stderr}"
                
        except Exception as e:
            return False, f"Logout error: {str(e)}"
    
    def check_authentication(self):
        """Check if user is authenticated with NordVPN"""
        try:
            result = subprocess.run(
                ["nordvpn", "account"],
                capture_output=True, text=True, timeout=10
            )
            
            if result.returncode == 0 and "You are not logged in" not in result.stdout:
                self.is_authenticated = True
                return True, "Authenticated"
            else:
                self.is_authenticated = False
                return False, "Not authenticated"
                
        except Exception as e:
            self.is_authenticated = False
            return False, f"Auth check failed: {str(e)}"
    
    def check_nordvpn_installed(self):
        """Check if NordVPN CLI is installed"""
        try:
            result = subprocess.run(['nordvpn', '--version'], 
                                  capture_output=True, text=True, timeout=10)
            return result.returncode == 0
        except (subprocess.SubprocessError, FileNotFoundError):
            return False
    
    def get_connection_status(self):
        """Get current VPN connection status"""
        try:
            result = subprocess.run(['nordvpn', 'status'], 
                                  capture_output=True, text=True, timeout=10)
            if result.returncode == 0:
                status_text = result.stdout
                if "Status: Connected" in status_text:
                    # Extract server info
                    for line in status_text.split('\n'):
                        if "Current server:" in line:
                            self.current_server = line.split(':')[1].strip()
                            break
                    self.is_connected = True
                    return True, self.current_server
                else:
                    self.is_connected = False
                    self.current_server = None
                    return False, "Disconnected"
            return False, "Error checking status"
        except subprocess.SubprocessError:
            return False, "NordVPN CLI not available"
    
    def connect_to_country(self, country):
        """Connect to a specific country"""
        try:
            # Disconnect first if connected
            if self.is_connected:
                subprocess.run(['nordvpn', 'disconnect'], 
                             capture_output=True, timeout=10)
                time.sleep(1)
            
            # Connect to country with shorter timeout
            result = subprocess.run(['nordvpn', 'connect', country], 
                                  capture_output=True, text=True, timeout=20)
            
            if result.returncode == 0:
                # Wait a moment for connection to establish
                time.sleep(2)
                connected, server = self.get_connection_status()
                return connected, f"Connected to {server}" if connected else "Connection failed"
            else:
                error_msg = result.stderr.strip() if result.stderr else result.stdout.strip()
                return False, f"Failed to connect: {error_msg}"
                
        except subprocess.TimeoutExpired:
            return False, "VPN connection timeout - this may have disrupted your SSH session"
        except subprocess.SubprocessError as e:
            return False, f"VPN connection error: {str(e)}"
    
    def disconnect(self):
        """Disconnect from VPN"""
        try:
            result = subprocess.run(['nordvpn', 'disconnect'], 
                                  capture_output=True, text=True, timeout=15)
            if result.returncode == 0:
                self.is_connected = False
                self.current_server = None
                return True, "Disconnected successfully"
            return False, "Failed to disconnect"
        except subprocess.SubprocessError as e:
            return False, f"Disconnect error: {str(e)}"
    
    def get_current_ip(self):
        """Get current public IP address"""
        try:
            # Try multiple IP services
            services = [
                "https://httpbin.org/ip",
                "https://api.ipify.org?format=json",
                "https://ipinfo.io/json"
            ]
            
            for service in services:
                try:
                    response = httpx.get(service, timeout=10)
                    if response.status_code == 200:
                        data = response.json()
                        if 'origin' in data:
                            return data['origin']
                        elif 'ip' in data:
                            return data['ip']
                except:
                    continue
            
            return "Unable to fetch IP"
        except Exception as e:
            return f"Error: {str(e)}"

# Global VPN manager instance
vpn_manager = VPNManager()

def init_database():
    """Initialize the SQLite database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create main emails table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT,
            email TEXT,
            phone TEXT,
            post_title TEXT,
            location TEXT,
            category TEXT,
            url TEXT UNIQUE,
            scan_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    # Create scanned URLs tracking table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS scanned_urls (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            scan_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            emails_found INTEGER DEFAULT 0
        )
    ''')
    
    # Create indexes for better performance
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_email ON emails(email)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_url ON emails(url)')
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_scanned_url ON scanned_urls(url)')
    
    conn.commit()
    conn.close()

def load_emails_from_db():
    """Load all emails from database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query('SELECT * FROM emails ORDER BY scan_date DESC', conn)
        conn.close()
        if df.empty:
            return pd.DataFrame(columns=DEFAULT_COLUMNS)
        return df[DEFAULT_COLUMNS].fillna("")
    except Exception:
        return pd.DataFrame(columns=DEFAULT_COLUMNS)

def save_email_to_db(email_data):
    """Save a single email record to database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO emails (name, email, phone, post_title, location, category, url, scan_date)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        ''', email_data)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error saving email: {e}")

def is_url_recently_scanned(url, hours=24):
    """Check if URL was scanned recently"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            SELECT COUNT(*) FROM scanned_urls 
            WHERE url = ? AND scan_date > datetime('now', '-{} hours')
        '''.format(hours), (url,))
        count = cursor.fetchone()[0]
        conn.close()
        return count > 0
    except Exception:
        return False

def record_scanned_url(url, emails_found=0):
    """Record that a URL has been scanned"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO scanned_urls (url, emails_found)
            VALUES (?, ?)
        ''', (url, emails_found))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error recording scanned URL: {e}")

async def search_craigslist(location="", category="", keywords="", max_results=50):
    """Search Craigslist for posts"""
    
    # Build search queries for Craigslist
    queries = []
    
    if location and category and keywords:
        queries.append(f"site:craigslist.org {location} {category} {keywords} email")
        queries.append(f"site:{location}.craigslist.org {category} {keywords}")
    elif location and keywords:
        queries.append(f"site:craigslist.org {location} {keywords} contact email")
        queries.append(f"site:{location}.craigslist.org {keywords}")
    elif category and keywords:
        queries.append(f"site:craigslist.org {category} {keywords} email contact")
    elif keywords:
        queries.append(f"site:craigslist.org {keywords} email contact")
        queries.append(f"site:craigslist.org {keywords} services")
    else:
        # Default broad searches
        queries = [
            "site:craigslist.org services email contact",
            "site:craigslist.org business services contact",
            "site:craigslist.org creative services email",
            "site:craigslist.org skilled trades contact"
        ]
    
    print(f"Searching with {len(queries)} queries...")
    
    # Search using DuckDuckGo
    all_results = []
    ddgs = DDGS()
    
    for query in queries:
        try:
            print(f"Searching: {query}")
            results = ddgs.text(query, max_results=max_results//len(queries))
            all_results.extend(results)
            await asyncio.sleep(1)  # Rate limiting
        except Exception as e:
            print(f"Search error: {e}")
            continue
    
    # Filter for Craigslist URLs and remove duplicates
    craigslist_urls = []
    seen_urls = set()
    
    for result in all_results:
        url = result.get('href', '')
        if 'craigslist.org' in url and url not in seen_urls:
            seen_urls.add(url)
            craigslist_urls.append({
                'url': url,
                'title': result.get('title', ''),
                'snippet': result.get('body', '')
            })
    
    print(f"Found {len(craigslist_urls)} unique Craigslist URLs")
    return craigslist_urls

async def extract_emails_from_post(url, title=""):
    """Extract emails and contact info from a Craigslist post"""
    try:
        async with httpx.AsyncClient(headers=HEADERS, timeout=15.0) as client:
            response = await client.get(url)
            if response.status_code != 200:
                return None
            
            soup = BeautifulSoup(response.text, 'html.parser')
            
            # Get the post content
            post_body = soup.find('section', {'id': 'postingbody'})
            if not post_body:
                return None
            
            text_content = post_body.get_text()
            
            # Extract emails
            emails = EMAIL_RE.findall(text_content)
            emails = [email.lower() for email in emails if not email.endswith('.png') and not email.endswith('.jpg')]
            
            if not emails:
                return None
            
            # Extract phones
            phones = PHONE_RE.findall(text_content)
            
            # Extract location from URL or page
            location = ""
            try:
                url_parts = url.split('/')
                if len(url_parts) > 2:
                    domain_parts = url_parts[2].split('.')
                    if len(domain_parts) > 1:
                        location = domain_parts[0]
            except:
                pass
            
            # Try to get location from page
            location_elem = soup.find('span', class_='postingtitletext')
            if location_elem:
                location_text = location_elem.get_text()
                if '(' in location_text and ')' in location_text:
                    location = location_text.split('(')[-1].split(')')[0]
            
            # Determine category from URL
            category = "general"
            if '/biz/' in url:
                category = "business services"
            elif '/crs/' in url:
                category = "creative services"  
            elif '/lbs/' in url:
                category = "labor/skilled trades"
            elif '/cps/' in url:
                category = "computer services"
            elif '/bts/' in url:
                category = "beauty/therapeutic services"
            elif '/evs/' in url:
                category = "event services"
            elif '/fgs/' in url:
                category = "financial/legal services"
            elif '/hss/' in url:
                category = "household services"
            elif '/lss/' in url:
                category = "lessons/tutoring"
            elif '/mas/' in url:
                category = "automotive services"
            elif '/rts/' in url:
                category = "real estate services"
            elif '/trv/' in url:
                category = "travel/vacation services"
            elif '/wet/' in url:
                category = "writing/editing/translation"
            
            # Extract name (try to find business/person name)
            name = ""
            
            # Look for name patterns in the text
            lines = text_content.split('\n')
            for line in lines[:5]:  # Check first few lines
                line = line.strip()
                if len(line) > 3 and len(line) < 50:
                    # Skip lines that are all caps or look like spam
                    if not line.isupper() and not any(spam in line.lower() for spam in ['call now', 'click here', 'visit']):
                        name = line
                        break
            
            if not name and title:
                name = title.split(' - ')[0].strip()[:50]
            
            return {
                'name': name,
                'email': ', '.join(emails[:3]),  # Limit to 3 emails
                'phone': ', '.join(phones[:2]),  # Limit to 2 phones
                'post_title': title,
                'location': location,
                'category': category,
                'url': url,
                'scan_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            }
            
    except Exception as e:
        print(f"Error extracting from {url}: {e}")
        return None

async def scan_craigslist_for_emails(location, category, keywords, max_results, skip_recent=True, progress_callback=None):
    """Main function to scan Craigslist for emails with VPN integration"""
    
    def log(msg):
        print(msg)
        if progress_callback:
            progress_callback(msg)
    
    # Check VPN status for enhanced anonymity
    try:
        vpn_connected, vpn_info = vpn_manager.get_connection_status()
        if vpn_connected:
            log(f"🔒 Scanning with VPN protection via {vpn_info}")
        else:
            log("⚠️ Scanning without VPN protection - consider using VPN tab")
    except Exception as e:
        log("⚠️ VPN status unknown - proceeding with scan")
    
    log("🔍 Starting Craigslist email scan...")
    
    # Search for Craigslist posts
    posts = await search_craigslist(location, category, keywords, max_results)
    
    if not posts:
        log("❌ No Craigslist posts found")
        return pd.DataFrame(columns=DEFAULT_COLUMNS)
    
    log(f"📄 Found {len(posts)} posts to scan")
    
    # Filter out recently scanned URLs if requested
    if skip_recent:
        filtered_posts = []
        skipped = 0
        for post in posts:
            if not is_url_recently_scanned(post['url'], hours=24):
                filtered_posts.append(post)
            else:
                skipped += 1
        
        if skipped > 0:
            log(f"⏭️ Skipped {skipped} recently scanned posts")
        
        posts = filtered_posts
    
    log(f"📧 Extracting emails from {len(posts)} posts...")
    
    # Extract emails from each post
    email_records = []
    processed = 0
    
    for i, post in enumerate(posts, 1):
        try:
            log(f"📄 Processing post {i}/{len(posts)}: {post['title'][:50]}...")
            
            email_data = await extract_emails_from_post(post['url'], post['title'])
            
            # Record that we scanned this URL
            emails_found = 0
            if email_data and email_data['email']:
                email_records.append(email_data)
                save_email_to_db(tuple(email_data[col] for col in DEFAULT_COLUMNS))
                emails_found = len(email_data['email'].split(','))
                log(f"✅ Found {emails_found} emails in post")
            
            record_scanned_url(post['url'], emails_found)
            processed += 1
            
            # Rate limiting
            if i % 5 == 0:
                await asyncio.sleep(2)
                
        except Exception as e:
            log(f"❌ Error processing post {i}: {e}")
            continue
    
    log(f"🎉 Scan complete! Found {len(email_records)} posts with emails out of {processed} processed")
    
    if email_records:
        return pd.DataFrame(email_records, columns=DEFAULT_COLUMNS)
    else:
        return pd.DataFrame(columns=DEFAULT_COLUMNS)

# Continuous Scanning Functions
continuous_console_output = ""
continuous_status_text = "**Continuous Status:** Stopped"

def update_continuous_console(msg):
    """Update the continuous console output"""
    global continuous_console_output
    timestamp = datetime.now().strftime("%H:%M:%S")
    continuous_console_output += f"[{timestamp}] {msg}\n"
    return continuous_console_output

def update_continuous_status(status):
    """Update the continuous status"""
    global continuous_status_text
    continuous_status_text = status
    return continuous_status_text

async def continuous_scan_worker(main_keywords, location, category, keyword_list, max_results, skip_recent, interval_minutes):
    """Worker function for continuous scanning"""
    global continuous_running
    
    # Handle optional keyword rotation - if no list provided, use main keywords
    if keyword_list and keyword_list.strip():
        keywords = [k.strip() for k in keyword_list.split('\n') if k.strip()]
        update_continuous_console(f"📝 Using keyword rotation: {len(keywords)} keywords")
    else:
        # If no keyword rotation list, use the main keywords field
        keywords = [main_keywords if main_keywords else ""]
        update_continuous_console(f"📝 Using main keywords field: '{main_keywords}'")
    
    if not keywords or (len(keywords) == 1 and not keywords[0]):
        keywords = [""]  # Empty search as fallback
    
    scan_count = 0
    
    while continuous_running:
        try:
            # Rotate through keywords (or use main keywords if no rotation list)
            current_keyword = keywords[scan_count % len(keywords)]
            scan_count += 1
            
            update_continuous_console(f"🔄 Starting continuous scan #{scan_count}")
            if keyword_list and keyword_list.strip():
                update_continuous_console(f"📝 Using keyword: '{current_keyword}'")
            else:
                update_continuous_console(f"📝 Using main keywords: '{current_keyword}'")
            update_continuous_status(f"**Continuous Status:** Running (Scan #{scan_count})")
            
            # Run the scan
            results = await scan_craigslist_for_emails(
                location, category, current_keyword, max_results, skip_recent, update_continuous_console
            )
            
            if not continuous_running:
                break
                
            update_continuous_console(f"✅ Scan #{scan_count} completed")
            update_continuous_console(f"⏰ Waiting {interval_minutes} minutes before next scan...")
            
            # Wait for the specified interval, checking every 2 seconds if we should stop
            wait_time = interval_minutes * 60
            for i in range(0, wait_time, 2):
                if not continuous_running:
                    break
                remaining = wait_time - i
                if remaining > 60:
                    mins = remaining // 60
                    update_continuous_status(f"**Continuous Status:** Waiting ({mins}m {remaining%60}s remaining)")
                else:
                    update_continuous_status(f"**Continuous Status:** Waiting ({remaining}s remaining)")
                await asyncio.sleep(2)
                
        except Exception as e:
            update_continuous_console(f"❌ Error in continuous scan: {str(e)}")
            # Wait less time before retrying for faster recovery
            await asyncio.sleep(10)
    
    update_continuous_console("🛑 Continuous scanning stopped")
    update_continuous_status("**Continuous Status:** Stopped")

def start_continuous_scan(main_keywords, location, category, keyword_list, max_results, skip_recent, interval_minutes, interval_text):
    """Start continuous scanning"""
    global continuous_running, continuous_thread
    
    if continuous_running:
        return "**Continuous Status:** Already running", "⚠️ Continuous scan is already running\n"
    
    continuous_running = True
    update_continuous_console("🚀 Initializing continuous scan...")
    
    if keyword_list and keyword_list.strip():
        update_continuous_console(f"📝 Keywords rotation: {keyword_list.replace(chr(10), ', ')}")
    else:
        update_continuous_console(f"📝 Using main keywords: '{main_keywords}'")
    
    update_continuous_console(f"⏰ Interval: {interval_text}")
    
    # Start the continuous scan in the background
    import threading
    def run_continuous():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        try:
            update_continuous_console("🔄 Starting continuous scan thread...")
            loop.run_until_complete(
                continuous_scan_worker(
                    main_keywords, location, category, keyword_list, max_results, skip_recent, interval_minutes
                )
            )
        except Exception as e:
            update_continuous_console(f"❌ Continuous scan error: {str(e)}")
            update_continuous_status("**Continuous Status:** Error")
        finally:
            loop.close()
    
    continuous_thread = threading.Thread(target=run_continuous, daemon=True)
    continuous_thread.start()
    
    update_continuous_status("**Continuous Status:** Starting...")
    return "**Continuous Status:** Starting...", continuous_console_output

def stop_continuous_scan():
    """Stop continuous scanning"""
    global continuous_running
    continuous_running = False
    update_continuous_console("🛑 Stopping continuous scan...")
    return "**Continuous Status:** Stopping...", continuous_console_output

# Gradio Interface
def create_interface():
    init_database()
    
    with gr.Blocks(title="Craigslist Email Scanner", theme=gr.themes.Soft()) as demo:
        gr.Markdown("# 📧 Craigslist Email Scanner\nExtract contact information from Craigslist posts efficiently")
        
        with gr.Tabs():
            with gr.Tab("🔍 Scanner"):
                with gr.Row():
                    with gr.Column():
                        gr.Markdown("### Search Parameters")
                        location_input = gr.Textbox(
                            label="Location (optional)", 
                            placeholder="e.g., seattle, nyc, losangeles",
                            info="Craigslist city/region identifier"
                        )
                        category_input = gr.Textbox(
                            label="Category (optional)", 
                            placeholder="e.g., services, gigs, jobs",
                            info="Craigslist category to focus on"
                        )
                        keywords_input = gr.Textbox(
                            label="Keywords", 
                            placeholder="e.g., photography, web design, cleaning",
                            info="What type of services/posts to find"
                        )
                        
                        with gr.Row():
                            max_results = gr.Slider(10, 200, value=50, step=10, label="Max Results")
                            skip_recent = gr.Checkbox(label="Skip Recently Scanned (24h)", value=True)
                        
                        scan_btn = gr.Button("🚀 Start Scanning", variant="primary", size="lg")
                        
                        # Continuous Scan Controls
                        gr.Markdown("### 🔄 Continuous Scanning")
                        with gr.Row():
                            continuous_scan = gr.Checkbox(
                                label="Enable Continuous Scanning",
                                value=False,
                                info="Automatically repeat scans with different keywords/locations"
                            )
                            scan_interval = gr.Slider(
                                1, 30, value=2, step=1, 
                                label="Interval (minutes)",
                                info="Time between scans (1-30 minutes for faster scanning)"
                            )
                        
                        with gr.Row():
                            fast_mode = gr.Checkbox(
                                label="Fast Mode (seconds)",
                                value=False,
                                info="Use seconds instead of minutes for ultra-fast scanning"
                            )
                            fast_interval = gr.Slider(
                                30, 300, value=60, step=30,
                                label="Fast Interval (seconds)",
                                info="Time between scans in seconds (30-300s)"
                            )
                        
                        with gr.Row():
                            start_continuous_btn = gr.Button("🔄 Start Continuous", variant="primary")
                            stop_continuous_btn = gr.Button("⏹️ Stop Continuous", variant="secondary")
                        
                        with gr.Row():
                            continuous_status = gr.Markdown("**Continuous Status:** Stopped")
                            refresh_continuous_btn = gr.Button("🔄 Refresh Status", variant="secondary", size="sm")
                        
                        # Keyword rotation for continuous scanning
                        gr.Markdown("#### Keyword Rotation (optional for continuous scans)")
                        keyword_list = gr.Textbox(
                            label="Keyword List (one per line) - Optional",
                            placeholder="photography\nweb design\ncleaning\nhandyman\ncatering\n\n(Leave empty to use main keywords field)",
                            lines=4,
                            info="Different keywords to rotate through during continuous scanning. If empty, will use the main Keywords field above."
                        )
                        
                    with gr.Column():
                        gr.Markdown("### Live Progress")
                        console = gr.Textbox(
                            label="Scan Progress",
                            placeholder="Scan progress will appear here...",
                            lines=15,
                            interactive=False,
                            show_copy_button=True
                        )
                        
                        scan_status = gr.Markdown("**Status:** Ready to scan")
            
            with gr.Tab("📧 Results"):
                with gr.Row():
                    gr.Markdown("### Found Emails")
                    email_count = gr.HTML("<div style='text-align: right; font-size: 18px; font-weight: bold; color: #2563eb;'>📊 Total Emails: 0</div>")
                
                results_table = gr.Dataframe(
                    value=load_emails_from_db(),
                    headers=DEFAULT_COLUMNS,
                    interactive=True,
                    wrap=True,
                    datatype=["str"] * len(DEFAULT_COLUMNS)
                )
                
                with gr.Row():
                    refresh_btn = gr.Button("🔄 Refresh Results")
                    clear_btn = gr.Button("🗑️ Clear All", variant="secondary")
                    export_csv = gr.Button("📄 Export CSV")
                    download_file = gr.File(label="Download", interactive=False)
            
            with gr.Tab("📊 Statistics"):
                with gr.Column():
                    stats_display = gr.Markdown("### Scanning Statistics\nLoading...")
                    
                    gr.Markdown("### Recent Activity")
                    recent_activity = gr.Dataframe(
                        headers=["URL", "Scan Date", "Emails Found"],
                        interactive=False
                    )
                    
                    refresh_stats_btn = gr.Button("🔄 Refresh Statistics")
            
            with gr.Tab("🔒 VPN Control"):
                gr.Markdown("### NordVPN Integration\nManage VPN connections for anonymous scanning and rate limit avoidance.")
                
                with gr.Row():
                    with gr.Column(scale=2):
                        gr.Markdown("#### Authentication")
                        with gr.Row():
                            token_input = gr.Textbox(
                                placeholder="Enter your NordVPN access token (e.g., e9f2abc95c7dc14d9f7d...)",
                                label="Access Token",
                                type="password",
                                info="Your NordVPN access token for CLI authentication"
                            )
                        
                        with gr.Row():
                            login_btn = gr.Button("🔑 Login with Token", variant="primary")
                            logout_btn = gr.Button("🚪 Logout", variant="secondary")
                        
                        auth_status = gr.Markdown("**Auth Status:** Not logged in")
                        
                        gr.Markdown("---")
                        
                        gr.Markdown("#### Connection Status")
                        vpn_status = gr.Markdown("**Status:** Checking...")
                        current_ip = gr.Markdown("**IP Address:** Fetching...")
                        
                        gr.Markdown("#### VPN Controls")
                        with gr.Row():
                            country_dropdown = gr.Dropdown(
                                choices=vpn_manager.available_countries,
                                label="Select Country",
                                value="United_States"
                            )
                            auto_rotate = gr.Checkbox(
                                label="Auto-rotate countries",
                                value=False,
                                info="Automatically change VPN country during scanning"
                            )
                        
                        with gr.Row():
                            connect_btn = gr.Button("🔌 Connect VPN", variant="primary")
                            disconnect_btn = gr.Button("🔌 Disconnect VPN", variant="secondary")
                            refresh_status_btn = gr.Button("🔄 Refresh Status")
                        
                        vpn_console = gr.Textbox(
                            label="VPN Activity Log",
                            placeholder="VPN connection logs will appear here...",
                            lines=8,
                            interactive=False,
                            show_copy_button=True
                        )
                    
                    with gr.Column(scale=1):
                        gr.Markdown("#### Quick Info")
                        vpn_info = gr.Markdown("""
**Benefits of VPN Usage:**
- 🛡️ **Anonymous scanning** - Hide your real IP
- 🚀 **Avoid rate limits** - Change location to reset limits  
- 🌍 **Geographic diversity** - Access region-specific content
- 🔄 **Rotation capability** - Auto-change countries during long scans

**Requirements:**
- NordVPN subscription
- NordVPN CLI installed (`sudo apt install nordvpn`)
- Login: `nordvpn login`

**Auto-Rotation:**
When enabled, the scanner will automatically change VPN countries every 10-15 searches to avoid detection and rate limits.
                        """)
                        
                        gr.Markdown("#### VPN Installation")
                        install_status = gr.Markdown("**NordVPN CLI:** Checking...")
                        install_btn = gr.Button("📋 Installation Guide", variant="secondary")
        
        # Event handlers
        def run_scan(location, category, keywords, max_results, skip_recent):
            console_output = ""
            
            def update_console(msg):
                nonlocal console_output
                console_output += msg + "\n"
                return console_output
            
            try:
                update_console("🚀 Starting Craigslist scan...")
                
                # Run the scan synchronously (we'll make it async in the background)
                import asyncio
                new_results = asyncio.run(scan_craigslist_for_emails(
                    location, category, keywords, max_results, skip_recent, update_console
                ))
                
                # Load all results from database
                all_results = load_emails_from_db()
                email_count_value = len(all_results)
                
                update_console(f"✅ Scan completed! Total emails in database: {email_count_value}")
                
                return console_output, "**Status:** Completed ✅", all_results
                
            except Exception as e:
                update_console(f"❌ Error during scan: {str(e)}")
                return console_output, "**Status:** Error ❌", load_emails_from_db()
        
        def refresh_results():
            data = load_emails_from_db()
            count = len(data)
            count_html = f"<div style='text-align: right; font-size: 18px; font-weight: bold; color: #2563eb;'>📊 Total Emails: {count}</div>"
            return data, count_html
        
        def clear_all_results():
            try:
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute('DELETE FROM emails')
                cursor.execute('DELETE FROM scanned_urls')
                conn.commit()
                conn.close()
                return pd.DataFrame(columns=DEFAULT_COLUMNS), "<div style='text-align: right; font-size: 18px; font-weight: bold; color: #2563eb;'>📊 Total Emails: 0</div>"
            except Exception:
                return results_table.value, email_count.value
        
        def export_to_csv():
            try:
                data = load_emails_from_db()
                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
                data.to_csv(temp_file.name, index=False)
                return temp_file.name
            except Exception:
                return None
        
        def get_statistics():
            try:
                conn = sqlite3.connect(DB_PATH)
                
                # Get overall stats
                cursor = conn.cursor()
                cursor.execute('SELECT COUNT(*) FROM emails')
                total_emails = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(DISTINCT url) FROM emails')
                unique_posts = cursor.fetchone()[0]
                
                cursor.execute("SELECT COUNT(*) FROM emails WHERE scan_date > datetime('now', '-24 hours')")
                recent_emails = cursor.fetchone()[0]
                
                cursor.execute('SELECT COUNT(*) FROM scanned_urls')
                total_scanned = cursor.fetchone()[0]
                
                # Get recent activity
                cursor.execute('''
                    SELECT url, scan_date, emails_found 
                    FROM scanned_urls 
                    ORDER BY scan_date DESC 
                    LIMIT 20
                ''')
                recent_data = cursor.fetchall()
                
                conn.close()
                
                stats_text = f"""### Scanning Statistics
- **Total Emails Found:** {total_emails}
- **Unique Posts Scanned:** {unique_posts}
- **Emails Found (24h):** {recent_emails}
- **Total URLs Scanned:** {total_scanned}
- **Success Rate:** {(unique_posts/max(total_scanned,1)*100):.1f}%"""
                
                recent_df = pd.DataFrame(recent_data, columns=["URL", "Scan Date", "Emails Found"])
                
                return stats_text, recent_df
                
            except Exception as e:
                return f"### Statistics\n**Error loading stats:** {str(e)}", pd.DataFrame()
        
        # Connect event handlers
        scan_btn.click(
            run_scan,
            inputs=[location_input, category_input, keywords_input, max_results, skip_recent],
            outputs=[console, scan_status, results_table]
        )
        
        # Continuous scan handlers
        def start_continuous_scanning(main_keywords, location, category, keyword_list, max_results, skip_recent, scan_interval, fast_mode, fast_interval):
            """Handle starting continuous scanning"""
            # Use fast mode interval if enabled, otherwise use regular interval
            actual_interval = fast_interval / 60 if fast_mode else scan_interval
            interval_text = f"{fast_interval}s" if fast_mode else f"{scan_interval}m"
            
            # Keywords are now optional - if no keyword list provided, it will use the main keywords field
            return start_continuous_scan(main_keywords, location, category, keyword_list, max_results, skip_recent, actual_interval, interval_text)
        
        def stop_continuous_scanning():
            """Handle stopping continuous scanning"""
            return stop_continuous_scan()
        
        def get_continuous_updates():
            """Get current continuous scan status and console output"""
            global continuous_console_output, continuous_status_text
            return continuous_status_text, continuous_console_output, load_emails_from_db()
        
        # Connect continuous scan events
        start_continuous_btn.click(
            start_continuous_scanning,
            inputs=[keywords_input, location_input, category_input, keyword_list, max_results, skip_recent, scan_interval, fast_mode, fast_interval],
            outputs=[continuous_status, console]
        )
        
        stop_continuous_btn.click(
            stop_continuous_scanning,
            outputs=[continuous_status, console]
        )
        
        # Connect refresh button for continuous status
        refresh_continuous_btn.click(
            get_continuous_updates,
            outputs=[continuous_status, console, results_table]
        )
        
        refresh_btn.click(refresh_results, outputs=[results_table, email_count])
        clear_btn.click(clear_all_results, outputs=[results_table, email_count])
        export_csv.click(export_to_csv, outputs=[download_file])
        refresh_stats_btn.click(get_statistics, outputs=[stats_display, recent_activity])
        
        # VPN Event Handlers
        def login_with_token(token):
            """Login to NordVPN with access token"""
            if not token or not token.strip():
                return "**Auth Status:** ❌ Please enter your access token", "⚠️ Please enter your NordVPN access token\n"
            
            vpn_log = f"🔑 Logging in with access token...\n"
            try:
                success, message = vpn_manager.login_with_token(token)
                vpn_log += f"{'✅' if success else '❌'} {message}\n"
                
                if success:
                    auth_text = "**Auth Status:** ✅ Authenticated"
                    vpn_log += "✅ Ready to connect to VPN servers\n"
                else:
                    auth_text = f"**Auth Status:** ❌ {message}"
                
                return auth_text, vpn_log
                
            except Exception as e:
                vpn_log += f"❌ Error: {str(e)}\n"
                return "**Auth Status:** ❌ Login error", vpn_log
        
        def logout_vpn():
            """Logout from NordVPN"""
            vpn_log = "🚪 Logging out from NordVPN...\n"
            try:
                success, message = vpn_manager.logout()
                vpn_log += f"{'✅' if success else '❌'} {message}\n"
                
                if success:
                    auth_text = "**Auth Status:** 🚪 Logged out"
                    vpn_log += "ℹ️ You'll need to login again to use VPN features\n"
                else:
                    auth_text = f"**Auth Status:** ❌ Logout failed"
                
                return auth_text, vpn_log
                
            except Exception as e:
                vpn_log += f"❌ Error: {str(e)}\n"
                return "**Auth Status:** ❌ Logout error", vpn_log
        
        def connect_vpn(country):
            """Connect to VPN country"""
            vpn_log = f"🔌 Connecting to {country}...\n"
            vpn_log += "⚠️  Warning: VPN connection may temporarily disrupt SSH sessions\n"
            try:
                # Check if authenticated first
                if not vpn_manager.is_authenticated:
                    auth_connected, auth_info = vpn_manager.check_authentication()
                    if not auth_connected:
                        vpn_log += "❌ Not authenticated. Please login with your access token first.\n"
                        return "**Status:** ❌ Authentication required", "**IP Address:** Not connected", vpn_log
                
                success, message = vpn_manager.connect_to_country(country)
                vpn_log += f"{'✅' if success else '❌'} {message}\n"
                
                if success:
                    # Get new IP
                    new_ip = vpn_manager.get_current_ip()
                    vpn_log += f"🌐 New IP: {new_ip}\n"
                    
                    status_text = f"**Status:** Connected to {vpn_manager.current_server}"
                    ip_text = f"**IP Address:** {new_ip}"
                else:
                    status_text = "**Status:** Connection failed"
                    ip_text = "**IP Address:** Connection failed"
                
                return status_text, ip_text, vpn_log
                
            except Exception as e:
                vpn_log += f"❌ Error: {str(e)}\n"
                return "**Status:** Error", "**IP Address:** Error", vpn_log
        
        def disconnect_vpn():
            """Disconnect from VPN"""
            vpn_log = "🔌 Disconnecting from VPN...\n"
            try:
                success, message = vpn_manager.disconnect()
                vpn_log += f"{'✅' if success else '❌'} {message}\n"
                
                if success:
                    # Get new IP
                    new_ip = vpn_manager.get_current_ip()
                    vpn_log += f"🌐 Current IP: {new_ip}\n"
                    
                    status_text = "**Status:** Disconnected"
                    ip_text = f"**IP Address:** {new_ip}"
                else:
                    status_text = "**Status:** Disconnect failed"
                    ip_text = "**IP Address:** Unknown"
                
                return status_text, ip_text, vpn_log
                
            except Exception as e:
                vpn_log += f"❌ Error: {str(e)}\n"
                return "**Status:** Error", "**IP Address:** Error", vpn_log
        
        def refresh_vpn_status():
            """Refresh VPN status, authentication, and IP"""
            vpn_log = "🔄 Refreshing VPN status...\n"
            try:
                # Check authentication first
                auth_connected, auth_info = vpn_manager.check_authentication()
                if auth_connected:
                    auth_text = "**Auth Status:** ✅ Authenticated"
                    vpn_log += "✅ Authentication: Valid\n"
                else:
                    auth_text = f"**Auth Status:** ❌ {auth_info}"
                    vpn_log += f"❌ Authentication: {auth_info}\n"
                
                # Check VPN connection
                connected, server_info = vpn_manager.get_connection_status()
                current_ip_addr = vpn_manager.get_current_ip()
                
                if connected:
                    status_text = f"**Status:** Connected to {server_info}"
                    vpn_log += f"✅ Connected to: {server_info}\n"
                else:
                    status_text = f"**Status:** {server_info}"
                    vpn_log += f"ℹ️ {server_info}\n"
                
                ip_text = f"**IP Address:** {current_ip_addr}"
                vpn_log += f"🌐 Current IP: {current_ip_addr}\n"
                
                # Check if NordVPN CLI is installed
                if vpn_manager.check_nordvpn_installed():
                    install_text = "**NordVPN CLI:** ✅ Installed"
                    vpn_log += "✅ NordVPN CLI is installed\n"
                else:
                    install_text = "**NordVPN CLI:** ❌ Not installed"
                    vpn_log += "❌ NordVPN CLI not found\n"
                
                return auth_text, status_text, ip_text, vpn_log, install_text
                
            except Exception as e:
                vpn_log += f"❌ Error: {str(e)}\n"
                return "**Auth Status:** Error", "**Status:** Error", "**IP Address:** Error", vpn_log, "**NordVPN CLI:** Unknown"
        
        def show_installation_guide():
            """Show VPN installation guide"""
            guide = """📋 **NordVPN CLI Installation Guide:**

**Ubuntu/Debian:**
```bash
# Download and install NordVPN
curl -sSf https://downloads.nordcdn.com/apps/linux/install.sh | sh

# Login to your account
nordvpn login

# Set auto-connect off (for manual control)
nordvpn set autoconnect off
```

**After Installation:**
1. Run `nordvpn login` and follow the browser login
2. Test connection with `nordvpn connect`
3. Check status with `nordvpn status`
4. Refresh this tab to verify installation

**Troubleshooting:**
- Make sure you have an active NordVPN subscription
- Run `sudo systemctl enable nordvpnd` if daemon issues
- Use `nordvpn logout` and `nordvpn login` to reset auth

**Commands:**
- `nordvpn countries` - List available countries
- `nordvpn connect [country]` - Connect to specific country
- `nordvpn disconnect` - Disconnect VPN
- `nordvpn status` - Check connection status"""
            
            return guide
        
        # Connect VPN event handlers
        login_btn.click(
            login_with_token,
            inputs=[token_input],
            outputs=[auth_status, vpn_console]
        )
        
        logout_btn.click(
            logout_vpn,
            outputs=[auth_status, vpn_console]
        )
        
        connect_btn.click(
            connect_vpn,
            inputs=[country_dropdown],
            outputs=[vpn_status, current_ip, vpn_console]
        )
        
        disconnect_btn.click(
            disconnect_vpn,
            outputs=[vpn_status, current_ip, vpn_console]
        )
        
        refresh_status_btn.click(
            refresh_vpn_status,
            outputs=[auth_status, vpn_status, current_ip, vpn_console, install_status]
        )
        
        install_btn.click(
            show_installation_guide,
            outputs=[vpn_console]
        )
        
        # Initialize components
        demo.load(refresh_results, outputs=[results_table, email_count])
        demo.load(get_statistics, outputs=[stats_display, recent_activity])
        demo.load(refresh_vpn_status, outputs=[auth_status, vpn_status, current_ip, vpn_console, install_status])
    
    return demo

if __name__ == "__main__":
    import socket
    
    def get_local_ip():
        """Get the local IP address"""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect(("8.8.8.8", 80))
            local_ip = s.getsockname()[0]
            s.close()
            return local_ip
        except Exception:
            return "127.0.0.1"
    
    demo = create_interface()
    local_ip = get_local_ip()
    port = 7861
    
    print("=" * 60)
    print("🔍 CRAIGSLIST SCANNER - VPN & SSH FRIENDLY STARTUP")
    print("=" * 60)
    print(f"🌐 Local IP: {local_ip}")
    print(f"🔌 Port: {port}")
    print("📍 Access URLs:")
    print(f"   • Localhost: http://localhost:{port}")
    print(f"   • Local IP:  http://{local_ip}:{port}")
    print(f"   • All IPs:   http://0.0.0.0:{port}")
    print("")
    print("� SSH/VS Code Access:")
    print(f"   • SSH Tunnel: ssh -L {port}:localhost:{port} user@server")
    print(f"   • VS Code Forward: Forward port {port} in VS Code")
    print(f"   • Then access: http://localhost:{port}")
    print("")
    print("�💡 Connection Tips:")
    print("   • VPN: If localhost doesn't work, try the Local IP URL")
    print("   • SSH: Use port forwarding for remote access")
    print("   • VS Code: Use 'Forward a Port' in terminal panel")
    print("   • Check VPN settings to allow local network access")
    print("=" * 60)
    print("")
    
    # Launch with VPN-friendly and SSH-friendly settings
    demo.launch(
        server_name="0.0.0.0",  # Bind to all interfaces for VPN/SSH compatibility
        server_port=port,
        share=False,
        inbrowser=False,        # Don't auto-open browser (better for SSH)
        debug=False,
        quiet=False,
        show_error=True,
        allowed_paths=["./"]    # Allow local file access
    )
