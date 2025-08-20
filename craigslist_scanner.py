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
from collections import deque
import builtins

# Configuration
DB_PATH = "craigslist_emails.db"
OPTIMIZATION_DB = "crawler_optimization.db"
EMAIL_RE = re.compile(r"[a-zA-Z0-9_.+\-]+@[a-zA-Z0-9\-]+\.[a-zA-Z0-9\-.]+")
PHONE_RE = re.compile(r"(?:\+?\d[\s.-]?)?(?:\(\d{3}\)|\d{3})[\s.-]?\d{3}[\s.-]?\d{4}")

# Default columns for the results table - matches CSV export format
DEFAULT_COLUMNS = ["first_name", "last_name", "title", "company_name", "email", "phone", "stage", "linkedin_url", "location", "category", "url", "scan_date"]

# Continuous scanning control variables
continuous_running = False
continuous_thread = None

# Auto Repeat Crawler live output management
auto_crawler_output = ""
super_verbose_mode = False
MAX_OUTPUT_CHARS = 40000  # Keep last 40k characters

def manage_auto_output(new_message):
    """Manage auto crawler output with character limit"""
    global auto_crawler_output
    timestamp = datetime.now().strftime("%H:%M:%S")
    auto_crawler_output += f"[{timestamp}] {new_message}\n"
    
    # Keep only last 40k characters
    if len(auto_crawler_output) > MAX_OUTPUT_CHARS:
        auto_crawler_output = auto_crawler_output[-MAX_OUTPUT_CHARS:]
    
    return auto_crawler_output

def get_auto_output():
    """Get current auto crawler output"""
    return auto_crawler_output

def clear_auto_output():
    """Clear auto crawler output"""
    global auto_crawler_output
    auto_crawler_output = ""

# Verbose console: keep only the last 10,000 lines
VERBOSE_MAX_LINES = 10000
_verbose_buffer = deque(maxlen=VERBOSE_MAX_LINES)

def verbose_log(message: str):
    """Append message lines to the global verbose buffer with timestamps."""
    ts = datetime.now().strftime("%H:%M:%S")
    # Split incoming text by lines to enforce true line count
    for line in str(message).splitlines():
        _verbose_buffer.append(f"[{ts}] {line}")

def get_verbose_text() -> str:
    """Return the entire verbose buffer as a single string."""
    return "\n".join(_verbose_buffer)

def clear_verbose_log():
    """Clear the verbose buffer."""
    _verbose_buffer.clear()

# Monkey-patch print to also log to the verbose console without recursion
_original_print = builtins.print
def _verbose_print(*args, **kwargs):
    _original_print(*args, **kwargs)
    try:
        msg = " ".join(str(a) for a in args)
        verbose_log(msg)
    except Exception:
        # Never let logging crash the app
        pass
builtins.print = _verbose_print

# Comprehensive Craigslist crawler data
CRAIGSLIST_LOCATIONS = [
    # Major US Cities
    "newyork", "losangeles", "chicago", "houston", "phoenix", "philadelphia", 
    "sanantonio", "sandiego", "dallas", "sanjose", "austin", "jacksonville",
    "fortworth", "columbus", "charlotte", "francisco", "indianapolis", "seattle",
    "denver", "boston", "elpaso", "detroit", "nashville", "portland", "memphis",
    "oklahomacity", "lasvegas", "louisville", "baltimore", "milwaukee", "albuquerque",
    "tucson", "fresno", "sacramento", "mesa", "kansascity", "atlanta", "longbeach",
    "colorado", "raleigh", "miami", "virginiabeach", "omaha", "oakland", "minneapolis",
    "tulsa", "cleveland", "wichita", "arlington", "neworleans", "bakersfield",
    "tampa", "honolulu", "anaheim", "aurora", "santaana", "stlouis", "riverside",
    "corpus", "lexington", "pittsburgh", "anchorage", "stockton", "cincinnati",
    "stpaul", "toledo", "greensboro", "newark", "plano", "henderson", "lincoln",
    "buffalo", "jerseycity", "chula", "fortwayne", "orlando", "laredo", "norfolk",
    "chandler", "madison", "lubbock", "baton", "durham", "garland", "glendale",
    "reno", "hialeah", "chesapeake", "scottsdale", "northlas", "irving", "fremont",
    "irvine", "birmingham", "rochester", "sanbernadino", "spokane", "gilbert",
    "arlington", "montgomery", "boise", "richmond", "desMoines", "modesto", "fayetteville",
    "shreveport", "akron", "tacoma", "aurora", "oxnard", "fontana", "yonkers",
    "augusta", "mobile", "littlerock", "amarillo", "moreno", "glendale", "huntington",
    "columbus", "grandrapids", "saltlake", "tallahassee", "worcester", "newport",
    "providence", "overland", "santaclara", "garden", "oceanside", "chattanooga",
    "fortlauderdale", "rancho", "santarosa", "tempe", "ontario", "eugene", "pembroke",
    "salem", "cape", "sioux", "springfield", "peoria", "lancaster", "hayward",
    "salinas", "jackson", "hollywood", "sunnyvale", "macon", "lakewood", "torrance",
    "mcallen", "joliet", "rockford", "naperville", "paterson", "savannah", "bridgeport",
    "alexandria", "pomona", "orange", "fullerton", "pasadena", "killeen", "hampton",
    "warren", "midland", "miami", "carrollton", "coral", "thousand", "cedar", "topeka",
    "simi", "stamford", "concord", "hartford", "kent", "lafayette", "ventura", "abilene",
    "sterling", "westminster", "provo", "waterbury", "manchester", "daly", "allentown"
]

CRAIGSLIST_CATEGORIES = [
    # Jobs
    "jjj",  # all jobs
    "acc", "ofc", "bus", "csr", "etc", "fbh", "gov", "hea", "hum", "eng", "edu",
    "fin", "gig", "hos", "hse", "hum", "lab", "leg", "mnu", "mkt", "med", "npo",
    "rej", "ret", "sls", "spa", "skd", "tch", "trp", "vol", "web", "wri",
    
    # Services
    "bbb",  # all services
    "aos", "aut", "bts", "biz", "cps", "crs", "evs", "fgs", "fns", "hss", "lgs",
    "lss", "mas", "nps", "pet", "rts", "sks", "thp", "wet",
    
    # For Sale
    "sss",  # all for sale
    "ata", "pts", "bab", "bar", "bik", "boo", "cds", "car", "cta", "clt", "cto",
    "ele", "emg", "grd", "hvy", "jwl", "mat", "mob", "msg", "pha", "pho", "rvs",
    "spo", "sys", "tls", "toy", "wan",
    
    # Gigs
    "ggg",  # all gigs
    "cpg", "crg", "cwg", "dmg", "evg", "lbg", "msg", "tlg", "wrg",
    
    # Housing
    "hhh",  # all housing
    "apa", "roo", "sub", "hsw", "off", "swp", "vac", "rea",
    
    # Community
    "ccc",  # all community
    "act", "ats", "chi", "cls", "eve", "grp", "vol", "gen"
]

# URL tracking database functions
def init_url_tracking_db():
    """Initialize URL tracking database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Create comprehensive URL tracking table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS url_queue (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            url TEXT UNIQUE NOT NULL,
            location TEXT,
            category TEXT,
            status TEXT DEFAULT 'pending',
            priority INTEGER DEFAULT 0,
            discovered_date DATETIME DEFAULT CURRENT_TIMESTAMP,
            scanned_date DATETIME,
            emails_found INTEGER DEFAULT 0,
            error_count INTEGER DEFAULT 0,
            last_error TEXT
        )
    ''')
    
    # Create crawl progress tracking
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS crawl_progress (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            location TEXT,
            category TEXT,
            last_crawled DATETIME DEFAULT CURRENT_TIMESTAMP,
            total_urls_found INTEGER DEFAULT 0,
            status TEXT DEFAULT 'active'
        )
    ''')
    
    conn.commit()
    conn.close()

def add_urls_to_queue(urls_data, location="", category=""):
    """Add discovered URLs to scanning queue, allowing re-scan of old URLs"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Debug logging for comprehensive discovery
    if super_verbose_mode:
        print(f"🔍 SUPER VERBOSE: add_urls_to_queue called with {len(urls_data)} URLs for {location}/{category}")

    # Additional debug: print first few URLs being processed
    if len(urls_data) > 0:
        sample_urls = [url_data.get('url', 'NO_URL') for url_data in urls_data[:3]]
        print(f"🔍 DEBUG: Sample URLs to add: {sample_urls}")

    added_count = 0
    for url_data in urls_data:
        try:
            url = url_data['url']
            
            print(f"🔍 DEBUG: Processing URL: {url}")  # Debug every URL
            
            # Super verbose logging
            if super_verbose_mode:
                print(f"🔍 SUPER VERBOSE: Processing URL for queue: {url}")
            
            # Check if URL exists and when it was last scanned
            cursor.execute('''
                SELECT id, scanned_date FROM url_queue 
                WHERE url = ?
            ''', (url,))
            existing = cursor.fetchone()
            
            if existing:
                if super_verbose_mode:
                    print(f"🔍 SUPER VERBOSE: URL exists in queue (ID: {existing[0]}, Last scan: {existing[1]})")
                
                # If URL exists, check if it's old enough to re-scan (older than 24 hours)
                if existing[1]:  # Has been scanned before
                    cursor.execute('''
                        SELECT datetime('now', '-24 hours') > scanned_date
                        FROM url_queue WHERE id = ?
                    ''', (existing[0],))
                    should_rescan = cursor.fetchone()[0]
                    
                    if should_rescan:
                        if super_verbose_mode:
                            print(f"🔍 SUPER VERBOSE: URL is old enough to re-scan - resetting to pending")
                        # Reset status to pending for re-scan
                        cursor.execute('''
                            UPDATE url_queue 
                            SET status = 'pending', error_count = 0, priority = 1
                            WHERE id = ?
                        ''', (existing[0],))
                        if cursor.rowcount > 0:
                            added_count += 1
                    else:
                        if super_verbose_mode:
                            print(f"🔍 SUPER VERBOSE: URL too recent to re-scan (< 24 hours)")
                else:
                    if super_verbose_mode:
                        print(f"🔍 SUPER VERBOSE: URL exists but never scanned - updating priority")
                    # URL exists but never scanned, update priority
                    cursor.execute('''
                        UPDATE url_queue 
                        SET status = 'pending', priority = 1
                        WHERE id = ?
                    ''', (existing[0],))
                    if cursor.rowcount > 0:
                        added_count += 1
            else:
                if super_verbose_mode:
                    print(f"🔍 SUPER VERBOSE: NEW URL - adding to queue")
                # New URL, insert it
                cursor.execute('''
                    INSERT INTO url_queue (url, location, category)
                    VALUES (?, ?, ?)
                ''', (url, location, category))
                if cursor.rowcount > 0:
                    added_count += 1
                    
        except sqlite3.Error as e:
            print(f"🔍 DEBUG: Database error adding URL {url_data.get('url', 'unknown')}: {e}")
            if super_verbose_mode:
                print(f"🔍 SUPER VERBOSE: Database error adding URL {url_data.get('url', 'unknown')}: {e}")
            continue
    
    print(f"🔍 DEBUG: About to commit {added_count} URLs to database")
    conn.commit()
    conn.close()
    
    print(f"🔍 DEBUG: Database commit completed for {added_count} URLs")
    
    # Enhanced debug logging
    if super_verbose_mode:
        print(f"🔍 SUPER VERBOSE: Queue operation complete - {added_count} URLs added/updated")
    
    # Additional verification for comprehensive discovery
    if added_count > 0:
        # Quick verification - check if URLs were actually saved
        verify_conn = sqlite3.connect(DB_PATH)
        verify_cursor = verify_conn.cursor()
        verify_cursor.execute('SELECT COUNT(*) FROM url_queue WHERE status = "pending"')
        current_pending = verify_cursor.fetchone()[0]
        verify_conn.close()
        
        if super_verbose_mode:
            print(f"🔍 SUPER VERBOSE: After commit, total pending URLs in database: {current_pending}")

    return added_count

def get_next_urls_to_scan(limit=10):
    """Get next URLs to scan from queue"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute('''
        SELECT id, url, location, category FROM url_queue 
        WHERE status = 'pending' AND error_count < 3
        ORDER BY priority DESC, discovered_date ASC 
        LIMIT ?
    ''', (limit,))
    
    urls = cursor.fetchall()
    conn.close()
    return urls

def mark_url_scanned(url_id, emails_found=0, error=None):
    """Mark URL as scanned in database"""
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    if error:
        cursor.execute('''
            UPDATE url_queue 
            SET error_count = error_count + 1, last_error = ?, 
                status = CASE WHEN error_count >= 2 THEN 'failed' ELSE 'pending' END
            WHERE id = ?
        ''', (error, url_id))
    else:
        # Update url_queue table
        cursor.execute('''
            UPDATE url_queue 
            SET status = 'scanned', scanned_date = CURRENT_TIMESTAMP, 
                emails_found = ?
            WHERE id = ?
        ''', (emails_found, url_id))
        
        # Also record in scanned_urls table for statistics tracking
        cursor.execute('SELECT url FROM url_queue WHERE id = ?', (url_id,))
        url_result = cursor.fetchone()
        if url_result:
            url = url_result[0]
            cursor.execute('''
                INSERT OR REPLACE INTO scanned_urls (url, emails_found, scan_date)
                VALUES (?, ?, CURRENT_TIMESTAMP)
            ''', (url, emails_found))
    
    conn.commit()
    conn.close()

def get_queue_stats():
    """Get URL queue statistics"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN status = 'pending' THEN 1 ELSE 0 END) as pending,
                SUM(CASE WHEN status = 'scanned' THEN 1 ELSE 0 END) as scanned,
                SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed,
                SUM(emails_found) as total_emails
            FROM url_queue
        ''')
        
        stats = cursor.fetchone()
        conn.close()
        return {
            'total': stats[0] or 0,
            'pending': stats[1] or 0,
            'scanned': stats[2] or 0,
            'failed': stats[3] or 0,
            'total_emails': stats[4] or 0
        }
    except Exception as e:
        print(f"Error getting queue stats: {e}")
        return {
            'total': 0,
            'pending': 0,
            'scanned': 0,
            'failed': 0,
            'total_emails': 0
        }

def get_email_count():
    """Get total count of emails found"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM emails')
        count = cursor.fetchone()[0]
        conn.close()
        return count or 0
    except Exception as e:
        print(f"Error getting email count: {e}")
        return 0

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
    
    # Create main emails table with expanded fields
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            first_name TEXT,
            last_name TEXT,
            title TEXT,
            company_name TEXT,
            email TEXT,
            phone TEXT,
            stage TEXT DEFAULT 'Cold',
            linkedin_url TEXT DEFAULT '',
            location TEXT,
            category TEXT,
            url TEXT UNIQUE,
            scan_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            raw_name TEXT,
            raw_post_title TEXT
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
    cursor.execute('CREATE INDEX IF NOT EXISTS idx_scanned_date ON scanned_urls(scan_date)')
    
    # Clean up old scanned URLs (older than 7 days) for performance
    try:
        cursor.execute('''
            DELETE FROM scanned_urls 
            WHERE scan_date < datetime('now', '-7 days')
        ''')
        deleted_count = cursor.rowcount
        if deleted_count > 0:
            print(f"🧹 Automatically cleaned up {deleted_count} old scanned URLs (>7 days) for better performance")
    except Exception as e:
        print(f"Warning: Could not clean old URLs: {e}")
    
    # Migrate existing data if needed
    try:
        # Check if old schema exists
        cursor.execute("PRAGMA table_info(emails)")
        columns = [row[1] for row in cursor.fetchall()]
        
        if 'name' in columns and 'first_name' not in columns:
            print("🔄 Migrating database to new schema...")
            
            # Add new columns
            cursor.execute('ALTER TABLE emails ADD COLUMN first_name TEXT DEFAULT ""')
            cursor.execute('ALTER TABLE emails ADD COLUMN last_name TEXT DEFAULT ""')
            cursor.execute('ALTER TABLE emails ADD COLUMN title TEXT DEFAULT ""')
            cursor.execute('ALTER TABLE emails ADD COLUMN company_name TEXT DEFAULT ""')
            cursor.execute('ALTER TABLE emails ADD COLUMN stage TEXT DEFAULT "Cold"')
            cursor.execute('ALTER TABLE emails ADD COLUMN linkedin_url TEXT DEFAULT ""')
            cursor.execute('ALTER TABLE emails ADD COLUMN raw_name TEXT DEFAULT ""')
            cursor.execute('ALTER TABLE emails ADD COLUMN raw_post_title TEXT DEFAULT ""')
            
            # Migrate existing data
            cursor.execute('SELECT id, name, post_title FROM emails WHERE name IS NOT NULL')
            records = cursor.fetchall()
            
            for record_id, name, post_title in records:
                # Parse name into first/last name and company
                first_name, last_name, company_name = parse_contact_name(name or "")
                
                # Update record with parsed data
                cursor.execute('''
                    UPDATE emails 
                    SET first_name = ?, last_name = ?, title = ?, company_name = ?, raw_name = ?, raw_post_title = ?
                    WHERE id = ?
                ''', (first_name, last_name, post_title or "", company_name, name or "", post_title or "", record_id))
            
            print(f"✅ Migrated {len(records)} existing records")
    
    except Exception as e:
        print(f"⚠️ Migration warning: {e}")
    
    conn.commit()
    conn.close()

def cleanup_old_scanned_urls(days=7):
    """Manually clean up scanned URLs older than specified days"""
    try:
        conn = sqlite3.connect(DATABASE)
        cursor = conn.cursor()
        
        cursor.execute('''
            DELETE FROM scanned_urls 
            WHERE scan_date < datetime('now', '-{} days')
        '''.format(days))
        
        deleted_count = cursor.rowcount
        conn.commit()
        conn.close()
        
        return deleted_count
    except Exception as e:
        print(f"Error cleaning up old URLs: {e}")
        return 0

def init_optimization_database():
    """Initialize the crawler optimization database for performance metrics"""
    try:
        conn = sqlite3.connect(OPTIMIZATION_DB)
        cursor = conn.cursor()
        
        # Performance metrics table
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS performance_metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                urls_per_second REAL,
                batch_size INTEGER,
                concurrent_threads INTEGER,
                emails_found INTEGER,
                cycle_time REAL,
                success_rate REAL,
                memory_usage REAL
            )
        ''')
        
        # Current optimization settings
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS optimization_settings (
                id INTEGER PRIMARY KEY,
                auto_optimize BOOLEAN DEFAULT 1,
                target_urls_per_second REAL DEFAULT 15.0,
                max_batch_size INTEGER DEFAULT 100,
                max_threads INTEGER DEFAULT 50,
                min_batch_size INTEGER DEFAULT 10,
                min_threads INTEGER DEFAULT 5,
                optimization_interval INTEGER DEFAULT 10
            )
        ''')
        
        # Insert default settings if not exists
        cursor.execute('''
            INSERT OR IGNORE INTO optimization_settings (id, auto_optimize, target_urls_per_second, max_batch_size, max_threads)
            VALUES (1, 1, 5.0, 10, 5)
        ''')
        
        conn.commit()
        conn.close()
        print("🎯 Optimization database initialized")
        
    except Exception as e:
        print(f"Warning: Could not initialize optimization database: {e}")

def get_optimization_settings():
    """Get current optimization settings"""
    try:
        conn = sqlite3.connect(OPTIMIZATION_DB)
        cursor = conn.cursor()
        cursor.execute('SELECT * FROM optimization_settings WHERE id = 1')
        result = cursor.fetchone()
        conn.close()
        
        if result:
            return {
                'auto_optimize': bool(result[1]),
                'target_urls_per_second': result[2],
                'max_batch_size': result[3],
                'max_threads': result[4],
                'min_batch_size': result[5],
                'min_threads': result[6],
                'optimization_interval': result[7]
            }
        else:
            return {
                'auto_optimize': True,
                'target_urls_per_second': 5.0,  # Reduced from 15.0 for better rate limit management
                'max_batch_size': 10,  # Reduced from 100
                'max_threads': 5,  # Reduced from 50
                'min_batch_size': 3,  # Reduced from 10
                'min_threads': 2,  # Reduced from 5
                'optimization_interval': 15  # Increased from 10 for more conservative optimization
            }
    except Exception as e:
        print(f"Error getting optimization settings: {e}")
        return {'auto_optimize': False}

def save_performance_metrics(urls_per_second, batch_size, threads, emails_found, cycle_time, success_rate):
    """Save performance metrics to optimization database"""
    try:
        conn = sqlite3.connect(OPTIMIZATION_DB)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT INTO performance_metrics 
            (urls_per_second, batch_size, concurrent_threads, emails_found, cycle_time, success_rate)
            VALUES (?, ?, ?, ?, ?, ?)
        ''', (urls_per_second, batch_size, threads, emails_found, cycle_time, success_rate))
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error saving performance metrics: {e}")

def optimize_crawler_settings():
    """Auto-optimize crawler settings based on recent performance"""
    try:
        conn = sqlite3.connect(OPTIMIZATION_DB)
        cursor = conn.cursor()
        
        # Get last 10 performance records
        cursor.execute('''
            SELECT urls_per_second, batch_size, concurrent_threads, success_rate, cycle_time
            FROM performance_metrics 
            ORDER BY timestamp DESC LIMIT 10
        ''')
        recent_metrics = cursor.fetchall()
        
        # Get current settings
        settings = get_optimization_settings()
        
        if len(recent_metrics) >= 3:  # Need some data to optimize
            avg_speed = sum(row[0] for row in recent_metrics) / len(recent_metrics)
            avg_success_rate = sum(row[3] for row in recent_metrics) / len(recent_metrics)
            target_speed = settings['target_urls_per_second']
            
            # Optimization logic
            if avg_speed < target_speed * 0.8 and avg_success_rate > 0.8:
                # Too slow but successful - increase batch size
                new_batch = min(settings['max_batch_size'], int(recent_metrics[0][1] * 1.2))
                new_threads = min(settings['max_threads'], int(recent_metrics[0][2] * 1.1))
            elif avg_success_rate < 0.7:
                # Low success rate - reduce load
                new_batch = max(settings['min_batch_size'], int(recent_metrics[0][1] * 0.8))
                new_threads = max(settings['min_threads'], int(recent_metrics[0][2] * 0.9))
            else:
                # Performing well - slight increase
                new_batch = min(settings['max_batch_size'], recent_metrics[0][1] + 5)
                new_threads = min(settings['max_threads'], recent_metrics[0][2] + 2)
            
            conn.close()
            return new_batch, new_threads
        
        conn.close()
        return 50, 20  # Default values
        
    except Exception as e:
        print(f"Error optimizing settings: {e}")
        return 50, 20

def parse_contact_name(raw_name):
    """Parse raw name into first_name, last_name, and company_name"""
    if not raw_name:
        return "", "", ""
    
    raw_name = raw_name.strip()
    
    # Enhanced business keywords for company detection
    business_keywords = [
        " llc", " inc", " co", " company", " corp", " ltd", " agency", " studio", 
        " group", " services", " solutions", " enterprises", " consulting", " design",
        " photography", " construction", " cleaning", " landscaping", " catering",
        " automotive", " repair", " maintenance", " technologies", " tech", " systems"
    ]
    
    business_indicators = [
        "& associates", "and associates", "& co", "and co", "& sons", "and sons",
        "contractors", "contract", "professional", "specialists", "experts"
    ]
    
    # Check if it's a company
    lower_name = f" {raw_name.lower()} "
    if any(keyword in lower_name for keyword in business_keywords):
        return "", "", raw_name
    
    if any(indicator in lower_name for indicator in business_indicators):
        return "", "", raw_name
    
    # Split into tokens
    tokens = [t for t in re.split(r"[\s,]+", raw_name) if t and len(t) > 1]
    
    if len(tokens) >= 2:
        first_name = tokens[0].title()
        last_name = tokens[-1].title()
        return first_name, last_name, ""
    elif len(tokens) == 1:
        first_name = tokens[0].title()
        return first_name, "", ""
    
    return "", "", ""

def extract_names_from_content(text_content, post_title=""):
    """Enhanced name extraction from post content"""
    names_found = []
    companies_found = []
    
    # Clean up text
    lines = text_content.split('\n')
    
    # Look for contact patterns
    contact_patterns = [
        r"contact\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
        r"call\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
        r"ask\s+for\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
        r"speak\s+(?:with|to)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
        r"my\s+name\s+is\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
        r"i'm\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)",
        r"(?:hi|hello),?\s+(?:i'm|this\s+is)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)"
    ]
    
    # Company patterns
    company_patterns = [
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:LLC|Inc|Co|Company|Corp|Ltd|Agency|Studio|Group|Services|Solutions)",
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:Photography|Construction|Cleaning|Landscaping|Catering|Automotive|Repair|Maintenance)",
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+(?:& Associates|and Associates|& Co|and Co|Contractors|Professional|Specialists)"
    ]
    
    full_text = text_content.lower()
    
    # Extract names using patterns
    for pattern in contact_patterns:
        matches = re.findall(pattern, text_content, re.IGNORECASE)
        for match in matches:
            if 2 <= len(match) <= 30 and not any(spam in match.lower() for spam in ['call now', 'click here', 'visit', 'www', 'http']):
                names_found.append(match.strip())
    
    # Extract companies using patterns
    for pattern in company_patterns:
        matches = re.findall(pattern, text_content, re.IGNORECASE)
        for match in matches:
            if 2 <= len(match) <= 50:
                companies_found.append(match.strip())
    
    # Look in first few lines for names/companies
    for i, line in enumerate(lines[:8]):
        line = line.strip()
        if 3 <= len(line) <= 50 and not line.isupper():
            # Skip obvious spam lines
            if any(spam in line.lower() for spam in ['call now', 'click here', 'visit', 'www', 'http', '$', 'free', 'deal']):
                continue
            
            # Check if line looks like a name
            tokens = line.split()
            if len(tokens) == 2 and all(token[0].isupper() and token[1:].islower() for token in tokens):
                names_found.append(line)
            elif any(biz in line.lower() for biz in ['llc', 'inc', 'company', 'services', 'solutions', 'group']):
                companies_found.append(line)
    
    # Also check post title for company names
    if post_title:
        title_lower = post_title.lower()
        if any(biz in title_lower for biz in ['llc', 'inc', 'company', 'services', 'solutions', 'group', 'photography', 'cleaning', 'construction']):
            # Extract potential company name from title
            title_parts = post_title.split(' - ')
            if title_parts:
                potential_company = title_parts[0].strip()
                if 5 <= len(potential_company) <= 50:
                    companies_found.append(potential_company)
    
    return names_found, companies_found

def extract_company_from_email(email):
    """Extract company name from email domain"""
    if not email or "@" not in email:
        return ""
    
    domain = email.split("@")[1].lower()
    
    # Skip common free email providers
    free_providers = [
        "gmail.com", "yahoo.com", "hotmail.com", "outlook.com", "aol.com",
        "icloud.com", "me.com", "live.com", "msn.com", "comcast.net",
        "verizon.net", "att.net", "charter.net", "cox.net"
    ]
    
    if domain in free_providers:
        return ""
    
    # Extract company name from domain
    # Remove common TLDs and subdomains
    domain_parts = domain.replace(".com", "").replace(".net", "").replace(".org", "")
    domain_parts = domain_parts.replace(".co", "").replace(".us", "").replace(".biz", "")
    domain_parts = domain_parts.replace("www.", "").replace("mail.", "")
    
    # Split by dots and take the main part
    main_part = domain_parts.split(".")[0]
    
    # Clean up and format
    if len(main_part) >= 3:
        # Convert to proper case and add common business suffix if missing
        company = main_part.replace("-", " ").replace("_", " ").title()
        
        # Add "Services" if it's a single word without business indicators
        if " " not in company and not any(suffix in company.lower() for suffix in ["llc", "inc", "corp", "co"]):
            company += " Services"
        
        return company
    
    return ""

def fix_company_names():
    """Scan and fix company names for emails with blank or 'QR' company names"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Find emails with blank company_name or containing "QR"
        cursor.execute('''
            SELECT id, email, first_name, last_name, company_name, raw_name, raw_post_title
            FROM emails 
            WHERE company_name IS NULL 
               OR company_name = '' 
               OR company_name LIKE '%QR%'
               OR company_name LIKE '%qr%'
        ''')
        
        records = cursor.fetchall()
        fixed_count = 0
        
        for record_id, email, first_name, last_name, old_company, raw_name, raw_post_title in records:
            new_company = ""
            
            # Try to extract from email domain first
            if email:
                new_company = extract_company_from_email(email)
            
            # If no company from email, try to re-parse raw_name with enhanced logic
            if not new_company and raw_name:
                # Enhanced company detection
                enhanced_keywords = [
                    "llc", "inc", "co", "company", "corp", "ltd", "agency", "studio", 
                    "group", "services", "solutions", "enterprises", "consulting", "design",
                    "photography", "construction", "cleaning", "landscaping", "catering",
                    "automotive", "repair", "maintenance", "technologies", "tech", "systems",
                    "contractors", "professional", "specialists", "experts", "associates"
                ]
                
                raw_lower = raw_name.lower()
                if any(keyword in raw_lower for keyword in enhanced_keywords):
                    new_company = raw_name.strip()
                elif "& " in raw_name or " and " in raw_name.lower():
                    new_company = raw_name.strip()
            
            # If still no company, try post title
            if not new_company and raw_post_title:
                title_lower = raw_post_title.lower()
                if any(keyword in title_lower for keyword in ["services", "company", "llc", "inc", "business"]):
                    # Extract potential company from title
                    title_parts = raw_post_title.split(" - ")
                    if title_parts:
                        potential = title_parts[0].strip()
                        if 5 <= len(potential) <= 50:
                            new_company = potential
            
            # If we found a better company name, update it
            if new_company and new_company != old_company:
                cursor.execute('''
                    UPDATE emails 
                    SET company_name = ?, first_name = '', last_name = ''
                    WHERE id = ?
                ''', (new_company, record_id))
                fixed_count += 1
            
            # If still no company but we have first/last name, keep as individual
            elif not new_company and (first_name or last_name):
                # Just clear the bad company name
                if old_company and ("QR" in old_company or "qr" in old_company):
                    cursor.execute('''
                        UPDATE emails 
                        SET company_name = ''
                        WHERE id = ?
                    ''', (record_id,))
                    fixed_count += 1
        
        conn.commit()
        conn.close()
        
        return fixed_count, len(records)
        
    except Exception as e:
        print(f"Error fixing company names: {e}")
        return 0, 0

def parse_email_for_name(email):
    """Extract potential name from email local part as fallback"""
    if not email or "@" not in email:
        return "", ""
    
    local_part = email.split("@")[0]
    # Replace common separators with spaces
    name_guess = local_part.replace(".", " ").replace("_", " ").replace("-", " ")
    
    tokens = [t for t in re.split(r"[\s,]+", name_guess) if t and len(t) > 1]
    
    if len(tokens) >= 2:
        return tokens[0].title(), tokens[-1].title()
    elif len(tokens) == 1:
        return tokens[0].title(), ""
    
    return "", ""

def save_email_to_db(email_data):
    """Save a single email record to database with parsed fields"""
    try:
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        cursor.execute('''
            INSERT OR REPLACE INTO emails (
                first_name, last_name, title, company_name, email, phone, 
                stage, linkedin_url, location, category, url, scan_date, 
                raw_name, raw_post_title
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', email_data)
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"Error saving email: {e}")

def load_emails_from_db():
    """Load all emails from database"""
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query('SELECT * FROM emails ORDER BY scan_date DESC', conn)
        conn.close()
        if df.empty:
            return pd.DataFrame(columns=DEFAULT_COLUMNS)
        
        # Ensure all expected columns exist
        for col in DEFAULT_COLUMNS:
            if col not in df.columns:
                df[col] = ""
        
        return df[DEFAULT_COLUMNS].fillna("")
    except Exception:
        return pd.DataFrame(columns=DEFAULT_COLUMNS)

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

# VPN rotation tracking
current_vpn_country = None
vpn_rotation_count = 0
last_ip_check = None
consecutive_network_errors = 0  # Track consecutive real network errors

async def rotate_vpn_if_needed(force_rotate=False, is_network_error=False):
    """Rotate VPN IP only for real network blocks, not HTTP errors"""
    global current_vpn_country, vpn_rotation_count, last_ip_check, consecutive_network_errors
    
    # Track consecutive network errors (not HTTP errors like 404)
    if is_network_error:
        consecutive_network_errors += 1
    else:
        # Reset on successful requests
        consecutive_network_errors = max(0, consecutive_network_errors - 1)
    
    # Only rotate for actual network blocks, not HTTP status codes
    should_rotate = force_rotate or consecutive_network_errors >= 8  # Increased from 3 to 8 to be less aggressive
    
    if not should_rotate:
        return True, last_ip_check or vpn_manager.get_current_ip()
    
    # Country rotation list (prioritizing fast countries)
    countries = ["United_States", "Canada", "United_Kingdom", "Germany", "Netherlands", "France"]
    
    try:
        # Get current IP before rotation
        old_ip = vpn_manager.get_current_ip()
        
        # Select next country
        if current_vpn_country in countries:
            current_index = countries.index(current_vpn_country)
            next_country = countries[(current_index + 1) % len(countries)]
        else:
            next_country = countries[0]
        
        print(f"🔄 Rotating VPN from {current_vpn_country} to {next_country} (after {consecutive_network_errors} network errors)")
        
        # Connect to new country
        success, message = vpn_manager.connect_to_country(next_country)
        
        if success:
            # Wait for connection to stabilize
            await asyncio.sleep(1)  # Reduced from 2 to 1 second for speed
            
            # Validate IP change
            new_ip = vpn_manager.get_current_ip()
            
            if new_ip != old_ip and new_ip != "Unable to fetch IP":
                current_vpn_country = next_country
                last_ip_check = new_ip
                consecutive_network_errors = 0  # Reset on successful rotation
                print(f"✅ VPN rotated successfully: {old_ip} → {new_ip}")
                return True, new_ip
            else:
                print(f"⚠️ IP didn't change after rotation: {old_ip} → {new_ip}")
                return False, old_ip
        else:
            print(f"❌ VPN rotation failed: {message}")
            return False, old_ip
        
    except Exception as e:
        print(f"❌ Error during VPN rotation: {str(e)}")
        return False, "Error"

async def extract_emails_from_post(url, title="", max_retries=3):
    """Extract emails and contact info from a Craigslist post with smart VPN rotation"""
    global vpn_rotation_count, consecutive_network_errors
    
    for attempt in range(max_retries):
        try:
            # Only rotate VPN if forced or experiencing consecutive network errors
            if attempt > 0:
                await rotate_vpn_if_needed(is_network_error=True)
            
            # ULTRA FAST timeout strategies
            timeout = 2.0 if attempt == 0 else 4.0  # Reduced from 5/8 to 2/4 seconds
            
            async with httpx.AsyncClient(
                headers={
                    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
                    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                    'Accept-Language': 'en-US,en;q=0.5',
                    'Accept-Encoding': 'gzip, deflate',
                    'Connection': 'keep-alive',
                },
                timeout=timeout,
                follow_redirects=True
            ) as client:
                
                response = await client.get(url)
                vpn_rotation_count += 1
                
                # Handle HTTP status codes - DON'T rotate VPN for HTTP errors
                if response.status_code == 404:
                    print(f"⚠️ HTTP 404 (Post Not Found) for {url} - skipping")
                    return None
                elif response.status_code == 403:
                    print(f"⚠️ HTTP 403 (Forbidden) for {url} - possible rate limit")
                    # Only count as network error for potential blocks
                    await rotate_vpn_if_needed(is_network_error=True)
                    if attempt < max_retries - 1:
                        await asyncio.sleep(2)
                        continue
                    return None
                elif response.status_code != 200:
                    print(f"⚠️ HTTP {response.status_code} for {url} - skipping")
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
                
                # Enhanced name and company extraction
                names_found, companies_found = extract_names_from_content(text_content, title)
                
                # Determine the best name and company
                best_name = ""
                best_company = ""
                
                # Prioritize companies if found
                if companies_found:
                    best_company = companies_found[0]  # Take the first/best company match
                
                # If no company, look for names
                if names_found and not best_company:
                    best_name = names_found[0]  # Take the first/best name match
                
                # Fallback: try to extract from first few lines (original logic)
                if not best_name and not best_company:
                    lines = text_content.split('\n')
                    for line in lines[:5]:
                        line = line.strip()
                        if 3 <= len(line) <= 50 and not line.isupper():
                            if not any(spam in line.lower() for spam in ['call now', 'click here', 'visit', 'www', 'http', '$']):
                                best_name = line
                                break
                
                # Final fallback: use title
                if not best_name and not best_company and title:
                    best_name = title.split(' - ')[0].strip()[:50]
                
                # Parse the best name/company found
                first_name, last_name, company_name = "", "", ""
                
                if best_company:
                    company_name = best_company
                elif best_name:
                    first_name, last_name, potential_company = parse_contact_name(best_name)
                    if potential_company:
                        company_name = potential_company
                        first_name, last_name = "", ""
                
                # If still no name, try to infer from email
                if not first_name and not last_name and not company_name:
                    email_first = emails[0] if emails else ""
                    first_name, last_name = parse_email_for_name(email_first)
                
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
                
                # Extract first email and phone for single values
                single_email = emails[0] if emails else ""
                single_phone = phones[0] if phones else ""
                
                # Reset network error count on successful extraction
                await rotate_vpn_if_needed(is_network_error=False)
                
                return {
                    'first_name': first_name,
                    'last_name': last_name,
                    'title': title,
                    'company_name': company_name,
                    'email': single_email,
                    'phone': single_phone,
                    'stage': 'Cold',
                    'linkedin_url': '',
                    'location': location,
                    'category': category,
                    'url': url,
                    'scan_date': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    'raw_name': best_company or best_name,
                    'raw_post_title': title
                }
                
        except (httpx.ConnectError, httpx.TimeoutException, httpx.NetworkError) as e:
            error_msg = str(e)
            if "Name or service not known" in error_msg or "ConnectError" in error_msg:
                print(f"🌐 Network error for {url}: {error_msg}")
                await rotate_vpn_if_needed(is_network_error=True)
                if attempt < max_retries - 1:
                    await asyncio.sleep(0.5)  # Reduced from 1 to 0.5 seconds
                    continue
                else:
                    print(f"❌ Network error after {max_retries} attempts: {url}")
                    return None
            else:
                print(f"❌ HTTP client error for {url}: {error_msg}")
                return None
                
        except Exception as e:
            error_msg = str(e)
            # Check if this is an HTTP client error that might indicate blocking
            is_client_error = "HTTP client error" in error_msg or "403" in error_msg or "429" in error_msg or "503" in error_msg
            
            if is_client_error and attempt < max_retries - 1:
                print(f"⚠️ HTTP client error for {url}: {error_msg} - rotating VPN and retrying...")
                await rotate_vpn_if_needed(is_network_error=True)
                await asyncio.sleep(3.0)  # Longer delay for client errors
                continue
            elif attempt < max_retries - 1:
                print(f"⚠️ Error extracting from {url}: {error_msg}, retrying...")
                await asyncio.sleep(0.5)  # Increased delay slightly
                continue
            else:
                print(f"❌ Failed to extract from {url} after {max_retries} attempts: {error_msg}")
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
    
    # Initialize VPN rotation
    if vpn_manager.is_authenticated:
        await rotate_vpn_if_needed(force_rotate=True)
    
    # Extract emails from posts with conservative parallel processing to avoid rate limits
    email_records = []
    processed = 0
    batch_size = 5  # Reduced from 50 to 5 for better rate limit management
    
    for i in range(0, len(posts), batch_size):
        batch = posts[i:i + batch_size]
        batch_tasks = []
        
        log(f"� Processing batch {i//batch_size + 1}: posts {i+1}-{min(i+batch_size, len(posts))}")
        
        # Create concurrent tasks for this batch
        for j, post in enumerate(batch):
            task = extract_emails_from_post(post['url'], post['title'])
            batch_tasks.append((task, post))
        
        # Execute batch concurrently
        try:
            results = await asyncio.gather(*[task for task, post in batch_tasks], return_exceptions=True)
            
            # Process results
            for idx, (result, (task, post)) in enumerate(zip(results, batch_tasks)):
                post_num = i + idx + 1
                try:
                    log(f"📄 Processing result {post_num}/{len(posts)}: {post['title'][:50]}...")
                    
                    if isinstance(result, Exception):
                        log(f"❌ Error in post {post_num}: {str(result)}")
                        record_scanned_url(post['url'], 0)
                        continue
                    
                    email_data = result
                    emails_found = 0
                    
                    if email_data and email_data['email']:
                        email_records.append(email_data)
                        # Convert to tuple format for database saving
                        db_tuple = (
                            email_data['first_name'], email_data['last_name'], email_data['title'],
                            email_data['company_name'], email_data['email'], email_data['phone'],
                            email_data['stage'], email_data['linkedin_url'], email_data['location'],
                            email_data['category'], email_data['url'], email_data['scan_date'],
                            email_data['raw_name'], email_data['raw_post_title']
                        )
                        save_email_to_db(db_tuple)
                        emails_found = 1
                        log(f"✅ Found email in post {post_num}")
                    
                    record_scanned_url(post['url'], emails_found)
                    processed += 1
                    
                except Exception as e:
                    log(f"❌ Error processing result {post_num}: {e}")
                    continue
            
            # Add delay between batches to avoid rate limiting and respect servers
            await asyncio.sleep(2.0)  # 2 second delay between batches to prevent overwhelming servers
                
        except Exception as e:
            log(f"❌ Error in batch processing: {e}")
            # Fallback to individual processing for this batch
            for task, post in batch_tasks:
                try:
                    email_data = await task
                    emails_found = 0
                    if email_data and email_data['email']:
                        email_records.append(email_data)
                        # Convert to tuple format for database saving
                        db_tuple = (
                            email_data['first_name'], email_data['last_name'], email_data['title'],
                            email_data['company_name'], email_data['email'], email_data['phone'],
                            email_data['stage'], email_data['linkedin_url'], email_data['location'],
                            email_data['category'], email_data['url'], email_data['scan_date'],
                            email_data['raw_name'], email_data['raw_post_title']
                        )
                        save_email_to_db(db_tuple)
                        emails_found = 1
                    record_scanned_url(post['url'], emails_found)
                    processed += 1
                except Exception as inner_e:
                    log(f"❌ Error in fallback processing: {inner_e}")
                    continue
    
    log(f"🎉 Scan complete! Found {len(email_records)} posts with emails out of {processed} processed")
    
    if email_records:
        return pd.DataFrame(email_records, columns=DEFAULT_COLUMNS)
    else:
        return pd.DataFrame(columns=DEFAULT_COLUMNS)

def is_url_in_database_batch(urls):
    """Check multiple URLs at once for better performance - only checks recent scans (7 days)"""
    try:
        if not urls:
            return {}
        
        conn = sqlite3.connect(DB_PATH)
        cursor = conn.cursor()
        
        # Create placeholder string for IN clause
        placeholders = ','.join('?' * len(urls))
        
        # Check both tables in one query each
        cursor.execute(f'SELECT url FROM url_queue WHERE url IN ({placeholders})', urls)
        queue_urls = {row[0] for row in cursor.fetchall()}
        
        # Only check scanned URLs from the last 7 days for performance optimization
        cursor.execute(f'''
            SELECT url FROM scanned_urls 
            WHERE url IN ({placeholders}) 
            AND scan_date > datetime('now', '-7 days')
        ''', urls)
        scanned_urls = {row[0] for row in cursor.fetchall()}
        
        conn.close()
        
        # Return dictionary of URL -> exists status
        existing_urls = queue_urls | scanned_urls
        return {url: url in existing_urls for url in urls}
        
    except Exception as e:
        print(f"Error checking URLs in database: {e}")
        return {url: False for url in urls}  # Default to not exists on error

def is_url_in_database(url):
    """Check if URL already exists in database (either queue or scanned)"""
    result = is_url_in_database_batch([url])
    return result.get(url, False)

async def discover_all_craigslist_locations():
    """Discover all Craigslist locations by scraping the sites page"""
    import httpx
    from bs4 import BeautifulSoup
    
    locations = []
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            # Get the main sites page
            response = await client.get("https://www.craigslist.org/about/sites")
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find all craigslist subdomain links
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if 'craigslist.org' in href and '//' in href:
                        # Extract location from URL like https://newyork.craigslist.org
                        if href.startswith('//'):
                            href = 'https:' + href
                        elif href.startswith('http'):
                            pass
                        else:
                            continue
                            
                        try:
                            location = href.split('//')[1].split('.craigslist.org')[0]
                            if location and location not in locations:
                                locations.append(location)
                        except:
                            continue
                            
        print(f"🌍 Discovered {len(locations)} Craigslist locations")
        return locations[:50]  # Limit to first 50 to avoid being too aggressive initially
        
    except Exception as e:
        print(f"❌ Error discovering locations: {e}")
        # Fallback to major US cities if discovery fails
        return ["newyork", "losangeles", "chicago", "houston", "phoenix", "philadelphia", 
                "sanantonio", "sandiego", "dallas", "austin", "fortworth", "columbus"]

async def discover_categories_from_location(location):
    """Discover all categories from a location's homepage"""
    import httpx
    from bs4 import BeautifulSoup
    
    categories = []
    try:
        async with httpx.AsyncClient(timeout=30.0) as client:
            response = await client.get(f"https://{location}.craigslist.org")
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Find all category links in the format /search/{category}
                for link in soup.find_all('a', href=True):
                    href = link['href']
                    if href.startswith('/search/') and len(href.split('/')) == 3:
                        category = href.split('/search/')[1]
                        if category and len(category) == 3 and category not in categories:
                            categories.append(category)
                            
        print(f"📁 Found {len(categories)} categories for {location}")
        return categories
        
    except Exception as e:
        print(f"❌ Error discovering categories for {location}: {e}")
        return []
async def discover_craigslist_urls(location, category, progress_callback=None):
    """Discover URLs by parsing Craigslist category listing pages (NOT individual posts)"""
    
    def log(msg):
        if progress_callback:
            progress_callback(msg)
    
    discovered_urls = []
    
    try:
        # Build the category listing page URL (not individual post URLs)
        base_url = f"https://{location}.craigslist.org"
        
        # Build category listing URL - this shows the list of posts, not individual posts
        if category in ["jjj", "acc", "ofc", "bus", "csr", "etc", "fbh", "gov", "hea", "hum", "eng", "edu"]:
            # Jobs categories - try both old and new URL formats
            listing_url = f"{base_url}/search/{category}"  # Simplified format
            if progress_callback and super_verbose_mode:
                log(f"🔍 SUPER VERBOSE: Using jobs URL format: {listing_url}")
        elif category in ["bbb", "aos", "aut", "bts", "biz", "cps", "crs", "evs", "fgs"]:
            # Services categories  
            listing_url = f"{base_url}/search/{category}"  # Simplified format
            if progress_callback and super_verbose_mode:
                log(f"🔍 SUPER VERBOSE: Using services URL format: {listing_url}")
        else:
            # General for-sale search
            listing_url = f"{base_url}/search/{category}"  # Simplified format
            if progress_callback and super_verbose_mode:
                log(f"🔍 SUPER VERBOSE: Using general URL format: {listing_url}")
        
        # Always log the URL being attempted (not just in super verbose mode)
        log(f"🌐 ATTEMPTING URL: {listing_url}")
        log(f"🔍 Discovering URLs from category listing: {listing_url}")
        
        # Super verbose mode logging
        if progress_callback and super_verbose_mode:
            log(f"🔍 SUPER VERBOSE: Attempting to fetch {listing_url}")
        
        # Parse the category listing page to extract post URLs
        async with httpx.AsyncClient(
            headers={
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            },
            timeout=3.0,  # Reduced from 10 to 3 seconds for SPEED
            follow_redirects=True
        ) as client:
            
            # Paginate through results using query parameters
            page = 0
            max_pages = 300  # Increased to 300 pages as requested
            results_per_page = 120  # Craigslist typically shows ~120 results per page
            
            while page < max_pages:
                # Add pagination parameter to URL
                if page == 0:
                    paginated_url = listing_url
                else:
                    # Use the format: /search/category?s=offset
                    offset = page * results_per_page
                    paginated_url = f"{listing_url}?s={offset}"
                
                log(f"📄 Checking page {page}: {paginated_url}")
                if progress_callback and super_verbose_mode:
                    log(f"🔍 SUPER VERBOSE: Trying page {page}: {paginated_url}")
                
                # First try the current URL format
                response = await client.get(paginated_url)
                
                if progress_callback and super_verbose_mode:
                    log(f"🔍 SUPER VERBOSE: HTTP {response.status_code} for {paginated_url}")
                    log(f"🔍 SUPER VERBOSE: Response length: {len(response.text)} characters")
                    log(f"🔍 SUPER VERBOSE: First 500 chars: {response.text[:500]}")
                    
                # If we get a 404 or redirect on first page, try alternative URL formats
                if page == 0 and (response.status_code == 404 or 'craigslist' not in response.text.lower()):
                    if progress_callback and super_verbose_mode:
                        log(f"🔍 SUPER VERBOSE: Primary URL failed, trying alternative formats...")
                    
                    # Try different URL formats
                    alt_urls = [
                        f"{base_url}/d/jobs/{category}",  # Jobs direct
                        f"{base_url}/d/gigs/{category}",  # Gigs
                        f"{base_url}/d/for-sale/{category}",  # For sale  
                        f"{base_url}/d/services/{category}",  # Services
                        f"{base_url}/{category}",  # Simple format
                    ]
                    
                    for alt_url in alt_urls:
                        try:
                            log(f"🔄 TRYING ALTERNATIVE URL: {alt_url}")
                            if progress_callback and super_verbose_mode:
                                log(f"🔍 SUPER VERBOSE: Trying alternative URL: {alt_url}")
                            
                            alt_response = await client.get(alt_url)
                            if alt_response.status_code == 200 and 'craigslist' in alt_response.text.lower():
                                response = alt_response
                                listing_url = alt_url  # Update base URL for pagination
                                log(f"✅ ALTERNATIVE URL WORKED: {alt_url}")
                                if progress_callback and super_verbose_mode:
                                    log(f"🔍 SUPER VERBOSE: Alternative URL worked: {alt_url}")
                                break
                            else:
                                log(f"❌ ALTERNATIVE URL FAILED: {alt_url} (Status: {alt_response.status_code})")
                        except Exception as e:
                            log(f"❌ ALTERNATIVE URL ERROR: {alt_url} - {e}")
                            if progress_callback and super_verbose_mode:
                                log(f"🔍 SUPER VERBOSE: Alternative URL {alt_url} failed: {e}")
                            continue
                
                if response.status_code != 200:
                    if progress_callback and super_verbose_mode:
                        log(f"🔍 SUPER VERBOSE: Failed to fetch listing page: HTTP {response.status_code}")
                    log(f"❌ Failed to fetch listing page: HTTP {response.status_code}")
                    break  # Exit pagination loop
                
                soup = BeautifulSoup(response.text, 'html.parser')
                
                # Check if page is valid Craigslist listing page
                if progress_callback and super_verbose_mode:
                    log(f"🔍 SUPER VERBOSE: Looking for Craigslist elements...")
                    log(f"🔍 SUPER VERBOSE: Page title: {soup.title.string if soup.title else 'No title'}")
                    log(f"🔍 SUPER VERBOSE: Has 'craigslist' in text: {'craigslist' in response.text.lower()}")
                    
                    # Check for common Craigslist elements
                    has_header = soup.find('header', class_='cl-header')
                    has_search = soup.find('form', class_='cl-search-form')
                    has_results = soup.find('div', class_='cl-results-page')
                    
                    log(f"🔍 SUPER VERBOSE: Has CL header: {bool(has_header)}")
                    log(f"🔍 SUPER VERBOSE: Has search form: {bool(has_search)}")
                    log(f"🔍 SUPER VERBOSE: Has results page: {bool(has_results)}")
                
                # Find all post links in the listing
                post_links = []
                
                # Try the new Craigslist structure first
                search_results = soup.find_all('li', class_='cl-static-search-result')
                if search_results:
                    for result in search_results:
                        link = result.find('a', href=True)
                        if link:
                            post_links.append(link)
                
                # Fallback to older structures
                if not post_links:
                    post_links = soup.find_all('a', class_='cl-app-anchor')
                if not post_links:
                    post_links = soup.find_all('a', {'data-id': True})
                if not post_links:
                    # Try href pattern matching for Craigslist posts
                    all_links = soup.find_all('a', href=True)
                    post_links = [link for link in all_links if '/d/' in link['href'] and any(char.isdigit() for char in link['href'])]
                
                # Debug: Show what we found
                if progress_callback and super_verbose_mode:
                    log(f"🔍 SUPER VERBOSE: Page {page} - Raw HTML title: {soup.title.string if soup.title else 'No title'}")
                    
                    # Show first few links to understand structure
                    all_links = soup.find_all('a', href=True)[:15]
                    log(f"🔍 SUPER VERBOSE: Page {page} - Found {len(all_links)} total links, showing first 15:")
                    for i, link in enumerate(all_links):
                        href = link.get('href', '')
                        text = link.get_text(strip=True)[:50]
                        log(f"🔍 SUPER VERBOSE:   Link {i+1}: '{text}' -> {href}")
                
                log(f"📄 Page {page}: Found {len(post_links)} posts")
                
                if progress_callback and super_verbose_mode:
                    log(f"🔍 SUPER VERBOSE: Found {len(post_links)} potential post links on page {page}")
                
                # If no posts found on this page, we've reached the end
                if len(post_links) == 0:
                    log(f"📄 No more posts found on page {page}, ending pagination")
                    break
                
                # Process links found on this page
                page_urls = 0
                for link in post_links:
                    try:
                        href = link.get('href', '')
                        if not href:
                            continue
                        
                        # Convert relative URLs to absolute
                        if href.startswith('/'):
                            full_url = base_url + href
                        elif href.startswith('http'):
                            full_url = href
                        else:
                            continue
                        
                        # Only include actual Craigslist post URLs
                        # Expected format: https://sanantonio.craigslist.org/aos/d/san-antonio-xxxxx/7874965162.html
                        if ('/d/' in full_url and full_url.endswith('.html') and 
                            any(char.isdigit() for char in full_url.split('/')[-1])):
                            # Check if URL already exists in database
                            if not is_url_in_database(full_url):
                                title = link.get_text(strip=True) or "No title"
                                
                                discovered_urls.append({
                                    'url': full_url,
                                    'title': title,
                                    'location': location,
                                    'category': category
                                })
                                page_urls += 1
                                
                                if progress_callback and super_verbose_mode:
                                    log(f"🔍 SUPER VERBOSE: Added URL from page {page}: {full_url}")
                    except Exception as e:
                        if progress_callback and super_verbose_mode:
                            log(f"🔍 SUPER VERBOSE: Error processing link: {e}")
                        continue
                
                log(f"📄 Page {page}: Added {page_urls} new URLs")
                
                # Move to next page
                page += 1
                
                # Add delay between page requests to be respectful to Craigslist
                if page < max_pages:  # Don't delay after the last page
                    await asyncio.sleep(2.0)  # 2 second delay between pages
                
            # End of pagination loop
            log(f"🎯 Total URLs discovered: {len(discovered_urls)} from {page} pages")
        
        if progress_callback and super_verbose_mode:
            log(f"🔍 SUPER VERBOSE: {location}/{category} - Final result: {len(discovered_urls)} URLs")
        
        return discovered_urls  # Return full objects with url, title, location, category
        
    except Exception as e:
        log(f"❌ Error discovering URLs: {str(e)}")
        return []

async def comprehensive_craigslist_crawl(progress_callback=None, max_locations=10, max_categories=10):
    """Comprehensive crawl of Craigslist to discover all URLs"""
    
    def log(msg):
        if progress_callback:
            progress_callback(msg)
    
    log("🚀 Starting comprehensive Craigslist crawl...")
    
    total_discovered = 0
    locations_to_crawl = CRAIGSLIST_LOCATIONS[:max_locations]
    categories_to_crawl = CRAIGSLIST_CATEGORIES[:max_categories]
    
    for i, location in enumerate(locations_to_crawl, 1):
        if not continuous_running:
            break
            
        log(f"🌍 Crawling location {i}/{len(locations_to_crawl)}: {location}")
        
        for j, category in enumerate(categories_to_crawl, 1):
            if not continuous_running:
                break
                
            try:
                log(f"📂 Category {j}/{len(categories_to_crawl)}: {category}")
                
                # Discover URLs for this location/category
                urls = await discover_craigslist_urls(location, category, progress_callback)
                
                if urls:
                    # Add to URL queue
                    added = add_urls_to_queue(urls, location, category)
                    total_discovered += added
                    log(f"✅ Added {added} new URLs to queue")
                
                # Update crawl progress
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute('''
                    INSERT OR REPLACE INTO crawl_progress 
                    (location, category, last_crawled, total_urls_found)
                    VALUES (?, ?, CURRENT_TIMESTAMP, ?)
                ''', (location, category, len(urls)))
                conn.commit()
                conn.close()
                
                # ULTRA FAST - minimal rate limiting between categories
                await asyncio.sleep(1.0)  # Increased from 0.1 to 1.0 seconds to be more respectful
                
            except Exception as e:
                log(f"❌ Error crawling {location}/{category}: {str(e)}")
                continue
        
        # Rate limiting between locations to avoid overwhelming servers
        await asyncio.sleep(3.0)  # Increased from 1.0 to 3.0 seconds for better rate limiting
    
    log(f"🎉 Crawl complete! Discovered {total_discovered} new URLs")
    return total_discovered

async def process_url_queue(progress_callback=None, batch_size=3, max_cycles=10):
    """Process URLs from the queue systematically with multiple processing cycles"""
    
    def log(msg):
        if progress_callback:
            progress_callback(msg)
    
    processed_total = 0
    cycles = 0
    
    while continuous_running and cycles < max_cycles:
        cycles += 1
        log(f"🔄 Starting processing cycle {cycles}/{max_cycles}")
        
        # Get next batch of URLs to scan
        urls_to_scan = get_next_urls_to_scan(batch_size)
        
        if not urls_to_scan:
            log("📋 URL queue is empty for this cycle")
            break
        
        log(f"📄 Processing {len(urls_to_scan)} URLs from queue (Cycle {cycles})...")
        
        cycle_processed = 0
        
        # Process URLs concurrently for MAXIMUM SPEED
        batch_tasks = []
        url_data = []
        
        for url_id, url, location, category in urls_to_scan:
            if not continuous_running:
                break
            task = extract_emails_from_post(url, f"Queue scan")
            batch_tasks.append(task)
            url_data.append((url_id, url, location, category))
        
        # Execute all tasks concurrently
        try:
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Process results quickly
            for idx, (result, (url_id, url, location, category)) in enumerate(zip(results, url_data)):
                if not continuous_running:
                    break
                    
                try:
                    emails_found = 0
                    
                    if isinstance(result, Exception):
                        log(f"❌ [{idx+1}] Error: {str(result)[:50]}...")
                        mark_url_scanned(url_id, 0, str(result))
                        continue
                    
                    email_data = result
                    if email_data and email_data.get('email'):
                        # Save to database efficiently
                        db_tuple = (
                            email_data['first_name'], email_data['last_name'], email_data['title'],
                            email_data['company_name'], email_data['email'], email_data['phone'],
                            email_data['stage'], email_data['linkedin_url'], email_data['location'],
                            email_data['category'], email_data['url'], email_data['scan_date'],
                            email_data['raw_name'], email_data['raw_post_title']
                        )
                        save_email_to_db(db_tuple)
                        emails_found = 1
                        log(f"✅ [{idx+1}] Email: {email_data['email'][:25]}...")
                    else:
                        log(f"⚪ [{idx+1}] No email found")
                    
                    # Mark as scanned
                    mark_url_scanned(url_id, emails_found)
                    cycle_processed += 1
                    processed_total += 1
                    
                except Exception as e:
                    log(f"❌ Error saving result {idx+1}: {e}")
                    mark_url_scanned(url_id, 0, str(e))
                    continue
                    
        except Exception as e:
            log(f"❌ Batch processing error: {e}")
            # Quick fallback - mark all as failed
            for url_id, url, location, category in url_data:
                mark_url_scanned(url_id, 0, "Batch processing failed")
        
        log(f"📊 Cycle {cycles} complete: Processed {cycle_processed} URLs")
        
        # Show queue stats after each cycle
        stats = get_queue_stats()
        log(f"� Queue stats: {stats['pending']} pending, {stats['scanned']} scanned, {stats['total_emails']} total emails")
        
        # Brief pause between cycles
        await asyncio.sleep(0.5)
    
    if cycles >= max_cycles:
        log(f"🏁 Completed {max_cycles} processing cycles. Total processed: {processed_total} URLs")
    else:
        log(f"🏁 Queue processing complete. Total processed: {processed_total} URLs")
    
    return processed_total

async def full_crawl_cycle(progress_callback=None, max_locations=10, max_categories=10, batch_size=20, urls_per_cycle=50):
    """Complete crawl cycle: Discover some URLs → Process them → Repeat"""
    
    def log(msg):
        if progress_callback:
            progress_callback(msg)
    
    log("🚀 Starting FULL CRAWL CYCLE...")
    
    # Phase 1: Limited Discovery - just get some URLs to start processing
    log(f"📋 Phase 1: Discovery - Finding {urls_per_cycle} new URLs")
    
    discovered = await discover_limited_urls(progress_callback, max_locations, max_categories, urls_per_cycle)
    
    if not continuous_running:
        return discovered, 0
    
    log(f"✅ Discovery complete. Found {discovered} new URLs")
    
    if discovered == 0:
        log("⚠️ No new URLs found - queue may be fully processed")
        return 0, 0
    
    log("📧 Phase 2: Processing - Extracting emails from discovered URLs")
    
    # Phase 2: Process what we just discovered
    processed = await process_url_queue(progress_callback, batch_size, max_cycles=3)
    
    log(f"🎉 CRAWL CYCLE COMPLETE!")
    log(f"📊 This cycle: {discovered} URLs discovered, {processed} URLs processed")
    
    return discovered, processed

async def discover_limited_urls(progress_callback=None, max_locations=10, max_categories=10, target_urls=50):
    """Discover a limited number of URLs quickly"""
    
    def log(msg):
        if progress_callback:
            progress_callback(msg)
    
    total_discovered = 0
    locations_to_crawl = CRAIGSLIST_LOCATIONS[:max_locations]
    categories_to_crawl = CRAIGSLIST_CATEGORIES[:max_categories]
    
    for i, location in enumerate(locations_to_crawl, 1):
        if not continuous_running or total_discovered >= target_urls:
            break
            
        log(f"🌍 Crawling location {i}/{len(locations_to_crawl)}: {location}")
        
        for j, category in enumerate(categories_to_crawl, 1):
            if not continuous_running or total_discovered >= target_urls:
                break
                
            try:
                log(f"📂 Category {j}/{len(categories_to_crawl)}: {category}")
                
                # Discover URLs for this location/category
                urls = await discover_craigslist_urls(location, category, progress_callback)
                
                if urls:
                    # Add to URL queue
                    added = add_urls_to_queue(urls, location, category)
                    total_discovered += added
                    log(f"✅ Added {added} new URLs (Total: {total_discovered})")
                    
                    # Stop if we've reached our target
                    if total_discovered >= target_urls:
                        log(f"🎯 Reached target of {target_urls} URLs")
                        break
                
                # Very brief delay between categories
                await asyncio.sleep(0.1)
                
            except Exception as e:
                log(f"❌ Error crawling {location}/{category}: {str(e)}")
                continue
        
        # Brief delay between locations  
        await asyncio.sleep(0.2)
        
        # Stop if we've reached our target
        if total_discovered >= target_urls:
            break
    
    log(f"✅ Limited discovery complete! Found {total_discovered} new URLs")
    return total_discovered

async def optimized_auto_repeat_crawl(progress_callback=None, auto_optimize=True, verbose_mode=False):
    """OPTIMIZED Auto-Repeat Crawl: Maximum speed, maximum efficiency, live output"""
    
    global super_verbose_mode
    super_verbose_mode = verbose_mode
    
    def log(msg, verbose_only=False):
        if progress_callback:
            if not verbose_only or super_verbose_mode:
                progress_callback(msg)
    
    log("� OPTIMIZED AUTO-REPEAT CRAWLER STARTING...")
    log("⚡ Configuration: ULTRA FAST mode with concurrent processing")
    log(f"🎯 Target: Continuous crawling until stopped")
    log("=" * 60)
    
    # Initialize performance tracking
    cycle_count = 0
    total_discovered = 0
    total_processed = 0
    total_emails = 0
    start_time = datetime.now()
    
    # Get initial optimization settings
    settings = get_optimization_settings()
    batch_size = 50
    max_threads = 20
    
    log(f"� Auto-optimization: {'ENABLED' if settings['auto_optimize'] else 'DISABLED'}")
    log(f"📊 Initial settings: Batch={batch_size}, Threads={max_threads}")
    log(f"🌍 Scanning all locations and categories")
    
    while continuous_running:
        cycle_count += 1
        cycle_start = datetime.now()
        log(f"\n🔄 ===== CYCLE {cycle_count} (CONTINUOUS) =====")
        
        # PHASE 1: Check for existing pending URLs first, then discover if needed
        queue_stats = get_queue_stats()
        
        if super_verbose_mode:
            log(f"📊 Pre-cycle queue: {queue_stats['pending']} pending, {queue_stats['scanned']} scanned")
        
        if queue_stats['pending'] >= batch_size:
            log(f"📋 Phase 1: Processing existing {queue_stats['pending']} pending URLs (skipping discovery)")
            discovered = 0  # No need to discover, we have plenty pending
            
            if super_verbose_mode:
                log(f"💡 Strategy: Using existing pending URLs - no discovery needed this cycle")
        else:
            target_urls = max(batch_size, 50)  # Always try to get at least 50 URLs
            log(f"📋 Phase 1: Lightning-fast URL discovery (target: {target_urls})")
            discovered = await ultra_fast_discovery(progress_callback, 15, 12, target_urls, verbose_mode)
            total_discovered += discovered
        
        if discovered == 0 and queue_stats['pending'] == 0:
            # Be more persistent - Craigslist always has new posts
            log(f"⚠️ No URLs available in cycle {cycle_count}")
            log("💡 This might be temporary - new posts appear constantly on Craigslist")
            if cycle_count > 10:  # Much more persistent - only stop after 10+ cycles
                log("🔄 Taking a longer break before trying comprehensive discovery...")
                # Try comprehensive discovery as a last resort
                log("📋 Attempting comprehensive discovery across all locations/categories...")
                discovered = await comprehensive_craigslist_crawl(progress_callback, 20, 15)
                total_discovered += discovered
                
                if discovered == 0:
                    log("🏁 No URLs found even with comprehensive search - pausing briefly")
                    await asyncio.sleep(30)  # Wait 30 seconds before next cycle
                else:
                    log(f"✅ Comprehensive discovery found {discovered} new URLs!")
            else:
                log("🔄 Continuing to next cycle - will try again shortly...")
                await asyncio.sleep(10)  # Brief pause before retrying
                continue
        
        log(f"✅ Discovery complete: {discovered} new URLs found")
        
        # PHASE 2: CONCURRENT PROCESSING
        log(f"📧 Phase 2: High-speed email extraction")
        
        processed, emails_found = await ultra_fast_processing(progress_callback, batch_size, verbose_mode)
        
        total_processed += processed
        total_emails += emails_found
        
        # Cycle summary
        cycle_time = (datetime.now() - cycle_start).total_seconds()
        log(f"🎉 CYCLE {cycle_count} COMPLETE in {cycle_time:.1f}s")
        log(f"📊 This cycle: +{discovered} URLs, +{processed} processed, +{emails_found} emails")
        
        # Running totals
        total_time = (datetime.now() - start_time).total_seconds()
        rate = total_processed / total_time if total_time > 0 else 0
        log(f"📈 TOTALS: {total_discovered} URLs found, {total_processed} processed, {total_emails} emails")
        log(f"⚡ Speed: {rate:.1f} URLs/second average")
        
        # Brief pause between cycles for system stability
        if processed > 0:
            log("⏸️ Quick pause (optimization interval)...")
            await asyncio.sleep(2)  # Very brief pause
        else:
            log("⏸️ No URLs processed - longer pause...")
            await asyncio.sleep(10)  # Longer pause if nothing to do
    
    # Only show final summary if user explicitly stopped the crawler
    if not continuous_running:  # User stopped the crawler
        total_time = (datetime.now() - start_time).total_seconds()
        rate = total_processed / total_time if total_time > 0 else 0
        
        log("\n" + "=" * 60)
        log("🏁 AUTO-REPEAT CRAWL STOPPED BY USER!")
        log(f"📊 FINAL RESULTS:")
        log(f"   🎯 Cycles completed: {cycle_count}")
        log(f"   📋 URLs discovered: {total_discovered}")
        log(f"   ⚡ URLs processed: {total_processed}")
        log(f"   📧 Emails found: {total_emails}")
        log(f"   ⏱️ Total time: {total_time:.1f} seconds")
        log(f"   🚀 Average speed: {rate:.2f} URLs/second")
        
        # Final queue stats
        stats = get_queue_stats()
        log(f"📈 Queue status: {stats['pending']} pending, {stats['total_emails']} total emails")
        log("✨ Crawl optimization complete!")
        log("=" * 60)
    else:
        # Crawler was stopped for other reasons (should rarely happen)
        log("🔄 Crawler paused - this should not normally happen")
        log("💡 If you see this message, the crawler may have encountered an unexpected condition")

async def comprehensive_discovery(progress_callback=None, target_urls=500000):
    """Comprehensive discovery across all Craigslist locations and categories"""
    
    def log(msg):
        if progress_callback:
            progress_callback(msg)
    
    log("🌍 Starting comprehensive Craigslist discovery...")
    log("🎯 Target: ~10,000 URLs per site across all locations")
    
    # Step 1: Discover all locations
    log("🔍 Discovering all Craigslist locations...")
    locations = await discover_all_craigslist_locations()
    log(f"📍 Found {len(locations)} locations to scan")
    
    # Step 2: Process each location with per-site limit
    total_discovered = 0
    sites_processed = 0
    target_per_site = 10000
    
    for location in locations:
        log(f"📁 Processing site: {location}")
        
        # Discover categories for this location
        categories = await discover_categories_from_location(location)
        
        if not categories:
            log(f"⚠️ {location}: No categories found, using defaults")
            # Fallback to major categories if discovery fails
            categories = ["jjj", "acc", "ofc", "bus", "csr", "etc", "fbh", "gov", "hea", "hum", "eng", "edu"]
        
        log(f"✅ {location}: {len(categories)} categories found")
        
        # Process categories for this location
        site_discovered = 0
        batch_size = 2  # Much smaller batch size to be gentler
        
        for i in range(0, len(categories), batch_size):
            batch_categories = categories[i:i + batch_size]
            
            # Process batch concurrently
            tasks = []
            for category in batch_categories:
                task = discover_craigslist_urls(location, category, progress_callback)
                tasks.append(task)
            
            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                for j, result in enumerate(results):
                    category = batch_categories[j]
                    
                    if isinstance(result, Exception):
                        log(f"❌ Discovery error {location}/{category}: {str(result)[:50]}...")
                        continue
                        
                    if result:  # URLs found
                        if super_verbose_mode:
                            log(f"🔍 SUPER VERBOSE: {location}/{category} returned {len(result)} URLs from discovery")
                        
                        added = add_urls_to_queue(result, location, category)
                        site_discovered += added
                        total_discovered += added
                        
                        if added > 0:
                            log(f"✅ {location}/{category}: +{added} URLs (site: {site_discovered}, total: {total_discovered})")
                        else:
                            log(f"⚪ {location}/{category}: No new URLs")
                            
                        # Stop this site if we've reached per-site target
                        if site_discovered >= target_per_site:
                            log(f"🎯 Site {location} target reached: {site_discovered} URLs")
                            break
                            
            except Exception as e:
                log(f"❌ Batch discovery error for {location}: {e}")
            
            # Stop this site if target reached
            if site_discovered >= target_per_site:
                break
                
            # Longer pause between batches to be respectful
            log(f"⏳ Pausing 3 seconds between batches...")
            await asyncio.sleep(3.0)
        
        sites_processed += 1
        log(f"🏁 {location} complete: {site_discovered} URLs discovered ({sites_processed} sites processed)")
        
        # Stop if we've reached overall target
        if total_discovered >= target_urls:
            log(f"🎯 Overall target reached: {total_discovered} URLs across {sites_processed} sites")
            return total_discovered
        
        # Longer pause between sites to be respectful
        log(f"⏳ Pausing 10 seconds before next site...")
        await asyncio.sleep(10.0)
    
    log(f"🏁 Comprehensive discovery complete: {total_discovered} URLs from {sites_processed} sites")
    return total_discovered

async def ultra_fast_discovery(progress_callback=None, max_locations=10, max_categories=8, target_urls=50, verbose_mode=False):
    """Conservative URL discovery with manageable concurrency"""
    
    def log(msg, verbose_only=False):
        if progress_callback:
            if not verbose_only or super_verbose_mode:
                progress_callback(msg)
    
    discovered = 0
    locations = CRAIGSLIST_LOCATIONS[:max_locations]  # Reduced from 15 to 10
    categories = CRAIGSLIST_CATEGORIES[:max_categories]  # Reduced from 12 to 8
    
    log(f"🌍 Discovering from {len(locations)} locations...")
    
    # Process in smaller batches to avoid overwhelming servers
    batch_size = 20  # Process 20 location/category pairs at a time
    all_tasks = []
    
    for location in locations:
        for category in categories:
            all_tasks.append((location, category))
    
    # Process in batches instead of all at once
    for i in range(0, len(all_tasks), batch_size):
        if discovered >= target_urls or not continuous_running:
            break
            
        batch = all_tasks[i:i + batch_size]
        log(f"⚡ Processing batch {i//batch_size + 1}: {len(batch)} location/category pairs...")
        
        # Create tasks for this batch
        discovery_tasks = []
        for location, category in batch:
            task = discover_craigslist_urls(location, category, None)
            discovery_tasks.append(task)
        
        try:
            # Execute batch with manageable concurrency
            results = await asyncio.gather(*discovery_tasks, return_exceptions=True)
            
            # Process results
            for (location, category), result in zip(batch, results):
                if not continuous_running or discovered >= target_urls:
                    break
                    
                if isinstance(result, Exception):
                    log(f"❌ Discovery error {location}/{category}: {str(result)[:50]}...", verbose_only=True)
                    continue
                    
                if result:  # URLs found
                    if super_verbose_mode:
                        log(f"🔍 SUPER VERBOSE: {location}/{category} returned {len(result)} URLs from discovery")
                    
                    added = add_urls_to_queue(result, location, category)
                    discovered += added
                    
                    if added > 0:
                        log(f"✅ {location}/{category}: +{added} URLs (total: {discovered})")
                        if super_verbose_mode:
                            log(f"� SUPER VERBOSE: Sample URLs: {[url[:50]+'...' for url in result[:2]]}")
                    else:
                        log(f"⚪ {location}/{category}: No new URLs (duplicates/rescans managed)")
                        if super_verbose_mode:
                            log(f"🔍 SUPER VERBOSE: All {len(result)} URLs were duplicates or too recent to re-scan")
                else:
                    if super_verbose_mode:
                        log(f"🔍 SUPER VERBOSE: {location}/{category} returned no URLs from discovery")
                        
        except Exception as e:
            log(f"❌ Batch discovery error: {e}")
        
        # Brief pause between batches to be respectful
        if i + batch_size < len(all_tasks) and discovered < target_urls:
            await asyncio.sleep(1.0)  # 1 second pause between batches
    
    log(f"🎯 Discovery complete: {discovered} URLs discovered")
    return discovered

async def ultra_fast_processing(progress_callback=None, batch_size=50, verbose_mode=False):
    """Ultra-fast URL processing with maximum concurrency"""
    
    def log(msg, verbose_only=False):
        if progress_callback:
            if not verbose_only or super_verbose_mode:
                progress_callback(msg)
    
    # Get URLs to process
    urls_to_scan = get_next_urls_to_scan(batch_size * 2)  # Get extra for efficiency
    
    if not urls_to_scan:
        log("⚠️ No URLs to process")
        return 0, 0
    
    log(f"⚡ Processing {len(urls_to_scan)} URLs with maximum concurrency...")
    
    processed = 0
    emails_found = 0
    
    # Process in batches for memory efficiency but maximum speed within batches
    for i in range(0, len(urls_to_scan), batch_size):
        if not continuous_running:
            break
            
        batch = urls_to_scan[i:i + batch_size]
        
        log(f"� Processing batch {i//batch_size + 1}: {len(batch)} URLs...")
        
        # Create all tasks for this batch
        batch_tasks = []
        batch_data = []
        
        for url_id, url, location, category in batch:
            if super_verbose_mode:
                log(f"� Scanning: {url}", verbose_only=True)
            task = extract_emails_from_post(url, "Ultra Fast Scan")
            batch_tasks.append(task)
            batch_data.append((url_id, url, location, category))
        
        try:
            # Execute batch concurrently
            results = await asyncio.gather(*batch_tasks, return_exceptions=True)
            
            # Process results super fast
            batch_emails = 0
            for (url_id, url, location, category), result in zip(batch_data, results):
                if not continuous_running:
                    break
                    
                try:
                    if isinstance(result, Exception):
                        log(f"❌ Scan error {url[:30]}...: {str(result)[:30]}...", verbose_only=True)
                        mark_url_scanned(url_id, 0, str(result))
                        processed += 1
                        continue
                    
                    email_data = result
                    found_email = 0
                    
                    if email_data and email_data.get('email'):
                        # Show found email in verbose mode
                        if super_verbose_mode:
                            log(f"✅ Email: {email_data['email']} from {url[:50]}...", verbose_only=True)
                        
                        # Quick database save
                        db_tuple = (
                            email_data['first_name'], email_data['last_name'], email_data['title'],
                            email_data['company_name'], email_data['email'], email_data['phone'],
                            email_data['stage'], email_data['linkedin_url'], email_data['location'],
                            email_data['category'], email_data['url'], email_data['scan_date'],
                            email_data['raw_name'], email_data['raw_post_title']
                        )
                        save_email_to_db(db_tuple)
                        found_email = 1
                        batch_emails += 1
                    else:
                        if super_verbose_mode:
                            log(f"⚪ No email found at {url[:50]}...", verbose_only=True)
                    
                    mark_url_scanned(url_id, found_email)
                    processed += 1
                    
                except Exception as e:
                    mark_url_scanned(url_id, 0, str(e))
                    processed += 1
                    continue
            
            emails_found += batch_emails
            log(f"✅ Batch complete: {len(batch)} processed, {batch_emails} emails found")
            
        except Exception as e:
            log(f"❌ Batch error: {e}")
            # Mark all as failed
            for url_id, url, location, category in batch_data:
                mark_url_scanned(url_id, 0, "Batch failed")
                processed += 1
    
    log(f"🎉 Processing complete: {processed} URLs processed, {emails_found} emails found")
    return processed, emails_found

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

def start_continuous_scan(main_keywords, location, category, keyword_list, max_results, skip_recent, interval_minutes, interval_text):
    """Start continuous scanning - REMOVED"""
    pass

def stop_continuous_scan():
    """Stop continuous scanning - REMOVED"""
    pass

# Gradio Interface
def create_interface():
    init_database()
    init_optimization_database()
    init_url_tracking_db()  # Initialize URL tracking
    
    custom_css = """
    .big-button {
        font-size: 18px !important;
        padding: 15px 30px !important;
        font-weight: bold !important;
    }
    .live-output {
        font-family: 'Courier New', monospace !important;
        background-color: #1a1a1a !important;
        color: #00ff00 !important;
        border: 2px solid #333 !important;
    }
    .status-running {
        color: #00ff00 !important;
        font-weight: bold !important;
    }
    .status-stopped {
        color: #ff6b6b !important;
        font-weight: bold !important;
    }
    """
    
    with gr.Blocks(title="Craigslist Email Scanner", theme=gr.themes.Soft(), css=custom_css) as demo:
        gr.Markdown("# 📧 Craigslist Email Scanner\nExtract contact information from Craigslist posts efficiently")
        
        with gr.Tabs():
            with gr.Tab(" Auto Repeat Crawler"):
                gr.Markdown("### � One-Click Auto Repeat Crawler\n**Optimized for maximum speed and simplicity - just click START!**")
                
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("#### 🎯 Continuous Crawler Settings")
                        
                        # Info about continuous mode
                        gr.Markdown("**🚀 Infinite Mode:** Crawler runs continuously until manually stopped.<br/>**🤖 Auto-Optimization:** Settings automatically adjust for best performance.")
                        
                        # Big start/stop buttons
                        with gr.Row():
                            auto_start_btn = gr.Button(
                                "🚀 START CONTINUOUS CRAWL", 
                                variant="primary", 
                                size="lg",
                                elem_classes=["big-button"]
                            )
                            auto_stop_btn = gr.Button(
                                "⏹️ STOP", 
                                variant="stop", 
                                size="lg"
                            )
                        
                        # Comprehensive discovery button
                        with gr.Row():
                            comprehensive_btn = gr.Button(
                                "🌍 COMPREHENSIVE DISCOVERY (All Locations)", 
                                variant="secondary", 
                                size="lg"
                            )
                        
                        gr.Markdown("**🌍 Comprehensive Mode:** Discovers ALL Craigslist locations and categories automatically, then scans up to 300 pages each!")
                        
                        # Live stats
                        auto_status = gr.Markdown("**Status:** Ready to start")
                        auto_stats = gr.Markdown("""**Quick Stats:**
- 🎯 Status: ⏹️ Ready
- 📋 URLs Pending: Loading...
- 🔍 Total Scanned: Loading...
- 📧 Total Emails: Loading...
- ⚡ Recent Activity: Loading...
- 📊 Success Rate: Loading...""")
                        
                    with gr.Column(scale=2):
                        gr.Markdown("#### 📺 Live Activity Monitor")
                        
                        auto_live_output = gr.Textbox(
                            label="🔴 LIVE CRAWLER OUTPUT",
                            placeholder="🚀 Click START to begin crawling...\n\n✨ The crawler will:\n1. Find new Craigslist URLs\n2. Extract emails concurrently\n3. Repeat automatically\n4. Show live progress here",
                            lines=20,
                            interactive=False,
                            show_copy_button=True,
                            elem_classes=["live-output"]
                        )
                        
                        # Auto-refresh toggle
                        with gr.Row():
                            auto_refresh = gr.Checkbox(
                                label="📺 Auto-refresh output (every 3 seconds)",
                                value=True,
                                info="Keep output updated automatically"
                            )
                            manual_refresh_btn = gr.Button("🔄 Manual Refresh", size="sm", variant="secondary")
                            super_verbose = gr.Checkbox(
                                label="🔍 Super Verbose Mode",
                                value=False,
                                info="Show detailed technical information (URLs, errors, timing)"
                            )
            
            with gr.Tab("📧 Results"):
                with gr.Row():
                    gr.Markdown("### Found Emails")
                    email_count = gr.HTML("<div style='text-align: right; font-size: 18px; font-weight: bold; color: #2563eb;'>📊 Total Emails: 0</div>")
                
                results_table = gr.Dataframe(
                    value=load_emails_from_db(),
                    headers=["First Name", "Last Name", "Title", "Company Name", "Email", "Phone", "Stage", "LinkedIn URL", "Location", "Category", "URL", "Scan Date"],
                    interactive=True,
                    wrap=True,
                    datatype=["str"] * len(DEFAULT_COLUMNS)
                )
                
                with gr.Row():
                    refresh_btn = gr.Button("🔄 Refresh Results")
                    clear_btn = gr.Button("🗑️ Clear All", variant="secondary")
                    fix_companies_btn = gr.Button("🏢 Fix Company Names", variant="primary")
                    export_csv = gr.Button("📄 Export CSV")
                    download_file = gr.File(label="Download", interactive=False)
            
            with gr.Tab(" VPN Control"):
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
            
            # Verbose Console Tab
            with gr.Tab("🧾 Verbose Console"):
                gr.Markdown("""
                ### Verbose Console
                Shows the last 10,000 log lines from the app (most recent last).
                """)
                with gr.Row():
                    refresh_verbose_btn = gr.Button("🔄 Refresh", variant="primary")
                    clear_verbose_btn = gr.Button("🧹 Clear", variant="secondary")
                verbose_textbox = gr.Textbox(
                    value=get_verbose_text(),
                    label="Logs (tail of 10,000 lines)",
                    lines=24,
                    interactive=False,
                    show_copy_button=True
                )
        
        # Status message for company fixing
        fix_status = gr.Markdown("")
        
        # Event handlers
        def _refresh_verbose():
            return get_verbose_text()

        def _clear_verbose():
            clear_verbose_log()
            return get_verbose_text()

        # Verbose console actions
        refresh_verbose_btn.click(_refresh_verbose, outputs=[verbose_textbox])
        clear_verbose_btn.click(_clear_verbose, outputs=[verbose_textbox])
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
        
        def fix_company_names_ui():
            """UI function to fix company names"""
            try:
                fixed_count, total_checked = fix_company_names()
                
                if fixed_count > 0:
                    message = f"✅ Fixed {fixed_count} company names out of {total_checked} records checked"
                    # Refresh the results table
                    updated_data = load_emails_from_db()
                    count = len(updated_data)
                    count_html = f"<div style='text-align: right; font-size: 18px; font-weight: bold; color: #2563eb;'>📊 Total Emails: {count}</div>"
                    return updated_data, count_html, message
                else:
                    message = f"ℹ️ No company names needed fixing. Checked {total_checked} records."
                    return load_emails_from_db(), email_count.value, message
                    
            except Exception as e:
                error_msg = f"❌ Error fixing company names: {str(e)}"
                return load_emails_from_db(), email_count.value, error_msg

        def export_to_csv():
            """Export current results to a CSV matching the sample format."""
            try:
                df = load_emails_from_db()
                
                if df.empty:
                    # Create empty DataFrame with correct columns
                    export_df = pd.DataFrame(columns=[
                        "First Name", "Last Name", "Title", "Company Name", 
                        "Email", "Phone", "Stage", "Person Linkedin Url"
                    ])
                else:
                    # Map database columns to export format
                    export_df = pd.DataFrame({
                        "First Name": df['first_name'],
                        "Last Name": df['last_name'],
                        "Title": df['title'],
                        "Company Name": df['company_name'],
                        "Email": df['email'],
                        "Phone": df['phone'],
                        "Stage": df['stage'],
                        "Person Linkedin Url": df['linkedin_url']
                    })
                
                # Drop duplicate emails if present
                if not export_df.empty:
                    export_df.drop_duplicates(subset=["Email"], inplace=True)

                temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
                export_df.to_csv(temp_file.name, index=False)
                return temp_file.name
            except Exception as e:
                print(f"Error exporting CSV: {e}")
                return None
        
        
        # Connect event handlers for results and stats
        refresh_btn.click(refresh_results, outputs=[results_table, email_count])
        clear_btn.click(clear_all_results, outputs=[results_table, email_count])
        fix_companies_btn.click(fix_company_names_ui, outputs=[results_table, email_count, fix_status])
        export_csv.click(export_to_csv, outputs=[download_file])
        
        # Comprehensive Crawler Event Handlers
        crawler_console_output = ""
        crawler_status_text = "**Crawler Status:** Stopped"
        
        def update_crawler_console(msg):
            nonlocal crawler_console_output
            timestamp = datetime.now().strftime("%H:%M:%S")
            crawler_console_output += f"[{timestamp}] {msg}\n"
            return crawler_console_output
        
        def update_crawler_status(status):
            nonlocal crawler_status_text
            crawler_status_text = status
            return crawler_status_text
        
        def start_comprehensive_crawler(max_locations, max_categories, batch_size, crawler_mode, speed_mode, urls_per_cycle, max_repeat_cycles):
            """Start the comprehensive crawler with speed optimization"""
            global continuous_running, continuous_thread
            
            if continuous_running:
                return "**Crawler Status:** Already running", "⚠️ Crawler is already running\n", "**Queue Stats:** Check above"
            
            continuous_running = True
            update_crawler_console("🚀 Starting comprehensive Craigslist crawler...")
            update_crawler_console(f"📊 Settings: {max_locations} locations, {max_categories} categories, batch size {batch_size}")
            update_crawler_console(f"🔧 Mode: {crawler_mode}")
            update_crawler_console(f"⚡ Speed Mode: {speed_mode}")
            
            if crawler_mode == "Auto-Repeat Crawl":
                update_crawler_console(f"🔄 Auto-Repeat: {urls_per_cycle} URLs per cycle, max {max_repeat_cycles} cycles")
            
            # Adjust batch size based on speed mode (reduced for better rate limit management)
            if speed_mode == "ULTRA FAST Mode":
                batch_size = min(batch_size + 5, 10)  # Conservative increase for ultra fast
                update_crawler_console(f"🚀 ULTRA FAST Mode: Increased batch size to {batch_size} (conservative)")
            elif speed_mode == "Fast Mode":
                batch_size = min(batch_size + 2, 8)  # Small increase for fast mode
                update_crawler_console(f"⚡ Fast Mode: Increased batch size to {batch_size} (conservative)")
            
            import threading
            def run_crawler():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    if crawler_mode == "Discovery Mode":
                        loop.run_until_complete(
                            comprehensive_craigslist_crawl(update_crawler_console, max_locations, max_categories)
                        )
                    elif crawler_mode == "Queue Processing":
                        loop.run_until_complete(
                            process_url_queue(update_crawler_console, batch_size, max_cycles=10)
                        )
                    elif crawler_mode == "Full Crawl":
                        # Single cycle: discover some URLs then process them
                        loop.run_until_complete(
                            full_crawl_cycle(update_crawler_console, max_locations, max_categories, batch_size, urls_per_cycle)
                        )
                    else:  # Auto-Repeat Crawl
                        # Multiple cycles: discover→scan→repeat
                        loop.run_until_complete(
                            auto_repeat_crawl(update_crawler_console, max_locations, max_categories, batch_size, urls_per_cycle, max_repeat_cycles)
                        )
                except Exception as e:
                    update_crawler_console(f"❌ Crawler error: {str(e)}")
                    update_crawler_status("**Crawler Status:** Error")
                finally:
                    loop.close()
                    update_crawler_status("**Crawler Status:** Stopped")
            
            continuous_thread = threading.Thread(target=run_crawler, daemon=True)
            continuous_thread.start()
            
            update_crawler_status("**Crawler Status:** Starting...")
            stats = get_queue_stats()
            stats_text = f"**Queue Stats:** {stats['pending']} pending, {stats['scanned']} scanned, {stats['total_emails']} emails found"
            
            return crawler_status_text, crawler_console_output, stats_text
        
        def stop_comprehensive_crawler():
            """Stop the comprehensive crawler"""
            global continuous_running
            continuous_running = False
            update_crawler_console("🛑 Stopping crawler...")
            update_crawler_status("**Crawler Status:** Stopping...")
            stats = get_queue_stats()
            stats_text = f"**Queue Stats:** {stats['pending']} pending, {stats['scanned']} scanned, {stats['total_emails']} emails found"
            return crawler_status_text, crawler_console_output, stats_text
        
        def refresh_queue_stats():
            """Refresh queue statistics"""
            stats = get_queue_stats()
            stats_text = f"""**Queue Statistics:**
- **Total URLs:** {stats['total']}
- **Pending:** {stats['pending']} 
- **Scanned:** {stats['scanned']}
- **Failed:** {stats['failed']}
- **Total Emails Found:** {stats['total_emails']}"""
            return stats_text
        
        # Auto Repeat Crawler Event Handlers
        def start_auto_repeat_crawler(verbose_mode):
            """Start the optimized continuous auto repeat crawler"""
            global continuous_running, continuous_thread
            
            if continuous_running:
                return "**Status:** Already running", get_auto_output(), get_auto_stats()
            
            continuous_running = True
            
            # Clear and initialize live output
            clear_auto_output()
            
            # Initial messages
            manage_auto_output("🚀 CONTINUOUS AUTO-REPEAT CRAWLER STARTING...")
            manage_auto_output(f"⚡ INFINITE MODE: Self-optimizing performance enabled")
            manage_auto_output(f"🎯 Configuration: Continuous crawling until stopped")
            if verbose_mode:
                manage_auto_output("🔍 SUPER VERBOSE MODE ENABLED - Detailed technical output")
            manage_auto_output("=" * 60)
            
            def update_auto_output(msg):
                manage_auto_output(msg)
            
            import threading
            def run_auto_crawler():
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    loop.run_until_complete(
                        optimized_auto_repeat_crawl(update_auto_output, auto_optimize=True, verbose_mode=verbose_mode)
                    )
                except Exception as e:
                    update_auto_output(f"❌ Crawler error: {str(e)}")
                finally:
                    loop.close()
                    global continuous_running
                    continuous_running = False
                    update_auto_output("🏁 AUTO-REPEAT CRAWLER STOPPED")
            
            continuous_thread = threading.Thread(target=run_auto_crawler, daemon=True)
            continuous_thread.start()
            
            status = "**Status:** 🚀 Starting optimized crawler..."
            stats = get_auto_stats()
            
            return status, get_auto_output(), stats
        
        def stop_auto_repeat_crawler():
            """Stop the auto repeat crawler"""
            global continuous_running
            continuous_running = False
            
            manage_auto_output("🛑 STOP REQUESTED - Crawler will stop after current cycle")
            
            status = "**Status:** 🛑 Stopping..."
            stats = get_auto_stats()
            
            return status, get_auto_output(), stats
        
        def get_auto_stats():
            """Get quick stats for auto repeat crawler"""
            try:
                stats = get_queue_stats()
                emails = get_email_count()
                
                # Get recent activity count
                conn = sqlite3.connect(DB_PATH)
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT COUNT(*) FROM (
                        SELECT email FROM emails WHERE scan_date >= datetime('now', '-1 hour')
                        UNION
                        SELECT url FROM url_queue WHERE scanned_date >= datetime('now', '-1 hour') AND emails_found > 0
                    )
                ''')
                recent_activity = cursor.fetchone()[0] or 0
                conn.close()
                
                status_emoji = "🚀" if continuous_running else "⏹️"
                status_text = "Active" if continuous_running else "Stopped"
                total_scanned = stats['scanned']
                
                return f"""**Quick Stats:**
- 🎯 Status: {status_emoji} {status_text}
- 📋 URLs Pending: {stats['pending']}
- 🔍 Total Scanned: {total_scanned}
- 📧 Total Emails: {emails}
- ⚡ Recent Activity: {recent_activity}/hour
- 📊 Success Rate: {(emails/max(total_scanned,1)*100):.1f}%"""
            except Exception as e:
                print(f"Error getting auto stats: {e}")
                return f"**Quick Stats:** Error loading stats - {str(e)}"
        
        # Connect auto repeat crawler events
        auto_start_btn.click(
            start_auto_repeat_crawler,
            inputs=[super_verbose],
            outputs=[auto_status, auto_live_output, auto_stats]
        )
        
        auto_stop_btn.click(
            stop_auto_repeat_crawler,
            outputs=[auto_status, auto_live_output, auto_stats]
        )
        
        # Connect comprehensive discovery
        def start_comprehensive_discovery():
            """Start comprehensive discovery in background"""
            
            # Use threading to run the async function
            import threading
            
            def run_comprehensive_sync():
                try:
                    manage_auto_output("🌍 Starting comprehensive discovery...")
                    # Create new event loop for this thread
                    loop = asyncio.new_event_loop()
                    asyncio.set_event_loop(loop)
                    discovered = loop.run_until_complete(comprehensive_discovery(
                        progress_callback=lambda msg: manage_auto_output(msg),
                        target_urls=500000  # Allow discovery across many sites, ~10k per site
                    ))
                    manage_auto_output(f"🏁 Comprehensive discovery complete: {discovered} URLs")
                    loop.close()
                except Exception as e:
                    manage_auto_output(f"❌ Comprehensive discovery error: {e}")
            
            # Start the task in a separate thread
            thread = threading.Thread(target=run_comprehensive_sync, daemon=True)
            thread.start()
            
            return "🌍 Comprehensive discovery started..."
        
        comprehensive_btn.click(
            start_comprehensive_discovery,
            outputs=[auto_live_output]
        )
        
        # Auto-refresh functionality for live output
        def refresh_auto_output():
            """Refresh the auto crawler output and stats"""
            try:
                stats = get_auto_stats()
                output = get_auto_output()
                if continuous_running:
                    status = "**Status:** 🚀 Running"
                else:
                    status = "**Status:** ⏹️ Stopped"
                return status, output, stats
            except Exception as e:
                print(f"Error in auto-refresh: {e}")
                return "**Status:** ❌ Error", f"Auto-refresh error: {str(e)}", "**Quick Stats:** Error loading stats"
        
        # Simple auto-refresh timer - only runs when checkbox is enabled
        auto_refresh_timer = gr.Timer(value=5.0, active=True)  # 5 second refresh for stability, start active
        auto_refresh_timer.tick(
            refresh_auto_output,
            outputs=[auto_status, auto_live_output, auto_stats]
        )
        
        # Control auto-refresh based on checkbox
        def toggle_auto_refresh(enabled):
            """Toggle the auto-refresh timer on/off"""
            print(f"Auto-refresh toggled: {enabled}")  # Debug logging
            return gr.Timer(active=enabled, value=5.0)  # Reset timer when toggling
        
        auto_refresh.change(
            toggle_auto_refresh,
            inputs=[auto_refresh],
            outputs=[auto_refresh_timer]
        )
        
        # Manual refresh button event handler
        manual_refresh_btn.click(
            refresh_auto_output,
            outputs=[auto_status, auto_live_output, auto_stats]
        )
        
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

        # Initialize components (must be inside Blocks context)
        demo.load(refresh_results, outputs=[results_table, email_count])
        demo.load(refresh_vpn_status, outputs=[auth_status, vpn_status, current_ip, vpn_console, install_status])
        demo.load(_refresh_verbose, outputs=[verbose_textbox])
    
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
    port = 7863
    
    print("=" * 60)
    print("🔍 CRAIGSLIST SCANNER - VPN & SSH FRIENDLY STARTUP")
    print("=" * 60)
    print(f"🔌 Port: {port}")
    print("📍 Access URLs:")
    print(f"   • Localhost: http://localhost:{port}")
    print(f"   • Your Network: http://192.168.1.107:{port}")
    print(f"   • All IPs:   http://0.0.0.0:{port}")
    print("")
    print("� SSH/VS Code Access:")
    print(f"   • SSH Tunnel: ssh -L {port}:localhost:{port} user@server")
    print(f"   • VS Code Forward: Forward port {port} in VS Code")
    print(f"   • Then access: http://localhost:{port}")
    print("")
    print("�💡 Connection Tips:")
    print("   • VPN: If localhost doesn't work, try the Network IP URL")
    print("   • SSH: Use port forwarding for remote access")
    print("   • VS Code: Use 'Forward a Port' in terminal panel")
    print("   • Check VPN settings to allow local network access")
    print("=" * 60)
    print("")
    
    # Launch with VPN-friendly and SSH-friendly settings
    demo.launch(
        server_name="0.0.0.0",  # Bind to all interfaces for VPN/SSH compatibility
        server_port=7861,       # Back to original port
        share=False,
        inbrowser=False,        # Don't auto-open browser (better for SSH)
        debug=False,
        quiet=False,
        show_error=True,
        allowed_paths=["./"]    # Allow local file access
    )
