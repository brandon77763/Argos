# Argos Lead Finder - Connection Guide

## 🚀 Running the Applications

### Main App (Argos Lead Finder)
```bash
# Option 1: Direct run
python app.py

# Option 2: VPN-friendly startup script
./start.sh

# Option 3: Manual activation
source .venv/bin/activate && python app.py
```
**Port:** 7860  
**URL:** http://localhost:7860

### Craigslist Scanner
```bash
# Option 1: Direct run
python craigslist_scanner.py

# Option 2: VPN-friendly startup script
./start_craigslist.sh
```
**Port:** 7861  
**URL:** http://localhost:7861

---

## 🔗 VS Code SSH Connection Setup

### Method 1: VS Code Port Forwarding (Recommended)
1. Connect to your server via SSH in VS Code
2. Open Terminal in VS Code
3. Run the app: `python app.py` or `python craigslist_scanner.py`
4. In VS Code, go to **Terminal** → **Ports** tab
5. Click **"Forward a Port"**
6. Enter port `7860` (for main app) or `7861` (for Craigslist scanner)
7. Access via: `http://localhost:7860` or `http://localhost:7861`

### Method 2: Manual SSH Tunneling
```bash
# For Main App (port 7860)
ssh -L 7860:localhost:7860 your-username@your-server-ip

# For Craigslist Scanner (port 7861)  
ssh -L 7861:localhost:7861 your-username@your-server-ip

# Combined (both ports)
ssh -L 7860:localhost:7860 -L 7861:localhost:7861 your-username@your-server-ip
```

### Method 3: VS Code tasks.json (Advanced)
Create `.vscode/tasks.json`:
```json
{
    "version": "2.0.0",
    "tasks": [
        {
            "label": "Start Argos Main App",
            "type": "shell",
            "command": "python",
            "args": ["app.py"],
            "group": "build",
            "presentation": {
                "echo": true,
                "reveal": "always",
                "focus": false,
                "panel": "new"
            },
            "isBackground": true
        },
        {
            "label": "Start Craigslist Scanner",
            "type": "shell", 
            "command": "python",
            "args": ["craigslist_scanner.py"],
            "group": "build",
            "presentation": {
                "echo": true,
                "reveal": "always", 
                "focus": false,
                "panel": "new"
            },
            "isBackground": true
        }
    ]
}
```

---

## 🛡️ VPN Compatibility

### NordVPN Users
```bash
# Add local network to bypass (if needed)
nordvpn whitelist add subnet 192.168.0.0/16
nordvpn whitelist add subnet 127.0.0.0/8

# Check current status
nordvpn status
```

### General VPN Tips
- Apps bind to `0.0.0.0` (all interfaces) for maximum compatibility
- Use local IP address if localhost doesn't work
- Consider adding localhost to VPN bypass list
- Check VPN settings for "Allow LAN traffic" option

---

## 🔧 Troubleshooting

### Connection Issues
1. **Port already in use:**
   ```bash
   # Check what's using the port
   netstat -tuln | grep :7860
   
   # Kill process if needed
   pkill -f "app.py"
   pkill -f "craigslist_scanner.py"
   ```

2. **VPN blocking access:**
   - Try using the local IP instead of localhost
   - Check VPN settings for LAN access
   - Temporarily disable VPN to test

3. **SSH/VS Code issues:**
   - Ensure port forwarding is set up correctly
   - Check VS Code's forwarded ports tab
   - Verify SSH connection is working

4. **Firewall blocking:**
   ```bash
   # Ubuntu/Debian
   sudo ufw allow 7860
   sudo ufw allow 7861
   
   # CentOS/RHEL
   sudo firewall-cmd --add-port=7860/tcp --permanent
   sudo firewall-cmd --add-port=7861/tcp --permanent
   sudo firewall-cmd --reload
   ```

### Performance Issues
- Increase delay settings in auto crawler
- Reduce concurrent requests
- Check available memory: `free -h`
- Monitor CPU usage: `htop`

---

## 📱 Access Methods Summary

| Method | Main App URL | Craigslist Scanner URL | Best For |
|--------|-------------|----------------------|----------|
| Direct Local | http://localhost:7860 | http://localhost:7861 | Local machine |
| Local IP | http://YOUR-LOCAL-IP:7860 | http://YOUR-LOCAL-IP:7861 | VPN users |
| SSH Tunnel | http://localhost:7860 | http://localhost:7861 | Remote access |
| VS Code Forward | http://localhost:7860 | http://localhost:7861 | VS Code users |

---

## 🚀 Quick Start Commands

```bash
# Full setup and start
git clone <your-repo>
cd Argos
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Start main app
python app.py

# Start craigslist scanner (in another terminal)
python craigslist_scanner.py
```

Both apps will show detailed connection information on startup!
