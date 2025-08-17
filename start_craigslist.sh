#!/bin/bash

# Craigslist Scanner VPN-Friendly Startup Script

echo "🔍 Starting Craigslist Scanner..."

# Navigate to the correct directory
cd "$(dirname "$0")"

# Activate virtual environment
if [ -d ".venv" ]; then
    echo "📦 Activating virtual environment..."
    source .venv/bin/activate
else
    echo "❌ Virtual environment not found. Please run setup first."
    exit 1
fi

echo ""
echo "🌐 Network Information:"
echo "   Local IP: $(hostname -I | awk '{print $1}')"
echo "   VPN Status: $(nordvpn status 2>/dev/null | grep Status || echo 'NordVPN not available')"
echo "   SSH Session: ${SSH_CLIENT:+Active}"
echo ""

echo "🔗 Connection Methods:"
echo "   • Direct: http://localhost:7861"
echo "   • Local IP: http://$(hostname -I | awk '{print $1}'):7861"
echo "   • SSH Tunnel: ssh -L 7861:localhost:7861 user@$(hostname -I | awk '{print $1}')"
echo "   • VS Code: Forward port 7861 in Terminal → Ports"
echo ""

echo "🚀 Starting Craigslist Scanner with VPN-friendly settings..."
echo "   Port: 7861"
echo "   Binding to all interfaces (0.0.0.0)"
echo ""

# Check if port is available
if netstat -tuln | grep :7861 > /dev/null; then
    echo "⚠️ Port 7861 is already in use. Trying to stop existing process..."
    pkill -f "craigslist_scanner.py" 2>/dev/null
    sleep 2
fi

# Start the scanner
python craigslist_scanner.py

echo ""
echo "🏁 Craigslist Scanner has been stopped."
