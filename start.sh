#!/bin/bash

# Argos Lead Finder Startup Script
# Handles VPN connectivity issues

echo "🚀 Starting Argos Lead Finder..."

# Navigate to the correct directory
cd "$(dirname "$0")"

# Activate virtual environment
if [ -d ".venv" ]; then
    echo "📦 Activating virtual environment..."
    source .venv/bin/activate
else
    echo "❌ Virtual environment not found. Please run: python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt"
    exit 1
fi

# Check if requirements are installed
echo "🔍 Checking dependencies..."
python -c "import gradio, pandas, ddgs" 2>/dev/null
if [ $? -ne 0 ]; then
    echo "📦 Installing dependencies..."
    pip install -r requirements.txt
fi

echo ""
echo "🌐 Network Information:"
echo "   Local IP: $(hostname -I | awk '{print $1}')"
echo "   Hostname: $(hostname)"
echo "   SSH Session: ${SSH_CLIENT:+Active}"
echo ""

echo "🔗 Connection Methods:"
echo "   • Direct: http://localhost:7860"
echo "   • Local IP: http://$(hostname -I | awk '{print $1}'):7860"
echo "   • SSH Tunnel: ssh -L 7860:localhost:7860 user@$(hostname -I | awk '{print $1}')"
echo "   • VS Code: Forward port 7860 in Terminal → Ports"
echo ""

# Offer different startup options
echo "🚀 Choose startup option:"
echo "   1) VPN-friendly server (recommended)"
echo "   2) Standard server"
echo "   3) Localhost only"
echo ""
read -p "Enter choice (1-3) [1]: " choice
choice=${choice:-1}

case $choice in
    1)
        echo "🌐 Starting VPN-friendly server..."
        python start_server.py
        ;;
    2)
        echo "🌐 Starting standard server..."
        python app.py
        ;;
    3)
        echo "🏠 Starting localhost-only server..."
        python -c "
import app
app.demo.queue().launch(
    server_name='127.0.0.1',
    server_port=7860,
    share=False,
    inbrowser=True
)"
        ;;
    *)
        echo "❌ Invalid choice. Starting VPN-friendly server..."
        python start_server.py
        ;;
esac
