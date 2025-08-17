#!/usr/bin/env python3
"""
VPN-friendly server startup script for Argos Lead Finder
This script helps resolve local connection issues when using VPN
"""

import os
import sys
import socket
import gradio as gr
from app import demo

def get_local_ip():
    """Get the local IP address"""
    try:
        # Connect to a remote address to determine local IP
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        local_ip = s.getsockname()[0]
        s.close()
        return local_ip
    except Exception:
        return "127.0.0.1"

def main():
    local_ip = get_local_ip()
    port = 7860
    
    print("=" * 60)
    print("🚀 ARGOS LEAD FINDER - VPN-FRIENDLY STARTUP")
    print("=" * 60)
    print(f"🌐 Local IP: {local_ip}")
    print(f"🔌 Port: {port}")
    print("📍 Access URLs:")
    print(f"   • Localhost: http://localhost:{port}")
    print(f"   • Local IP:  http://{local_ip}:{port}")
    print(f"   • All IPs:   http://0.0.0.0:{port}")
    print("")
    print("💡 VPN Tips:")
    print("   • If localhost doesn't work, try the Local IP URL")
    print("   • You can also try 127.0.0.1:7860")
    print("   • Check VPN settings to allow local network access")
    print("   • Consider adding 127.0.0.1 to VPN bypass list")
    print("=" * 60)
    print("")
    
    # Launch with multiple binding options
    try:
        demo.queue().launch(
            server_name="0.0.0.0",  # Bind to all interfaces
            server_port=port,
            share=False,
            inbrowser=True,
            debug=False,
            quiet=False,
            show_error=True,
            # Additional options for VPN compatibility
            enable_queue=True,
            max_threads=10
        )
    except Exception as e:
        print(f"❌ Failed to start server: {e}")
        print("\n🔧 Troubleshooting:")
        print("1. Check if port 7860 is already in use")
        print("2. Try disabling VPN temporarily")
        print("3. Check firewall settings")
        print("4. Try running: python app.py directly")

if __name__ == "__main__":
    main()
