#!/usr/bin/env python3
"""
Banking Data Validation App Launcher
Direct launcher for the Streamlit banking data validation application.
Run this file directly instead of using 'streamlit run streamlit_app.py'
"""

import os
import sys
import subprocess
import webbrowser
import time
from pathlib import Path

def check_streamlit_installed():
    """Check if Streamlit is installed."""
    try:
        import streamlit
        return True
    except ImportError:
        return False

def install_streamlit():
    """Install Streamlit if not already installed."""
    print("📦 Streamlit not found. Installing Streamlit...")
    try:
        subprocess.check_call([sys.executable, "-m", "pip", "install", "streamlit"])
        print("✅ Streamlit installed successfully!")
        return True
    except subprocess.CalledProcessError as e:
        print(f"❌ Failed to install Streamlit: {e}")
        return False

def check_dependencies():
    """Check and install required dependencies."""
    required_packages = [
        'streamlit',
        'pandas',
        'google-cloud-bigquery',
        'openpyxl',
        'plotly'
    ]
    
    missing_packages = []
    
    for package in required_packages:
        try:
            __import__(package.replace('-', '_'))
        except ImportError:
            missing_packages.append(package)
    
    if missing_packages:
        print(f"📦 Installing missing packages: {', '.join(missing_packages)}")
        try:
            subprocess.check_call([
                sys.executable, "-m", "pip", "install"
            ] + missing_packages)
            print("✅ All dependencies installed successfully!")
        except subprocess.CalledProcessError as e:
            print(f"❌ Failed to install dependencies: {e}")
            return False
    
    return True

def get_app_path():
    """Get the path to streamlit_app.py."""
    current_dir = Path(__file__).parent
    app_path = current_dir / "streamlit_app.py"
    
    if not app_path.exists():
        print(f"❌ Error: streamlit_app.py not found at {app_path}")
        return None
    
    return str(app_path)

def run_streamlit_app():
    """Run the Streamlit app directly."""
    print("Banking Data Validation Framework")
    print("=" * 50)
    
    # Check if Streamlit is installed
    if not check_streamlit_installed():
        if not install_streamlit():
            print("❌ Cannot proceed without Streamlit. Please install it manually:")
            print("   pip install streamlit")
            return False
    
    # Check other dependencies
    if not check_dependencies():
        print("❌ Cannot proceed without required dependencies.")
        return False
    
    # Get the app path
    app_path = get_app_path()
    if not app_path:
        return False
    
    print(f"App location: {app_path}")
    print("Starting Streamlit server...")
    print("The app will open in your default browser")
    print("Press Ctrl+C to stop the server")
    print("-" * 50)
    
    try:
        # Run streamlit with the app file
        cmd = [sys.executable, "-m", "streamlit", "run", app_path]
        
        # Set environment variables for better experience
        env = os.environ.copy()
        env['STREAMLIT_BROWSER_GATHER_USAGE_STATS'] = 'false'
        env['STREAMLIT_SERVER_HEADLESS'] = 'false'
        
        # Start the Streamlit process
        process = subprocess.Popen(
            cmd,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            universal_newlines=True,
            bufsize=1
        )
        
        # Monitor the process output
        app_url = None
        for line in iter(process.stdout.readline, ''):
            print(line.rstrip())
            
            # Extract the URL when Streamlit starts
            if "Local URL:" in line:
                app_url = line.split("Local URL:")[-1].strip()
            elif "Network URL:" in line and not app_url:
                app_url = line.split("Network URL:")[-1].strip()
            
            # Open browser once we have the URL
            if app_url and "You can now view your Streamlit app" in line:
                print(f"🌐 Opening browser at: {app_url}")
                try:
                    webbrowser.open(app_url)
                except Exception as e:
                    print(f"⚠️  Could not open browser automatically: {e}")
                    print(f"   Please manually open: {app_url}")
                break
        
        # Wait for the process to complete
        process.wait()
        
    except KeyboardInterrupt:
        print("\n🛑 Stopping Streamlit server...")
        if 'process' in locals():
            process.terminate()
        print("✅ Server stopped successfully!")
        return True
        
    except Exception as e:
        print(f"❌ Error running Streamlit app: {e}")
        return False
    
    return True

def main():
    """Main entry point."""
    try:
        success = run_streamlit_app()
        if success:
            print("\n✅ App session completed successfully!")
        else:
            print("\n❌ App failed to start. Please check the error messages above.")
            sys.exit(1)
            
    except Exception as e:
        print(f"\n❌ Unexpected error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
