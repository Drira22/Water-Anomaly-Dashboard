import subprocess
import time
import threading
import webbrowser
import sys
import os

# Add the project root to Python path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, project_root)

def run_fastapi():
    """Run FastAPI backend"""
    subprocess.run([
        "uvicorn", 
        "dashboard.backend.main:app", 
        "--host", "0.0.0.0", 
        "--port", "8000",
        "--reload"
    ])

def run_streamlit():
    """Run Streamlit frontend"""
    time.sleep(3)  # Wait for FastAPI to start
    subprocess.run([
        "streamlit", 
        "run", 
        "dashboard/frontend/app.py",
        "--server.port", "8501"
    ])

if __name__ == "__main__":
    print("🚀 Starting Water Flow Monitoring Dashboard...")
    
    # Start FastAPI in background
    api_thread = threading.Thread(target=run_fastapi, daemon=True)
    api_thread.start()
    
    print("⚡ FastAPI starting on http://localhost:8000")
    print("🎨 Streamlit will start on http://localhost:8501")
    
    # Start Streamlit
    run_streamlit()