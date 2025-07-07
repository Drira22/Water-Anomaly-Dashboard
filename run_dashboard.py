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

def run_nextjs():
    """Run Next.js frontend"""
    time.sleep(3)  # Wait for FastAPI to start
    
    # Change to frontend directory and run Next.js
    frontend_dir = os.path.join(project_root, "dashboard", "frontend")
    os.chdir(frontend_dir)
    
    subprocess.run([
        "npm", "run", "dev"
    ])

def run_streamlit():
    """Run Streamlit frontend (backup/legacy)"""
    time.sleep(3)  # Wait for FastAPI to start
    subprocess.run([
        "streamlit", 
        "run", 
        "dashboard/frontend_backup/app.py",
        "--server.port", "8501"
    ])

if __name__ == "__main__":
    print("🚀 Starting Water Flow Monitoring Dashboard...")
    
    # Ask user which frontend to run
    print("\nChoose frontend:")
    print("1. Next.js Dashboard (Modern - Recommended)")
    print("2. Streamlit Dashboard (Legacy)")
    
    choice = input("Enter choice (1 or 2): ").strip()
    
    # Start FastAPI in background
    api_thread = threading.Thread(target=run_fastapi, daemon=True)
    api_thread.start()
    
    print("⚡ FastAPI starting on http://localhost:8000")
    
    if choice == "2":
        print("🎨 Streamlit will start on http://localhost:8501")
        run_streamlit()
    else:
        print("🎨 Next.js will start on http://localhost:3000")
        run_nextjs()