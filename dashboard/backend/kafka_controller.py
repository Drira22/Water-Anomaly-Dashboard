import subprocess
import psutil
import os
import signal
import time
from typing import Dict, List, Optional
import threading
from pathlib import Path

class KafkaController:
    def __init__(self):
        self.producer_process = None
        self.consumer_process = None
        self.producer_logs = []
        self.consumer_logs = []
        self.max_log_lines = 100
        
        # Paths to your Kafka scripts
        self.project_root = Path(__file__).parent.parent.parent
        self.producer_script = self.project_root / "kafka_app" / "producer" / "producer_manager.py"
        self.consumer_script = self.project_root / "kafka_app" / "consumer" / "dma_consumer.py"
    
    def get_status(self) -> Dict:
        """Get current status of producer and consumer"""
        producer_status = "running" if self._is_process_running(self.producer_process) else "stopped"
        consumer_status = "running" if self._is_process_running(self.consumer_process) else "stopped"
        
        return {
            "producer": {
                "status": producer_status,
                "pid": self.producer_process.pid if self.producer_process else None,
                "uptime": self._get_process_uptime(self.producer_process) if self.producer_process else None
            },
            "consumer": {
                "status": consumer_status,
                "pid": self.consumer_process.pid if self.consumer_process else None,
                "uptime": self._get_process_uptime(self.consumer_process) if self.consumer_process else None
            }
        }
    
    def start_producer(self) -> str:
        """Start Kafka producer"""
        if self._is_process_running(self.producer_process):
            return "Producer is already running"
        
        try:
            # Set environment variables
            env = os.environ.copy()
            env['PYTHONPATH'] = str(self.project_root)
            
            # Start producer process
            self.producer_process = subprocess.Popen(
                ["python", str(self.producer_script)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                cwd=str(self.project_root),
                env=env
            )
            
            # Start log monitoring thread
            threading.Thread(
                target=self._monitor_logs, 
                args=(self.producer_process, "producer"), 
                daemon=True
            ).start()
            
            return f"Producer started with PID {self.producer_process.pid}"
            
        except Exception as e:
            return f"Failed to start producer: {str(e)}"
    
    def stop_producer(self) -> str:
        """Stop Kafka producer"""
        if not self._is_process_running(self.producer_process):
            return "Producer is not running"
        
        try:
            self.producer_process.terminate()
            self.producer_process.wait(timeout=10)
            self.producer_process = None
            return "Producer stopped successfully"
        except subprocess.TimeoutExpired:
            self.producer_process.kill()
            self.producer_process = None
            return "Producer force-killed after timeout"
        except Exception as e:
            return f"Failed to stop producer: {str(e)}"
    
    def start_consumer(self) -> str:
        """Start Kafka consumer"""
        if self._is_process_running(self.consumer_process):
            return "Consumer is already running"
        
        try:
            # Set environment variables
            env = os.environ.copy()
            env['PYTHONPATH'] = str(self.project_root)
            
            # Start consumer process
            self.consumer_process = subprocess.Popen(
                ["python", str(self.consumer_script)],
                stdout=subprocess.PIPE,
                stderr=subprocess.STDOUT,
                universal_newlines=True,
                cwd=str(self.project_root),
                env=env
            )
            
            # Start log monitoring thread
            threading.Thread(
                target=self._monitor_logs, 
                args=(self.consumer_process, "consumer"), 
                daemon=True
            ).start()
            
            return f"Consumer started with PID {self.consumer_process.pid}"
            
        except Exception as e:
            return f"Failed to start consumer: {str(e)}"
    
    def stop_consumer(self) -> str:
        """Stop Kafka consumer"""
        if not self._is_process_running(self.consumer_process):
            return "Consumer is not running"
        
        try:
            self.consumer_process.terminate()
            self.consumer_process.wait(timeout=10)
            self.consumer_process = None
            return "Consumer stopped successfully"
        except subprocess.TimeoutExpired:
            self.consumer_process.kill()
            self.consumer_process = None
            return "Consumer force-killed after timeout"
        except Exception as e:
            return f"Failed to stop consumer: {str(e)}"
    
    def get_logs(self, service: str, lines: int = 20) -> List[str]:
        """Get recent logs for producer or consumer"""
        if service == "producer":
            return self.producer_logs[-lines:] if self.producer_logs else []
        elif service == "consumer":
            return self.consumer_logs[-lines:] if self.consumer_logs else []
        else:
            return []
    
    def _is_process_running(self, process) -> bool:
        """Check if a process is still running"""
        if process is None:
            return False
        try:
            return process.poll() is None
        except:
            return False
    
    def _get_process_uptime(self, process) -> Optional[str]:
        """Get process uptime"""
        if not self._is_process_running(process):
            return None
        
        try:
            p = psutil.Process(process.pid)
            create_time = p.create_time()
            uptime_seconds = time.time() - create_time
            
            hours = int(uptime_seconds // 3600)
            minutes = int((uptime_seconds % 3600) // 60)
            seconds = int(uptime_seconds % 60)
            
            return f"{hours:02d}:{minutes:02d}:{seconds:02d}"
        except:
            return None
    
    def _monitor_logs(self, process, service_name):
        """Monitor process logs in background thread"""
        logs_list = self.producer_logs if service_name == "producer" else self.consumer_logs
        
        try:
            for line in iter(process.stdout.readline, ''):
                if line:
                    timestamp = time.strftime("%H:%M:%S")
                    log_entry = f"[{timestamp}] {line.strip()}"
                    logs_list.append(log_entry)
                    
                    # Keep only recent logs
                    if len(logs_list) > self.max_log_lines:
                        logs_list.pop(0)
                
                if process.poll() is not None:
                    break
        except Exception as e:
            logs_list.append(f"[ERROR] Log monitoring failed: {str(e)}")