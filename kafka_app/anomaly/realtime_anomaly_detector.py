import pandas as pd
from typing import Dict, Optional, List
from datetime import datetime, timedelta
import logging
import psycopg2

logger = logging.getLogger(__name__)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'yorkshire',
    'user': 'yorkshire',
    'password': 'yorkshire'
}

class RealtimeAnomalyDetector:
    def __init__(self, threshold: float = 5.0):
        self.threshold = threshold  # L/min
        self.anomaly_count = {}  # {region_dma_id: count}
        
    def get_forecast_for_timestamp(self, region: str, dma_id: str, timestamp: str) -> Optional[float]:
        """
        Get forecast value for a specific timestamp from database
        """
        try:
            # Parse timestamp and get the date
            dt = pd.to_datetime(timestamp)
            forecast_date = dt.date()
            
            # Round timestamp to nearest 15 minutes for matching
            rounded_dt = dt.replace(second=0, microsecond=0)
            if rounded_dt.minute % 15 != 0:
                rounded_dt = rounded_dt.replace(minute=(rounded_dt.minute // 15) * 15)
            
            # Query database for forecast
            conn = psycopg2.connect(**DB_CONFIG)
            cursor = conn.cursor()
            
            table_name = f"flow_{region.lower()}_forecast"
            query = f"""
            SELECT forecasted_flow 
            FROM {table_name} 
            WHERE dma_id = %s 
            AND forecast_date = %s 
            AND forecast_timestamp = %s
            LIMIT 1
            """
            
            cursor.execute(query, (dma_id, forecast_date, rounded_dt))
            result = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if result:
                return float(result[0])
            else:
                logger.warning(f"⚠️ No forecast found for {region}_{dma_id} at {rounded_dt} on {forecast_date}")
                return None
                
        except Exception as e:
            logger.error(f"❌ Error getting forecast from database: {e}")
            return None
    
    def check_anomaly(self, region: str, dma_id: str, timestamp: str, actual_flow: float) -> Optional[Dict]:
        """
        Check if current data point is anomalous compared to forecast from database
        Returns anomaly info if detected, None otherwise
        """
        key = f"{region}_{dma_id}"
        
        # Get forecast from database
        forecast_flow = self.get_forecast_for_timestamp(region, dma_id, timestamp)
        
        if forecast_flow is None:
            logger.warning(f"⚠️ No forecast available for {key} at {timestamp}")
            return None
        
        # Calculate absolute error
        error = abs(actual_flow - forecast_flow)
        
        # Check if anomalous
        is_anomaly = error > self.threshold
        
        if is_anomaly:
            # Increment anomaly count
            self.anomaly_count[key] = self.anomaly_count.get(key, 0) + 1
            
            anomaly_info = {
                'region': region,
                'dma_id': dma_id,
                'timestamp': timestamp,
                'actual_flow': float(actual_flow),
                'forecast_flow': float(forecast_flow),
                'error': float(error),
                'threshold': self.threshold,
                'is_anomaly': True,
                'anomaly_count_today': self.anomaly_count[key],
                'detection_time': datetime.now().isoformat()
            }
            
            logger.warning(f"🚨 REAL-TIME ANOMALY DETECTED: {key} at {timestamp}")
            logger.warning(f"   Actual: {actual_flow:.2f} L/min, Forecast: {forecast_flow:.2f} L/min, Error: {error:.2f} L/min")
            
            return anomaly_info
        else:
            logger.debug(f"✅ Normal point: {key} - Error: {error:.2f} L/min (< {self.threshold})")
            return None
    
    def reset_daily_count(self, region: str, dma_id: str):
        """Reset anomaly count for new day"""
        key = f"{region}_{dma_id}"
        self.anomaly_count[key] = 0
        logger.info(f"🔄 Reset anomaly count for {key}")
    
    def get_stats(self, region: str, dma_id: str) -> Dict:
        """Get anomaly statistics for a DMA"""
        key = f"{region}_{dma_id}"
        
        # Check if forecast exists for today
        today = datetime.now().date()
        forecast_exists = self.get_forecast_for_timestamp(region, dma_id, datetime.now().isoformat()) is not None
        
        return {
            'anomaly_count_today': self.anomaly_count.get(key, 0),
            'has_forecast': forecast_exists,
            'threshold': self.threshold
        }

# Global real-time anomaly detector instance
realtime_detector = RealtimeAnomalyDetector(threshold=5.0)