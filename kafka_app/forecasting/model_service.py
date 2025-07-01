import tensorflow as tf
from tensorflow.keras.models import load_model
from tensorflow.keras.layers import Layer
import numpy as np
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import os
from pathlib import Path

# === Custom PositionalEncoding Layer ===
class PositionalEncoding(Layer):
    def __init__(self, sequence_len, d_model, **kwargs):
        super().__init__(**kwargs)
        self.sequence_len = sequence_len
        self.d_model = d_model
        self.pos_encoding = self._generate_positional_encoding(sequence_len, d_model)

    def _generate_positional_encoding(self, sequence_len, d_model):
        positions = np.arange(sequence_len)[:, np.newaxis]
        dims = np.arange(d_model)[np.newaxis, :]
        angle_rates = 1 / np.power(10000, (2 * (dims // 2)) / np.float32(d_model))
        angle_rads = positions * angle_rates
        angle_rads[:, 0::2] = np.sin(angle_rads[:, 0::2])
        angle_rads[:, 1::2] = np.cos(angle_rads[:, 1::2])
        return tf.cast(angle_rads[np.newaxis, ...], dtype=tf.float32)

    def call(self, inputs):
        return inputs + self.pos_encoding[:, :inputs.shape[1], :]

    def get_config(self):
        config = super().get_config()
        config.update({
            'sequence_len': self.sequence_len,
            'd_model': self.d_model
        })
        return config

    @classmethod
    def from_config(cls, config):
        return cls(**config)

# === Forecasting Service ===
class ForecastingService:
    def __init__(self, model_path="models/1day_forecast.keras"): 
        self.input_len = 672      # 7 days × 96 points
        self.forecast_len = 96    # 1 day
        self.model_path = model_path
        self.model = None
        self._load_model()
    
    def _load_model(self):
        """Load the Keras model with custom objects"""
        try:
            # Get absolute path
            project_root = Path(__file__).parent.parent.parent
            full_model_path = project_root / self.model_path
            
            print(f"[Forecast] Loading model from: {full_model_path}")
            
            self.model = load_model(
                str(full_model_path), 
                custom_objects={'PositionalEncoding': PositionalEncoding}
            )
            print("[Forecast] Model loaded successfully!")
            
        except Exception as e:
            print(f"[Forecast] Failed to load model: {e}")
            self.model = None
    
    def add_time_features(self, df):
        """Add cyclical time features exactly like your notebook"""
        df = df.copy()
        
        # Convert timestamp to datetime if it's not already
        if not pd.api.types.is_datetime64_any_dtype(df['timestamp']):
            df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=True)

        # Extract time components
        df['hour'] = df['timestamp'].dt.hour
        df['dayofweek'] = df['timestamp'].dt.dayofweek
        
        # Create cyclical features
        df['hour_sin'] = np.sin(2 * np.pi * df['hour'] / 24)
        df['hour_cos'] = np.cos(2 * np.pi * df['hour'] / 24)
        df['day_sin'] = np.sin(2 * np.pi * df['dayofweek'] / 7)
        df['day_cos'] = np.cos(2 * np.pi * df['dayofweek'] / 7)
        
        return df
    
    def forecast_next_day(self, historical_data):
        """
        Generate forecast for next 24 hours (96 points)
        
        Args:
            historical_data: List of dicts [{'timestamp': ..., 'flow': ...}, ...]
                            Should contain exactly 672 records (7 days)
        
        Returns:
            List of forecasted flow values (96 values for next day)
        """
        if self.model is None:
            print("[Forecast] Model not loaded!")
            return None
        
        if len(historical_data) < self.input_len:
            print(f"[Forecast] Not enough data. Need {self.input_len}, got {len(historical_data)}")
            return None
        
        try:
            # Convert to DataFrame
            df = pd.DataFrame(historical_data)
            
            # Take exactly 672 most recent records
            df = df.tail(self.input_len).copy()
            
            # Add time features
            df = self.add_time_features(df)
            
            # Features used by model (same order as training)
            features = ['flow', 'hour_sin', 'hour_cos', 'day_sin', 'day_cos']
            
            # Prepare data for model
            feature_data = df[features].values
            
            # Scale data (per-window scaling like in your notebook)
            scaler = MinMaxScaler()
            input_scaled = scaler.fit_transform(feature_data)
            
            # Reshape for model: (1, 672, 5)
            X_seq = np.expand_dims(input_scaled, axis=0)
            
            print(f"[Forecast] Input shape: {X_seq.shape}")
            
            # Generate prediction
            Y_pred_scaled = self.model.predict(X_seq, verbose=0)
            
            print(f"[Forecast] Prediction shape: {Y_pred_scaled.shape}")
            
            # Denormalize predictions
            # Create dummy array for inverse transform
            dummy_features = np.zeros((self.forecast_len, 4))  # 4 time features
            pred_with_features = np.hstack([Y_pred_scaled.reshape(-1, 1), dummy_features])
            
            # Inverse transform to get actual flow values
            Y_pred_denorm = scaler.inverse_transform(pred_with_features)[:, 0]
            
            print(f"[Forecast] Generated {len(Y_pred_denorm)} predictions")
            
            return Y_pred_denorm.tolist()
            
        except Exception as e:
            print(f"[Forecast] Prediction failed: {e}")
            return None
    
    def is_model_ready(self):
        """Check if model is loaded and ready"""
        return self.model is not None

# === Create global instance ===
forecasting_service = ForecastingService()