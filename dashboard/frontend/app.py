import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
import json

# Page configuration
st.set_page_config(
    page_title="Water Distribution Monitoring",
    page_icon="💧",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Configuration
API_BASE_URL = "http://localhost:8000"

@st.cache_data(ttl=1)  # Faster cache for real-time data
def fetch_regions():
    """Fetch available regions from API"""
    try:
        response = requests.get(f"{API_BASE_URL}/regions")
        if response.status_code == 200:
            return response.json()["regions"]
        return []
    except:
        return []

@st.cache_data(ttl=1)
def fetch_dmas_for_region(region):
    """Fetch DMAs for a specific region"""
    try:
        response = requests.get(f"{API_BASE_URL}/dmas/{region}")
        if response.status_code == 200:
            return response.json()["dmas"]
        return []
    except:
        return []

@st.cache_data(ttl=1)
def fetch_forecast_data(region, dma_id, forecast_date=None):
    """Fetch forecast data for a specific DMA"""
    try:
        url = f"{API_BASE_URL}/forecast-data/{region}/{dma_id}"
        if forecast_date:
            url += f"?forecast_date={forecast_date}"
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()["data"], response.json().get("forecast_date")
        return [], None
    except:
        return [], None

def fetch_actual_flow_data(region, dma_id, days=8):
    """Fetch actual flow data (last N days)"""
    try:
        # Get data for last N days
        limit = days * 96  # 96 points per day
        response = requests.get(f"{API_BASE_URL}/flow-data/{region}/{dma_id}?limit={limit}")
        if response.status_code == 200:
            return response.json()["data"]
        return []
    except:
        return []

def fetch_kafka_status():
    """Fetch Kafka services status"""
    try:
        response = requests.get(f"{API_BASE_URL}/kafka/status")
        if response.status_code == 200:
            return response.json()["status"]
        return None
    except:
        return None

def control_kafka_service(service, action):
    """Start or stop Kafka service"""
    try:
        response = requests.post(f"{API_BASE_URL}/kafka/{service}/{action}")
        if response.status_code == 200:
            return response.json()["message"]
        return f"Failed to {action} {service}"
    except Exception as e:
        return f"Error: {str(e)}"

def fetch_kafka_logs(service):
    """Fetch logs for Kafka service"""
    try:
        response = requests.get(f"{API_BASE_URL}/kafka/logs/{service}?lines=20")
        if response.status_code == 200:
            return response.json()["logs"]
        return []
    except Exception as e:
        st.error(f"Error fetching logs: {str(e)}")
        return []

def main():
    # Header
    st.title("💧 Water Distribution Monitoring Dashboard")
    st.markdown("Real-time flow data visualization for District Metered Areas (DMA)")
    
    # Create tabs
    tab1, tab2, tab3 = st.tabs(["📊 Dashboard", "⚙️ Kafka Control", "📋 Logs"])
    
    with tab1:
        dashboard_tab()
    with tab2:
        kafka_control_tab()
    with tab3:
        logs_tab()

def dashboard_tab():
    """Simplified dashboard with 2-line visualization: Actual + Forecast"""
    st.sidebar.header("🔧 Dashboard Controls")
    
    # Get regions (now excludes forecast tables)
    regions = fetch_regions()
    
    if not regions:
        st.error("❌ No regions found. Make sure your API is running and data is available.")
        return
    
    # Region selection
    selected_region = st.sidebar.selectbox(
        "Select Region",
        options=[""] + regions,
        index=0
    )
    
    if not selected_region:
        st.info("👆 Please select a region to start monitoring")
        return
    
    # Get DMAs for selected region
    dmas = fetch_dmas_for_region(selected_region)
    
    if not dmas:
        st.error(f"❌ No DMAs found for region {selected_region}")
        return
    
    # DMA selection
    selected_dma = st.sidebar.selectbox(
        "Select DMA",
        options=[""] + dmas,
        index=0
    )
    
    if not selected_dma:
        st.info("👆 Please select a DMA to view data")
        return
    
    # Real-time settings
    auto_refresh = st.sidebar.checkbox("Auto Refresh", value=True)
    refresh_interval = st.sidebar.selectbox("Refresh Interval", [1, 2, 5, 10], index=0)
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Now"):
        st.cache_data.clear()
        st.rerun()
    
    # Main content
    st.header(f"📊 Region {selected_region.upper()} - DMA {selected_dma}")
    
    # Fetch actual flow data (last 8 days max)
    actual_data = fetch_actual_flow_data(selected_region, selected_dma, days=8)
    
    # Fetch latest forecast (if available)
    forecast_data, forecast_date = fetch_forecast_data(selected_region, selected_dma)
    
    if not actual_data:
        st.warning("No flow data available for this DMA")
        return
    
    # Create DataFrame for actual data
    actual_df = pd.DataFrame(actual_data)
    actual_df['timestamp'] = pd.to_datetime(actual_df['timestamp'], dayfirst=True)
    actual_df = actual_df.sort_values('timestamp', ascending=True).reset_index(drop=True)
    
    # SIMPLIFIED 2-LINE VISUALIZATION
    st.subheader("🎯 Real-time Flow with Forecast")
    
    # Create the plot
    fig = go.Figure()
    
    # 1. BLACK LINE: All actual flow data (past + real-time)
    fig.add_trace(go.Scatter(
        x=actual_df['timestamp'],
        y=actual_df['flow'],
        mode='lines',
        line=dict(color='black', width=2),
        name='Actual Flow',
        showlegend=True,
        hovertemplate='<b>Actual:</b> %{x}<br><b>Flow:</b> %{y:.2f}<extra></extra>'
    ))
    
    # 2. RED LINE: Forecast data (if available)
    if forecast_data:
        forecast_df = pd.DataFrame(forecast_data)
        forecast_df['timestamp'] = pd.to_datetime(forecast_df['timestamp'], dayfirst=True)
        forecast_df = forecast_df.sort_values('timestamp', ascending=True)
        
        fig.add_trace(go.Scatter(
            x=forecast_df['timestamp'],
            y=forecast_df['flow'],
            mode='lines',
            line=dict(color='red', width=3),
            name=f'Forecast ({forecast_date})',
            showlegend=True,
            hovertemplate='<b>Forecast:</b> %{x}<br><b>Flow:</b> %{y:.2f}<extra></extra>'
        ))
        
        st.info(f"📅 Forecast available for: {forecast_date}")
    else:
        st.info("🔮 No forecast available yet (need 7+ days of data)")
    
    # Layout
    fig.update_layout(
        title=f"Flow Monitoring - DMA {selected_dma} ({selected_region.upper()})",
        xaxis_title="Date & Time",
        yaxis_title="Flow Rate",
        height=700,
        plot_bgcolor='rgba(240,240,240,0.8)',
        paper_bgcolor='white',
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        xaxis=dict(
            tickformat='%d %b<br>%H:%M',
            nticks=12,
            gridcolor='rgba(128,128,128,0.3)',
            showgrid=True,
            tickangle=0
        ),
        yaxis=dict(
            gridcolor='rgba(128,128,128,0.3)',
            showgrid=True
        ),
        hovermode='x unified'
    )
    
    # Display the chart
    chart_key = f"flow_chart_{selected_region}_{selected_dma}_{int(time.time())}"
    st.plotly_chart(fig, use_container_width=True, key=chart_key)
    
    # Data summary
    col1, col2 = st.columns(2)
    
    with col1:
        st.metric("Actual Data Points", len(actual_df))
        st.metric("Time Range", f"{(actual_df['timestamp'].max() - actual_df['timestamp'].min()).days} days")
    
    with col2:
        if forecast_data:
            st.metric("Forecast Points", len(forecast_data))
            st.metric("Forecast Date", forecast_date)
        else:
            st.metric("Forecast Points", "Not available")
            st.metric("Status", "Waiting for 7+ days")
    
    # Auto-refresh logic
    if auto_refresh:
        countdown_placeholder = st.empty()
        for i in range(refresh_interval, 0, -1):
            countdown_placeholder.info(f"🔄 Auto-refresh in {i} seconds...")
            time.sleep(1)
        countdown_placeholder.empty()
        st.cache_data.clear()
        st.rerun()

def kafka_control_tab():
    """Kafka control tab"""
    st.header("⚙️ Kafka Services Control")
    
    # Fetch Kafka status
    kafka_status = fetch_kafka_status()
    
    if not kafka_status:
        st.error("❌ Unable to fetch Kafka status. Make sure the API is running.")
        return
    
    # Status display
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📤 Producer Status")
        producer_status = kafka_status["producer"]
        
        if producer_status["status"] == "running":
            st.success(f"✅ Running (PID: {producer_status['pid']})")
            if producer_status["uptime"]:
                st.info(f"⏱️ Uptime: {producer_status['uptime']}")
        else:
            st.error("❌ Stopped")
        
        # Producer controls
        col1_1, col1_2 = st.columns(2)
        with col1_1:
            if st.button("▶️ Start Producer", key="start_producer"):
                with st.spinner("Starting producer..."):
                    result = control_kafka_service("producer", "start")
                    st.success(result)
                    time.sleep(2)
                    st.rerun()
        
        with col1_2:
            if st.button("⏹️ Stop Producer", key="stop_producer"):
                with st.spinner("Stopping producer..."):
                    result = control_kafka_service("producer", "stop")
                    st.success(result)
                    time.sleep(2)
                    st.rerun()
    
    with col2:
        st.subheader("📥 Consumer Status")
        consumer_status = kafka_status["consumer"]
        
        if consumer_status["status"] == "running":
            st.success(f"✅ Running (PID: {consumer_status['pid']})")
            if consumer_status["uptime"]:
                st.info(f"⏱️ Uptime: {consumer_status['uptime']}")
        else:
            st.error("❌ Stopped")
        
        # Consumer controls
        col2_1, col2_2 = st.columns(2)
        with col2_1:
            if st.button("▶️ Start Consumer", key="start_consumer"):
                with st.spinner("Starting consumer..."):
                    result = control_kafka_service("consumer", "start")
                    st.success(result)
                    time.sleep(2)
                    st.rerun()
        
        with col2_2:
            if st.button("⏹️ Stop Consumer", key="stop_consumer"):
                with st.spinner("Stopping consumer..."):
                    result = control_kafka_service("consumer", "stop")
                    st.success(result)
                    time.sleep(2)
                    st.rerun()
    
    # Quick actions
    st.subheader("🚀 Quick Actions")
    col3, col4, col5 = st.columns(3)
    
    with col3:
        if st.button("🔄 Restart Both Services"):
            with st.spinner("Restarting services..."):
                control_kafka_service("producer", "stop")
                control_kafka_service("consumer", "stop")
                time.sleep(3)
                control_kafka_service("producer", "start")
                time.sleep(2)
                control_kafka_service("consumer", "start")
                st.success("Services restarted!")
                time.sleep(2)
                st.rerun()
    
    with col4:
        if st.button("⏹️ Stop All Services"):
            with st.spinner("Stopping all services..."):
                control_kafka_service("producer", "stop")
                control_kafka_service("consumer", "stop")
                st.success("All services stopped!")
                time.sleep(2)
                st.rerun()
    
    with col5:
        if st.button("▶️ Start All Services"):
            with st.spinner("Starting all services..."):
                control_kafka_service("producer", "start")
                time.sleep(2)
                control_kafka_service("consumer", "start")
                st.success("All services started!")
                time.sleep(2)
                st.rerun()

def logs_tab():
    """Logs viewing tab with proper auto-refresh"""
    st.header("📋 Service Logs (Auto-refresh every 2 seconds)")
    
    # Service selection
    service = st.selectbox("Select Service", ["producer", "consumer"], key="log_service_select")
    
    # Create containers for logs
    logs_container = st.container()
    
    # Fetch and display logs
    try:
        logs = fetch_kafka_logs(service)
        
        with logs_container:
            if logs:
                st.subheader(f"📝 {service.title()} Logs (Last 20 lines)")
                
                # Display logs in a scrollable text area
                log_text = "\n".join(logs)
                st.text_area(
                    f"{service.title()} Output:",
                    value=log_text,
                    height=400,
                    key=f"logs_{service}_{int(time.time())}"
                )
                
                # Show log count
                st.info(f"📊 Showing {len(logs)} log lines")
            else:
                st.warning(f"No logs available for {service}")
                st.info("Make sure the service is running and generating logs.")
    
    except Exception as e:
        st.error(f"Error fetching logs: {str(e)}")
    
    # Auto-refresh countdown
    refresh_placeholder = st.empty()
    for i in range(2, 0, -1):
        refresh_placeholder.info(f"🔄 Refreshing logs in {i} seconds...")
        time.sleep(1)
    
    refresh_placeholder.empty()
    st.rerun()

if __name__ == "__main__":
    main()