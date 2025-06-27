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

@st.cache_data(ttl=5)  # Shorter cache for more dynamic updates
def fetch_regions():
    """Fetch available regions from API"""
    try:
        response = requests.get(f"{API_BASE_URL}/regions")
        if response.status_code == 200:
            return response.json()["regions"]
        return []
    except:
        return []

@st.cache_data(ttl=5)  # Shorter cache for more dynamic updates
def fetch_dmas_for_region(region):
    """Fetch DMAs for a specific region"""
    try:
        response = requests.get(f"{API_BASE_URL}/dmas/{region}")
        if response.status_code == 200:
            return response.json()["dmas"]
        return []
    except:
        return []

@st.cache_data(ttl=5)  # Shorter cache for more dynamic updates
def fetch_dma_flow_data(region, dma_id, limit=500):  # Increased limit for 7-day view
    """Fetch flow data for a specific DMA"""
    try:
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
    except:
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
    """Main dashboard tab with 7-day flow visualization"""
    st.sidebar.header("🔧 Dashboard Controls")
    
    # Get regions
    regions = fetch_regions()
    
    if not regions:
        st.error("❌ No regions found. Make sure your API is running and data is available.")
        return
    
    # Single region selection
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
    
    # Single DMA selection
    selected_dma = st.sidebar.selectbox(
        "Select DMA",
        options=[""] + dmas,
        index=0
    )
    
    if not selected_dma:
        st.info("👆 Please select a DMA to view data")
        return
    
    # Data settings
    data_limit = st.sidebar.slider("Data Points (for 7-day view)", 100, 1000, 500)
    
    # Auto-refresh settings
    auto_refresh = st.sidebar.checkbox("Auto Refresh", value=True)
    refresh_interval = st.sidebar.selectbox("Refresh Interval", [5, 10, 15, 30], index=1)
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Now"):
        # Clear cache to force fresh data
        st.cache_data.clear()
        st.rerun()
    
    # Main content
    st.header(f"📊 Region {selected_region.upper()} - DMA {selected_dma}")
    
    # Fetch data for selected DMA
    flow_data = fetch_dma_flow_data(selected_region, selected_dma, data_limit)
    
    if not flow_data:
        st.warning("No data available for this DMA")
        return
    
    # Create DataFrame and sort by timestamp
    df = pd.DataFrame(flow_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    df = df.sort_values('timestamp')
    
    # Two columns: Latest Records (smaller) and Big Chart (larger)
    col1, col2 = st.columns([1, 3])  # 1:3 ratio for more chart space
    
    with col1:
        st.subheader("📋 Latest Records")
        # Display latest 10 records
        display_df = df.head(10)[['timestamp', 'flow']].copy()
        display_df['timestamp'] = display_df['timestamp'].dt.strftime('%m-%d %H:%M')
        st.dataframe(display_df, use_container_width=True, height=300)
        
        # Latest value
        latest = df.iloc[0]  # Most recent (first after sorting DESC from API)
        st.metric(
            "Latest Flow", 
            f"{latest['flow']:.2f}",
            delta=None
        )
        
        # Statistics
        st.subheader("📈 Statistics")
        st.metric("Average", f"{df['flow'].mean():.2f}")
        st.metric("Max", f"{df['flow'].max():.2f}")
        st.metric("Min", f"{df['flow'].min():.2f}")
        
        # Data info
        st.info(f"📊 **{len(df)}** data points\n\n📅 From: {df['timestamp'].min().strftime('%Y-%m-%d %H:%M')}\n\n🕐 To: {df['timestamp'].max().strftime('%Y-%m-%d %H:%M')}")
    
    with col2:
        st.subheader("📈 7-Day Flow Visualization")
        
        # Create the big beautiful plot like your CSV tool
        fig = go.Figure()
        
        # Main flow line with better styling
        fig.add_trace(go.Scatter(
            x=df['timestamp'],
            y=df['flow'],
            mode='lines',
            line=dict(color='#1f77b4', width=2),  # Nice blue color
            name='Flow Rate',
            showlegend=False,
            hovertemplate='<b>Time:</b> %{x}<br><b>Flow:</b> %{y:.2f}<extra></extra>'
        ))
        
        # Add markers for recent data points (last 20 points)
        recent_df = df.head(20)
        fig.add_trace(go.Scatter(
            x=recent_df['timestamp'],
            y=recent_df['flow'],
            mode='markers',
            marker=dict(
                color='#ff7f0e',  # Orange for recent points
                size=6,
                line=dict(width=1, color='white')
            ),
            name='Recent Data',
            showlegend=True,
            hovertemplate='<b>Recent:</b> %{x}<br><b>Flow:</b> %{y:.2f}<extra></extra>'
        ))
        
        # Beautiful layout like your CSV tool
        fig.update_layout(
            title=f"Real-time Flow Data - DMA {selected_dma}",
            xaxis_title="Time",
            yaxis_title="Flow Rate",
            height=600,  # Big height for better visibility
            plot_bgcolor='rgba(240,240,240,0.8)',  # Light background
            paper_bgcolor='white',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            ),
            xaxis=dict(
                tickformat='%m-%d\n%H:%M',  # Date and time format
                dtick=3600000 * 6,  # 6-hour intervals
                gridcolor='rgba(128,128,128,0.3)',
                showgrid=True
            ),
            yaxis=dict(
                gridcolor='rgba(128,128,128,0.3)',
                showgrid=True
            ),
            hovermode='x unified'
        )
        
        # Display the big beautiful chart
        st.plotly_chart(fig, use_container_width=True, key=f"flow_chart_{selected_region}_{selected_dma}_{int(time.time())}")
        
        # Time range info
        time_span = df['timestamp'].max() - df['timestamp'].min()
        st.info(f"📊 **Data Span:** {time_span.days} days, {time_span.seconds//3600} hours")
    
    # Auto-refresh logic with cache clearing
    if auto_refresh:
        # Create a placeholder for countdown
        countdown_placeholder = st.empty()
        
        # Countdown display
        for i in range(refresh_interval, 0, -1):
            countdown_placeholder.info(f"🔄 Auto-refresh in {i} seconds...")
            time.sleep(1)
        
        countdown_placeholder.empty()
        
        # Clear cache and refresh
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
    """Logs viewing tab"""
    st.header("📋 Service Logs (Auto-refresh)")
    
    # Service selection
    service = st.selectbox("Select Service", ["producer", "consumer"])
    
    # Fetch and display logs
    logs = fetch_kafka_logs(service)
    
    if logs:
        st.subheader(f"📝 {service.title()} Logs")
        log_text = "\n".join(logs)
        st.code(log_text, language="text")
    else:
        st.info(f"No logs available for {service}")
    
    # Auto-refresh every 3 seconds
    time.sleep(3)
    st.rerun()

if __name__ == "__main__":
    main()