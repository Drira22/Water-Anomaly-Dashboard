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
def fetch_dma_flow_data(region, dma_id):
    """Fetch exactly 7 days of flow data (672 points = 96 points/day * 7 days)"""
    try:
        # Fixed limit for exactly 7 days of data
        limit = 672  # 96 points per day * 7 days
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
    """Main dashboard tab with real-time 7-day flow visualization"""
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
    
    # Real-time settings
    auto_refresh = st.sidebar.checkbox("Auto Refresh", value=True)
    refresh_interval = st.sidebar.selectbox("Refresh Interval", [1, 2, 5, 10], index=0)
    
    # Manual refresh button
    if st.sidebar.button("🔄 Refresh Now"):
        st.cache_data.clear()
        st.rerun()
    
    # Main content
    st.header(f"📊 Region {selected_region.upper()} - DMA {selected_dma}")
    
    # Fetch exactly 7 days of data
    flow_data = fetch_dma_flow_data(selected_region, selected_dma)
    
    if not flow_data:
        st.warning("No data available for this DMA")
        return
    
    # Create DataFrame and PROPERLY sort by timestamp
    df = pd.DataFrame(flow_data)
    df['timestamp'] = pd.to_datetime(df['timestamp'], dayfirst=True)
    
    # CRITICAL FIX: Sort by timestamp ASCENDING (oldest first) for proper line plotting
    df = df.sort_values('timestamp', ascending=True).reset_index(drop=True)
    
    # Remove any duplicate timestamps to avoid weird lines
    df = df.drop_duplicates(subset=['timestamp']).reset_index(drop=True)
    
    # FIRST ROW: Latest Records + Statistics (Horizontal Layout)
    st.subheader("📋 Latest Data & Statistics")
    
    # Create 4 columns for horizontal layout
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.markdown("**📊 Latest Records**")
        # Display latest 5 records (from the end of sorted data)
        display_df = df.tail(5)[['timestamp', 'flow']].copy()
        display_df['timestamp'] = display_df['timestamp'].dt.strftime('%d-%m %H:%M')
        st.dataframe(display_df, use_container_width=True, height=200)
    
    with col2:
        st.markdown("**🕐 Current Status**")
        latest = df.iloc[-1]  # Most recent (last in sorted data)
        st.metric("Latest Flow", f"{latest['flow']:.2f}")
        st.metric("Data Points", f"{len(df)}")
    
    with col3:
        st.markdown("**📈 Statistics**")
        st.metric("Average", f"{df['flow'].mean():.2f}")
        st.metric("Max", f"{df['flow'].max():.2f}")
    
    with col4:
        st.markdown("**📅 Time Range**")
        time_span = df['timestamp'].max() - df['timestamp'].min()
        st.metric("Days", f"{time_span.days}")
        st.metric("Hours", f"{time_span.seconds//3600}")
        st.info(f"From: {df['timestamp'].min().strftime('%d-%m %H:%M')}")
        st.info(f"To: {df['timestamp'].max().strftime('%d-%m %H:%M')}")
    
    # Add some spacing
    st.markdown("---")
    
    # SECOND ROW: Full-Width 7-Day Plot
    st.subheader("📈 7-Day Real-Time Flow Visualization")
    
    # Create the FIXED plot
    fig = go.Figure()
    
    # Main flow line - PROPERLY SORTED DATA
    fig.add_trace(go.Scatter(
        x=df['timestamp'],
        y=df['flow'],
        mode='lines+markers',
        line=dict(color='#1f77b4', width=2),
        marker=dict(size=3, color='#1f77b4'),
        name='Flow Rate',
        showlegend=True,
        hovertemplate='<b>Time:</b> %{x}<br><b>Flow:</b> %{y:.2f}<extra></extra>'
    ))
    
    # Highlight recent points (last 10% of data)
    recent_count = max(10, len(df) // 10)
    recent_df = df.tail(recent_count)
    fig.add_trace(go.Scatter(
        x=recent_df['timestamp'],
        y=recent_df['flow'],
        mode='markers',
        marker=dict(
            color='#ff7f0e',
            size=6,
            line=dict(width=1, color='white')
        ),
        name='Recent Data',
        showlegend=True,
        hovertemplate='<b>Recent:</b> %{x}<br><b>Flow:</b> %{y:.2f}<extra></extra>'
    ))
    
    # FIXED layout with CORRECTED DATE FORMAT
    fig.update_layout(
        title=f"Real-time Flow Data - DMA {selected_dma} (7 Days)",
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
            # FIXED: Use DD-MM format with proper day-first parsing
            tickformat='%d %b<br>%H:%M',
            nticks=10,  # Maximum 10 labels on x-axis
            gridcolor='rgba(128,128,128,0.3)',
            showgrid=True,
            tickangle=0,
            # Ensure proper range
            range=[df['timestamp'].min(), df['timestamp'].max()]
        ),
        yaxis=dict(
            gridcolor='rgba(128,128,128,0.3)',
            showgrid=True
        ),
        hovermode='x unified'
    )
    
    # Display the FIXED full-width chart
    chart_key = f"flow_chart_{selected_region}_{selected_dma}_{int(time.time())}"
    st.plotly_chart(fig, use_container_width=True, key=chart_key)
    
    # Show data info
    st.info(f"📊 Showing {len(df)} data points (7 days) from {df['timestamp'].min().strftime('%Y-%m-%d %H:%M')} to {df['timestamp'].max().strftime('%Y-%m-%d %H:%M')}")
    
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
    """FIXED Logs viewing tab with proper auto-refresh"""
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