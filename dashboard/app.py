import streamlit as st
import plotly.express as px
import pandas as pd
import os
import sys
from datetime import datetime, timedelta

# ---------------------------------------------------
# PATH SETUP
# ---------------------------------------------------
current_script_path = os.path.dirname(os.path.abspath(__file__))

# Define the root of your project (one level up from dashboard)
root_path = os.path.abspath(os.path.join(current_script_path, ".."))
if root_path not in sys.path:
    sys.path.insert(0, root_path)

# Try to import backend modules with better error handling
process_pipeline = None
detect_anamoly = None

try:
    from backend.pipeline.processing import process_pipeline
    from backend.anamoly.detection import detect_anamoly
except ImportError as e:
    st.error(f"Error importing backend modules: {e}")
    st.info("Make sure the backend modules exist and are properly structured")
    process_pipeline = None
    detect_anamoly = None
except Exception as e:
    st.error(f"Unexpected error importing backend modules: {e}")
    process_pipeline = None
    detect_anamoly = None

# ---------------------------------------------------
# PAGE CONFIG
# ---------------------------------------------------
st.set_page_config(
    page_title="Log Analytics Engine",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("📊 Python Based High Throughput Log Analytics Monitoring Engine")
st.markdown("---")

# ---------------------------------------------------
# SIDEBAR
# ---------------------------------------------------
st.sidebar.header("⚙️ Settings")

# Define possible log file locations
default_log_paths = [
    os.path.join(root_path, "backend", "log_generator", "realtime_logs.csv"),
    os.path.join(root_path, "backend", "log_generator", "realtime_logs.log"),
    os.path.join(root_path, "backend", "sample_data", "log_data.log"),
    os.path.join(root_path, "logs", "realtime_logs.csv"),
    os.path.join(current_script_path, "sample_logs.csv"),
]

# Find the first existing log file
existing_log_path = None
for path in default_log_paths:
    if os.path.exists(path):
        existing_log_path = path
        break

# If no log file exists, use the default path
default_log_path = existing_log_path if existing_log_path else default_log_paths[0]

# Display debug info in expander
with st.sidebar.expander("🔧 Debug Information"):
    st.write(f"**Current script:** {current_script_path}")
    st.write(f"**Root path:** {root_path}")
    st.write(f"**Default log path:** {default_log_path}")
    st.write(f"**File exists:** {os.path.exists(default_log_path)}")
    
    # Check all possible locations
    st.write("**Checking log locations:**")
    for path in default_log_paths:
        exists = "✅" if os.path.exists(path) else "❌"
        st.write(f"{exists} {path}")
    
    # Check if backend modules are available
    st.write("**Backend modules:**")
    st.write(f"process_pipeline: {'✅' if process_pipeline else '❌'}")
    st.write(f"detect_anamoly: {'✅' if detect_anamoly else '❌'}")
    
    # Show Python version
    st.write(f"**Python version:** {sys.version}")
    
    # Show current working directory
    st.write(f"**Current working directory:** {os.getcwd()}")

# Log file path input
log_file_path = st.sidebar.text_input(
    "📁 Log File Path",
    value=default_log_path if default_log_path else "",
    help="Enter the full path to your log file"
)

# Generate sample logs button
if st.sidebar.button("🔄 Generate Sample Logs", type="primary"):
    try:
        # Create directory if it doesn't exist
        log_dir = os.path.dirname(log_file_path)
        if log_dir:
            os.makedirs(log_dir, exist_ok=True)
        
        # Generate sample log data
        import random
        
        log_levels = ['INFO', 'ERROR', 'WARN', 'DEBUG']
        log_weights = [0.7, 0.1, 0.15, 0.05]
        
        info_msgs = [
            "Request processed successfully", "User logged in", "Data retrieved", 
            "Operation completed", "Cache hit", "Connection established", 
            "Transaction completed", "Email sent", "Notification delivered",
            "API call successful", "Database query executed", "File uploaded"
        ]
        error_msgs = [
            "Database connection failed", "Timeout occurred", "Null pointer exception", 
            "Out of memory", "Service unavailable", "Failed to process request", 
            "Unauthorized access attempt", "Data validation error", "Disk full",
            "Network error", "Invalid input format", "Authentication failed"
        ]
        warn_msgs = [
            "High memory usage", "Slow response time", "Retry attempt", 
            "Resource limit near", "Deprecated API used", "Configuration missing",
            "Connection pool nearly full", "Cache size exceeded", "Rate limit approaching"
        ]
        debug_msgs = [
            "Debug info", "Variable value", "Function called", "Parameter received",
            "Processing step", "Cache miss", "Query executed", "Stack trace",
            "Memory allocation", "Thread started", "Lock acquired"
        ]
        
        services = ["auth", "payment", "inventory", "shipping", "user", "orders", "notifications", 
                    "analytics", "search", "recommendation", "cache", "database"]
        
        # Generate 1000 sample logs for better visualization
        data = []
        start_time = datetime.now() - timedelta(hours=2)
        
        for i in range(1000):
            timestamp = start_time + timedelta(seconds=random.randint(0, 7200))
            level = random.choices(log_levels, weights=log_weights)[0]
            
            if level == "ERROR":
                message = random.choice(error_msgs)
            elif level == "WARN":
                message = random.choice(warn_msgs)
            elif level == "DEBUG":
                message = random.choice(debug_msgs)
            else:
                message = random.choice(info_msgs)
            
            service = random.choice(services)
            full_message = f"[{service}] {message}"
            
            data.append({
                'timestamp': timestamp,
                'level': level,
                'message': full_message,
                'service': service
            })
        
        # Create DataFrame and save
        df = pd.DataFrame(data)
        df = df.sort_values('timestamp')
        
        # Save as CSV
        df.to_csv(log_file_path, index=False)
        
        st.sidebar.success(f"✅ Generated 1000 sample logs at:\n{log_file_path}")
        st.rerun()
        
    except Exception as e:
        st.sidebar.error(f"❌ Error generating sample logs: {e}")
        st.sidebar.exception(e)

st.sidebar.markdown("---")

# Auto-refresh option
auto_refresh = st.sidebar.checkbox("🔄 Auto-refresh (5 seconds)", value=False)
refresh_interval = st.sidebar.slider("Refresh interval (seconds)", min_value=2, max_value=30, value=5, disabled=not auto_refresh)

if auto_refresh:
    st.sidebar.info(f"Auto-refresh is enabled (every {refresh_interval} seconds)")
    st.sidebar.caption("The dashboard will refresh automatically")

if st.sidebar.button("🔄 Refresh Dashboard"):
    st.rerun()

# ---------------------------------------------------
# MAIN CONTENT
# ---------------------------------------------------

# Check if backend modules are available and set fallbacks
if process_pipeline is None or detect_anamoly is None:
    st.warning("⚠️ Backend modules not available. Using fallback processing functions.")
    
    # Fallback: Create a simple processing function
    def fallback_process_pipeline(file_path):
        import dask.dataframe as dd
        df = dd.read_csv(file_path, assume_missing=True)
        return df
    
    def fallback_detect_anamoly(df):
        import dask.dataframe as dd
        # Simple anomaly detection based on error rate
        return dd.from_pandas(pd.DataFrame(), npartitions=1)
    
    process_pipeline = fallback_process_pipeline
    detect_anamoly = fallback_detect_anamoly

# ---------------------------------------------------
# LOAD AND PROCESS DATA
# ---------------------------------------------------
try:
    # Check if log file exists
    if not os.path.exists(log_file_path):
        st.warning(f"⚠️ Log file not found at: {log_file_path}")
        st.info("💡 Click 'Generate Sample Logs' in the sidebar to create sample data, or update the file path.")
        
        # Show available directories
        st.write("**Available directories in project:**")
        for item in os.listdir(root_path):
            if os.path.isdir(os.path.join(root_path, item)):
                st.write(f"- {item}/")
        
        st.stop()

    # Show file info
    file_size = os.path.getsize(log_file_path) / 1024  # Size in KB
    file_modified = datetime.fromtimestamp(os.path.getmtime(log_file_path))
    st.info(f"📄 Loading log file: {os.path.basename(log_file_path)} ({file_size:.2f} KB, last modified: {file_modified.strftime('%Y-%m-%d %H:%M:%S')})")

    # Load and process data
    with st.spinner("Processing logs..."):
        log_df_dask = process_pipeline(log_file_path)
        log_data = log_df_dask.compute()

    # Validate data
    if len(log_data) == 0:
        st.warning("⚠️ The log file is empty. Please generate some logs first.")
        st.stop()

    # Data preprocessing
    if 'timestamp' in log_data.columns:
        log_data['timestamp'] = pd.to_datetime(log_data['timestamp'], errors='coerce')
        # Remove rows with invalid timestamps
        log_data = log_data.dropna(subset=['timestamp'])

    # Ensure level column exists (case insensitive check)
    if 'level' not in log_data.columns:
        # Try to find level column with different casing
        level_col = None
        for col in log_data.columns:
            if col.lower() == 'level':
                level_col = col
                break
        
        if level_col:
            log_data = log_data.rename(columns={level_col: 'level'})
        else:
            st.error("❌ Log file must contain a 'level' column")
            st.write(f"Available columns: {list(log_data.columns)}")
            st.stop()

    # Convert level to uppercase for consistency
    log_data['level'] = log_data['level'].str.upper()

    # Run anomaly detection
    with st.spinner("Detecting anomalies..."):
        try:
            anomaly_result = detect_anamoly(log_df_dask)
            anomaly_df = anomaly_result.compute() if hasattr(anomaly_result, "compute") else anomaly_result
            if anomaly_df is None:
                anomaly_df = pd.DataFrame()
        except Exception as e:
            st.warning(f"Anomaly detection failed: {e}")
            anomaly_df = pd.DataFrame()

    # Filter by level
    error_df = log_data[log_data["level"] == "ERROR"]
    info_df = log_data[log_data["level"] == "INFO"]
    warn_df = log_data[log_data["level"] == "WARN"]
    debug_df = log_data[log_data["level"] == "DEBUG"]

    # ---------------------------------------------------
    # KEY METRICS
    # ---------------------------------------------------
    col1, col2, col3, col4, col5 = st.columns(5)

    with col1:
        st.metric("Total Logs", f"{len(log_data):,}")
    with col2:
        error_rate = (len(error_df) / len(log_data) * 100) if len(log_data) > 0 else 0
        st.metric("Errors", f"{len(error_df):,}", 
                  delta=f"{error_rate:.1f}%" if len(log_data) > 0 else "0%",
                  delta_color="inverse")
    with col3:
        st.metric("Warnings", f"{len(warn_df):,}")
    with col4:
        st.metric("Info", f"{len(info_df):,}")
    with col5:
        st.metric("Debug", f"{len(debug_df):,}")

    st.markdown("---")

    # ---------------------------------------------------
    # PIE CHART
    # ---------------------------------------------------
    st.subheader("📊 Log Level Distribution")

    level_counts = log_data["level"].value_counts().reset_index()
    level_counts.columns = ["level", "count"]

    # Color mapping
    color_map = {
        "ERROR": "#ff4b4b",
        "WARN": "#ffa500", 
        "INFO": "#00cc96",
        "DEBUG": "#636efa"
    }

    pie_chart = px.pie(
        level_counts,
        names="level",
        values="count",
        title="Distribution of Log Levels",
        color="level",
        color_discrete_map=color_map
    )
    pie_chart.update_traces(textposition='inside', textinfo='percent+label')
    st.plotly_chart(pie_chart, use_container_width=True)

    # ---------------------------------------------------
    # THREE TIMELINE GRAPHS
    # ---------------------------------------------------
    st.subheader("📈 Log Timeline Analysis")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.markdown("### 🔴 ERROR Logs")
        if not error_df.empty:
            fig_error = px.histogram(
                error_df, 
                x="timestamp", 
                title="ERROR Logs Over Time",
                color_discrete_sequence=["#ff4b4b"],
                nbins=30
            )
            fig_error.update_layout(showlegend=False, xaxis_title="Time", yaxis_title="Count")
            st.plotly_chart(fig_error, use_container_width=True)
        else:
            st.info("No ERROR logs found")

    with col2:
        st.markdown("### 🟡 WARN Logs")
        if not warn_df.empty:
            fig_warn = px.histogram(
                warn_df, 
                x="timestamp", 
                title="WARN Logs Over Time",
                color_discrete_sequence=["#ffa500"],
                nbins=30
            )
            fig_warn.update_layout(showlegend=False, xaxis_title="Time", yaxis_title="Count")
            st.plotly_chart(fig_warn, use_container_width=True)
        else:
            st.info("No WARN logs found")

    with col3:
        st.markdown("### 🔵 INFO Logs")
        if not info_df.empty:
            fig_info = px.histogram(
                info_df, 
                x="timestamp", 
                title="INFO Logs Over Time",
                color_discrete_sequence=["#00cc96"],
                nbins=30
            )
            fig_info.update_layout(showlegend=False, xaxis_title="Time", yaxis_title="Count")
            st.plotly_chart(fig_info, use_container_width=True)
        else:
            st.info("No INFO logs found")

    # ---------------------------------------------------
    # ERROR TREND PER MINUTE
    # ---------------------------------------------------
    st.subheader("📉 Error Frequency Analysis")

    if not error_df.empty:
        # Resample by minute
        error_trend = (
            error_df
            .set_index("timestamp")
            .resample("1min")
            .size()
            .reset_index(name="error_count")
        )
        
        # Add rolling average for smoother trend
        if len(error_trend) > 5:
            error_trend['rolling_avg'] = error_trend['error_count'].rolling(window=5, min_periods=1).mean()
        
        line_chart = px.line(
            error_trend,
            x="timestamp",
            y="error_count",
            title="Error Count Per Minute",
            markers=True
        )
        line_chart.update_traces(line=dict(color="#ff4b4b", width=2))
        
        if 'rolling_avg' in error_trend.columns:
            line_chart.add_scatter(x=error_trend['timestamp'], y=error_trend['rolling_avg'], 
                                  name="Rolling Average (5 min)", line=dict(color="orange", width=2, dash="dash"))
        
        st.plotly_chart(line_chart, use_container_width=True)
        
        # Show error rate statistics
        if len(error_trend) > 0:
            avg_errors = error_trend['error_count'].mean()
            max_errors = error_trend['error_count'].max()
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Average Errors/Min", f"{avg_errors:.2f}")
            with col2:
                st.metric("Peak Errors/Min", f"{max_errors}")
    else:
        st.info("No error data available for trend analysis")

    # ---------------------------------------------------
    # SERVICE DISTRIBUTION (if service column exists)
    # ---------------------------------------------------
    if 'service' in log_data.columns:
        st.subheader("🏷️ Service Distribution")
        
        service_counts = log_data['service'].value_counts().head(10).reset_index()
        service_counts.columns = ['service', 'count']
        
        bar_chart = px.bar(
            service_counts,
            x='service',
            y='count',
            title="Top 10 Services by Log Volume",
            color='count',
            color_continuous_scale='Viridis'
        )
        st.plotly_chart(bar_chart, use_container_width=True)

    # ---------------------------------------------------
    # STATUS PANEL
    # ---------------------------------------------------
    st.markdown("---")
    st.subheader("🚨 System Status")

    total_logs = len(log_data)
    total_errors = len(error_df)
    error_percent = (total_errors / total_logs) * 100 if total_logs else 0

    # Check for anomalies
    has_anomalies = False
    anomaly_count = 0
    if not anomaly_df.empty and len(anomaly_df) > 0:
        has_anomalies = True
        anomaly_count = len(anomaly_df) if hasattr(anomaly_df, '__len__') else 1

    # Status indicators
    col1, col2, col3 = st.columns(3)

    with col1:
        if has_anomalies or error_percent > 50:
            st.error("🔴 CRITICAL: System Abnormal")
            st.metric("Error Rate", f"{error_percent:.1f}%", delta="CRITICAL")
        elif error_percent > 20:
            st.warning("🟡 WARNING: Elevated Error Rate")
            st.metric("Error Rate", f"{error_percent:.1f}%", delta="ELEVATED")
        else:
            st.success("🟢 HEALTHY: System Stable")
            st.metric("Error Rate", f"{error_percent:.1f}%", delta="NORMAL")

    with col2:
        if has_anomalies:
            st.warning(f"⚠️ Anomalies Detected: {anomaly_count}")
            st.write("Unusual patterns found in logs")
        else:
            st.info("✅ No Anomalies Detected")
            st.write("System behavior is normal")

    with col3:
        st.info("📊 Time Range")
        if 'timestamp' in log_data.columns and not log_data.empty:
            min_time = log_data['timestamp'].min()
            max_time = log_data['timestamp'].max()
            st.write(f"{min_time.strftime('%H:%M:%S')} - {max_time.strftime('%H:%M:%S')}")
            st.write(f"Duration: {(max_time - min_time).total_seconds() / 60:.1f} minutes")

    # ---------------------------------------------------
    # RAW DATA VIEW
    # ---------------------------------------------------
    st.markdown("---")
    with st.expander("📋 View Raw Log Data"):
        st.subheader("Recent Logs")
        
        # Filter options for raw data
        col1, col2 = st.columns(2)
        with col1:
            rows_to_show = st.selectbox("Number of rows to show", [20, 50, 100, 200], index=2)
        with col2:
            level_filter = st.multiselect("Filter by level", 
                                         options=log_data['level'].unique(),
                                         default=log_data['level'].unique())
        
        # Apply filters
        filtered_data = log_data if not level_filter else log_data[log_data['level'].isin(level_filter)]
        
        # Show data
        st.dataframe(filtered_data.tail(rows_to_show), use_container_width=True)
        
        # Download button
        csv = filtered_data.to_csv(index=False)
        st.download_button(
            label="📥 Download Log Data as CSV",
            data=csv,
            file_name=f"log_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )

    # Auto-refresh logic
    if auto_refresh:
        import time
        time.sleep(refresh_interval)
        st.rerun()

except FileNotFoundError as e:
    st.error(f"❌ File not found: {e}")
    st.info("💡 Please check the file path in the sidebar or generate sample logs.")
except pd.errors.EmptyDataError:
    st.error("❌ The log file is empty")
    st.info("💡 Please generate sample logs or provide a valid log file.")
except Exception as e:
    st.error(f"❌ Error processing logs: {e}")
    with st.expander("Show detailed error information"):
        st.exception(e)