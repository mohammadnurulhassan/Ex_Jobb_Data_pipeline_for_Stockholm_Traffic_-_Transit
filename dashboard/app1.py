f"""
FILE 24: streamlit_app_v2.py
Enhanced Streamlit Dashboard with 7-Day ML Predictions
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta
import time
from config import DUCKDB_DATABASE, STOCKHOLM_STATIONS

# Page configuration
st.set_page_config(
    page_title="Stockholm Traffic Analytics with AI Predictions",
    page_icon="🚇",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
    <style>
    .main {padding: 0rem 1rem;}
    .stMetric {background-color: #f0f2f6; padding: 15px; border-radius: 10px;}
    h1 {color: #1f77b4;}
    .prediction-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        padding: 20px;
        border-radius: 10px;
        margin: 10px 0;
    }
    </style>
""", unsafe_allow_html=True)

@st.cache_resource
def get_db_connection():
    try:
        return duckdb.connect(DUCKDB_DATABASE, read_only=True)
    except Exception as e:
        st.error(f"Database connection error: {e}")
        return None

@st.cache_data(ttl=30)
def get_live_statistics():
    """Get real-time statistics"""
    conn = get_db_connection()
    if not conn:
        return None
    
    try:
        query = """
            SELECT 
                COUNT(*) as total_departures,
                COUNT(DISTINCT station_id) as active_stations,
                COUNT(DISTINCT line_number) as active_lines,
                AVG(delay_minutes) as avg_delay,
                MAX(delay_minutes) as max_delay,
                SUM(CASE WHEN delay_minutes > 5 THEN 1 ELSE 0 END) as significant_delays,
                SUM(CASE WHEN has_deviation THEN 1 ELSE 0 END) as active_disruptions,
                MAX(ingestion_timestamp) as last_update
            FROM raw_traffic.realtime_departures
            WHERE ingestion_timestamp >= CURRENT_TIMESTAMP - INTERVAL '15 minutes'
        """
        result = conn.execute(query).fetchone()
        
        return {
            'total_departures': result[0] or 0,
            'active_stations': result[1] or 0,
            'active_lines': result[2] or 0,
            'avg_delay': result[3] or 0,
            'max_delay': result[4] or 0,
            'significant_delays': result[5] or 0,
            'active_disruptions': result[6] or 0,
            'last_update': result[7]
        }
    except Exception as e:
        st.error(f"Error: {e}")
        return None

@st.cache_data(ttl=300)
def get_predictions():
    """Get 7-day predictions"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT *
            FROM analytics.congestion_predictions
            WHERE timestamp >= CURRENT_TIMESTAMP
            ORDER BY timestamp
        """
        return conn.execute(query).df()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_hourly_trends(hours=24):
    """Get hourly trends"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = f"""
            SELECT 
                hour,
                total_departures,
                avg_delay_minutes,
                delayed_departures,
                delay_percentage
            FROM analytics.fact_hourly_delays
            WHERE hour >= CURRENT_TIMESTAMP - INTERVAL '{hours} hours'
            ORDER BY hour
        """
        return conn.execute(query).df()
    except:
        return pd.DataFrame()

@st.cache_data(ttl=60)
def get_congestion_data():
    """Get congestion scores"""
    conn = get_db_connection()
    if not conn:
        return pd.DataFrame()
    
    try:
        query = """
            SELECT 
                hour,
                station_name,
                congestion_score,
                congestion_level,
                traffic_status,
                avg_delay
            FROM analytics.fact_congestion_score
            WHERE hour >= CURRENT_TIMESTAMP - INTERVAL '24 hours'
            ORDER BY hour DESC
        """
        return conn.execute(query).df()
    except:
        return pd.DataFrame()

def main():
    # Header
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.title("🚇 Stockholm Traffic Analytics")
        st.markdown("**Real-time monitoring with AI-powered 7-day predictions**")
    
    with col2:
        auto_refresh = st.checkbox("🔄 Auto-refresh (30s)", value=True)
    
    with col3:
        if st.button("🔃 Refresh Now"):
            st.cache_data.clear()
            st.rerun()
    
    if auto_refresh:
        time.sleep(0.1)
        st.rerun()
    
    # Get data
    stats = get_live_statistics()
    
    if not stats:
        st.error("⚠️ Unable to connect to database!")
        return
    
    # Last update info
    if stats['last_update']:
        last_update_time = pd.to_datetime(stats['last_update'])
        time_diff = datetime.now() - last_update_time.replace(tzinfo=None)
        seconds_ago = int(time_diff.total_seconds())
        
        if seconds_ago < 60:
            update_text = f"{seconds_ago} seconds ago"
        else:
            update_text = f"{seconds_ago // 60} minutes ago"
        
        st.info(f"📡 Last data update: **{update_text}**")
    
    st.divider()
    
    # KPI Metrics
    st.subheader("📊 Live Metrics (Last 15 Minutes)")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Departures", f"{stats['total_departures']:,}", 
                 f"{stats['active_stations']} stations")
    
    with col2:
        st.metric("Average Delay", f"{stats['avg_delay']:.1f} min",
                 f"Max: {stats['max_delay']:.0f} min", delta_color="inverse")
    
    with col3:
        st.metric("Significant Delays", stats['significant_delays'],
                 f"{(stats['significant_delays']/max(stats['total_departures'],1)*100):.1f}%",
                 delta_color="inverse")
    
    with col4:
        st.metric("Active Lines", stats['active_lines'],
                 f"{stats['active_disruptions']} disruptions", delta_color="inverse")
    
    st.divider()
    
    # Main Tabs
    tab1, tab2, tab3, tab4 = st.tabs([
        "🔮 7-Day Predictions",
        "📈 Current Trends",
        "📊 Congestion Analysis",
        "🎯 Detailed Forecasts"
    ])
    
    # Tab 1: Predictions Overview
    with tab1:
        st.subheader("🤖 AI-Powered 7-Day Congestion Forecast")
        
        predictions_df = get_predictions()
        
        if not predictions_df.empty:
            # Check if predictions are recent
            latest_pred = pd.to_datetime(predictions_df['generated_at'].iloc[0])
            pred_age = datetime.now() - latest_pred.replace(tzinfo=None)
            
            if pred_age.total_seconds() < 86400:  # Less than 24 hours old
                st.success(f"✅ Predictions generated {pred_age.total_seconds()/3600:.1f} hours ago")
            else:
                st.warning(f"⚠️ Predictions are {pred_age.days} days old. Run prediction job.")
            
            # Average prediction by day
            predictions_df['date'] = pd.to_datetime(predictions_df['date'])
            daily_avg = predictions_df.groupby('date')['predicted_congestion'].mean().reset_index()
            
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=daily_avg['date'],
                y=daily_avg['predicted_congestion'],
                mode='lines+markers',
                name='Predicted Congestion',
                line=dict(color='#667eea', width=4),
                marker=dict(size=10)
            ))
            fig.add_hline(y=50, line_dash="dash", line_color="orange",
                         annotation_text="Moderate Congestion Threshold")
            fig.add_hline(y=75, line_dash="dash", line_color="red",
                         annotation_text="High Congestion Threshold")
            
            fig.update_layout(
                title='7-Day Average Predicted Congestion',
                xaxis_title='Date',
                yaxis_title='Congestion Score (0-100)',
                height=400,
                hovermode='x unified'
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Prediction cards for next 3 days
            st.subheader("📅 Next 3 Days Preview")
            
            next_3_days = predictions_df[
                predictions_df['date'] <= predictions_df['date'].min() + timedelta(days=2)
            ]
            
            days_grouped = next_3_days.groupby('date').agg({
                'predicted_congestion': 'mean',
                'congestion_level': lambda x: x.mode()[0] if not x.empty else 'Unknown'
            }).reset_index()
            
            cols = st.columns(3)
            for idx, row in days_grouped.iterrows():
                with cols[idx]:
                    date_str = row['date'].strftime('%A, %b %d')
                    congestion = row['predicted_congestion']
                    level = row['congestion_level']
                    
                    color = {
                        'Low': 'green',
                        'Moderate': 'orange',
                        'High': 'red',
                        'Critical': 'darkred'
                    }.get(level, 'gray')
                    
                    st.markdown(f"""
                        <div style="background: linear-gradient(135deg, {color}40 0%, {color}20 100%);
                                    padding: 20px; border-radius: 10px; border-left: 4px solid {color};">
                            <h4 style="margin: 0;">{date_str}</h4>
                            <h2 style="margin: 10px 0;">{congestion:.0f}/100</h2>
                            <p style="margin: 0;"><strong>{level}</strong> Congestion</p>
                        </div>
                    """, unsafe_allow_html=True)
            
            # Hourly heatmap
            st.subheader("🗓️ Hourly Forecast Heatmap")
            
            pivot_df = predictions_df.pivot_table(
                values='predicted_congestion',
                index='station_name',
                columns='date',
                aggfunc='mean'
            )
            
            fig = go.Figure(data=go.Heatmap(
                z=pivot_df.values,
                x=pivot_df.columns.strftime('%a %m/%d'),
                y=pivot_df.index,
                colorscale='RdYlGn_r',
                zmid=50
            ))
            fig.update_layout(
                title='Predicted Congestion by Station and Day',
                xaxis_title='Date',
                yaxis_title='Station',
                height=500
            )
            st.plotly_chart(fig, use_container_width=True)
            
        else:
            st.warning("""
                ⚠️ No predictions available yet.
                
                To generate predictions:
                1. Ensure you have at least 30 days of data
                2. Run: `python -m ml_models.congestion_predictor train`
                3. Run: `python -m ml_models.congestion_predictor predict`
                
                Or use Dagster to automate this process.
            """)
    
    # Tab 2: Current Trends
    with tab2:
        st.subheader("📈 Current Traffic Trends")
        
        hourly_df = get_hourly_trends(hours=24)
        
        if not hourly_df.empty:
            fig1 = go.Figure()
            fig1.add_trace(go.Scatter(
                x=hourly_df['hour'],
                y=hourly_df['total_departures'],
                mode='lines+markers',
                name='Departures',
                line=dict(color='#1f77b4', width=3),
                fill='tozeroy'
            ))
            fig1.update_layout(
                title='Departures per Hour (Last 24h)',
                xaxis_title='Hour',
                yaxis_title='Departures',
                height=400
            )
            st.plotly_chart(fig1, use_container_width=True)
            
            col1, col2 = st.columns(2)
            
            with col1:
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(
                    x=hourly_df['hour'],
                    y=hourly_df['avg_delay_minutes'],
                    mode='lines+markers',
                    name='Avg Delay',
                    line=dict(color='#ff7f0e', width=3)
                ))
                fig2.update_layout(
                    title='Average Delay Trend',
                    xaxis_title='Hour',
                    yaxis_title='Delay (min)',
                    height=350
                )
                st.plotly_chart(fig2, use_container_width=True)
            
            with col2:
                fig3 = go.Figure()
                fig3.add_trace(go.Bar(
                    x=hourly_df['hour'],
                    y=hourly_df['delay_percentage'],
                    name='Delay %',
                    marker_color='#d62728'
                ))
                fig3.update_layout(
                    title='Delay Percentage',
                    xaxis_title='Hour',
                    yaxis_title='Percentage',
                    height=350
                )
                st.plotly_chart(fig3, use_container_width=True)
    
    # Tab 3: Congestion Analysis
    with tab3:
        st.subheader("📊 Current Congestion Analysis")
        
        congestion_df = get_congestion_data()
        
        if not congestion_df.empty:
            # Current congestion by station
            latest_congestion = congestion_df.groupby('station_name')['congestion_score'].last().sort_values(ascending=False)
            
            fig = px.bar(
                x=latest_congestion.values,
                y=latest_congestion.index,
                orientation='h',
                title='Current Congestion Score by Station',
                color=latest_congestion.values,
                color_continuous_scale='RdYlGn_r',
                labels={'x': 'Congestion Score', 'y': 'Station'}
            )
            fig.update_layout(height=500)
            st.plotly_chart(fig, use_container_width=True)
            
            # Congestion timeline
            fig2 = px.line(
                congestion_df,
                x='hour',
                y='congestion_score',
                color='station_name',
                title='Congestion Timeline (Last 24h)'
            )
            fig2.update_layout(height=400)
            st.plotly_chart(fig2, use_container_width=True)
    
    # Tab 4: Detailed Forecasts
    with tab4:
        st.subheader("🎯 Detailed Station Forecasts")
        
        predictions_df = get_predictions()
        
        if not predictions_df.empty:
            # Station selector
            selected_station = st.selectbox(
                "Select Station",
                options=predictions_df['station_name'].unique()
            )
            
            station_preds = predictions_df[
                predictions_df['station_name'] == selected_station
            ].copy()
            
            # 7-day hourly forecast
            fig = go.Figure()
            
            for date in station_preds['date'].unique()[:7]:
                day_data = station_preds[station_preds['date'] == date]
                fig.add_trace(go.Scatter(
                    x=day_data['hour'],
                    y=day_data['predicted_congestion'],
                    mode='lines+markers',
                    name=pd.to_datetime(date).strftime('%a %m/%d')
                ))
            
            fig.update_layout(
                title=f'{selected_station} - 7-Day Hourly Forecast',
                xaxis_title='Hour of Day',
                yaxis_title='Predicted Congestion',
                height=500
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Peak hours prediction
            st.subheader("⚠️ Predicted Peak Congestion Hours")
            
            peak_hours = station_preds.nlargest(10, 'predicted_congestion')[
                ['timestamp', 'hour', 'predicted_congestion', 'congestion_level']
            ]
            peak_hours['timestamp'] = pd.to_datetime(peak_hours['timestamp']).dt.strftime('%a %b %d, %H:%M')
            
            st.dataframe(
                peak_hours,
                column_config={
                    "timestamp": "Date & Time",
                    "hour": "Hour",
                    "predicted_congestion": st.column_config.NumberColumn("Congestion", format="%.0f"),
                    "congestion_level": "Level"
                },
                hide_index=True,
                use_container_width=True
            )
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Dashboard Settings")
        
        st.divider()
        
        st.header("🤖 ML Model Info")
        try:
            import os
            model_path = "ml_models/saved_models/congestion_predictor.pkl"
            if os.path.exists(model_path):
                model_time = datetime.fromtimestamp(os.path.getmtime(model_path))
                model_age = datetime.now() - model_time
                st.success(f"✅ Model trained {model_age.days} days ago")
                st.caption(f"Last training: {model_time.strftime('%Y-%m-%d %H:%M')}")
            else:
                st.warning("⚠️ No trained model found")
        except:
            st.error("Error checking model")
        
        st.divider()
        
        st.header("📍 Monitored Stations")
        for station_id, station_name in STOCKHOLM_STATIONS.items():
            st.markdown(f"• {station_name}")
        
        st.divider()
        
        st.header("ℹ️ About")
        st.markdown("""
        **Features**:
        - Real-time monitoring
        - 7-day ML predictions
        - Congestion analysis
        - Station comparisons
        
        **Technology**:
        - Random Forest ML
        - DuckDB storage
        - Streamlit interface
        - Plotly visualizations
        """)

if __name__ == "__main__":
    main()