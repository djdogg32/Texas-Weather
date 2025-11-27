import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

"""
INTERACTIVE WEATHER DASHBOARD WITH TORNADO ALERTS
Real-time weather data visualization using Streamlit
"""

# ============================================================================
# PAGE CONFIGURATION
# ============================================================================

st.set_page_config(
    page_title="Texas Weather Dashboard",
    page_icon="🌤️",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# CUSTOM CSS
# ============================================================================

st.markdown("""
    <style>
    .main-header {
        font-size: 3rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background-color: #f0f2f6;
        padding: 1rem;
        border-radius: 0.5rem;
        text-align: center;
    }
    .active-temp-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 1rem;
        color: white;
        text-align: center;
        box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
    }
    .active-temp-value {
        font-size: 3rem;
        font-weight: bold;
        margin: 0.5rem 0;
    }
    .active-temp-city {
        font-size: 1.5rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    .active-temp-conditions {
        font-size: 1rem;
        opacity: 0.9;
        margin-top: 0.5rem;
    }
    </style>
    """, unsafe_allow_html=True)


# ============================================================================
# DATABASE CONNECTION
# ============================================================================

@st.cache_resource
def get_database_connection():
    """Create a cached database connection"""
    return duckdb.connect('weather_project.duckdb', read_only=True)


@st.cache_data(ttl=600)  # Cache for 10 minutes
def load_forecast_data():
    """Load forecast data from DuckDB"""
    con = get_database_connection()
    df = con.execute("SELECT * FROM forecast_data ORDER BY city, date").df()
    df['date'] = pd.to_datetime(df['date'])
    return df


@st.cache_data(ttl=300)  # Cache for 5 minutes (alerts are more urgent)
def load_alerts_data():
    """Load severe weather alerts from DuckDB"""
    try:
        con = get_database_connection()
        df = con.execute("SELECT * FROM alerts_data ORDER BY extracted_at DESC").df()
        return df
    except:
        # Return empty DataFrame if table doesn't exist yet
        return pd.DataFrame()


# ============================================================================
# MAIN DASHBOARD
# ============================================================================

# Header
st.markdown('<h1 class="main-header">🌤️ Texas Weather Dashboard</h1>', unsafe_allow_html=True)
st.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")

# Load data
try:
    df = load_forecast_data()
    alerts_df = load_alerts_data()

    # ========================================================================
    # ACTIVE CURRENT TEMPERATURES (NEW SECTION!)
    # ========================================================================
    
    st.markdown("### 🌡️ Active Current Temperatures")
    
    # Get today's data (most recent date in dataset represents "current" conditions)
    today = df['date'].max()
    current_temps = df[df['date'] == today].sort_values('city')
    
    if len(current_temps) > 0:
        # Create columns for each city
        num_cities = len(current_temps)
        cols = st.columns(min(num_cities, 4))  # Max 4 per row
        
        for idx, (_, row) in enumerate(current_temps.iterrows()):
            col_idx = idx % 4
            with cols[col_idx]:
                # Temperature color coding
                temp = row['temp_avg']
                if temp >= 90:
                    bg_color = "linear-gradient(135deg, #f093fb 0%, #f5576c 100%)"  # Hot - Red/Pink
                elif temp >= 75:
                    bg_color = "linear-gradient(135deg, #ffecd2 0%, #fcb69f 100%)"  # Warm - Orange
                elif temp >= 60:
                    bg_color = "linear-gradient(135deg, #a8edea 0%, #fed6e3 100%)"  # Mild - Teal/Pink
                else:
                    bg_color = "linear-gradient(135deg, #667eea 0%, #764ba2 100%)"  # Cool - Blue/Purple
                
                st.markdown(f"""
                    <div style="background: {bg_color}; padding: 1.5rem; border-radius: 1rem; 
                                color: white; text-align: center; box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
                                margin-bottom: 1rem;">
                        <div style="font-size: 1.3rem; font-weight: 600; margin-bottom: 0.5rem;">
                            📍 {row['city']}
                        </div>
                        <div style="font-size: 2.5rem; font-weight: bold; margin: 0.5rem 0;">
                            {row['temp_avg']:.0f}°F
                        </div>
                        <div style="font-size: 0.9rem; opacity: 0.95;">
                            High: {row['temp_max']:.0f}°F | Low: {row['temp_min']:.0f}°F
                        </div>
                        <div style="font-size: 1rem; margin-top: 0.5rem; opacity: 0.9;">
                            {row['conditions']}
                        </div>
                        <div style="font-size: 0.85rem; margin-top: 0.3rem; opacity: 0.85;">
                            💧 {row['humidity']:.0f}% | 💨 {row['wind_speed']:.0f} mph
                        </div>
                    </div>
                """, unsafe_allow_html=True)
        
        # Add a summary row with additional current conditions
        st.markdown("#### 📊 Current Conditions Summary")
        summary_cols = st.columns(4)
        
        with summary_cols[0]:
            hottest_city = current_temps.loc[current_temps['temp_max'].idxmax()]
            st.metric("🔥 Hottest Location", 
                     f"{hottest_city['city']}", 
                     f"{hottest_city['temp_max']:.0f}°F")
        
        with summary_cols[1]:
            coolest_city = current_temps.loc[current_temps['temp_min'].idxmin()]
            st.metric("❄️ Coolest Location", 
                     f"{coolest_city['city']}", 
                     f"{coolest_city['temp_min']:.0f}°F")
        
        with summary_cols[2]:
            highest_humidity = current_temps.loc[current_temps['humidity'].idxmax()]
            st.metric("💧 Most Humid", 
                     f"{highest_humidity['city']}", 
                     f"{highest_humidity['humidity']:.0f}%")
        
        with summary_cols[3]:
            highest_wind = current_temps.loc[current_temps['wind_speed'].idxmax()]
            st.metric("💨 Windiest", 
                     f"{highest_wind['city']}", 
                     f"{highest_wind['wind_speed']:.0f} mph")
    
    st.markdown("---")

    # ========================================================================
    # SEVERE WEATHER ALERTS BANNER (Top Priority!)
    # ========================================================================

    if len(alerts_df) > 0:
        # Get only the most recent alerts (within last 24 hours)
        alerts_df['extracted_at'] = pd.to_datetime(alerts_df['extracted_at'])
        recent_alerts = alerts_df[
            (datetime.now() - alerts_df['extracted_at']).dt.total_seconds() < 86400
            ]

        if len(recent_alerts) > 0:
            # Check for tornado alerts (HIGHEST PRIORITY)
            tornado_alerts = recent_alerts[
                recent_alerts['event'].str.contains('Tornado', case=False, na=False)
            ]

            if len(tornado_alerts) > 0:
                st.error("🌪️ **TORNADO ALERT!** Tornado Warning or Watch in effect!")
                for _, alert in tornado_alerts.head(3).iterrows():
                    with st.expander(f"⚠️ {alert['event']} - {alert['area_desc']}", expanded=True):
                        st.markdown(f"**Severity:** {alert['severity']}")
                        st.markdown(f"**Urgency:** {alert['urgency']}")
                        st.markdown(f"**Expires:** {alert['expires']}")
                        st.markdown(f"**Headline:** {alert['headline']}")
                        if alert['instruction']:
                            st.markdown(f"**What to do:** {alert['instruction'][:300]}")

            # Check for severe thunderstorm warnings (HIGH PRIORITY)
            thunderstorm_alerts = recent_alerts[
                recent_alerts['event'].str.contains('Severe Thunderstorm', case=False, na=False)
            ]

            if len(thunderstorm_alerts) > 0:
                st.warning("⛈️ **SEVERE THUNDERSTORM WARNING!** Dangerous weather approaching!")
                for _, alert in thunderstorm_alerts.head(3).iterrows():
                    with st.expander(f"⚡ {alert['event']} - {alert['area_desc']}", expanded=True):
                        st.markdown(f"**Severity:** {alert['severity']}")
                        st.markdown(f"**Urgency:** {alert['urgency']}")
                        st.markdown(f"**Expires:** {alert['expires']}")
                        st.markdown(f"**Headline:** {alert['headline']}")
                        if alert['instruction']:
                            st.markdown(f"**What to do:** {alert['instruction'][:300]}")

            # Check for other severe weather (flash floods, winter storms, etc.)
            other_severe_alerts = recent_alerts[
                ~recent_alerts['event'].str.contains('Tornado|Severe Thunderstorm', case=False, na=False) &
                recent_alerts['severity'].isin(['Extreme', 'Severe'])
                ]

            if len(other_severe_alerts) > 0:
                st.warning(f"⚠️ **{len(other_severe_alerts)} Other Severe Weather Alert(s) Active**")
                for _, alert in other_severe_alerts.head(3).iterrows():
                    with st.expander(f"🚨 {alert['event']} - {alert['area_desc']}"):
                        st.markdown(f"**Severity:** {alert['severity']}")
                        st.markdown(f"**Urgency:** {alert['urgency']}")
                        st.markdown(f"**Expires:** {alert['expires']}")
                        st.markdown(f"**Headline:** {alert['headline']}")

            # Show info message for moderate/minor alerts
            moderate_alerts = recent_alerts[
                recent_alerts['severity'].isin(['Moderate', 'Minor'])
            ]

            if len(moderate_alerts) > 0:
                st.info(f"ℹ️ {len(moderate_alerts)} weather advisory/watch alert(s) active")

    else:
        st.success("✅ No active severe weather alerts")

    st.markdown("---")

    # ========================================================================
    # SIDEBAR FILTERS
    # ========================================================================

    st.sidebar.header("🎛️ Filters")

    # City filter
    cities = ['All'] + sorted(df['city'].unique().tolist())
    selected_city = st.sidebar.selectbox("Select City", cities)

    # Date range filter
    min_date = df['date'].min().date()
    max_date = df['date'].max().date()
    date_range = st.sidebar.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date
    )

    # Temperature filter
    temp_range = st.sidebar.slider(
        "Temperature Range (°F)",
        int(df['temp_min'].min()),
        int(df['temp_max'].max()),
        (int(df['temp_min'].min()), int(df['temp_max'].max()))
    )

    # Apply filters
    filtered_df = df.copy()
    if selected_city != 'All':
        filtered_df = filtered_df[filtered_df['city'] == selected_city]

    if len(date_range) == 2:
        filtered_df = filtered_df[
            (filtered_df['date'].dt.date >= date_range[0]) &
            (filtered_df['date'].dt.date <= date_range[1])
            ]

    filtered_df = filtered_df[
        (filtered_df['temp_max'] >= temp_range[0]) &
        (filtered_df['temp_max'] <= temp_range[1])
        ]

    # ========================================================================
    # KEY METRICS
    # ========================================================================

    st.markdown("### 📊 Key Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        avg_high = filtered_df['temp_max'].mean()
        st.metric(
            label="Avg High Temp",
            value=f"{avg_high:.1f}°F",
            delta=f"{avg_high - df['temp_max'].mean():.1f}°F"
        )

    with col2:
        avg_low = filtered_df['temp_min'].mean()
        st.metric(
            label="Avg Low Temp",
            value=f"{avg_low:.1f}°F",
            delta=f"{avg_low - df['temp_min'].mean():.1f}°F"
        )

    with col3:
        avg_humidity = filtered_df['humidity'].mean()
        st.metric(
            label="Avg Humidity",
            value=f"{avg_humidity:.0f}%"
        )

    with col4:
        avg_precip = filtered_df['precipitation_prob'].mean()
        st.metric(
            label="Avg Precip Prob",
            value=f"{avg_precip:.0f}%"
        )

    # ========================================================================
    # TEMPERATURE TRENDS
    # ========================================================================

    st.markdown("### 🌡️ Temperature Trends")

    fig_temp = go.Figure()

    for city in filtered_df['city'].unique():
        city_data = filtered_df[filtered_df['city'] == city].sort_values('date')

        # High temperature line
        fig_temp.add_trace(go.Scatter(
            x=city_data['date'],
            y=city_data['temp_max'],
            name=f'{city} - High',
            mode='lines+markers',
            line=dict(width=2),
            marker=dict(size=8)
        ))

        # Low temperature line
        fig_temp.add_trace(go.Scatter(
            x=city_data['date'],
            y=city_data['temp_min'],
            name=f'{city} - Low',
            mode='lines+markers',
            line=dict(width=2, dash='dash'),
            marker=dict(size=6)
        ))

    fig_temp.update_layout(
        title="Temperature Forecast",
        xaxis_title="Date",
        yaxis_title="Temperature (°F)",
        hovermode='x unified',
        height=500,
        template='plotly_white'
    )

    st.plotly_chart(fig_temp, use_container_width=True)

    # ========================================================================
    # CITY COMPARISON
    # ========================================================================

    col1, col2 = st.columns(2)

    with col1:
        st.markdown("### 📍 Average Temperature by City")

        city_avg = filtered_df.groupby('city').agg({
            'temp_max': 'mean',
            'temp_min': 'mean',
            'temp_avg': 'mean'
        }).round(1).reset_index()

        fig_bar = go.Figure()
        fig_bar.add_trace(go.Bar(
            x=city_avg['city'],
            y=city_avg['temp_max'],
            name='High',
            marker_color='#FF6B6B'
        ))
        fig_bar.add_trace(go.Bar(
            x=city_avg['city'],
            y=city_avg['temp_min'],
            name='Low',
            marker_color='#4ECDC4'
        ))

        fig_bar.update_layout(
            barmode='group',
            height=400,
            template='plotly_white',
            yaxis_title="Temperature (°F)"
        )

        st.plotly_chart(fig_bar, use_container_width=True)

    with col2:
        st.markdown("### 💧 Humidity vs Precipitation")

        fig_scatter = px.scatter(
            filtered_df,
            x='humidity',
            y='precipitation_prob',
            color='city',
            size='temp_max',
            hover_data=['date', 'conditions'],
            title="",
            labels={
                'humidity': 'Humidity (%)',
                'precipitation_prob': 'Precipitation Probability (%)',
                'temp_max': 'High Temp (°F)'
            }
        )

        fig_scatter.update_layout(
            height=400,
            template='plotly_white'
        )

        st.plotly_chart(fig_scatter, use_container_width=True)

    # ========================================================================
    # WEATHER CONDITIONS
    # ========================================================================

    st.markdown("### ☁️ Weather Conditions Distribution")

    col1, col2 = st.columns(2)

    with col1:
        conditions_count = filtered_df['conditions'].value_counts().head(10)

        fig_pie = px.pie(
            values=conditions_count.values,
            names=conditions_count.index,
            title="Most Common Weather Conditions"
        )

        fig_pie.update_layout(height=400)
        st.plotly_chart(fig_pie, use_container_width=True)

    with col2:
        st.markdown("#### 💨 Wind Speed Analysis")

        fig_wind = px.line(
            filtered_df.sort_values('date'),
            x='date',
            y='wind_speed',
            color='city',
            markers=True,
            labels={'wind_speed': 'Wind Speed (mph)', 'date': 'Date'}
        )

        fig_wind.update_layout(
            height=400,
            template='plotly_white'
        )

        st.plotly_chart(fig_wind, use_container_width=True)

    # ========================================================================
    # TEMPERATURE HEATMAP
    # ========================================================================

    st.markdown("### 🗺️ Temperature Heatmap")

    heatmap_data = filtered_df.pivot_table(
        index='city',
        columns='date',
        values='temp_max',
        aggfunc='mean'
    )

    fig_heatmap = go.Figure(data=go.Heatmap(
        z=heatmap_data.values,
        x=[d.strftime('%m/%d') for d in heatmap_data.columns],
        y=heatmap_data.index,
        colorscale='RdYlBu_r',
        text=heatmap_data.values.round(0),
        texttemplate='%{text}°',
        textfont={"size": 10},
        colorbar=dict(title="Temp (°F)")
    ))

    fig_heatmap.update_layout(
        title="Daily High Temperature by City",
        height=300,
        template='plotly_white'
    )

    st.plotly_chart(fig_heatmap, use_container_width=True)

    # ========================================================================
    # DETAILED SEVERE WEATHER ALERTS SECTION
    # ========================================================================

    if len(alerts_df) > 0:
        st.markdown("### 🚨 Severe Weather Alerts History")

        col1, col2, col3, col4 = st.columns(4)

        with col1:
            tornado_count = len(alerts_df[alerts_df['event'].str.contains('Tornado', case=False, na=False)])
            st.metric("🌪️ Tornado Alerts", tornado_count, delta=None)

        with col2:
            thunderstorm_count = len(
                alerts_df[alerts_df['event'].str.contains('Severe Thunderstorm', case=False, na=False)])
            st.metric("⛈️ Severe T-Storm", thunderstorm_count, delta=None)

        with col3:
            flood_count = len(alerts_df[alerts_df['event'].str.contains('Flood', case=False, na=False)])
            st.metric("🌊 Flood Alerts", flood_count, delta=None)

        with col4:
            total_alerts = len(alerts_df)
            st.metric("📊 Total Alerts", total_alerts, delta=None)

        # Alert timeline
        st.markdown("#### Alert Timeline")
        alerts_df['extracted_at'] = pd.to_datetime(alerts_df['extracted_at'])
        alert_timeline = alerts_df.groupby(alerts_df['extracted_at'].dt.date).size().reset_index()
        alert_timeline.columns = ['Date', 'Count']

        fig_timeline = px.bar(
            alert_timeline,
            x='Date',
            y='Count',
            title='Severe Weather Alerts Over Time',
            labels={'Count': 'Number of Alerts', 'Date': 'Date'}
        )
        fig_timeline.update_layout(height=300, template='plotly_white')
        st.plotly_chart(fig_timeline, use_container_width=True)

        # Recent alerts table
        st.markdown("#### Recent Alerts")
        recent_alerts_display = alerts_df[['extracted_at', 'event', 'severity', 'area_desc', 'expires']].head(10)
        st.dataframe(recent_alerts_display, use_container_width=True, height=300)

    # ========================================================================
    # DATA TABLE
    # ========================================================================

    st.markdown("### 📋 Detailed Forecast Data")

    # Display options
    col1, col2 = st.columns([3, 1])
    with col1:
        show_all_columns = st.checkbox("Show all columns", value=False)
    with col2:
        rows_to_show = st.selectbox("Rows to display", [10, 25, 50, 100], index=1)

    if show_all_columns:
        display_df = filtered_df
    else:
        display_df = filtered_df[['city', 'date', 'temp_max', 'temp_min',
                                  'conditions', 'humidity', 'precipitation_prob', 'wind_speed']]

    st.dataframe(
        display_df.head(rows_to_show).sort_values(['city', 'date']),
        use_container_width=True,
        height=400
    )

    # ========================================================================
    # DOWNLOAD DATA
    # ========================================================================

    st.markdown("### 💾 Export Data")

    csv = filtered_df.to_csv(index=False)
    st.download_button(
        label="📥 Download Filtered Data as CSV",
        data=csv,
        file_name=f"weather_data_{datetime.now().strftime('%Y%m%d')}.csv",
        mime="text/csv"
    )

    # ========================================================================
    # STATISTICS
    # ========================================================================

    with st.expander("📈 View Detailed Statistics"):
        st.markdown("#### Temperature Statistics by City")

        stats = filtered_df.groupby('city').agg({
            'temp_max': ['mean', 'min', 'max', 'std'],
            'temp_min': ['mean', 'min', 'max', 'std'],
            'humidity': 'mean',
            'precipitation_prob': 'mean',
            'wind_speed': 'mean'
        }).round(2)

        st.dataframe(stats, use_container_width=True)

except Exception as e:
    st.error(f"⚠️ Error loading data: {str(e)}")
    st.info("Make sure your pipeline has run and created the 'weather_project.duckdb' database.")
    st.info("Run: `python weather_pipeline.py` first!")

# ========================================================================
# FOOTER
# ========================================================================

st.markdown("---")
st.markdown("""
    <div style='text-align: center; color: gray;'>
        <p>🌤️ Texas Weather Dashboard | Data updates automatically | Built with Streamlit & DuckDB</p>
    </div>
    """, unsafe_allow_html=True)
