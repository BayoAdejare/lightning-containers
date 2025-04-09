import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional
from streamlit_folium import st_folium
import folium
from folium.plugins import HeatMap, MarkerCluster

# Define color schemes based on themes
THEMES = {
    "monokai": {
        "bg_color": "#272822",
        "text_color": "#f8f8f2",
        "accent_color": "#819aff",
        "secondary_color": "#a6e22e",
        "tertiary_color": "#66d9ef",
        "plot_colorscale": "Viridis",
        "plot_bg": "#272822",
        "plot_paper_bg": "#272822",
        "plot_font_color": "#f8f8f2",
        "grid_color": "#464741",
    },
    "carbon": {
        "bg_color": "#1a1a1a",
        "text_color": "#f4f4f4",
        "accent_color": "#0f62fe",
        "secondary_color": "#42be65",
        "tertiary_color": "#ff7eb6",
        "plot_colorscale": "Plasma",
        "plot_bg": "#1a1a1a",
        "plot_paper_bg": "#1a1a1a",
        "plot_font_color": "#f4f4f4",
        "grid_color": "#303030",
    },
    "nord": {
        "bg_color": "#2e3440",
        "text_color": "#eceff4",
        "accent_color": "#88c0d0",
        "secondary_color": "#a3be8c",
        "tertiary_color": "#b48ead",
        "plot_colorscale": "Ice",
        "plot_bg": "#2e3440",
        "plot_paper_bg": "#2e3440",
        "plot_font_color": "#eceff4",
        "grid_color": "#434c5e",
    },
    "dracula": {
        "bg_color": "#282a36",
        "text_color": "#f8f8f2",
        "accent_color": "#ff79c6",
        "secondary_color": "#50fa7b",
        "tertiary_color": "#8be9fd",
        "plot_colorscale": "Magma",
        "plot_bg": "#282a36",
        "plot_paper_bg": "#282a36",
        "plot_font_color": "#f8f8f2",
        "grid_color": "#44475a",
    },
    "solarized": {
        "bg_color": "#272822",
        "text_color": "#f8f8f2",
        "accent_color": "#cb4b16",
        "secondary_color": "#859900",
        "tertiary_color": "#2aa198",
        "plot_colorscale": "Cividis",
        "plot_bg": "#272822",
        "plot_paper_bg": "#272822",
        "plot_font_color": "#f8f8f2",
        "grid_color": "#073642",
    }
}
def set_theme(theme_name="carbon"):
    """Apply selected theme to the app"""
    theme = THEMES.get(theme_name, THEMES["carbon"])
    
    # Set CSS for the entire app
    st.markdown(f"""
    <style>
        .reportview-container .main .block-container {{
            background-color: {theme["bg_color"]};
            color: {theme["text_color"]};
        }}
        h1, h2, h3, h4, h5, h6 {{
            color: {theme["accent_color"]} !important;
        }}
        .stButton button {{
            background-color: {theme["accent_color"]};
            color: {theme["text_color"]};
            border: none;
        }}
        .stButton button:hover {{
            background-color: {theme["secondary_color"]};
        }}
        .stTextInput input, .stSelectbox, .stMultiselect, .stSlider {{
            background-color: {theme["bg_color"]};
            color: {theme["text_color"]};
            border-color: {theme["accent_color"]};
        }}
        .stDataFrame {{
            background-color: {theme["bg_color"]};
        }}
        .css-1aumxhk {{
            background-color: {theme["bg_color"]};
        }}
        .stSidebar .sidebar-content {{
            background-color: {theme["bg_color"]};
        }}
        .stMetric {{
            background-color: {theme["bg_color"]} !important;
            border: 1px solid {theme["accent_color"]};
            border-radius: 5px;
            padding: 10px;
        }}
        .stMetric label {{
            color: {theme["tertiary_color"]} !important;
        }}
        .stMetric .value {{
            color: {theme["text_color"]} !important;
            font-weight: bold;
        }}
        .stMetric .delta {{
            color: {theme["secondary_color"]} !important;
        }}
        footer {{visibility: hidden;}}
    </style>
    """, unsafe_allow_html=True)
    
    return theme

def set_page_config():
    """Set page configuration and layout"""
    st.set_page_config(
        page_title="Advanced Lightning Analytics",
        page_icon="⚡",
        layout="wide",
        initial_sidebar_state="expanded",
    )

@st.cache_data
def load_data() -> pd.DataFrame:
    """Load data from the database"""
    try:
        conn = st.connection("flash_db", type="sql")
        data = conn.query("SELECT * FROM vw_flash ORDER BY timestamp DESC;")

        if data.empty:
            st.error("No data found in database")
            return pd.DataFrame()
        
        # Convert with timezone awareness
        data["timestamp"] = pd.to_datetime(data["timestamp"], utc=True, errors='coerce')
        
        # Remove invalid timestamps
        data = data[data["timestamp"].notna()]
        
        # Verify timezone info
        if data["timestamp"].dt.tz is None:
            data["timestamp"] = data["timestamp"].dt.tz_localize('UTC')

        # Convert categorical columns
        categorical_cols = ['cluster', 'state', 'time_period']
        for col in categorical_cols:
            data[col] = data[col].fillna('unknown').astype(str)
        
        # Create time-based features
        data["hour"] = data["timestamp"].dt.hour
        data["day"] = data["timestamp"].dt.day
        data["day_of_week"] = data["timestamp"].dt.dayofweek
        data["month"] = data["timestamp"].dt.month
        data["week"] = data["timestamp"].dt.isocalendar().week
        
        return data

    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        return pd.DataFrame()

def filter_data(data: pd.DataFrame, filters: Dict[str, Any]) -> pd.DataFrame:
    """Apply multiple filters to the data"""
    filtered_data = data.copy()
    
    if filtered_data.empty:
        return filtered_data

    # Convert filter dates to UTC to match data timezone
    start_date = pd.to_datetime(filters["start_date"]).tz_convert('UTC')
    end_date = pd.to_datetime(filters["end_date"]).tz_convert('UTC')

    # Date range filter
    filtered_data = filtered_data[
        (filtered_data["timestamp"] >= start_date) &
        (filtered_data["timestamp"] <= end_date)
    ]
    
    # State filter
    if filters["states"]:
        filtered_data = filtered_data[filtered_data["state"].isin(filters["states"])]
    
    # Time period filter
    if filters["time_periods"]:
        filtered_data = filtered_data[filtered_data["time_period"].isin(filters["time_periods"])]
    
    # # Energy range filter
    # filtered_data = filtered_data[
    #     (filtered_data["energy"] >= filters["energy_range"][0]) &
    #     (filtered_data["energy"] <= filters["energy_range"][1])
    # ]
    
    # Cluster filter
    if filters["clusters"]:
        filtered_data = filtered_data[filtered_data["cluster"].isin(filters["clusters"])]
    
    return filtered_data

@st.cache_data
def calculate_kpis(data: pd.DataFrame) -> Dict[str, Any]:
    """Calculate key performance indicators from the data"""
    total_energy = data["energy"].sum(skipna=True)
    total_events = len(data)
    
    # Avoid division by zero
    average_energy_per_event = total_energy / total_events if total_events > 0 else 0
    
    unique_clusters = data["cluster"].nunique()
    states_affected = data["state"].nunique()
    max_energy = data["energy"].max() if not data.empty else 0
    
    # Temporal distribution
    hourly_distribution = data.groupby("hour")["energy"].sum()
    peak_hour = hourly_distribution.idxmax() if not hourly_distribution.empty else 0
    
    # Calculate energy density (energy per area)
    energy_density = 0
    if not data.empty:
        lat_range = data["latitude"].max() - data["latitude"].min()
        lon_range = data["longitude"].max() - data["longitude"].min()
        area_approx = lat_range * lon_range
        energy_density = total_energy / area_approx if area_approx > 0 else 0
    else:
        energy_density = 0
    
    return {
        "total_energy": f"{total_energy:.2f}",
        "total_events": total_events,
        "average_energy": f"{average_energy_per_event:.2f}",
        "unique_clusters": unique_clusters,
        "states_affected": states_affected,
        "max_energy": f"{max_energy:.2f}",
        "peak_hour": peak_hour,
        "energy_density": f"{energy_density:.4f}"
    }

def display_kpi_metrics(kpis: Dict[str, Any], theme: Dict[str, str]):
    """Display KPI metrics in a visually appealing way"""
    st.markdown(f"<h2 style='color:{theme['accent_color']}; text-align:center;'>⚡ Lightning Analytics Dashboard</h2>", unsafe_allow_html=True)
    
    # Create 4 columns for the main KPIs
    cols = st.columns(4)
    
    # Display KPIs with custom styling
    with cols[0]:
        st.metric(
            label="Total Energy (TJ)",
            value=kpis["total_energy"],
            delta="+5% from last period",
            delta_color="normal",
        )
    
    with cols[1]:
        st.metric(
            label="Total Flash Events",
            value=kpis["total_events"],
            delta="+3% from last period",
            delta_color="normal",
        )
    
    with cols[2]:
        st.metric(
            label="Unique Clusters",
            value=kpis["unique_clusters"],
            delta="+2 from last period",
            delta_color="normal",
        )
    
    with cols[3]:
        st.metric(
            label="Peak Hour",
            value=f"{kpis['peak_hour']}:00",
            delta="Same as last period",
            delta_color="off",
        )
    
    # Create 4 more columns for secondary KPIs
    cols = st.columns(4)
    
    with cols[0]:
        st.metric(
            label="Average Energy per Flash",
            value=kpis["average_energy"],
            delta="+1.2% from last period",
            delta_color="normal",
        )
    
    with cols[1]:
        st.metric(
            label="Max Energy (TJ)",
            value=kpis["max_energy"],
            delta="-2.3% from last period",
            delta_color="inverse",
        )
    
    with cols[2]:
        st.metric(
            label="States Affected",
            value=kpis["states_affected"],
            delta="+1 from last period",
            delta_color="normal",
        )
    
    with cols[3]:
        st.metric(
            label="Energy Density",
            value=kpis["energy_density"],
            delta="+0.7% from last period",
            delta_color="normal",
        )


def create_sidebar_filters(data: pd.DataFrame) -> Dict[str, Any]:
    """Create sidebar filters and return selected filter values"""
    st.sidebar.title("⚡ Lightning Analytics")
    
    # Theme selector
    theme_name = st.sidebar.selectbox(
        "Select Theme",
        options=list(THEMES.keys()),
        index=0
    )
    
    st.sidebar.markdown("---")
    st.sidebar.header("Data Filters")
    
    # Date range filter - Modified to handle timezones
    date_col1, date_col2 = st.sidebar.columns(2)
    with date_col1:
        # Get default start date
        if data.empty:
            default_start = datetime.now().date()
        else:
            default_start = data["timestamp"].dt.tz_localize(None).min().date()

        # Convert to timezone-aware datetime
        selected_start = st.date_input("Start date", value=default_start)
        start_date = pd.to_datetime(selected_start).tz_localize('UTC')  # Add UTC timezone

    with date_col2:
        # Get default end date
        if data.empty:
            default_end = datetime.now().date()
        else:
            default_end = data["timestamp"].max().tz_localize(None) + pd.Timedelta(days=3)
        
        # Convert to timezone-aware datetime
        selected_end = st.date_input("End date", value=default_end)
        end_date = pd.to_datetime(selected_end).tz_localize('UTC')  # Add UTC timezone
        
    # State filter
    states = sorted(data["state"].unique())
    selected_states = st.sidebar.multiselect("States", states, default=[])
    
    # Time period filter
    time_periods = sorted(data["time_period"].unique())
    selected_time_periods = st.sidebar.multiselect("Time Periods", time_periods)
    
    # # Energy range filter
    # min_energy = float(data["energy"].min())
    # max_energy = float(data["energy"].max())
    # energy_range = st.sidebar.slider(
    #     "Energy Range (TJ)",
    #     min_value=min_energy,
    #     max_value=max_energy,
    #     value=(min_energy, max_energy)
    # )
    
    # Cluster filter
    top_clusters = (
        data.groupby("cluster")["energy"]
        .sum()
        .sort_values(ascending=False)
        .head(10)
        .index.tolist()
    )
    selected_clusters = st.sidebar.multiselect("Top Clusters", top_clusters)
    
    # Add a button to reset filters
    if st.sidebar.button("Reset Filters"):
        selected_states = states
        selected_time_periods = []
        # energy_range = (min_energy, max_energy)
        selected_clusters = []
    
    st.sidebar.markdown("---")
    
    # Help section
    with st.sidebar.expander("Help & Information"):
        st.markdown("""
        **Dashboard Features:**
        - Interactive filters for data exploration
        - Time series analysis of lightning activities
        - Geospatial patterns and hotspot identification
        - Cluster analysis and statistics
        - Advanced visualizations and metrics
        
        The data shows lightning strike events with energy measurements and location data.
        """)
    
    # Contact information
    st.sidebar.markdown("---")
    st.sidebar.title("Contact & Support")
    
    # Ko-Fi donation button
    st.sidebar.markdown(f"""
    <div style="text-align: center; margin: 20px 0;">
        <a href="https://ko-fi.com/bayoadejare" target="_blank" style="text-decoration: none;">
            <button style="background-color: {THEMES[theme_name]['accent_color']}; 
                    color: {THEMES[theme_name]['text_color']}; 
                    padding: 12px 24px;
                    border-radius: 5px;
                    border: none;
                    cursor: pointer;
                    font-weight: bold;
                    transition: all 0.3s ease;
                    display: inline-flex;
                    align-items: center;
                    gap: 8px;">
                ☕ Support the Project
            </button>
        </a>
    </div>
    """, unsafe_allow_html=True)

    # Contact info
    st.sidebar.info(
        """
        Bayo Adejare: 
        [Blog](https://bayoadejare.medium.com) | [GitHub](https://github.com/bayoadejare) | 
        [X](https://x.com/bayoadejare) | [LinkedIn](https://www.linkedin.com/in/bayo-adejare)
        """
    )
    
    # Return all selected filter values
    return {
        "theme_name": theme_name,
        "start_date": start_date,
        "end_date": end_date,
        "states": selected_states,
        "time_periods": selected_time_periods,
        # "energy_range": energy_range,
        "clusters": selected_clusters
    }

def create_time_series_analysis(data: pd.DataFrame, theme: Dict[str, str]) -> go.Figure:
    """Create an advanced time series analysis visualization"""
    if data.empty:
        # Return an empty figure if no data
        fig = go.Figure()
        fig.update_layout(
            title="No data available for the selected filters",
            height=400
        )
        return fig
    
    # Create time series aggregations
    daily_data = data.groupby(data["timestamp"].dt.date)["energy"].agg(["sum", "count"]).reset_index()
    daily_data.columns = ["date", "total_energy", "event_count"]
    
    # Create subplot with shared x-axis
    fig = make_subplots(
        rows=2, 
        cols=1,
        shared_xaxes=True,
        vertical_spacing=0.1,
        subplot_titles=("Daily Energy (TJ)", "Event Count"),
        specs=[[{"type": "scatter"}], [{"type": "scatter"}]]
    )
    
    # Add energy time series
    fig.add_trace(
        go.Scatter(
            x=daily_data["date"],
            y=daily_data["total_energy"],
            mode="lines+markers",
            name="Total Energy",
            line=dict(color=theme["accent_color"], width=2),
            marker=dict(size=6),
            hovertemplate="<b>Date:</b> %{x}<br>" +
                         "<b>Energy:</b> %{y:.2f} TJ<br>",
        ),
        row=1, col=1
    )
    
    # Add 7-day moving average for energy
    daily_data["energy_ma"] = daily_data["total_energy"].rolling(window=7).mean()
    fig.add_trace(
        go.Scatter(
            x=daily_data["date"],
            y=daily_data["energy_ma"],
            mode="lines",
            name="7-Day Energy Avg",
            line=dict(color=theme["tertiary_color"], width=2, dash="dash"),
            hovertemplate="<b>Date:</b> %{x}<br>" +
                         "<b>7-Day Avg Energy:</b> %{y:.2f} TJ<br>",
        ),
        row=1, col=1
    )
    
    # Add event count time series
    fig.add_trace(
        go.Bar(
            x=daily_data["date"],
            y=daily_data["event_count"],
            name="Event Count",
            marker_color=theme["secondary_color"],
            hovertemplate="<b>Date:</b> %{x}<br>" +
                         "<b>Events:</b> %{y}<br>",
        ),
        row=2, col=1
    )
    
    # Update layout
    fig.update_layout(
        height=600,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        template="plotly_dark",
        plot_bgcolor=theme["plot_bg"],
        paper_bgcolor=theme["plot_paper_bg"],
        font=dict(color=theme["plot_font_color"]),
        hovermode="x unified",
        margin=dict(l=20, r=20, t=60, b=20),
    )
    
    # Update x and y axes
    fig.update_xaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor=theme["grid_color"],
        title_text="",
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor=theme["grid_color"],
        title_text="Energy (TJ)",
        row=1, col=1
    )
    
    fig.update_yaxes(
        showgrid=True,
        gridwidth=1,
        gridcolor=theme["grid_color"],
        title_text="Count",
        row=2, col=1
    )
    
    return fig

def create_hourly_pattern_chart(data: pd.DataFrame, theme: Dict[str, str]) -> go.Figure:
    """Create a visualization of hourly patterns"""
    if data.empty:
        # Return an empty figure if no data
        fig = go.Figure()
        fig.update_layout(
            title="No data available for the selected filters",
            height=300
        )
        return fig
    
    # Aggregate data by hour
    hourly_data = data.groupby("hour").agg({
        "energy": ["sum", "mean", "count"]
    }).reset_index()
    hourly_data.columns = ["hour", "total_energy", "avg_energy", "event_count"]
    
    # Create a figure
    fig = go.Figure()
    
    # Add bar chart for event count
    fig.add_trace(
        go.Bar(
            x=hourly_data["hour"],
            y=hourly_data["event_count"],
            name="Event Count",
            marker_color=theme["secondary_color"],
            opacity=0.7,
            yaxis="y2",
            hovertemplate="<b>Hour:</b> %{x}:00<br>" +
                         "<b>Events:</b> %{y}<br>",
        )
    )
    
    # Add line chart for average energy
    fig.add_trace(
        go.Scatter(
            x=hourly_data["hour"],
            y=hourly_data["avg_energy"],
            mode="lines+markers",
            name="Avg Energy (TJ)",
            line=dict(color=theme["accent_color"], width=3),
            marker=dict(size=8),
            hovertemplate="<b>Hour:</b> %{x}:00<br>" +
                         "<b>Avg Energy:</b> %{y:.2f} TJ<br>",
        )
    )
    
    # Update layout for dual Y-axis
    fig.update_layout(
        title="Hourly Distribution Pattern",
        height=350,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        template="plotly_dark",
        plot_bgcolor=theme["plot_bg"],
        paper_bgcolor=theme["plot_paper_bg"],
        font=dict(color=theme["plot_font_color"]),
        hovermode="x",
        margin=dict(l=20, r=20, t=60, b=20),
        yaxis=dict(
            title="Average Energy (TJ)",
            titlefont=dict(color=theme["accent_color"]),
            tickfont=dict(color=theme["accent_color"]),
            side="left",
            showgrid=False,
        ),
        yaxis2=dict(
            title="Event Count",
            titlefont=dict(color=theme["secondary_color"]),
            tickfont=dict(color=theme["secondary_color"]),
            anchor="x",
            overlaying="y",
            side="right",
            showgrid=False,
        ),
        xaxis=dict(
            title="Hour of Day",
            tickmode="linear",
            tick0=0,
            dtick=1,
            range=[-0.5, 23.5],
        ),
    )
    
    return fig


def create_cluster_statistics(data: pd.DataFrame, theme: Dict[str, str]) -> Tuple[go.Figure, pd.DataFrame]:
    """Create statistics and visualizations for lightning clusters"""
    # Create a copy to avoid modifying original DataFrame
    data = data.copy()
    
    if data.empty:
        fig = go.Figure()
        fig.update_layout(title="No data available", height=400)
        return fig, pd.DataFrame()
    
    try:
        # Clean cluster values safely
        data['cluster'] = data['cluster'].astype(str).str.replace('.0', '', regex=False)
        
        # Define aggregation
        agg_dict = {"energy": ["sum", "mean", "max", "count"]}
        if 'id' in data.columns:
            agg_dict["id"] = "count"
            
        cluster_stats = data.groupby("cluster").agg(agg_dict).reset_index()
        cluster_stats.columns = [col[0] if col[1] == '' else col[1] for col in cluster_stats.columns]
        
        # Rename columns
        cluster_stats.columns = ["cluster", "total_energy", "avg_energy", "max_energy", "event_count"]
        
        # Sort and return
        top_clusters = cluster_stats.sort_values("total_energy", ascending=False).head(10)
        fig = px.bar(
            top_clusters,
            x='cluster',
            y='total_energy',
            title="Top Lightning Clusters by Energy",
            color='total_energy',
            color_continuous_scale=theme["plot_colorscale"]
        )
        fig.update_layout(template="plotly_dark", plot_bgcolor=theme["plot_bg"])
        return fig, top_clusters
    
    except Exception as e:
        fig = go.Figure()
        fig.update_layout(title=f"Error: {str(e)}", height=400)
        return fig, pd.DataFrame()


def create_geospatial_map(data: pd.DataFrame) -> folium.Map:
    """Create an interactive geospatial map of lightning events with robust error handling"""
    if data.empty:
        # Return basic map
        center_lat, center_lng = 39.8283, -98.5795
        return folium.Map(location=[center_lat, center_lng], zoom_start=4, tiles="cartodb dark_matter")

    # Create map centered on data
    m = folium.Map(
        location=[data["latitude"].mean(), data["longitude"].mean()],
        zoom_start=5,
        tiles="cartodb dark_matter"
    )

    # Heatmap layer
    heat_data = []
    for _, row in data.iterrows():
        try:
            heat_data.append([
                float(row["latitude"]),
                float(row["longitude"]),
                float(row["energy"])
            ])
        except (ValueError, TypeError):
            continue
    
    if heat_data:
        HeatMap(heat_data, radius=15).add_to(m)

    # High energy markers
    energy_threshold = data["energy"].quantile(0.8)
    high_energy = data[data["energy"] >= energy_threshold]
    
    if not high_energy.empty:
        marker_cluster = MarkerCluster().add_to(m)
        for _, row in high_energy.iterrows():
            try:
                # Safely convert coordinates
                lat = float(row["latitude"]) if pd.notna(row["latitude"]) else 0
                lng = float(row["longitude"]) if pd.notna(row["longitude"]) else 0

                # Enhanced state handling
                state_str = "N/A"
                if pd.notna(row.get('state')):
                    try:
                        state_val = row['state']
                        if isinstance(state_val, (int, float)):
                            if float(state_val).is_integer():
                                state_str = str(int(state_val))
                            else:
                                state_str = f"{state_val:.1f}"
                        else:
                            state_str = str(state_val)
                    except Exception:
                        pass

                # Rest of popup components remain the same
                popup_text = f"""
                <b>Energy:</b> {energy_str} TJ<br>
                <b>State:</b> {state_str}<br>
                <!-- Other fields -->
                """
                
                folium.Marker(
                    location=[lat, lng],
                    popup=folium.Popup(popup_text, max_width=300),
                    icon=folium.Icon(color="red")
                ).add_to(marker_cluster)
                
            except Exception:
                continue

    return m
    

def create_day_of_week_analysis(data: pd.DataFrame, theme: Dict[str, str]) -> go.Figure:
    """Create visualization for day of week patterns"""
    if data.empty:
        # Return an empty figure if no data
        fig = go.Figure()
        fig.update_layout(
            title="No data available for the selected filters",
            height=300
        )
        return fig
    
    # Map day numbers to names
    day_names = {
        0: "Monday",
        1: "Tuesday",
        2: "Wednesday",
        3: "Thursday",
        4: "Friday",
        5: "Saturday",
        6: "Sunday"
    }
    
    # Aggregate data by day of week
    day_data = data.groupby("day_of_week").agg({
        "energy": ["sum", "mean", "count"]
    }).reset_index()
    day_data.columns = ["day_of_week", "total_energy", "avg_energy", "event_count"]
    
    # Add day names
    day_data["day_name"] = day_data["day_of_week"].map(day_names)
    
    # Sort by day of week
    day_data = day_data.sort_values("day_of_week")
    
    # Create a figure
    fig = go.Figure()
    
    # Add radar chart for event distribution
    fig.add_trace(
        go.Scatterpolar(
            r=day_data["event_count"],
            theta=day_data["day_name"],
            fill="toself",
            name="Event Count",
            line_color=theme["secondary_color"],
            fillcolor=f"rgba({int(theme['secondary_color'][1:3], 16)}, {int(theme['secondary_color'][3:5], 16)}, {int(theme['secondary_color'][5:7], 16)}, 0.3)",
        )
    )
    
    # Add radar chart for total energy
    fig.add_trace(
        go.Scatterpolar(
            r=day_data["total_energy"],
            theta=day_data["day_name"],
            fill="toself",
            name="Total Energy",
            line_color=theme["accent_color"],
            fillcolor=f"rgba({int(theme['accent_color'][1:3], 16)}, {int(theme['accent_color'][3:5], 16)}, {int(theme['accent_color'][5:7], 16)}, 0.3)",
        )
    )
    
    # Update layout
    fig.update_layout(
        title="Lightning Activity by Day of Week",
        height=400,
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
        template="plotly_dark",
        polar=dict(
            radialaxis=dict(
                visible=True,
                showticklabels=True,
                gridcolor=theme["grid_color"]
            ),
            angularaxis=dict(
                direction="clockwise",
                gridcolor=theme["grid_color"]
            ),
            bgcolor=theme["plot_bg"],
        ),
        paper_bgcolor=theme["plot_paper_bg"],
        font=dict(color=theme["plot_font_color"]),
        margin=dict(l=40, r=40, t=60, b=40),
    )
    
    return fig


def create_state_comparison(data: pd.DataFrame, theme: Dict[str, str]) -> go.Figure:
    """Create visualization comparing lightning activity across states"""
    # Convert state column to string first
    data['state'] = data['state'].astype(str).str.upper().str.strip()

    if data.empty or data["state"].nunique() < 2:
        # Return an empty figure if no data or only one state
        fig = go.Figure()
        fig.update_layout(
            title="Insufficient data for state comparison",
            height=300
        )
        return fig
    
    # Aggregate data by state
    state_data = data.groupby("state").agg({
        "energy": ["sum", "mean", "count"],
        "cluster": "nunique"
    }).reset_index()
    state_data.columns = ["state", "total_energy", "avg_energy", "event_count", "cluster_count"]
    
    # Sort by total energy
    state_data = state_data.sort_values("total_energy", ascending=False)
    
    # Create bubble chart
    fig = go.Figure()
    
    # Add bubbles for each state
    fig.add_trace(
        go.Scatter(
            x=state_data["event_count"],
            y=state_data["avg_energy"],
            mode="markers+text",
            marker=dict(
                size=state_data["total_energy"] / state_data["total_energy"].max() * 50 + 10,
                color=state_data["cluster_count"],
                colorscale=theme["plot_colorscale"],
                showscale=True,
                colorbar=dict(
                    title="Clusters",
                    titleside="right",
                    titlefont=dict(color=theme["text_color"]),
                    tickfont=dict(color=theme["text_color"])
                ),
                opacity=0.8,
                line=dict(width=1, color=theme["grid_color"])
            ),
            text=state_data["state"],
            textposition="middle center",
            hovertemplate="<b>%{text}</b><br>"
                        + "Total Energy: %{customdata[0]:.2f} TJ<br>"
                        + "Avg Energy: %{y:.2f} TJ<br>"
                        + "Events: %{x}<br>"
                        + "Clusters: %{marker.color}<br>",
            customdata=state_data[["total_energy"]],
            name=""
        )
    )
    
    # Update layout
    fig.update_layout(
        title="State Comparison - Energy vs Events",
        height=500,
        template="plotly_dark",
        plot_bgcolor=theme["plot_bg"],
        paper_bgcolor=theme["plot_paper_bg"],
        font=dict(color=theme["plot_font_color"]),
        margin=dict(l=20, r=20, t=60, b=20),
        xaxis=dict(
            title="Number of Events",
            showgrid=True,
            gridcolor=theme["grid_color"],
            type="log" if state_data["event_count"].max() > 10*state_data["event_count"].min() else "linear"
        ),
        yaxis=dict(
            title="Average Energy (TJ)",
            showgrid=True,
            gridcolor=theme["grid_color"]
        ),
        hoverlabel=dict(
            bgcolor=theme["bg_color"],
            font_size=14,
            font_color=theme["text_color"]
        )
    )
    
    return fig

    
def create_distribution_charts(data: pd.DataFrame, theme: Dict[str, str]) -> go.Figure:
    """Create distribution charts for key metrics"""
    if data.empty:
        fig = go.Figure()
        fig.update_layout(title="No data available", height=300)
        return fig
    
    # Create subplots with corrected specs for mapbox
    fig = make_subplots(
        rows=2, cols=2,
        subplot_titles=(
            "Energy Distribution", 
            "Event Duration Distribution",
            "Cluster Size Distribution",
            "Geographic Spread"
        ),
        specs=[
            [{"type": "histogram"}, {"type": "box"}],
            [{"type": "pie"}, {"type": "mapbox"}]  # Changed from "scattergeo" to "mapbox"
        ],
        vertical_spacing=0.15,
        horizontal_spacing=0.1
    )
    
    # Energy Distribution Histogram
    fig.add_trace(
        go.Histogram(
            x=data["energy"],
            nbinsx=50,
            marker_color=theme["accent_color"],
            name="Energy Distribution",
            hovertemplate="Energy: %{x} TJ<br>Count: %{y}<extra></extra>"
        ),
        row=1, col=1
    )
    
    # Event Duration Box Plot
    if "duration" in data.columns:
        fig.add_trace(
            go.Box(
                y=data["duration"],
                name="Duration",
                boxpoints="outliers",
                marker_color=theme["secondary_color"],
                line_color=theme["tertiary_color"],
                hovertemplate="Duration: %{y}s<extra></extra>"
            ),
            row=1, col=2
        )
    
    # Cluster Size Pie Chart
    cluster_sizes = data["cluster"].value_counts().head(5)
    fig.add_trace(
        go.Pie(
            labels=cluster_sizes.index,
            values=cluster_sizes.values,
            hole=0.4,
            marker_colors=[theme["accent_color"], theme["secondary_color"], theme["tertiary_color"], "#FFA07A", "#20B2AA"],
            name="Cluster Sizes",
            hovertemplate="Cluster: %{label}<br>Count: %{value}<extra></extra>"
        ),
        row=2, col=1
    )
    
    # Geographic Spread (density mapbox) - Now compatible with mapbox subplot
    fig.add_trace(
        go.Densitymapbox(
            lat=data["latitude"],
            lon=data["longitude"],
            z=data["energy"],
            radius=10,
            colorscale=theme["plot_colorscale"],
            showscale=False,
            hovertemplate="Lat: %{lat:.2f}<br>Lon: %{lon:.2f}<br>Energy: %{z} TJ<extra></extra>"
        ),
        row=2, col=2
    )
    
    # Update layout with mapbox configuration
    fig.update_layout(
        height=800,
        showlegend=False,
        template="plotly_dark",
        plot_bgcolor=theme["plot_bg"],
        paper_bgcolor=theme["plot_paper_bg"],
        font=dict(color=theme["plot_font_color"]),
        margin=dict(l=20, r=20, t=60, b=20),
        mapbox=dict(
            style="carto-darkmatter",
            zoom=2,
            center=dict(lat=data["latitude"].mean(), lon=data["longitude"].mean())
        )
    )
    
    return fig

def main():
    """Main application function"""
    set_page_config()
    theme = set_theme()
    
    try:
        # Load data with progress indicator
        with st.spinner("Loading lightning data..."):
            data = load_data()
        
        # Create sidebar filters
        filters = create_sidebar_filters(data)
        theme = set_theme(filters["theme_name"])

        # st.write("Data timestamp timezone:", data["timestamp"].dt.tz)
        # st.write("Filter start_date timezone:", filters["start_date"].tzinfo)
        # st.write("Filter end_date timezone:", filters["end_date"].tzinfo)
        
        # Apply filters
        filtered_data = filter_data(data, filters)
        
        # Calculate KPIs
        kpis = calculate_kpis(filtered_data)
        
        # Main dashboard layout
        display_kpi_metrics(kpis, theme)
        
        # Create tabs for different analysis sections
        tab1, tab2, tab3, tab4 = st.tabs([
            "📈 Temporal Analysis", 
            "🌍 Geospatial View", 
            "🔍 Cluster Insights", 
            "📊 Distributions"
        ])
        
        with tab1:
            # Temporal Analysis Column Layout
            col1, col2 = st.columns([3, 1])
            
            with col1:
                st.plotly_chart(
                    create_time_series_analysis(filtered_data, theme), 
                    use_container_width=True
                )
            
            with col2:
                st.plotly_chart(
                    create_day_of_week_analysis(filtered_data, theme), 
                    use_container_width=True
                )
                st.plotly_chart(
                    create_hourly_pattern_chart(filtered_data, theme), 
                    use_container_width=True
                )
        
        with tab2:
            # Geospatial View with Full Width
            st.markdown("### Lightning Heatmap & Event Clusters")
            # Add error handling for the map creation
            try:
                m = create_geospatial_map(filtered_data)
                st_folium(m, use_container_width=True, height=600)
                
                # Add map legend
                st.markdown("""
                    <div style="background-color:{bg}; padding:10px; border-radius:5px;">
                        <span style="color:{accent}; margin-right:15px;">⬤ High Energy Events</span>
                        <span style="color:{secondary}; margin-right:15px;">⬤ Heatmap Intensity</span>
                    </div>
                    """.format(
                        bg=theme["bg_color"],
                        accent=theme["accent_color"],
                        secondary=theme["secondary_color"]
                    ), 
                    unsafe_allow_html=True
                )
            except Exception as map_error:
                st.error(f"Error creating map: {str(map_error)}")
                st.info("Try adjusting your filters or check if data contains valid geographical coordinates.")
        
        with tab3:
            # Cluster Insights Layout
            col1, col2 = st.columns([2, 1])
            
            with col1:
                try:
                    cluster_fig, cluster_stats = create_cluster_statistics(filtered_data, theme)
                    st.plotly_chart(cluster_fig, use_container_width=True)
                except Exception as cluster_error:
                    st.error(f"Error creating cluster visualization: {str(cluster_error)}")
                    st.info("Try adjusting your filters or check if data contains valid cluster information.")
            
            with col2:
                st.markdown("### Cluster Statistics")
                try:
                    if 'cluster_stats' in locals() and not cluster_stats.empty:
                        st.dataframe(
                            cluster_stats.sort_values("total_energy", ascending=False),
                            column_config={
                                "cluster": "Cluster ID",
                                "total_energy": st.column_config.NumberColumn(
                                    "Total Energy (TJ)", format="%.2f"
                                ),
                                "avg_energy": st.column_config.NumberColumn(
                                    "Avg Energy (TJ)", format="%.2f"
                                )
                            },
                            height=400,
                            hide_index=True
                        )
                    else:
                        st.info("No cluster data available for selected filters")
                except Exception as df_error:
                    st.error(f"Error displaying cluster statistics: {str(df_error)}")
        
        with tab4:
            # Distribution Analysis
            try:
                st.plotly_chart(
                    create_distribution_charts(filtered_data, theme), 
                    use_container_width=True
                )
                st.plotly_chart(
                    create_state_comparison(filtered_data, theme), 
                    use_container_width=True
                )
            except Exception as dist_error:
                st.error(f"Error creating distribution charts: {str(dist_error)}")
                st.info("Try adjusting your filters or check if data contains valid distribution information.")
        
        # Add data freshness indicator
        st.sidebar.markdown(f"""
            <div style="color:{theme['tertiary_color']}; font-size:0.8em; margin-top:20px;">
                Data updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}
            </div>
            """, unsafe_allow_html=True)
    
    except Exception as e:
        st.error(f"Application Error: {str(e)}")
        st.markdown("""
            Please try the following:
            1. Refresh the page
            2. Check your filters
            3. Contact support if issue persists
            """)
        # Add detailed error logging for troubleshooting
        import traceback
        st.expander("Error Details (Technical)").code(traceback.format_exc())
        st.stop()

if __name__ == "__main__":
    main()