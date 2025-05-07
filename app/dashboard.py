import folium
import streamlit as st
import pandas as pd
import duckdb as db
import numpy as np
import plotly.express as px
import plotly.graph_objects as go

from plotly.subplots import make_subplots
from datetime import datetime, timedelta
from typing import List, Tuple, Dict, Any, Optional
from streamlit_folium import st_folium
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

class DataLoader:
    """Handles data loading and caching"""
    
    @staticmethod
    @st.cache_data
    def load_lightning_data() -> pd.DataFrame:
        """Load lightning data from DuckDB"""
        try:
            conn = db.connect("data/Load/glmFlash.db", read_only=True)
            query = """
                SELECT *, 
                       EXTRACT(HOUR FROM timestamp) AS hour,
                       EXTRACT(DAY FROM timestamp) AS day,
                       EXTRACT(MONTH FROM timestamp) AS month,
                       EXTRACT(WEEK FROM timestamp) AS week,
                       EXTRACT(DOW FROM timestamp) AS day_of_week
                FROM vw_flash 
                ORDER BY timestamp DESC 
            """
            df = conn.execute(query).fetchdf()
            conn.close()

            if df.empty:
                raise ValueError("No data found in database")

            df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
            return df.astype({
                'latitude': 'float32',
                'longitude': 'float32',
                'energy': 'float32'
            })

        except Exception as e:
            st.error(f"Database error: {str(e)}")
            return pd.DataFrame()

    @staticmethod
    @st.cache_data
    def load_cluster_data() -> pd.DataFrame:
        """Load and preprocess cluster outage data from parquet including type conversions"""
        try:
            df = pd.read_parquet("src/data/Analytics/clustering_results.parquet")
            
            # Define type conversion mapping (modify based on your schema)
            datetime_cols = ['Datetime Event Began', 'Datetime Restoration', 'start_time', 'end_time']  # Ex. datetime columns
            categorical_cols = ['state', 'county']                                                      # Ex. categorical columns
            numeric_cols = ['duration', 'min_customers']                                                # Ex. numeric cols
            
            # Convert datetime columns
            for col in datetime_cols:
                if col in df.columns:
                    try:
                        df[col] = pd.to_datetime(df[col], errors='coerce')
                    except Exception as dt_error:
                        st.warning(f"Datetime conversion failed for {col}: {str(dt_error)}")
            
            # Convert categorical columns
            for col in categorical_cols:
                if col in df.columns:
                    df[col] = df[col].astype('category')
            
            # Ensure numeric columns
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            return df
        
        except Exception as e:
            st.error(f"Cluster data error: {str(e)}")
            st.error("Returning empty DataFrame. Some functionality may be limited.")
            return pd.DataFrame()


class ThemeManager:
    """Manages UI theme configuration"""
    
    @staticmethod
    def set_theme(theme_name: str = "carbon") -> Dict:
        """Apply selected theme to the app"""
        theme = THEMES.get(theme_name, THEMES["carbon"])
        ThemeManager._inject_css(theme)
        return theme

    @staticmethod
    def _inject_css(theme: Dict):
        """Inject CSS styles for the selected theme"""
        st.markdown(f"""
        <style>
            .reportview-container {{
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

        
class VisualizationEngine:
    """Creates and manages data visualizations"""
    
    def __init__(self, theme: Dict):
        self.theme = theme
    
    def _base_layout(self):
        """Define the base layout for plots"""
        return {
            "title_font_family": "Arial",
            "font_family": "Arial",
            "plot_bgcolor": self.theme.get("plot_bg", "white"),
            "paper_bgcolor": self.theme.get("plot_paper_bg", "white"),
            "font_color": self.theme.get("plot_font_color", "black"),
            "title_font_color": self.theme.get("plot_font_color", "black"),
            "margin": dict(l=60, r=30, t=50, b=50),
            "height": 600
        }
    
    def _empty_figure(self, message: str) -> go.Figure:
        """Create an empty figure with message"""
        fig = go.Figure()
        fig.add_annotation(
            text=message,
            xref="paper", yref="paper",
            x=0.5, y=0.5, showarrow=False,
            font=dict(size=16, color=self.theme.get("text_color", "black"))
        )
        fig.update_layout(**self._base_layout())
        return fig
    
    def create_time_series(self, data: pd.DataFrame) -> go.Figure:
        """Create time series analysis visualization"""
        if data.empty:
            return self._empty_figure("No data available")
        
        daily = data.resample('D', on='timestamp').agg(
            total_energy=('energy', 'sum'),
            event_count=('energy', 'count')
        ).reset_index()
        
        fig = make_subplots(rows=2, cols=1, shared_xaxes=True)
        
        # Energy plot
        fig.add_trace(go.Scatter(
            x=daily["timestamp"],
            y=daily["total_energy"],
            name="Energy",
            line=dict(color=self.theme["accent_color"])
        ), row=1, col=1)
        
        # Event count plot
        fig.add_trace(go.Bar(
            x=daily["timestamp"],
            y=daily["event_count"],
            name="Events",
            marker_color=(self.theme["secondary_color"])
        ), row=2, col=1)
        
        fig.update_layout(**self._base_layout())
        return fig

    def create_metrics_overview(self, data: pd.DataFrame) -> go.Figure:
        """Create overview of key metrics as gauges and indicators"""
        if data.empty:
            return self._empty_figure("No metrics available")
        
        # Calculate key metrics
        total_energy = data['energy'].sum()
        max_energy = data['energy'].max()
        event_count = len(data)
        avg_energy = data['energy'].mean()
        
        # Create a figure with subplots for metrics
        fig = make_subplots(
            rows=2, 
            cols=2,
            specs=[[{"type": "indicator"}, {"type": "indicator"}],
                   [{"type": "indicator"}, {"type": "indicator"}]],
            subplot_titles=("Total Energy (TJ)", "Max Energy (TJ)", 
                           "Event Count", "Avg Energy (TJ)")
        )
        
        # Add indicator traces
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=total_energy,
                number={"font": {"color": self.theme.get("accent_color")}},
                delta={"reference": total_energy * 0.9}
            ),
            row=1, col=1
        )
        
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=max_energy,
                number={"font": {"color": self.theme.get("accent_color")}}
            ),
            row=1, col=2
        )
        
        fig.add_trace(
            go.Indicator(
                mode="number",
                value=event_count,
                number={"font": {"color": self.theme.get("accent_color")}}
            ),
            row=2, col=1
        )
        
        fig.add_trace(
            go.Indicator(
                mode="gauge+number",
                value=avg_energy,
                gauge={
                    "axis": {"range": [0, max_energy]},
                    "bar": {"color": self.theme.get("accent_color")},
                    "threshold": {
                        "line": {"color": self.theme.get("secondary_color"), "width": 4},
                        "thickness": 0.75,
                        "value": avg_energy * 1.5
                    }
                }
            ),
            row=2, col=2
        )
        
        # Update layout
        layout = self._base_layout()
        layout["height"] = 500  # Adjust height for this specific visualization
        fig.update_layout(**layout)
        
        return fig
    
    def create_geospatial_map(self, lightning_data: pd.DataFrame, outage_data: pd.DataFrame) -> folium.Map:
        """Create interactive geospatial map with lightning heatmap and outage clusters"""
        # Create base map with dark theme
        m = folium.Map(location=[39.8283, -98.5795], zoom_start=4, tiles="cartodb dark_matter")

        # --- Lightning HeatMap (FIXED) ---
        if not lightning_data.empty:
            # Clean data: Drop NaN values in coordinates/energy
            heatmap_data = lightning_data[['latitude', 'longitude', 'energy']].dropna()
            
            # Only proceed if we have valid data points
            if not heatmap_data.empty:
                HeatMap(
                    heatmap_data.values.tolist(),  # Now guaranteed no NaNs
                    radius=15,
                    blur=20,
                    max_zoom=13,
                    name='Lightning Activity'
                ).add_to(m)

        # --- Outage Clusters (Existing Code) ---
        if not outage_data.empty:
            outage_data = outage_data.copy()
            outage_data['cluster'] = outage_data['cluster'].astype(str).str.split('.').str[0]
            
            cluster_groups = outage_data.groupby('cluster').agg({
                'latitude': 'mean',
                'longitude': 'mean',
                'mean_customers': 'sum',
                'event_id': 'count',
                'duration': 'mean'
            }).reset_index().dropna()  # Already handles NaNs

            # Dynamic radius calculation based on impact
            max_impact = cluster_groups['mean_customers'].max()
            min_impact = cluster_groups['mean_customers'].min()
            base_radius = 500  # Base radius in meters

            outage_layer = folium.FeatureGroup(name='Outage Impact Zones')
            
            for _, cluster in cluster_groups.iterrows():
                try:
                    # Calculate normalized impact (0-1 range)
                    normalized_impact = ((cluster['mean_customers'] - min_impact) 
                                    / (max_impact - min_impact + 1e-9))
                    
                    # Scale radius between base_radius and 3*base_radius
                    dynamic_radius = base_radius * (1 + 2 * normalized_impact)
                    
                    folium.Circle(
                        location=[cluster['latitude'], cluster['longitude']],
                        radius=dynamic_radius,
                        color=self.theme['secondary_color'],
                        fill=True,
                        fill_color=self.theme['tertiary_color'],
                        fill_opacity=0.3,
                        weight=2,
                        popup=f"""
                        <strong>Cluster {cluster['cluster']}</strong><br>
                        State: {cluster.get('state', 'N/A')}<br>
                        County: {cluster.get('county', 'N/A')}<br>
                        Affected Customers: {cluster['mean_customers']:,.0f}<br>
                        Outage Events: {cluster['event_id']}<br>
                        Avg Duration: {cluster['duration']:.1f} hours
                        """
                    ).add_to(outage_layer)
                except KeyError as e:
                    print(f"Missing column in data: {e}")
                    continue
            
            outage_layer.add_to(m)

        # Add layer control and fit bounds
        folium.LayerControl(position='topright').add_to(m)
        
        # Set map bounds
        locations = []
        if not lightning_data.empty:
            locations.extend(lightning_data[['latitude', 'longitude']].values.tolist())
        if not outage_data.empty:
            locations.extend(cluster_groups[['latitude', 'longitude']].values.tolist())
        
        if locations:
            m.fit_bounds([[min(lat for lat, lon in locations), 
                        min(lon for lat, lon in locations)],
                        [max(lat for lat, lon in locations), 
                        max(lon for lat, lon in locations)]])

        return m


class Dashboard:
    """Main dashboard application class"""
    
    def __init__(self):
        self.setup_page_config()
        self.initialize_session_state()
        self.load_data()
        self.setup_theme()
        self.setup_layout()
    
    def setup_page_config(self):
        """Configure page settings"""
        st.set_page_config(
            page_title="Lightning Analytics Dashboard",
            page_icon="⚡",
            layout="wide"
        )
    
    def initialize_session_state(self):
        """Initialize session state variables"""
        if 'theme' not in st.session_state:
            st.session_state.theme = "carbon"
        if 'data_filtered' not in st.session_state:
            st.session_state.data_filtered = None
    
    def load_data(self):
        """Load and prepare data for dashboard"""
        self.lightning_data = DataLoader.load_lightning_data()
        self.cluster_data = DataLoader.load_cluster_data()
        
        # Initialize filtered data
        if st.session_state.data_filtered is None and not self.lightning_data.empty:
            st.session_state.data_filtered = self.lightning_data.copy()
    
    def setup_theme(self):
        """Setup theme manager and visualization engine"""
        self.current_theme = ThemeManager.set_theme(st.session_state.theme)
        self.vis_engine = VisualizationEngine(self.current_theme)
    
    def setup_layout(self):
        """Create the main dashboard layout"""
        # Header
        st.title("⚡ Lightning Analytics Dashboard")
        
        # Sidebar
        self.setup_sidebar()
        
        # Main content
        if self.lightning_data.empty:
            st.error("No data available. Please check database connection.")
            return
        
        # Dashboard metrics
        self.create_metrics_section()
        
        # Main visualizations
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Time Series Analysis")
            fig_time = self.vis_engine.create_time_series(st.session_state.data_filtered)
            st.plotly_chart(fig_time, use_container_width=True)
        
        with col2:
            st.subheader("Key Metrics Overview")
            fig_metrics = self.vis_engine.create_metrics_overview(st.session_state.data_filtered)
            st.plotly_chart(fig_metrics, use_container_width=True)
        
        # Map visualization
        st.subheader("Geospatial Distribution")
        m = self.vis_engine.create_geospatial_map(
            st.session_state.data_filtered, # Lightning data
            self.cluster_data)              # Outage cluster data
        st_folium(m, width=1400, height=600)

        st.table(self.cluster_data.head())

    def setup_sidebar(self):
        """Setup sidebar with filters and settings"""
        st.sidebar.title("Dashboard Settings")
        
        # Theme selector
        st.sidebar.subheader("Theme")
        selected_theme = st.sidebar.selectbox(
            "Select Theme",
            options=list(THEMES.keys()),
            index=list(THEMES.keys()).index(st.session_state.theme)
        )
        
        # Apply theme if changed
        if selected_theme != st.session_state.theme:
            st.session_state.theme = selected_theme
            st.rerun()

        st.sidebar.markdown("---")
        
        # Data filters
        st.sidebar.subheader("Data Filters")
        
        if not self.cluster_data.empty:
            # Date range filter
            min_date = self.cluster_data['Datetime Event Began'].min().date()
            max_date = self.cluster_data['Datetime Event Began'].max().date()
            
            start_date = st.sidebar.date_input(
                "Start Date", 
                value=min_date,
                min_value=min_date,
                max_value=max_date
            )
            
            end_date = st.sidebar.date_input(
                "End Date", 
                value=max_date,
                min_value=min_date,
                max_value=max_date
            )
            
            # Energy threshold filter
            min_energy = float(self.lightning_data['energy'].min())
            max_energy = float(self.lightning_data['energy'].max())
            
            energy_range = st.sidebar.slider(
                "Energy Range (TJ)", 
                min_value=min_energy,
                max_value=max_energy,
                value=(min_energy, max_energy)
            )
            
            # Apply filters button
            if st.sidebar.button("Apply Filters"):
                # filtered_data = self.lightning_data.copy()
                filtered_data = self.cluster_data.copy()

                # Apply date filter
                filtered_data = filtered_data[
                    (filtered_data['Datetime Event Began'].dt.date >= start_date) & 
                    (filtered_data['Datetime Event Began'].dt.date <= end_date)
                ]
                
                # Apply energy filter
                filtered_data = filtered_data[
                    (filtered_data['energy'] >= energy_range[0]) & 
                    (filtered_data['energy'] <= energy_range[1])
                ]
                
                st.session_state.data_filtered = filtered_data
                st.rerun()

            st.sidebar.markdown("---")

            # Help section
            with st.sidebar.expander("Help & Information"):
                st.markdown("""
                    **Dashboard Features:**
                    - Interactive filters for data exploration
                    - Time series analysis of lightning activities
                    - Geospatial patterns and hotspot identification
                    - Historical outages with cluster analysis
    
                    The data shows lightning strike events with energy measurements and location data.
                """)
            
            st.sidebar.markdown("---")


            # Contact information
            st.sidebar.title("Contact & Support")
    
            # Ko-Fi donation button
            st.sidebar.markdown(f"""
            <div style="text-align: center; margin: 20px 0;">
                <a href="https://ko-fi.com/bayoadejare" target="_blank" style="text-decoration: none;">
                    <button style="background-color: #72A5F2; 
                            color: #FFFFFF; 
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
    
    def create_metrics_section(self):
        """Create the metrics section with KPIs"""
        data = st.session_state.data_filtered
        
        if data.empty:
            st.warning("No data available after filtering.")
            return
            
        # Calculate KPIs
        total_events = len(data)
        total_energy = data['energy'].sum()
        avg_energy = data['energy'].mean()
        date_range = f"{data['timestamp'].min().date()} to {data['timestamp'].max().date()}"
        
        # Display metrics
        st.subheader("Key Performance Indicators")
        cols = st.columns(4)
        
        with cols[0]:
            st.metric("Total Events", f"{total_events:,}")
            
        with cols[1]:
            st.metric("Total Energy (TJ)", f"{total_energy:.2f}")
            
        with cols[2]:
            st.metric("Avg Energy per Event (TJ)", f"{avg_energy:.2f}")
            
        with cols[3]:
            st.metric("Date Range", date_range)


if __name__ == "__main__":
    dashboard = Dashboard()