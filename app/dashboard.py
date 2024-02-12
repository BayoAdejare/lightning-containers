import streamlit as st
import pandas as pd
import plotly.express as px

from typing import List, Tuple


def set_page_config():
    st.set_page_config(
        page_title="Lightning Containers Dashboard",
        page_icon=":lightning:",
        layout="wide",
        initial_sidebar_state="expanded",
    )
    st.markdown("<style> footer {visibility: hidden;} </style>", unsafe_allow_html=True)


@st.cache_data
def load_data() -> pd.DataFrame:
    # Create the SQL connection to flash_db as specified in your secrets file.
    conn = st.connection("flash_db", type="sql")
    # Query and display the data you inserted
    data = conn.query(
        "select id, ts_date as date_time, h_time as time, latitude, longitude, energy, time_period, cluster, state from vw_flash"
    )
    data["date_time"] = pd.to_datetime(data["date_time"])
    return data


def filter_data(data: pd.DataFrame, column: str, values: List[str]) -> pd.DataFrame:
    return data[data[column].isin(values)] if values else data


@st.cache_data
def calculate_kpis(data: pd.DataFrame) -> List[float]:
    total_energy = data["energy"].sum()
    formatted_total_energy = f"{total_energy:.2}"
    total_events = data["energy"].count()
    average_energy_per_event = f"{total_energy / total_events / 1000:.2}"
    unique_events = data["energy"].nunique()
    return [
        formatted_total_energy,
        total_events,
        average_energy_per_event,
        unique_events,
    ]


def display_kpi_metrics(kpis: List[float], kpi_names: List[str]):
    st.header("Flash Events")
    for i, (col, (kpi_name, kpi_value)) in enumerate(
        zip(st.columns(4), zip(kpi_names, kpis))
    ):
        col.metric(label=kpi_name, value=kpi_value)


def display_sidebar(data: pd.DataFrame) -> Tuple[List[str], List[str], List[str]]:
    st.sidebar.info(
        """
        + [Wep Map](https://lightning-containers.streamlit.io)
        + [GitHub Repo](https://github.com/bayoadejare/lightning-containers)
        """
    )
    st.sidebar.header("Filters")
    start_date = pd.Timestamp(
        st.sidebar.date_input("Start date", data["date_time"].min().date())
    )
    end_date = pd.Timestamp(
        st.sidebar.date_input("End date", data["date_time"].max().date())
    )

    states = sorted(data["state"].unique())
    selected_states = st.sidebar.multiselect("Selected State(s)", states, states)

    selected_time_period = st.sidebar.multiselect(
        "Time Period", data["time_period"].unique()
    )

    st.sidebar.title("Contact")
    st.sidebar.info(
        """
        Bayo Adejare: 
        [Blog](https://bayoadejare.medium.com) | [GitHub](https://github.com/bayoadejare) | [Twitter](https://twitter.com/bayoadejare) | [LinkedIn](https://www.linkedin.com/in/bayo-adejare)
        """
    )

    return selected_states, selected_time_period


def display_charts(data: pd.DataFrame):

    fig = px.area(
        data,
        x="date_time",
        y="energy",
        title="Radiance Energy Over Time",
        width=900,
        height=500,
    )

    fig.update_layout(margin=dict(l=20, r=20, t=50, b=20))
    fig.update_xaxes(rangemode="tozero", showgrid=False)
    fig.update_yaxes(rangemode="tozero", showgrid=True)

    st.plotly_chart(fig, use_container_width=True)


def display_tables(data: pd.DataFrame):

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("Top 10 Clusters")
        top_clusters = (
            data.groupby("cluster")["energy"]
            .sum()
            .reset_index()
            .sort_values("energy", ascending=False)
            .head(10)
        )
        formatted_top_clusters = top_clusters.style.format({"energy": "{:.2}".format})
        st.write(formatted_top_clusters)

    with col2:
        st.subheader("Top 10 Energy by Location")
        top_energy = (
            data.groupby(["latitude", "longitude"])["energy"]
            .sum()
            .reset_index()
            .sort_values("energy", ascending=False)
            .head(10)
        )
        formatted_top_energy = top_energy.style.format(
            {"energy": "{:.2}", "latitude": "{:.4f}", "longitude": "{:.4f}".format}
        )
        st.write(formatted_top_energy)

    with col3:
        st.subheader("Total Events by Clusters")
        total_sales_by_product_line = (
            data.groupby("latitude")["energy"].sum().reset_index()
        )
        formatted_total_sales_by_product_line = (
            total_sales_by_product_line.style.format({"energy": "{:.2}".format})
        )
        st.write(formatted_total_sales_by_product_line)


def main():
    set_page_config()

    data = load_data()

    st.title("⚡Lightning Containers Dashboard")

    selected_states, selected_time_period = display_sidebar(data)

    filtered_data = data.copy()
    filtered_data = filter_data(filtered_data, "state", selected_states)
    filtered_data = filter_data(filtered_data, "time_period", selected_time_period)

    kpis = calculate_kpis(filtered_data)
    kpi_names = [
        "Total Energy",
        "Total Events",
        "Average Energy per Event",
        "Unique Events",
    ]

    display_kpi_metrics(kpis, kpi_names)

    col1, col2 = st.columns(2)
    with col1:
        st.map(data=load_data(), color="#ffaa0088")

    with col2:
        display_charts(filtered_data)

    # display_tables(filtered_data)


if __name__ == "__main__":
    main()
