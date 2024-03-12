import os
import types
from typing import List
from urllib.parse import quote_plus

import altair as alt
import duckdb
import pandas as pd
import plotly.express as px
import requests
import streamlit as st
import streamlit.components.v1 as components
from bs4 import BeautifulSoup
from google_images_search import GoogleImagesSearch
from googleapiclient.errors import HttpError
from pandas import DataFrame
from streamlit_modal import Modal
from streamlit_searchbox import st_searchbox

START_YEAR = 2020
END_YEAR = 2023

st.set_page_config(
    page_title="NBA Players Stats Dashboard",
    page_icon="🏀",
    layout="wide",
    initial_sidebar_state="expanded",
)

alt.themes.enable("dark")

# Define offensive profiles and their associated stats
offensive_profiles = {
    "Creator/Facilitator": ["AST%", "USG%", "AST/TOV"],
    "Pure Shooter/Stretcher": ["3P%", "3PAr", "eFG%", "USG%"],
    "Paint Threat": ["%FGA 0-3", "ORB%", "eFG%"],
    "Slasher": ["%FGA 0-3", "%FGA 3-10", "%FGA 10-16", "FTr", "%FGA 3P"],
    "Versatile Scorer": ["std_areas_FGA", "USG%", "FG"],
}

# Define defensive profiles and their associated stats
defensive_profiles = {
    "Paint Protector/Rim Defender": ["DRB%", "BLK%", "DBPM"],
    "Perimeter Lockdown Defender": ["STL%", "DBPM"],
    "Switchable Defender": ["DRB%", "BLK%", "DBPM ranked", "DWS"],
    "Rebounding Specialist": ["TRB%", "DRB%"],
}

# API
gis = GoogleImagesSearch("AIzaSyC9hnhOztmyJ0S3uLueplwCtyBT3q3OQWY", "86df1f2dbf516493a")


con = duckdb.connect()

# Loop over the range of years
for year in range(START_YEAR, END_YEAR + 1):
    # Load data for the current year
    players_stats = pd.read_csv(
        f"datasets/combined/regular_dataset_{year}_{year + 1}.csv"
    )
    players_stats_ranked = pd.read_csv(
        f"datasets/combined/ranked_dataset_{year}_{year + 1}.csv"
    )

    # Create tables in DuckDB
    con.register("players_stats_" + str(year) + "_" + str(year + 1), players_stats)
    con.register(
        "players_stats_ranked_" + str(year) + "_" + str(year + 1), players_stats_ranked
    )

    # Get the list of all players
    all_players_query = con.execute(
        f"SELECT player FROM players_stats_{year}_{year + 1} ORDER BY player"
    ).fetchall()
    all_players = [row[0] for row in all_players_query]

    all_players_df = pd.DataFrame({"player": all_players})
    con.register("all_players_" + str(year) + "_" + str(year + 1), all_players_df)

stats_list = players_stats.columns.tolist()

ranking_east_2024 = pd.read_csv("datasets/ranking/ranking_east_2023_2024.csv")
ranking_west_2024 = pd.read_csv("datasets/ranking/ranking_west_2023_2024.csv")

with open("docs/homepage.txt", "r") as file:
    homepage_text = file.read()

homepage = f"""
<div style='text-align: justify;'>
{homepage_text}
</div>
"""


def search_player(player: str) -> List[any]:
    if player:
        player_lower = player.lower()
        result = con.execute(
            f"SELECT player FROM players_stats_{year}_{year + 1} WHERE LOWER(player) LIKE '%{player_lower}%'"
        ).fetchall()
        return [row[0] for row in result]
    else:
        return []


## function to create the pie + stats
def compare_player(player, stat, ranked_stat):
    selected_player_comparison = st_searchbox(
        search_player,
        key="player_comparison_searchbox",
        label="Select a player",
        default_options=all_players,
    )

    player_compared = selected_player_comparison.replace("'", "''")
    col1, col2, col3 = st.columns([1, 2, 2])  # Adjusting widths relative to each other

    # Header row for the table
    col1.markdown("<p style='text-align:center;'>Stat</p>", unsafe_allow_html=True)
    col2.markdown("<p style='text-align:center;'>{player}</p>", unsafe_allow_html=True)
    col3.markdown(
        "<p style='text-align:center;'>{player_compared}</p>", unsafe_allow_html=True
    )

    with col1:
        st.write("<br>", unsafe_allow_html=True)
        st.write("", unsafe_allow_html=True)
        st.write("<br>", unsafe_allow_html=True)
        st.write("<p style='text-align:center;'>Stat</p>", unsafe_allow_html=True)
    with col2:
        st.write("<p><hr><p style='text-align:center;'>0</p>", unsafe_allow_html=True)
        st.write("")
    with col3:
        st.write("<p><hr><p style='text-align:center;'>0</p>", unsafe_allow_html=True)
        st.write("")


def show_glossary(glossary):
    modal = Modal(
        "Glossary",
        key="glossary-modal",
        # Optional
        padding=20,
        max_width=744,
    )
    open_modal = st.button("Show glossary")
    if open_modal:
        modal.open()

    if modal.is_open():
        with modal.container():
            with open(glossary, "r") as file:
                content = file.read()
            st.markdown(content, unsafe_allow_html=True)


@st.cache_data
def search_images(keyword, num_images=5):
    try:
        search_params = {
            "q": keyword,
            "num": num_images,
            "safe": "high",
        }
        gis.search(search_params=search_params)

        # Download and save the URLs of the images
        image_urls = [image.url for image in gis.results()]

        return image_urls
    except HttpError as e:
        if e.resp.status == 429:
            st.error("Quota exceeded. Please try again later.")
        return []


def create_pie(player_name, stat, ranked_stat, year):
    player = player_name.replace("'", "''")
    percentile_query = con.execute(
        f"SELECT \"{ranked_stat}\" FROM players_stats_ranked_{year}_{year + 1} WHERE player = '{player}'"
    )
    percentile_result = percentile_query.fetchone()
    if percentile_result:
        percentile = percentile_result[0]
        fig = px.pie(
            values=[percentile * 10, 100 - percentile * 10],
            names=["Top", "Rest"],
            hole=0.5,
        )

        # Update layout to adjust the size and position of the chart
        fig.update_layout(
            width=300,
            height=300,
            margin=dict(l=20, r=20, t=20, b=20),
            grid=dict(rows=1, columns=2),
        )

        # Add annotation to position the pie chart
        fig.add_annotation(
            x=0.15,
            y=0.5,
            xref="paper",
            yref="paper",
            text="",
            showarrow=False,
        )

        # Add annotation to display "Top X%" inside the donut
        fig.add_annotation(
            x=0.5,
            y=0.5,
            text=f"Top {percentile*10}%<br>{stat}",  # Text to display inside the donut, REPLACE BY STAT
            showarrow=False,
            font=dict(size=15),  # Adjust font size as needed
        )

        # Customize colors and remove text from the chart
        fig.update_traces(
            marker=dict(colors=["red", "lightgrey"], line=dict(color="white", width=2)),
            textinfo="none",
            showlegend=False,
        )

        st.plotly_chart(fig, use_container_width=True)

        value_query = con.execute(
            f"SELECT \"{stat}\" FROM players_stats_{year}_{year + 1} WHERE player = '{player}'"
        )
        value_result = percentile_query.fetchone()
        st.write(
            f"### {player} averages {value_result[0]} {stat} per game in {year}-{year + 1}"
        )


with st.sidebar:
    st.title("NBA Players Stats Dashboard")

    year_list = range(START_YEAR, END_YEAR + 1)
    year = st.selectbox("Select a year", year_list, index=len(year_list) - 1)

    all_players_query = con.execute(
        f"SELECT player FROM all_players_{year}_{year + 1}"
    ).fetchall()
    all_players = [row[0] for row in all_players_query]

    selected_player = st_searchbox(
        search_player,
        key="player_searchbox",
        label="Select a player",
        default_options=all_players,
    )

    google_image_query = f"{selected_player} {year} NBA"
    num_images = 1
    image_url = search_images(google_image_query, num_images)

    if selected_player:
        if image_url:
            st.image(image_url, width=350, use_column_width=False)
        else:
            st.write(f"No image found for {selected_player}")
    else:
        st.write("Select a player")

    # Button to open the popup
    show_glossary("filtered_glossary.txt")


if selected_player:
    st.header(f"{selected_player} statistics in {year} ")

    # Replace single quotes with two single quotes in the player name
    escaped_player_name = selected_player.replace("'", "''")

    recap_stats = con.execute(
        f"SELECT * from players_stats_{year}_{year + 1} WHERE player = '{escaped_player_name}'"
    ).df()
    recap_stats.drop(["Unnamed: 0", "index", "level_0"], axis=1, inplace=True)
    if not recap_stats.empty:
        st.write(recap_stats)
    else:
        st.write(f"Player was inactive in {year}_{year + 1}")
else:
    st.markdown(f"# NBA Analysis Dashboard")
    st.markdown(f"<h4>{homepage}</h4>", unsafe_allow_html=True)
    if year:
        st.write(f"Rankings in {year}-{year + 1}")
    else:
        st.write("Rankings for the current year")
    col1, col2 = st.columns(2)
    if os.path.exists(f"datasets/ranking/ranking_west_{year}_{year + 1}.csv"):
        with col1:
            ranking_west_df = pd.read_csv(
                f"datasets/ranking/ranking_west_{year}_{year + 1}.csv"
            )
            st.dataframe(ranking_west_df)
    else:
        with col1:
            st.write("Ranking not available yet")

    if os.path.exists(f"datasets/ranking/ranking_east_{year}_{year + 1}.csv"):
        with col2:
            ranking_east_df = pd.read_csv(
                f"datasets/ranking/ranking_east_{year}_{year + 1}.csv"
            )
            st.dataframe(ranking_east_df)
    else:
        with col2:
            st.write("Ranking not available yet")


if selected_player and not recap_stats.empty:
    tab2, tab3 = st.tabs(
        [f"{selected_player} dashboard", "Compare with an other player"]
    )

if selected_player and not recap_stats.empty:
    with tab2:
        col1, col2 = st.columns(2)
        if selected_player:
            with col1:
                st.markdown(
                    "<p style='text-align:center; font-size: 2em;'>Offensive Profile 🔫</p>",
                    unsafe_allow_html=True,
                )
                escaped_player_name = selected_player.replace("'", "''")
                offensive_profile_cursor = con.execute(
                    f"SELECT \"Offensive Profile\" FROM players_stats_{year}_{year + 1} WHERE player = '{escaped_player_name}'"
                )
                offensive_profile_result = offensive_profile_cursor.fetchone()
                if offensive_profile_result:
                    offensive_profile = offensive_profile_result[0]
                    st.markdown(
                        f"""
                        <div style="display: flex; justify-content: center; align-items: center; height: 100%; border: 5px solid orange; padding: 10px; border-radius: 5px; background-color: #f9f9f9; text-align: center;">
                            <p style='font-size: 1.5em;'>{offensive_profile}</p>
                        </div>""",
                        unsafe_allow_html=True,
                    )
                    for profile, stats in offensive_profiles.items():
                        if offensive_profile == profile:
                            for stat in stats:
                                create_pie(
                                    selected_player, stat, f"{stat} ranked", year
                                )
        else:
            st.write("No offensive profile found for the selected player.")
        with col2:
            st.markdown(
                "<p style='text-align:center; font-size: 2em;'>Defensive Profile 🛡️</p>",
                unsafe_allow_html=True,
            )
            defensive_profile_cursor = con.execute(
                f"SELECT \"Defensive Profile\" FROM players_stats_{year}_{year + 1} WHERE player = '{escaped_player_name}'"
            )
            defensive_profile_result = defensive_profile_cursor.fetchone()
            if defensive_profile_result:
                defensive_profile = defensive_profile_result[0]
                st.markdown(
                    f"""
                        <div style="display: flex; justify-content: center; align-items: center; height: 100%; border: 5px solid orange; padding: 10px; border-radius: 5px; background-color: #f9f9f9; text-align: center;">
                            <p style='font-size: 1.5em;'>{defensive_profile}</p>
                        </div>""",
                    unsafe_allow_html=True,
                )
                for profile, stats in defensive_profiles.items():
                    if defensive_profile == profile:
                        for stat in stats:
                            create_pie(selected_player, stat, f"{stat} ranked", year)
            else:
                st.write("No defensive profile found for the selected player.")
        ## comparison with an other player
        with tab3:
            all_players_query1 = con.execute(
                f"SELECT player FROM all_players_{year}_{year + 1}"
            ).fetchall()
            all_players1 = [row[0] for row in all_players_query1]
            selected_player_comparison = st_searchbox(
                search_player,
                key="player_comparison_searchbox",
                label="Select a player",
                default_options=all_players1,
            )
            col1, col2, col3 = st.columns([1, 2, 2])
            if selected_player_comparison:
                player_compared = selected_player_comparison.replace("'", "''")
                # Fetch stat values for Player B
                values_player_B = {}
                for stat in stats_list[2:]:
                    value_stat_player_B = con.execute(
                        f"SELECT \"{stat}\" FROM players_stats_{year}_{year + 1} WHERE player = '{player_compared}'"
                    ).fetchone()
                    values_player_B[stat] = (
                        value_stat_player_B[0] if value_stat_player_B else None
                    )

                # Iterate over player stats and display them in the columns
                for index in range(len(stats_list[2:])):
                    stat = stats_list[3:][index]
                    with col1:
                        st.write("")
                        st.write(
                            f"<h5 style='text-align:center;'>{stat}</h5>",
                            unsafe_allow_html=True,
                        )
                        st.write("<hr>", unsafe_allow_html=True)

                    with col2:
                        value_stat_player_A = con.execute(
                            f"SELECT \"{stat}\" FROM players_stats_{year}_{year + 1} WHERE player = '{selected_player}'"
                        ).fetchone()
                        value_stat_player_A = (
                            value_stat_player_A[0] if value_stat_player_A else None
                        )  # Fetch the actual value
                        if index < 5:
                            COLOR_A = COLOR_B = "black"
                        elif value_stat_player_A > values_player_B[stat]:
                            COLOR_A = "green"
                            COLOR_B = "red"
                        elif value_stat_player_A < values_player_B[stat]:
                            COLOR_A = "red"
                            COLOR_B = "green"
                        else:
                            COLOR_A = COLOR_B = "black"
                        st.write("")
                        st.write(
                            f"<h5 style='text-align:center; color:{COLOR_A};'>{value_stat_player_A}</h5>",
                            unsafe_allow_html=True,
                        )
                        st.write("<hr>", unsafe_allow_html=True)

                    with col3:
                        st.write("")
                        st.write(
                            f"<h5 style='text-align:center; color:{COLOR_B};'>{values_player_B[stat]}</h5>",
                            unsafe_allow_html=True,
                        )
                        st.write("<hr>", unsafe_allow_html=True)
            else:
                st.write(f"Please select a player to compare to {selected_player}")
else:
    st.write("WIP")

con.close()
