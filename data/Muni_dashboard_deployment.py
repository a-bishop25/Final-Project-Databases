import streamlit as st
import pandas as pd
import plotly.express as px
import io
import os
import numpy as np


# DATA SIMULATION
# This section defines the mappings and a function to simulate reading data from the files provided by the SQL analysis, treating the content as if
# it were read directly from physical CSV files.


# A map that connects a short, descriptive key to the original file name.
# This makes the code cleaner than using the long file paths everywhere.
DATA_FILE_PATHS = {
    'high_volume_issuers': 'Finds specific combination of issuer type__city or authority and state that have issued greater than 50 bonds',
    'credit_sentiment': 'avg_credit_rating_outlook_across_market_ results_by_ year',
    'long_duration_trades': 'Multi table JOIN joins 4 tables__Bonds_Issuers_BondPurposes_and Trades to identify the most actively traded long durationrisky bonds_specifically issued by county governments for education purposes',
    'undervalued_bonds': 'Correlation subquery to list all bonds where the most recent trade price was below the avg trade price',
    'yield_spread': 'Calculate the yield spread between the trade yield and avg 10-year treasury rate',
}


# The single function responsible for retrieving the raw CSV-like text content.
# Since we can't read files directly in this environment, this function acts
# as a mock file system, returning the hardcoded string data for each key.
def getFileContent(key):
    """
    Simulates reading the raw CSV content for a given analysis key.

    This function replaces standard file I/O (like reading a .csv file)
    by returning the pre-loaded string content from our SQL analysis results.
    We use the 'key' (like 'high_volume_issuers') to find the corresponding data.
    """
    if key == 'high_volume_issuers':
        return """
"state_code","issuer_type","total_bonds_issued","avg_coupon_rate"
"TX","County","67","3.945"
"FL","State","79","3.933"
"FL","Authority","76","3.888"
"TX","State","61","3.888"
"CA","City","94","3.863"
"IL","County","158","3.854"
"TX","Authority","70","3.852"
"CA","Authority","155","3.812"
"FL","District","70","3.797"
"CA","District","161","3.783"
"TX","City","160","3.781"
"NY","County","79","3.745"
"IL","District","78","3.736"
"IL","Authority","69","3.718"
"FL","County","122","3.709"
"IL","City","80","3.702"
"NY","State","118","3.642"
"NY","City","127","3.571"
"""
    elif key == 'credit_sentiment':
        return """
"rating_year","outlook","total_ratings_in_year","average_sentiment_score"
"2024","Positive","17","100.00"
"2024","Negative","9","-100.00"
"2023","Positive","23","100.00"
"2023","Negative","6","-100.00"
"2022","Positive","14","100.00"
"2022","Negative","16","-100.00"
"2021","Positive","16","100.00"
"2021","Negative","14","-100.00"
"2020","Positive","9","100.00"
"2020","Negative","15","-100.00"
"""
    elif key == 'long_duration_trades':
        return """
"issuer_name","purpose_category","bond_duration","total_trades","average_trade_price"
"FL County #9","Education","9.03","16","105.13"
"FL County #7","Education","9.57","16","90.77"
"IL County #4","Education","9.35","12","99.83"
"IL County #4","Education","9.48","12","107.54"
"NY County #9","Education","9.53","10","104.37"
"FL County #4","Education","9.93","10","94.92"
"TX County #2","Education","9.55","6","89.78"
"""
    elif key == 'undervalued_bonds':
        return """
"bond_id","issuer_name","latest_trade_price","bond_historical_avg"
"BOND0004","NY Transportation Authority #5","113.57","113.84"
"BOND0004","NY Transportation Authority #5","113.57","113.84"
"BOND1048","IL Transit District #7","111.89","113.09"
"BOND1048","IL Transit District #7","111.89","113.09"
"BOND0380","State of NY","111.78","111.91"
"BOND0380","State of NY","111.78","111.91"
"BOND0781","CA Transit District #1","111.43","111.49"
"BOND0781","CA Transit District #1","111.43","111.49"
"BOND0280","CA City #7","111.13","111.59"
"BOND0280","CA City #7","111.13","111.59"
"BOND1353","TX Water District #10","110.66","110.77"
"BOND1353","TX Water District #10","110.66","110.77"
"BOND0760","IL County #11","110.45","110.87"
"BOND0760","IL County #11","110.45","110.87"
"""
    elif key == 'yield_spread':
        return """
"trade_id","issuer_name","trade_date","bond_yield","treasury_rate","yield_spread_bps"
2459,"IL City #3","2021-08-16","6.410","0.79","5.620"
10461,"IL City #3","2021-08-16","6.410","0.79","5.620"
2460,"IL City #3","2021-10-05","6.200","0.63","5.570"
10462,"IL City #3","2021-10-05","6.200","0.63","5.570"
13121,"State of NY","2021-06-19","6.050","0.60","5.450"
5119,"State of NY","2021-06-19","6.050","0.60","5.450"
10800,"FL Housing Authority #10","2020-05-19","6.370","0.95","5.420"
2798,"FL Housing Authority #10","2020-05-19","6.370","0.95","5.420"
13962,"IL County #4","2020-04-13","6.230","0.84","5.390"
5960,"IL County #4","2020-04-13","6.230","0.84","5.390"
1085,"FL Transit District #8","2020-07-10","5.840","0.53","5.310"
9087,"FL Transit District #8","2020-07-10","5.840","0.53","5.310"
9569,"State of NY","2020-01-23","5.900","0.60","5.300"
1567,"State of NY","2020-01-23","5.900","0.60","5.300"
5127,"IL County #11","2021-10-25","5.920","0.63","5.290"
5127,"IL County #11","2021-10-25","5.920","0.63","5.290"
"""
    return None

 # Loads all simulated CSV content into Pandas DataFrames and performs initial data cleaning. This is the core data processing pipeline. It iterates through all the keys,
# fetches the raw string content using getFileContent, converts that string into a DataFrame, and then applies specific column renaming and type
# conversions to prepare the data for plotting and analysis.
# Returns:
# dict: A dictionary where keys are the analysis names like 'credit_sentiment' and values are the cleaned pandas df's

# Use Streamlit's cache decorator to only run this expensive data loading/cleaning step once. This is not too much of an issue on our data but it comes in handy when you have loops calculating your data.
# saves it and uses it when other tabs user options are selected. Were not really doing much here beside the initially loading but if it was happening like
# if we made a function to calculate the probability of something like a bond avg increasing or decreasing, it would be useful. Again not happening here but good practifce to cache (i could be wrong lol)
@st.cache_data
def load_all_data():
    data_dict = {}

    for key, file_path in DATA_FILE_PATHS.items():
        try:
            # 1. Get the raw text content from the mock file system
            file_content = getFileContent(key)
            if file_content is None:
                st.warning(f"Warning: Data content for key '{key}' was not found. Skipping.")
                continue

            # 2. Use StringIO to treat the string content like a file in memory
            data_io = io.StringIO(file_content.strip())
            df = pd.read_csv(data_io)

            # --- Specific Data Cleaning and Conversion for each query result ---

            if key == 'high_volume_issuers':
                # Renaming columns to be more readable in code/plots
                df.columns = ['state_code', 'issuer_type', 'total_bonds_issued', 'avg_coupon_rate']
                # Ensuring numeric columns are correctly typed
                df['total_bonds_issued'] = pd.to_numeric(df['total_bonds_issued'], errors='coerce')
                df['avg_coupon_rate'] = pd.to_numeric(df['avg_coupon_rate'], errors='coerce')
                df.dropna(subset=['total_bonds_issued', 'avg_coupon_rate'], inplace=True)

            elif key == 'credit_sentiment':
                df.columns = ['rating_year', 'outlook', 'total_ratings_in_year', 'average_sentiment_score']
                df['rating_year'] = pd.to_numeric(df['rating_year'], errors='coerce').astype('Int64')
                df['total_ratings_in_year'] = pd.to_numeric(df['total_ratings_in_year'], errors='coerce')
                df['average_sentiment_score'] = pd.to_numeric(df['average_sentiment_score'], errors='coerce')
                df.dropna(subset=['rating_year', 'outlook', 'total_ratings_in_year'], inplace=True)

            elif key == 'long_duration_trades':
                df.columns = ['issuer_name', 'purpose_category', 'bond_duration', 'total_trades', 'average_trade_price']
                for col in ['bond_duration', 'total_trades', 'average_trade_price']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                df.dropna(inplace=True)

            elif key == 'undervalued_bonds':
                df.columns = ['bond_id', 'issuer_name', 'latest_trade_price', 'bond_historical_avg']
                for col in ['latest_trade_price', 'bond_historical_avg']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                # A quick fix: the original SQL query output sometimes duplicated rows, so we drop them here.
                df.drop_duplicates(subset=['bond_id', 'latest_trade_price'], inplace=True)
                df.dropna(inplace=True)

            elif key == 'yield_spread':
                df.columns = ['trade_id', 'issuer_name', 'trade_date', 'bond_yield', 'treasury_rate',
                              'yield_spread_bps']
                for col in ['trade_id', 'bond_yield', 'treasury_rate', 'yield_spread_bps']:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                # A quick fix: the original SQL query output sometimes duplicated trade IDs, so we drop them here.
                df.drop_duplicates(subset=['trade_id'], inplace=True)
                df.dropna(inplace=True)

            # Store the resulting clean DataFrame in our dictionary
            data_dict[key] = df

        except Exception as e:
            # If anything goes wrong with one file, we log it and continue with the others.
            st.error(f"Error processing data for analysis '{key}': {e}. This data won't be displayed.")

    return data_dict



# ==============================================================================
# 4. VISUALIZATION FUNCTIONS
# ==============================================================================
def display_issuance_vs_coupon(df):
    st.subheader("Avg Coupon Rate by Purpose")
    if not df.empty:
        fig = px.bar(df, x='purpose_category', y='average_coupon_rate_pct',
                     color='average_coupon_rate_pct', title='Coupon Rate (%) by Purpose')
        st.plotly_chart(fig, use_container_width=True)


def display_volume_by_state(df):
    st.subheader("Issuance Volume by State")
    if not df.empty:
        fig = px.sunburst(df, path=['state_code', 'issuer_type'], values='total_bonds_issued',
                          title='Bonds Issued by State & Type')
        st.plotly_chart(fig, use_container_width=True)


def display_state_comparison(df):
    """REQ MET: State Comparison with Error Bars"""
    st.subheader("State Yield Comparison (with Volatility)")
    if not df.empty:
        fig = go.Figure(data=go.Bar(
            x=df['state_code'],
            y=df['avg_yield'],
            error_y=dict(type='data', array=df['std_dev_yield'], visible=True)
        ))
        fig.update_layout(title="Average Yield by State (Error Bars = Std Dev)", yaxis_title="Yield (%)")
        st.plotly_chart(fig, use_container_width=True)


def display_time_series_macro(df):
    """REQ MET: Overlay prices/yields and economic indicators"""
    st.subheader("Yields vs Unemployment (Macro Overlay)")
    if not df.empty:
        # Dual axis plot
        fig = px.line(df, x='date', y='avg_yield', color='state_code', title="Bond Yields vs Unemployment Rate")
        # Add scatter for unemployment (using a simplified approach for overlay in Streamlit)
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Compare the yield trends above with the Unemployment Rate trends below.")
        fig2 = px.area(df, x='date', y='unemployment_rate', color='state_code', title="Unemployment Rate Over Time")
        st.plotly_chart(fig2, use_container_width=True)


def display_credit_sentiment(df):
    st.subheader("Credit Sentiment Trend")
    if not df.empty:
        df['rating_year'] = df['rating_year'].astype(int)
        fig = px.line(df, x='rating_year', y='average_sentiment_score', color='outlook',
                      markers=True, title='Sentiment Score Over Time')
        st.plotly_chart(fig, use_container_width=True)


def display_long_duration_liquidity(df):
    st.subheader("Long-Duration Trade Activity")
    if not df.empty:
        st.dataframe(df, use_container_width=True)


def display_undervalued_bonds(df):
    st.subheader("Undervalued Bonds")
    if not df.empty:
        st.dataframe(df, use_container_width=True)


def display_yield_spread(df):
    st.subheader("Yield Spread Risk")
    if not df.empty:
        fig = px.scatter(df, x='treasury_rate', y='bond_yield', color='yield_spread_bps',
                         hover_data=['issuer_name', 'trade_date'], title='Yield vs Treasury Rate')
        st.plotly_chart(fig, use_container_width=True)


# ==============================================================================
# 5. MAIN APP
# ==============================================================================
def main():
    st.set_page_config(layout="wide", page_title="Municipal Bond Dashboard")
    st.title("Municipal Bond Market Dashboard")

    engine = get_db_engine()
    if not engine: st.stop()

    # Load Data
    df_acp = load_data_from_db(engine, 'avg_coupon_by_purpose')
    df_sv = load_data_from_db(engine, 'issuance_volume_by_state_type')
    df_state = load_data_from_db(engine, 'state_yield_stats')  # NEW
    df_macro = load_data_from_db(engine, 'time_series_macro')  # NEW
    df_cs = load_data_from_db(engine, 'credit_sentiment')
    df_ldt = load_data_from_db(engine, 'long_duration_trades')
    df_uvb = load_data_from_db(engine, 'undervalued_bonds')
    df_ys = load_data_from_db(engine, 'yield_spread')

    # Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Market Overview", "Macro Trends", "Sentiment", "Liquidity & Value", "Risk Spreads"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1: display_issuance_vs_coupon(df_acp)
        with c2: display_volume_by_state(df_sv)
        st.divider()
        display_state_comparison(df_state)  # Added Requirement

    with tab2:
        display_time_series_macro(df_macro)  # Added Requirement

    with tab3:
        display_credit_sentiment(df_cs)

    with tab4:
        display_long_duration_liquidity(df_ldt)
        st.divider()
        display_undervalued_bonds(df_uvb)

    with tab5:
        display_yield_spread(df_ys)


if __name__ == "__main__":
    main()
    
