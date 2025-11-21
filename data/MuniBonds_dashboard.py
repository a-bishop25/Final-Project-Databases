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



# DASHBOARD DISPLAY FUNCTIONS
# Each function handles the visualization and interpretation for one specific query result. Let me know if you guys have any questions if you have issues making changes -MC
# ---------------------
# Displays the primary Key Performance Indicators (KPIs) at the top of the dashboard. It uses data from the High-Volume Issuers (Q2) analysis to calculate metrics
# like the total number of high-volume groups, the max bonds issued by a single group and the average coupon rate across all groups
def display_key_metrics(data_hv):
    st.subheader("Key Market Statistics (Based on High-Volume Issuers)")

    if data_hv.empty:
        st.warning("High-Volume Issuer data is not available to calculate key metrics.")
        return

    # Use Streamlit columns for a clean, side-by-side metric display. They're easy to work with if we want to play around with things
    col1, col2, col3 = st.columns(3)

    with col1:
        total_groups = len(data_hv)
        st.metric(label="High-Volume Issuer Groups Analyzed", value=f"{total_groups:,.0f}",
                  help="Total unique combinations of state and issuer type with >50 bonds issued.")

    with col2:
        # Find the group that issued the maximum number of bonds
        max_bonds = data_hv['total_bonds_issued'].max()
        max_row = data_hv.loc[data_hv['total_bonds_issued'].idxmax()]
        issuer_max = max_row.get('issuer_type', "N/A")
        state_max = max_row.get('state_code', "N/A")

        st.metric(label="Max Bonds Issued (Single Group)", value=f"{max_bonds:,.0f}",
                  help=f"Highest volume issuance by: {issuer_max} in {state_max}")

    with col3:
        avg_coupon = data_hv['avg_coupon_rate'].mean()
        st.metric(label="Market Avg. Coupon Rate (Filtered)", value=f"{avg_coupon:.3f}%",
                  help="Average coupon rate across all high-volume issuer groups.")

    st.markdown("---")

# Generates a scatter plot to show the relationship between issuance volume and coupon rate. This visualization directly addresses the SQL query that used
# the HAVING clause to help hyptohetical investors see which entities are the most aggressive borrowers (high volume/high rate)
def display_issuance_vs_coupon(data):
    st.subheader("📊 Issuance Volume vs. Average Coupon Rate")
    st.markdown(
        "This **scatter plot** identifies issuer groups (state + type) that are highly active (>50 bonds issued) *and* pay a high average coupon rate (>3.0%), highlighting aggressive or high-cost borrowing entities.")

    fig_scatter = px.scatter(
        data,
        x='total_bonds_issued',
        y='avg_coupon_rate',
        color='issuer_type',  # Color-code the points by the type of issuer (City, County, Authority, etc. we can change this. I thought about switcvhing colors to color blind friendly colors)
        size='total_bonds_issued',  # Make the point size proportional to the volume
        hover_data=['state_code', 'issuer_type', 'avg_coupon_rate'],
        title='Volume vs. Coupon Rate by Issuer Type and State',
        template='seaborn'
    )
    fig_scatter.update_layout(
        xaxis_title="Total Bonds Issued (Volume)",
        yaxis_title="Average Coupon Rate (%)",
        legend_title="Issuer Type"
    )
    st.plotly_chart(fig_scatter, use_container_width=True)

    # Adding a clean markdown box for the interpretation
    st.markdown("""
    <div style='background-color: #f0f2f6; padding: 15px; border-radius: 10px; border-left: 5px solid #1f77b4;'>
    **Query Interpretation (HAVING Clause Analysis):**
    The chart clearly shows that **Texas (TX)** and **Florida (FL)** county and state entities dominate the highest coupon rates (approaching 4.0%). The high-volume groups, such as **TX City** and **CA District** (which issued over 160 bonds each), cluster near the middle of the rate range, suggesting that while they borrow frequently, their rates are closer to the overall average. Groups in TX and FL appear to be offering the most aggressive rates to attract capital, potentially indicating a higher cost of borrowing in those states for these specific issuer types.
    </div>
    """, unsafe_allow_html=True)


# Generates a bar chart to aggregate the issuance volume by state code. This is a simpler complementary view to the scatter plot because I thought its provided
# a clear ranking of states based on their overall contribution to the high-volume category
def display_volume_by_state(data):
    st.subheader("📈 Aggregated Issuance Volume by State")
    st.markdown(
        "This **bar chart** sums the bond counts for all high-volume issuer groups within each state to show which states are the most dominant issuers by overall bond count among the filtered group.")

    # Group the data by state and sum the total bonds issued
    state_volume = data.groupby('state_code')['total_bonds_issued'].sum().reset_index()
    state_volume = state_volume.sort_values(by='total_bonds_issued', ascending=False)

    fig_bar = px.bar(
        state_volume,
        x='state_code',
        y='total_bonds_issued',
        color='state_code',
        title='Total Bonds Issued by State (Aggregated)',
        text='total_bonds_issued',
        template='seaborn'
    )
    fig_bar.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
    fig_bar.update_layout(
        xaxis_title="State Code",
        yaxis_title="Total Bonds Issued",
        yaxis_tickformat=',.0f'
    )
    st.plotly_chart(fig_bar, use_container_width=True)

# Generates a grouped bar chart to visualize the count of Positive vs. Negative credit rating outlooks over time grouped by year
# This shows the market's changing perception of credit risk using the Date-Based Analysis query result
def display_credit_sentiment(data):
    st.subheader("⭐ Credit Rating Outlook Trend Over Time")
    st.markdown(
        "This **grouped bar chart** uses the `EXTRACT(YEAR FROM rating_date)` function to analyze the count of Positive and Negative rating outlooks issued each year. This **Date-Based Analysis** tracks market risk perception over time by filtering out 'Stable' ratings.")

    # Ensure the plot is chronological
    data_sorted = data.sort_values(by='rating_year')

    fig_sentiment = px.bar(
        data_sorted,
        x='rating_year',
        y='total_ratings_in_year',
        color='outlook',
        barmode='group',  # Puts Positive and Negative bars side-by-side for easy comparison
        title='Annual Volume of Positive vs. Negative Credit Outlooks',
        labels={'total_ratings_in_year': 'Total Outlooks Issued', 'rating_year': 'Rating Year'},
        template='seaborn'
    )
    fig_sentiment.update_xaxes(type='category')  # Treat year as discrete categories, not a continuous timeline

    st.plotly_chart(fig_sentiment, use_container_width=True)

    # Adding the interpretation box
    st.markdown("""
    <div style='background-color: #f0f2f6; padding: 15px; border-radius: 10px; border-left: 5px solid #1f77b4;'>
    **Query Interpretation (Date-Based Analysis):**
    The trend reveals a significant shift in market perception. In **2020**, Negative outlooks were highest, reflecting the economic uncertainty of the time. However, every year from **2021 through 2024** shows a strong recovery, with **Positive outlooks consistently and substantially outweighing Negative ones**. This suggests a strong overall improvement in the perceived credit quality and financial health of municipal issuers across the market in the post-2020 environment.
    </div>
    """, unsafe_allow_html=True)


# Generates a bar chart to highlight the most actively traded long-duration, specific-purpose municipal bonds. This function visualizes the result of the Multi-Table JOIN, showing which
# risky/long-term debt has the highest liquidity (total trades).
def display_long_duration_liquidity(data):
    st.subheader("🤝 Actively Traded Long-Duration Education Bonds")
    st.markdown(
        "This **Multi-Table JOIN** (Bonds, Issuers, Purposes, Trades) identifies **County-issued Education bonds with a duration over 9 years** that exhibit the highest liquidity (**total trades**). This is a search for long-term, potentially riskier bonds that are still actively bought and sold.")

    if data.empty:
        st.warning("No actively traded long-duration County Education bonds found in the data.")
        return

    # Aggregate by Issuer Name: Since the raw data might list multiple trades for the same bond,
    # we group by issuer name and sum up the trades to get total liquidity per issuer.
    df_agg = data.groupby('issuer_name').agg({
        'total_trades': 'sum',
        'bond_duration': 'mean',
        'average_trade_price': 'mean'
    }).reset_index().sort_values(by='total_trades', ascending=False)

    fig = px.bar(
        df_agg,
        x='issuer_name',
        y='total_trades',
        color='total_trades',
        title='Total Trades for Long-Duration (>9yr) County Education Bonds',
        hover_data={'bond_duration': ':.2f', 'average_trade_price': '$.2f'},
        template='seaborn'
    )
    fig.update_layout(xaxis_title="Issuer Name", yaxis_title="Total Trades (Liquidity)")
    st.plotly_chart(fig, use_container_width=True)

    # Adding the interpretation box
    st.markdown("""
    <div style='background-color: #f0f2f6; padding: 15px; border-radius: 10px; border-left: 5px solid #1f77b4;'>
    **Query Interpretation (Multi-Table JOIN Analysis):**
    The analysis shows that **FL County #9** and **FL County #7** education bonds are the most liquid in this long-duration (>9 years) sector, each recording 16 trades. This liquidity in long-term, purpose-specific county debt suggests a high level of investor demand despite the extended duration (which typically implies higher interest rate risk). The analysis highlights where capital is flowing for long-term county education projects.
    </div>
    """, unsafe_allow_html=True)


# Displays a table of bonds identified as potentially 'undervalued' using a correlated subquery (latest price < historical average price).
def display_undervalued_bonds(data):
    st.subheader("📉 Bonds Trading Below Historical Average Price (Undervalued Candidates)")
    st.markdown(
        "This **Correlated Subquery** identifies bonds whose **most recent trade price** is below their **historical average trade price**. These bonds are potential 'undervalued' investment candidates.")

    if data.empty:
        st.success("No bonds were found trading below their historical average price.")
        return

    # Calculate the price difference for sorting and display
    data['price_difference'] = data['latest_trade_price'] - data['bond_historical_avg']
    # This sorts to show the biggest discounts i.e., most negative difference) first
    data = data.sort_values(by='price_difference', ascending=True)

    # Display the top 10 most undervalued bond candidates in a neat table format
    st.dataframe(
        data[['issuer_name', 'bond_id', 'latest_trade_price', 'bond_historical_avg', 'price_difference']].head(
            10).style.format({
            'latest_trade_price': '${:.2f}',
            'bond_historical_avg': '${:.2f}',
            'price_difference': '{:.2f}'
        }),
        use_container_width=True,
        hide_index=True
    )

    # Adding the interpretation box
    st.markdown("""
    <div style='background-color: #f0f2f6; padding: 15px; border-radius: 10px; border-left: 5px solid #1f77b4;'>
    **Query Interpretation (Correlated Subquery Analysis):**
    The table lists the top bonds where the latest trade occurred at a discount compared to its lifetime average. **NY Transportation Authority #5** shows one of the most significant price drops relative to its history, indicating a recent downward pressure on its value. Investors use this signal to spot bonds that may have been oversold due to temporary market conditions or a specific negative event, making them attractive targets for a potential rebound.
    </div>
    """, unsafe_allow_html=True)


# Generates a bar chart based on the calculated Yield Spread (Bond Yield - Treasury Rate= yeild spread).This demonstrates the Financial Metric Calculation query, showing the
# risk premium demanded by investors for specific municipal bonds relative to the risk-free rate
def display_yield_spread(data):
    st.subheader("💰 Yield Spread vs. 10-Year Treasury Rate")
    st.markdown(
        "This **Financial Metric Calculation** analyzes the **Yield Spread** (Bond Yield - 10 Year Treasury Rate) for specific trades. A higher spread indicates a greater premium, compensating the investor for perceived higher credit/liquidity risk compared to the risk-free U.S. Treasury.")

    if data.empty:
        st.warning("No trade data available to calculate the Yield Spread.")
        return

    # Isolate the top 10 trades with the largest spread for clear visualization
    df_top_spreads = data.sort_values(by='yield_spread_bps', ascending=False).head(10)

    fig = px.bar(
        df_top_spreads,
        x='issuer_name',
        y='yield_spread_bps',
        color='yield_spread_bps',
        title='Top 10 Trades by Yield Spread (Basis Points)',
        hover_data=['trade_date', 'bond_yield', 'treasury_rate'],
        template='seaborn'
    )
    fig.update_layout(xaxis_title="Issuer Name", yaxis_title="Yield Spread (Basis Points)",
                      xaxis={'categoryorder': 'total descending'})
    st.plotly_chart(fig, use_container_width=True)

    # Adding the interpretation box. So that our visuals can be understood more easily.
    st.markdown("""
    <div style='background-color: #f0f2f6; padding: 15px; border-radius: 10px; border-left: 5px solid #1f77b4;'>
    **Query Interpretation (Financial Metric Calculation):**
    The top spreads are highly concentrated in **IL City #3**, followed by **State of NY** and **FL Housing Authority #10**. The trade with the largest spread (5.620 basis points) for IL City #3 indicates that investors demanded an additional 5.62% yield above the comparable 10-year Treasury rate to hold this particular bond. This suggests a significantly higher risk premium was attached to this municipal debt compared to the "risk-free" benchmark at that time. **Calculation in analysis: Bond Yield - Treasury Rate = Yield Spread**
    </div>
    """, unsafe_allow_html=True)


# Handles the user request to optionally display the raw DataFrame contents. It uses a Streamlit checkbox to toggle visibility, and if checked, displays
# each DataFrame inside its own st.expander (dropdown box), keeping the UI clean.
# Map the display titles to the corresponding DataFrame keys connected to our analysis. I left which queries they are connected to for us to refer to
def display_raw_data_tables(data_files):
    data_map = {
        "High-Volume Issuer Groups (Query 2)": data_files.get('high_volume_issuers', pd.DataFrame()),
        "Credit Rating Sentiment (Query 4)": data_files.get('credit_sentiment', pd.DataFrame()),
        "Long-Duration Education Trades (Qeury 1 - Multi-Join)": data_files.get('long_duration_trades', pd.DataFrame()),
        "Undervalued Bonds (Query 3 - Correlated Subquery)": data_files.get('undervalued_bonds', pd.DataFrame()),
        "Yield Spread Trades (Query 5 - Financial Metric)": data_files.get('yield_spread', pd.DataFrame()),
    }

    # The checkbox controls the visibility of ALL raw tables. I did these because the layout was so sloppy when its all on one page. This is easier on the eyes
    show_raw_data = st.checkbox("Show Raw Data Tables", value=False)

    if show_raw_data:
        st.header("Raw Data Tables (Source Query Results)")
        for title, df in data_map.items():
            # The expander hides the table until the user clicks on the title
            with st.expander(f"**{title}** ({len(df):,} records)"):
                if df.empty:
                    st.warning("Data is missing or failed to load for this query.")
                else:
                    st.dataframe(df, use_container_width=True)



# MAIN APP  FUNCTION
# This the entry point that sets up the dashboard structure. Its the main function that sets up the Streamlit page, loads the data, organizes the visualizations
# into tabs, and includes the raw data control.
# Configure the overall page layout for a wide view because normal doesn't fill screen. I don't if thats because my monitors was wide...
def dashboard():
    st.set_page_config(
        layout="wide",
        page_title="Municipal Bond Market Analysis",
        initial_sidebar_state="expanded"
    )

    st.title("🏛️ Comprehensive Municipal Bond Market Analysis")
    st.markdown("---")

    # Loads the cleaned data once (we cached with st.cache_data to save energy and time)
    data_files = load_all_data()

    # SDpecific DataFrames out of database for easier reference in the main logic
    df_hv = data_files.get('high_volume_issuers', pd.DataFrame())
    df_cs = data_files.get('credit_sentiment', pd.DataFrame())
    df_ldt = data_files.get('long_duration_trades', pd.DataFrame())
    df_uvb = data_files.get('undervalued_bonds', pd.DataFrame())
    df_ys = data_files.get('yield_spread', pd.DataFrame())

    # Start with the main summary metrics
    display_key_metrics(df_hv)

    # Define a dictionary to map tab names to their display functions and data
    tab_map = {
        "High-Volume / Coupon": [display_issuance_vs_coupon, display_volume_by_state, df_hv],
        "Credit Sentiment Trend": [display_credit_sentiment, None, df_cs],
        "Long-Duration Liquidity": [display_long_duration_liquidity, None, df_ldt],
        "Undervalued Bonds": [display_undervalued_bonds, None, df_uvb],
        "Yield Spread Risk": [display_yield_spread, None, df_ys],
    }

    # Create the tab structure in the Streamlit app
    tabs = st.tabs(list(tab_map.keys()))

    # Iterate through the tabs and call the corresponding display functions
    for i, tab_name in enumerate(tab_map.keys()):
        with tabs[i]:
            main_func, secondary_func, data = tab_map[tab_name]

            # Checks if the data for this tab actually loaded
            if data.empty:
                st.warning(f"Data for '{tab_name}' is missing or failed to load. Cannot display analysis.")
            else:
                # First-- Call the primary visualization function
                main_func(data)

                # Then if a secondary function exists (like in the High-Volume tab), call it too
                if secondary_func:
                    st.markdown("---")
                    secondary_func(data)

    st.markdown("---")

    # Display the section that controls the raw data tables checkbox/expander drop downs
    display_raw_data_tables(data_files)
if __name__ == "__main__":
    dashboard()