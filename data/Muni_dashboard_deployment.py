import streamlit as st
import pandas as pd
import plotly.express as px
import io

# -----------------------------
# DATA SIMULATION
# -----------------------------
DATA_FILE_PATHS = {
    'high_volume_issuers': 'Finds specific combination of issuer type__city or authority and state that have issued greater than 50 bonds',
    'credit_sentiment': 'avg_credit_rating_outlook_across_market_ results_by_ year',
    'long_duration_trades': 'Multi table JOIN joins 4 tables__Bonds_Issuers_BondPurposes_and Trades to identify the most actively traded long durationrisky bonds_specifically issued by county governments for education purposes',
    'undervalued_bonds': 'Correlation subquery to list all bonds where the most recent trade price was below the avg trade price',
    'yield_spread': 'Calculate the yield spread between the trade yield and avg 10-year treasury rate',
}

def getFileContent(key):
    """
    Simulates reading the raw CSV content for a given analysis key.
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
"BOND1048","IL Transit District #7","111.89","113.09"
"BOND0380","State of NY","111.78","111.91"
"BOND0781","CA Transit District #1","111.43","111.49"
"BOND0280","CA City #7","111.13","111.59"
"BOND1353","TX Water District #10","110.66","110.77"
"BOND0760","IL County #11","110.45","110.87"
"""
    elif key == 'yield_spread':
        return """
"trade_id","issuer_name","trade_date","bond_yield","treasury_rate","yield_spread_bps"
2459,"IL City #3","2021-08-16","6.410","0.79","5.620"
2460,"IL City #3","2021-10-05","6.200","0.63","5.570"
13121,"State of NY","2021-06-19","6.050","0.60","5.450"
10800,"FL Housing Authority #10","2020-05-19","6.370","0.95","5.420"
13962,"IL County #4","2020-04-13","6.230","0.84","5.390"
1085,"FL Transit District #8","2020-07-10","5.840","0.53","5.310"
9569,"State of NY","2020-01-23","5.900","0.60","5.300"
5127,"IL County #11","2021-10-25","5.920","0.63","5.290"
"""
    return None


# -----------------------------
# DATA LOADING FUNCTION
# -----------------------------
@st.cache_data
def load_all_data():
    data_dict = {}
    for key in DATA_FILE_PATHS.keys():
        content = getFileContent(key)
        if content is None:
            continue
        df = pd.read_csv(io.StringIO(content.strip()))
        # Type conversions
        if key == 'high_volume_issuers':
            df.columns = ['state_code', 'issuer_type', 'total_bonds_issued', 'avg_coupon_rate']
            df['total_bonds_issued'] = pd.to_numeric(df['total_bonds_issued'])
            df['avg_coupon_rate'] = pd.to_numeric(df['avg_coupon_rate'])
        elif key == 'credit_sentiment':
            df.columns = ['rating_year', 'outlook', 'total_ratings_in_year', 'average_sentiment_score']
            df['rating_year'] = df['rating_year'].astype(int)
            df['total_ratings_in_year'] = pd.to_numeric(df['total_ratings_in_year'])
            df['average_sentiment_score'] = pd.to_numeric(df['average_sentiment_score'])
        elif key == 'long_duration_trades':
            df.columns = ['issuer_name', 'purpose_category', 'bond_duration', 'total_trades', 'average_trade_price']
            df[['bond_duration', 'total_trades', 'average_trade_price']] = df[['bond_duration', 'total_trades', 'average_trade_price']].apply(pd.to_numeric)
        elif key == 'undervalued_bonds':
            df.columns = ['bond_id', 'issuer_name', 'latest_trade_price', 'bond_historical_avg']
            df[['latest_trade_price','bond_historical_avg']] = df[['latest_trade_price','bond_historical_avg']].apply(pd.to_numeric)
            df.drop_duplicates(subset=['bond_id'], inplace=True)
        elif key == 'yield_spread':
            df.columns = ['trade_id','issuer_name','trade_date','bond_yield','treasury_rate','yield_spread_bps']
            df[['trade_id','bond_yield','treasury_rate','yield_spread_bps']] = df[['trade_id','bond_yield','treasury_rate','yield_spread_bps']].apply(pd.to_numeric)
            df.drop_duplicates(subset=['trade_id'], inplace=True)
        data_dict[key] = df
    return data_dict


# -----------------------------
# PLOT FUNCTIONS (New Visualizations)
# -----------------------------
def display_issuance_vs_coupon(data):
    st.subheader("📊 Issuance Volume vs. Average Coupon Rate")
    fig = px.scatter(
        data, x='total_bonds_issued', y='avg_coupon_rate',
        color='issuer_type', size='total_bonds_issued',
        hover_data=['state_code','issuer_type','avg_coupon_rate'],
        template='seaborn'
    )
    fig.update_layout(xaxis_title="Total Bonds Issued", yaxis_title="Average Coupon Rate (%)")
    st.plotly_chart(fig, use_container_width=True)


def display_volume_by_state(data):
    st.subheader("📈 Aggregated Issuance Volume by State")
    state_volume = data.groupby('state_code')['total_bonds_issued'].sum().reset_index().sort_values('total_bonds_issued', ascending=False)
    fig = px.bar(state_volume, x='state_code', y='total_bonds_issued', text='total_bonds_issued', template='seaborn')
    fig.update_traces(texttemplate='%{text:,.0f}', textposition='outside')
    st.plotly_chart(fig, use_container_width=True)


def display_credit_sentiment(data):
    st.subheader("⭐ Credit Rating Outlook Trend Over Time")
    data_sorted = data.sort_values('rating_year')
    fig = px.bar(data_sorted, x='rating_year', y='total_ratings_in_year', color='outlook', barmode='group', template='seaborn')
    st.plotly_chart(fig, use_container_width=True)


def display_long_duration_liquidity(data):
    st.subheader("🤝 Actively Traded Long-Duration Education Bonds")
    df_agg = data.groupby('issuer_name').agg({'total_trades':'sum','bond_duration':'mean','average_trade_price':'mean'}).reset_index().sort_values('total_trades', ascending=False)
    fig = px.bar(df_agg, x='issuer_name', y='total_trades', color='total_trades', hover_data={'bond_duration':':.2f','average_trade_price':'$.2f'}, template='seaborn')
    st.plotly_chart(fig, use_container_width=True)


def display_undervalued_bonds(data):
    st.subheader("📉 Bonds Trading Below Historical Average Price")
    data['price_difference'] = data['latest_trade_price'] - data['bond_historical_avg']
    df_sorted = data.sort_values('price_difference')
    st.dataframe(df_sorted[['issuer_name','bond_id','latest_trade_price','bond_historical_avg','price_difference']], use_container_width=True)


def display_yield_spread(data):
    st.subheader("💰 Yield Spread vs. 10-Year Treasury Rate")
    df_top = data.sort_values('yield_spread_bps', ascending=False).head(10)
    fig = px.bar(df_top, x='issuer_name', y='yield_spread_bps', color='yield_spread_bps', hover_data=['trade_date','bond_yield','treasury_rate'], template='seaborn')
    st.plotly_chart(fig, use_container_width=True)


# -----------------------------
# DASHBOARD
# -----------------------------
def dashboard():
    st.set_page_config(layout="wide", page_title="Municipal Bond Market Analysis")
    st.title("🏛️ Comprehensive Municipal Bond Market Analysis")
    st.markdown("---")

    # Load data
    data_files = load_all_data()
    df_hv = data_files.get('high_volume_issuers', pd.DataFrame())
    df_cs = data_files.get('credit_sentiment', pd.DataFrame())
    df_ldt = data_files.get('long_duration_trades', pd.DataFrame())
    df_uvb = data_files.get('undervalued_bonds', pd.DataFrame())
    df_ys = data_files.get('yield_spread', pd.DataFrame())

    # Tabs
    tabs = st.tabs(["Market Overview", "Credit Sentiment", "Liquidity & Value", "Yield Spread"])

    with tabs[0]:
        display_issuance_vs_coupon(df_hv)
        st.divider()
        display_volume_by_state(df_hv)

    with tabs[1]:
        display_credit_sentiment(df_cs)

    with tabs[2]:
        display_long_duration_liquidity(df_ldt)
        st.divider()
        display_undervalued_bonds(df_uvb)

    with tabs[3]:
        display_yield_spread(df_ys)

if __name__ == "__main__":
    dashboard()
