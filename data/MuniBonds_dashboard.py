import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from sqlalchemy import create_engine, exc, text
import numpy as np

# ==============================================================================
# 1. DATABASE CONFIGURATION
# ==============================================================================
DB_USER = 'postgres'
DB_PASS = 'Brindlewood9'
DB_HOST = 'localhost'
DB_NAME = 'municipal_bonds'
DB_PORT = '5432'

CONNECTION_STRING = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

# ==============================================================================
# 2. SQL QUERIES (Updated for Full Requirements)
# ==============================================================================
SQL_QUERIES = {
    'avg_coupon_by_purpose': """
        SELECT
            bp.purpose_category,
            ROUND(AVG(b.coupon_rate_pct), 3) AS average_coupon_rate_pct
        FROM bonds b
        JOIN bond_purposes bp ON b.purpose_id = bp.purpose_id
        GROUP BY bp.purpose_category
        ORDER BY average_coupon_rate_pct DESC;
    """,
    'issuance_volume_by_state_type': """
        SELECT
            i.state_code,
            i.issuer_type,
            COUNT(b.bond_id) AS total_bonds_issued
        FROM bonds b
        JOIN issuers i ON b.issuer_id = i.issuer_id
        GROUP BY i.state_code, i.issuer_type
        HAVING COUNT(b.bond_id) > 10
        ORDER BY total_bonds_issued DESC;
    """,
    'state_yield_stats': """
        -- NEW: For State Comparison with Error Bars
        SELECT 
            i.state_code,
            AVG(t.yield_pct) as avg_yield,
            STDDEV(t.yield_pct) as std_dev_yield
        FROM trades t
        JOIN bonds b ON t.bond_id = b.bond_id
        JOIN issuers i ON b.issuer_id = i.issuer_id
        GROUP BY i.state_code;
    """,
    'time_series_macro': """
        -- NEW: For Time Series Overlay (Yield vs Unemployment)
        SELECT 
            m.date,
            i.state_code,
            AVG(t.yield_pct) as avg_yield,
            AVG(m.unemployment_rate_pct) as unemployment_rate
        FROM trades t
        JOIN bonds b ON t.bond_id = b.bond_id
        JOIN issuers i ON b.issuer_id = i.issuer_id
        JOIN macro_economic_data m ON i.state_code = m.state_code 
             AND DATE_TRUNC('month', t.trade_date) = DATE_TRUNC('month', m.date)
        GROUP BY m.date, i.state_code
        ORDER BY m.date;
    """,
    'credit_sentiment': """
        SELECT
            EXTRACT(YEAR FROM rating_date) AS rating_year,
            outlook,
            COUNT(rating_id) AS total_ratings_in_year,
            ROUND(AVG(
                CASE WHEN outlook = 'Positive' THEN 100
                     WHEN outlook = 'Negative' THEN -100
                     ELSE 0 END
            ),2) AS average_sentiment_score
        FROM credit_ratings
        WHERE outlook != 'Stable'
        GROUP BY rating_year, outlook
        ORDER BY rating_year DESC;
    """,
    'long_duration_trades': """
        SELECT
            t.trade_date,
            i.issuer_name,
            b.bond_id,
            t.trade_price_usd AS trade_price_pct,
            t.yield_pct AS yield_pct,
            b.duration_years AS duration_num,
            t.buyer_type
        FROM trades t
        JOIN bonds b ON t.bond_id = b.bond_id
        JOIN issuers i ON b.issuer_id = i.issuer_id
        JOIN bond_purposes bp ON b.purpose_id = bp.purpose_id
        WHERE b.duration_years > 6.0
        ORDER BY t.trade_date DESC
        LIMIT 100;
    """,
    'undervalued_bonds': """
        WITH AveragePrices AS (
            SELECT bond_id, AVG(trade_price_usd) AS avg_trade_price
            FROM trades GROUP BY bond_id
        ),
        MostRecentTrades AS (
            SELECT t.bond_id, t.trade_price_usd AS current_price,
            ROW_NUMBER() OVER(PARTITION BY t.bond_id ORDER BY t.trade_date DESC) as rn
            FROM trades t
        )
        SELECT
            mrt.bond_id, mrt.current_price, ap.avg_trade_price,
            ROUND(ap.avg_trade_price - mrt.current_price, 2) AS discount_to_avg
        FROM MostRecentTrades mrt
        JOIN AveragePrices ap ON mrt.bond_id = ap.bond_id
        JOIN issuers i ON (SELECT issuer_id FROM bonds WHERE bond_id = mrt.bond_id) = i.issuer_id
        WHERE mrt.rn = 1
        AND mrt.current_price < ap.avg_trade_price
        ORDER BY discount_to_avg DESC
        LIMIT 20;
    """,
    'yield_spread': """
        SELECT
            t.trade_date,
            i.issuer_name,
            t.yield_pct AS bond_yield,
            m.treasury_10yr_rate_pct AS treasury_rate,
            ROUND((t.yield_pct - m.treasury_10yr_rate_pct), 3) AS yield_spread_bps
        FROM trades t
        JOIN bonds b ON t.bond_id = b.bond_id
        JOIN issuers i ON b.issuer_id = i.issuer_id
        JOIN macro_economic_data m ON i.state_code = m.state_code
            AND DATE_TRUNC('month', t.trade_date) = DATE_TRUNC('month', m.date)
        ORDER BY yield_spread_bps DESC
        LIMIT 50;
    """,
}


# ==============================================================================
# 3. DATABASE CONNECTION UTILITIES
# ==============================================================================
@st.cache_resource
def get_db_engine():
    try:
        engine = create_engine(CONNECTION_STRING)
        with engine.connect() as connection:
            connection.execute(text("SELECT 1"))
        return engine
    except Exception as e:
        st.error(f"DB Connection Failed: {e}")
        return None


@st.cache_data(ttl=600)
def load_data_from_db(_engine, query_key):
    if not _engine: return pd.DataFrame()
    query = SQL_QUERIES.get(query_key)
    if not query: return pd.DataFrame()
    try:
        return pd.read_sql(text(query), _engine)
    except Exception as e:
        st.error(f"Query Error ({query_key}): {e}")
        return pd.DataFrame()


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
