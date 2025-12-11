import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from pymongo import MongoClient
import numpy as np

# ====================================================================
# 1. MongoDB Configuration
# ====================================================================
MONGO_URI = "mongodb+srv://abishop25_db_user:5J7qJlNNtg5gudAf@cluster0.szrzgd6.mongodb.net/?appName=Cluster0"
client = MongoClient(MONGO_URI)
db = client["municipal_bonds"]

# ====================================================================
# 2. MongoDB Data Loading Functions
# ====================================================================

@st.cache_data(ttl=600)
def load_avg_coupon_by_purpose():
    pipeline = [
        {"$lookup": {"from": "bond_purposes", "localField": "purpose_id", "foreignField": "purpose_id", "as": "purpose_info"}},
        {"$unwind": "$purpose_info"},
        {"$group": {"_id": "$purpose_info.purpose_category", "average_coupon_rate_pct": {"$avg": "$coupon_rate"}}},
        {"$sort": {"average_coupon_rate_pct": -1}}
    ]
    df = pd.DataFrame(list(db.bonds.aggregate(pipeline)))
    if df.empty: return df
    df.rename(columns={"_id": "purpose_category"}, inplace=True)
    return df


@st.cache_data(ttl=600)
def load_issuance_volume_by_state():
    pipeline = [
        {"$lookup": {"from": "issuers", "localField": "issuer_id", "foreignField": "issuer_id", "as": "issuer_info"}},
        {"$unwind": "$issuer_info"},
        {"$group": {"_id": {"state": "$issuer_info.state", "issuer_type": "$issuer_info.issuer_type"}, "total_bonds_issued": {"$sum": 1}}},
        {"$match": {"total_bonds_issued": {"$gt": 10}}},
        {"$sort": {"total_bonds_issued": -1}}
    ]
    df = pd.DataFrame(list(db.bonds.aggregate(pipeline)))
    if df.empty: return df
    df["state_code"] = df["_id"].apply(lambda x: x.get("state") if isinstance(x, dict) else None)
    df["issuer_type"] = df["_id"].apply(lambda x: x.get("issuer_type") if isinstance(x, dict) else None)
    return df.drop(columns=["_id"])


@st.cache_data(ttl=600)
def load_state_yield_stats():
    pipeline = [
        {"$lookup": {"from": "bonds", "localField": "bond_id", "foreignField": "bond_id", "as": "bond_info"}},
        {"$unwind": "$bond_info"},
        {"$lookup": {"from": "issuers", "localField": "bond_info.issuer_id", "foreignField": "issuer_id", "as": "issuer_info"}},
        {"$unwind": "$issuer_info"},
        {"$group": {"_id": "$issuer_info.state", "avg_yield": {"$avg": "$yield"}, "std_dev_yield": {"$stdDevPop": "$yield"}}}
    ]
    df = pd.DataFrame(list(db.trades.aggregate(pipeline)))
    if df.empty: return df
    df.rename(columns={"_id": "state_code"}, inplace=True)
    return df


@st.cache_data(ttl=600)
def load_time_series_macro():
    pipeline = [
        # Join trades → bonds
        {"$lookup": {"from": "bonds", "localField": "bond_id", "foreignField": "bond_id", "as": "bond_info"}},
        {"$unwind": "$bond_info"},
        # Join bonds → issuers
        {"$lookup": {"from": "issuers", "localField": "bond_info.issuer_id", "foreignField": "issuer_id", "as": "issuer_info"}},
        {"$unwind": "$issuer_info"},
        # Join issuers → economic indicators
        {"$lookup": {"from": "economic_indicators", "localField": "issuer_info.state", "foreignField": "state", "as": "macro_info"}},
        {"$unwind": "$macro_info"},
        # Truncate dates to month
        {"$addFields": {
            "trade_month": {"$dateTrunc": {"date": "$trade_date", "unit": "month"}},
            "macro_month": {"$dateTrunc": {"date": "$macro_info.date", "unit": "month"}}
        }},
        # Keep only matching months
        {"$match": {"$expr": {"$eq": ["$trade_month", "$macro_month"]}}},
        # Group by date and state
        {"$group": {
            "_id": {"date": "$macro_info.date", "state": "$issuer_info.state"},
            "avg_yield": {"$avg": "$yield"},
            "unemployment_rate": {"$avg": "$macro_info.unemployment_rate"}
        }},
        {"$sort": {"_id.date": 1}}
    ]
    df = pd.DataFrame(list(db.trades.aggregate(pipeline)))
    if df.empty: return df
    df["date"] = df["_id"].apply(lambda x: x.get("date"))
    df["state_code"] = df["_id"].apply(lambda x: x.get("state"))
    return df.drop(columns=["_id"])

@st.cache_data(ttl=600)
def load_credit_sentiment():
    # Step 1: Filter out "Stable" outlooks and extract the year
    pipeline = [
        {"$match": {"outlook": {"$ne": "Stable"}}},
        {"$project": {"rating_year": {"$year": "$rating_date"}, "outlook": 1}}
    ]
    df = pd.DataFrame(list(db.credit_ratings.aggregate(pipeline)))
    if df.empty:
        return df

    # Step 2: Assign sentiment values (+100 for Positive, -100 for Negative)
    df["sentiment_value"] = df["outlook"].apply(lambda x: 100 if x == "Positive" else -100)

    # Step 3: Group by year and calculate average sentiment
    df_sentiment = df.groupby("rating_year").agg(
        average_sentiment_score=("sentiment_value", "mean")
    ).reset_index()

    return df_sentiment



@st.cache_data(ttl=600)
def load_long_duration_trades():
    pipeline = [
        {"$lookup": {"from": "bonds", "localField": "bond_id", "foreignField": "bond_id", "as": "bond_info"}},
        {"$unwind": "$bond_info"},
        {"$lookup": {"from": "issuers", "localField": "bond_info.issuer_id", "foreignField": "issuer_id", "as": "issuer_info"}},
        {"$unwind": "$issuer_info"},
        {"$match": {"bond_info.duration": {"$gt": 6}}},
        {"$sort": {"trade_date": -1}},
        {"$limit": 100},
        {"$project": {"trade_date": 1, "issuer_name": "$issuer_info.issuer_name", "bond_id": 1,
                      "trade_price": "$trade_price", "yield": 1,
                      "duration_num": "$bond_info.duration", "buyer_type": 1}}
    ]
    df = pd.DataFrame(list(db.trades.aggregate(pipeline)))
    return df


@st.cache_data(ttl=600)
def load_undervalued_bonds():
    pipeline = [
        {"$sort": {"trade_date": -1}},  # most recent first
        {"$group": {
            "_id": "$bond_id",
            "avg_trade_price": {"$avg": "$trade_price"},
            "most_recent_trade": {"$first": "$$ROOT"}
        }},
        {"$project": {
            "bond_id": "$_id",
            "avg_trade_price": 1,
            "current_price": "$most_recent_trade.trade_price"
        }},
        {"$match": {"$expr": {"$lt": ["$current_price", "$avg_trade_price"]}}},
        {"$limit": 20}
    ]
    result = list(db.trades.aggregate(pipeline))
    return pd.DataFrame(result) if result else pd.DataFrame()


@st.cache_data(ttl=600)
def load_yield_spread():
    pipeline = [
        # Join trades → bonds
        {"$lookup": {"from": "bonds", "localField": "bond_id", "foreignField": "bond_id", "as": "bond_info"}},
        {"$unwind": "$bond_info"},
        # Join bonds → issuers
        {"$lookup": {"from": "issuers", "localField": "bond_info.issuer_id", "foreignField": "issuer_id", "as": "issuer_info"}},
        {"$unwind": "$issuer_info"},
        # Join issuers → economic indicators
        {"$lookup": {"from": "economic_indicators", "localField": "issuer_info.state", "foreignField": "state", "as": "macro_info"}},
        {"$unwind": "$macro_info"},
        # Truncate dates to month
        {"$addFields": {
            "trade_month": {"$dateTrunc": {"date": "$trade_date", "unit": "month"}},
            "macro_month": {"$dateTrunc": {"date": "$macro_info.date", "unit": "month"}},
            "treasury_rate": {"$ifNull": ["$macro_info.treasury_10yr", 0]}
        }},
        {"$match": {"$expr": {"$eq": ["$trade_month", "$macro_month"]}}},
        # Project only needed fields
        {"$project": {
            "trade_date": 1,
            "issuer_name": "$issuer_info.issuer_name",
            "bond_yield": "$yield",
            "treasury_rate": 1
        }},
        # Calculate yield spread
        {"$addFields": {"yield_spread_bps": {"$subtract": ["$bond_yield", "$treasury_rate"]}}},
        {"$sort": {"yield_spread_bps": -1}},
        {"$limit": 50}
    ]
    result = list(db.trades.aggregate(pipeline))
    return pd.DataFrame(result) if result else pd.DataFrame()


# ====================================================================
# 3. Visualization Functions
# ====================================================================

def display_issuance_vs_coupon(df):
    st.subheader("Avg Coupon Rate by Purpose")
    if not df.empty:
        fig = px.bar(df, x='purpose_category', y='average_coupon_rate_pct',
                     color='average_coupon_rate_pct', title='Coupon Rate (%) by Purpose')
        st.plotly_chart(fig, use_container_width=True)


def display_volume_by_state(df):
    st.subheader("Issuance Volume by State")
    if not df.empty:
        df['state_code'] = df['state_code'].fillna('Unknown State')
        df['issuer_type'] = df['issuer_type'].fillna('Unknown Type')

        fig = px.sunburst(
            df,
            path=['state_code', 'issuer_type'],
            values='total_bonds_issued',
            title='Bonds Issued by State & Type'
        )
        st.plotly_chart(fig, use_container_width=True)


def display_state_comparison(df):
    st.subheader("State Yield Comparison (with Volatility)")
    if not df.empty:
        fig = go.Figure(data=go.Bar(x=df['state_code'], y=df['avg_yield'],
                                    error_y=dict(type='data', array=df['std_dev_yield'], visible=True)))
        fig.update_layout(title="Average Yield by State (Error Bars = Std Dev)", yaxis_title="Yield")
        st.plotly_chart(fig, use_container_width=True)


def display_time_series_macro(df):
    st.subheader("Yields vs Unemployment (Macro Overlay)")
    if not df.empty:
        fig = px.line(df, x='date', y='avg_yield', color='state_code', title="Bond Yields vs Unemployment Rate")
        st.plotly_chart(fig, use_container_width=True)
        st.caption("Compare the yield trends above with the Unemployment Rate trends below.")
        fig2 = px.area(df, x='date', y='unemployment_rate', color='state_code', title="Unemployment Rate Over Time")
        st.plotly_chart(fig2, use_container_width=True)


def display_credit_sentiment(df):
    st.subheader("Credit Sentiment Trend")
    if not df.empty:
        df['rating_year'] = df['rating_year'].astype(int)
        fig = px.line(df, x='rating_year', y='average_sentiment_score',
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


# ====================================================================
# 4. Main App
# ====================================================================
def main():
    st.set_page_config(layout="wide", page_title="Municipal Bond Dashboard")
    st.title("Municipal Bond Market Dashboard")

    # Load Data
    df_acp = load_avg_coupon_by_purpose()
    df_sv = load_issuance_volume_by_state()
    df_state = load_state_yield_stats()
    df_macro = load_time_series_macro()
    df_cs = load_credit_sentiment()
    df_ldt = load_long_duration_trades()
    df_uvb = load_undervalued_bonds()
    df_ys = load_yield_spread()

    # Tabs
    tab1, tab2, tab3, tab4, tab5 = st.tabs(
        ["Market Overview", "Macro Trends", "Sentiment", "Liquidity & Value", "Risk Spreads"])

    with tab1:
        c1, c2 = st.columns(2)
        with c1: display_issuance_vs_coupon(df_acp)
        with c2: display_volume_by_state(df_sv)
        st.divider()
        display_state_comparison(df_state)

    with tab2:
        display_time_series_macro(df_macro)

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
