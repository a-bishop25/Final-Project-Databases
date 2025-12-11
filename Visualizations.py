import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime

# --- Configuration for Data Loading ---
# NOTE: The script assumes your CSV files are in the same directory as the script.
CSV_FILES = {
    'issuers': 'issuers.csv',
    'purposes': 'bond_purposes.csv',
    'bonds': 'bonds.csv',
    'ratings': 'credit_ratings.csv',
    'trades': 'trades.csv',
    'macro': 'economic_indicators.csv'
}

# Define a function to map complex ratings to a numerical scale for sorting
RATING_ORDER = {
    'AAA': 1, 'AA+': 2, 'AA': 3, 'AA-': 4,
    'A+': 5, 'A': 6, 'A-': 7,
    'BBB+': 8, 'BBB': 9, 'BBB-': 10,
    'BB+': 11, 'BB': 12, 'BB-': 13,
    'B+': 14, 'B': 15, 'B-': 16,
    'CCC': 17, 'CC': 18, 'C': 19, 'D': 20
}


def load_and_prepare_data():
    """Loads all data, merges them into a master DataFrame, and prepares columns."""
    try:
        # Load all dataframes
        df_data = {key: pd.read_csv(f) for key, f in CSV_FILES.items()}
    except FileNotFoundError as e:
        print(f"Error: Required CSV file not found: {e}")
        print("Please ensure all CSV files are in the same directory.")
        return None

    # --- Data Preparation and Merging ---

    # 1. Merge Bonds with Issuers and Purposes (Dimensions)
    df_bonds = df_data['bonds'].merge(df_data['issuers'][['issuer_id', 'state', 'issuer_name']], on='issuer_id',
                                      how='left')
    df_bonds = df_bonds.merge(df_data['purposes'], on='purpose_id', how='left')

    # 2. Prepare Trades Data (Time-series fact table)
    df_trades = df_data['trades'].copy()
    df_trades['trade_date'] = pd.to_datetime(df_trades['trade_date'])

    # 3. Prepare Ratings Data (Time-series: find the latest rating for analysis)
    df_ratings = df_data['ratings'].copy()
    df_ratings['rating_date'] = pd.to_datetime(df_ratings['rating_date'])
    # Get the latest rating for each bond by date
    df_latest_ratings = df_ratings.sort_values('rating_date').drop_duplicates(subset=['bond_id'], keep='last')
    df_latest_ratings['rating_code_num'] = df_latest_ratings['rating'].map(RATING_ORDER)

    # 4. Create Master DataFrame by merging latest trades and latest ratings with bonds
    # We use the latest trade date and yield for cross-sectional analysis (Yield Curve, State Comparison)
    df_latest_trades = df_trades.sort_values('trade_date').drop_duplicates(subset=['bond_id'], keep='last')

    df_master = df_bonds.merge(df_latest_trades[['bond_id', 'yield', 'trade_date', 'trade_price']], on='bond_id',
                               how='left')
    df_master = df_master.merge(df_latest_ratings[['bond_id', 'rating', 'rating_code_num']], on='bond_id', how='left')

    # --- Calculations ---
    # Convert dates for calculation
    df_master['maturity_date'] = pd.to_datetime(df_master['maturity_date'])

    # Calculate Time to Maturity in Years (for Yield Curve)
    TODAY = datetime(2024, 6, 1)  # Assume a cut-off date for calculation
    df_master['time_to_maturity'] = (df_master['maturity_date'] - TODAY).dt.days / 365.25

    return df_master, df_trades, df_latest_ratings, df_data['macro']


def create_visualizations(df_master, df_trades, df_latest_ratings, df_macro):
    """Generates the six required Plotly visualizations."""

    # 1. Yield Curve (Yield vs. Time to Maturity, color by rating)
    fig1 = px.scatter(
        df_master.dropna(subset=['time_to_maturity', 'yield', 'rating']),
        x='time_to_maturity',
        y='yield',
        color='rating',
        color_discrete_sequence=px.colors.sequential.Viridis,
        title='1. Municipal Bond Yield Curve (Yield vs. Time to Maturity)',
        labels={'time_to_maturity': 'Time to Maturity (Years)', 'yield': 'Latest Yield (%)', 'rating': 'Credit Rating'},
        hover_data=['issuer_name', 'bond_type', 'coupon_rate']
    )
    fig1.update_layout(height=600, template="plotly_white")

    # 2. Credit Rating Distribution
    rating_counts = df_latest_ratings['rating'].value_counts().reset_index()
    rating_counts.columns = ['Rating', 'Count']
    # Sort ratings using the numerical map
    rating_counts['Order'] = rating_counts['Rating'].map(RATING_ORDER)
    rating_counts = rating_counts.sort_values('Order').drop(columns='Order')

    fig2 = px.bar(
        rating_counts,
        x='Rating',
        y='Count',
        color='Rating',
        title='2. Distribution of Latest Credit Ratings',
        labels={'Count': 'Number of Bonds', 'Rating': 'Credit Rating'},
        color_discrete_sequence=px.colors.qualitative.D3
    )
    fig2.update_layout(height=600, template="plotly_white")

    # 3. State Comparison (Avg. Yields by State)
    df_state_yield = df_master.groupby('state')['yield'].agg(['mean', 'std']).reset_index()
    df_state_yield.columns = ['State', 'Avg_Yield', 'Std_Dev']

    fig3 = go.Figure(data=[
        go.Bar(
            x=df_state_yield['State'],
            y=df_state_yield['Avg_Yield'],
            error_y=dict(type='data', array=df_state_yield['Std_Dev'], visible=True, color='gray'),
            marker_color=px.colors.qualitative.Pastel
        )
    ])
    fig3.update_layout(
        title='3. Average Bond Yield by State (with Std Dev Error Bars)',
        xaxis_title='State',
        yaxis_title='Average Yield (%)',
        height=600,
        template="plotly_white"
    )

    # 4. Time Series Analysis (Prices/Yields and Economic Indicators)
    # Aggregate trades data monthly
    df_trades_monthly = df_trades.set_index('trade_date').resample('M').agg({
        'yield': 'mean',
        'trade_price': 'mean'
    }).reset_index()
    df_trades_monthly.rename(columns={'trade_date': 'date'}, inplace=True)

    # Prepare Macro Data
    df_macro.rename(columns={'state': 'state_code', 'unemployment_rate': 'unemployment_rate_pct'}, inplace=True)
    df_macro['date'] = pd.to_datetime(df_macro['date'])
    df_macro_avg = df_macro.groupby('date')[['treasury_10yr', 'vix_index']].mean().reset_index()

    df_ts = df_trades_monthly.merge(df_macro_avg, on='date', how='inner')

    # Create figure with secondary y-axis
    fig4 = go.Figure()
    fig4.add_trace(go.Scatter(x=df_ts['date'], y=df_ts['yield'], name='Avg Bond Yield (%)', yaxis='y1'))
    fig4.add_trace(go.Scatter(x=df_ts['date'], y=df_ts['treasury_10yr'], name='Avg 10Yr Treasury Rate (%)', yaxis='y1'))
    fig4.add_trace(go.Scatter(x=df_ts['date'], y=df_ts['vix_index'], name='VIX Index', yaxis='y2'))

    fig4.update_layout(
        title='4. Time Series: Bond Yields vs. Key Economic Indicators',
        xaxis_title='Date',
        yaxis=dict(title='Yields / Rates (%)', showgrid=False),
        yaxis2=dict(title='VIX Index', overlaying='y', side='right', showgrid=True),
        legend=dict(x=0.01, y=0.99),
        height=600,
        template="plotly_white"
    )

    # 5. Sector Performance (Heatmap: Avg Yield vs. Bond Type)
    df_sector_perf = df_master.groupby(['purpose_category', 'bond_type'])['yield'].mean().reset_index()
    df_heatmap = df_sector_perf.pivot(index='purpose_category', columns='bond_type', values='yield').fillna(0)

    fig5 = go.Figure(data=go.Heatmap(
        z=df_heatmap.values,
        x=df_heatmap.columns,
        y=df_heatmap.index,
        colorscale='Viridis',
        colorbar_title='Average Yield (%)'
    ))
    fig5.update_layout(
        title='5. Sector Performance: Average Yield Heatmap by Purpose and Bond Type',
        xaxis_title='Bond Type',
        yaxis_title='Bond Purpose (Sector)',
        height=600,
        template="plotly_white"
    )

    # 6. Trading Activity (Volume over time, Buyer type distribution)
    # 6a. Volume over Time (Aggregated Daily Volume)
    df_volume_daily = df_trades.groupby('trade_date')['quantity'].sum().reset_index()
    fig6a = px.line(
        df_volume_daily,
        x='trade_date',
        y='quantity',
        title='6a. Trading Volume Over Time (Total Bonds Traded)',
        labels={'trade_date': 'Trade Date', 'quantity': 'Total Volume (Bonds)'},
        line_shape='spline'
    )
    fig6a.update_layout(height=400, template="plotly_white")

    # 6b. Buyer Type Distribution
    df_buyer_dist = df_trades['buyer_type'].value_counts().reset_index()
    df_buyer_dist.columns = ['Buyer Type', 'Trade Count']

    fig6b = px.pie(
        df_buyer_dist,
        names='Buyer Type',
        values='Trade Count',
        title='6b. Distribution of Trading Activity by Buyer Type',
        hole=.3,
        color_discrete_sequence=px.colors.qualitative.Safe
    )
    fig6b.update_layout(height=400, template="plotly_white")

    # --- Save all figures ---
    print("Saving 6 required visualizations as HTML files...")
    fig1.write_html("viz_1_yield_curve.html")
    fig2.write_html("viz_2_rating_distribution.html")
    fig3.write_html("viz_3_state_comparison.html")
    fig4.write_html("viz_4_time_series.html")
    fig5.write_html("viz_5_sector_heatmap.html")
    fig6a.write_html("viz_6a_volume_over_time.html")
    fig6b.write_html("viz_6b_buyer_distribution.html")
    print("All visualizations saved. Use these HTML files to create your final submission PDF.")


if __name__ == '__main__':
    data = load_and_prepare_data()
    if data:
        df_master, df_trades, df_latest_ratings, df_macro = data
        create_visualizations(df_master, df_trades, df_latest_ratings, df_macro)