import plotly.express as px
from datetime import datetime, timedelta, date
from typing import List
import plotly.io as pyo
import math
import pandas as pd
from scipy.stats import zscore
import numpy as np
import matplotlib.pyplot as plt
import pathlib
from datetime import datetime, date, timedelta
from xbbg import blp
import blpapi
import os
import win32com.client as win32
from sklearn.preprocessing import StandardScaler
from scipy.stats.mstats import winsorize
import plotly.express as px
import plotly.figure_factory as ff
import plotly.graph_objects as go
from plotly.io import to_html
from plotly.subplots import make_subplots
import statsmodels.api as sm
from plotly.offline import plot
import scipy.optimize as spop
from scipy.stats import t
from scipy.stats import norm
from statsmodels.regression.rolling import RollingOLS
from datetime import datetime
import requests
from bs4 import BeautifulSoup
import re
import matplotlib.colors as mcolors
import sqlite3
from sql_cache_utils import read_cache, write_cache, append_to_cache, list_cached_tables
from pandas.tseries.offsets import BDay

LABEL_TO_FIELD = {
    'Forward PE': 'BEST_PE_RATIO',
    'Price to Book': 'PX_TO_BOOK_RATIO',
    'CAPE': 'LONG_TERM_PRICE_EARNINGS_RATIO',
    'EV/Trailing EBITDA': 'CURRENT_EV_TO_T12M_EBITDA',
    'Price to Sales': 'PX_TO_SALES_RATIO',
    'Valuation Composite': 'Valuation Composite'
}

cmap = mcolors.LinearSegmentedColormap.from_list(
    "soft_RdYlGn", ["lightcoral", "white", "lightgreen"]
)

####### MISC PHOTOS ############
Our_Asset_Class_Views = '<img src="https://i.imgur.com/wMquJZo.png" width="800">'
Fla = '<img src="https://i.imgur.com/xPi0AgA.png" width="800">'
Funda = '<img src="https://i.imgur.com/fYmUJ4d.png" width="800">'
Trad = '<img src="https://i.imgur.com/5hqqFJP.png" width="800">'
Cfs_fc = '<img src="https://i.imgur.com/Nh18Zhx.png" width="800">'
basecase = '<img src="https://i.imgur.com/mWuqeJA.png" width="700">'
cape_chart = '<img src="https://i.imgur.com/RsszGsQ.png" width="700">'
concentration = '<img src="https://i.imgur.com/JJQxKdV.png" width="700">'
eco_surprise = '<img src="https://i.imgur.com/mdfxHrD.png" width="700">'
recession = '<img src="https://i.imgur.com/6zODN1y.png" width="700">'
sys_outputs = '<img src="https://i.imgur.com/ENC01In.png" width="700">'
region_positioning = '<img src="https://i.imgur.com/VFEfrIh.png" width="700">'
returns_2022 = '<img src="https://i.imgur.com/e6xcaLa.png" width="700">' 
crsp_dimensional = '<img src="https://i.imgur.com/Rz9Kqrz.png" width="800">'
deficit_matrix = '<img src="https://imgur.com/0e160541-a5fc-4e44-9543-dd2453b4f153" width="800">'
tariff_2025 = '<img src="https://i.imgur.com/ZKUHcjO.png" width="800">'
tariff_2025_asia = '<img src="https://i.imgur.com/T9y1j74.png" width="800">'

# Scrape from Nowcast
today_date = datetime.now().strftime("%B %d, %Y")
url = "https://www.atlantafed.org/cqer/research/gdpnow"
response = requests.get(url)
soup = BeautifulSoup(response.text, "html.parser")

page_text = soup.get_text()
keyword = "Latest estimate:"
pos = page_text.find(keyword)

if pos != -1:
    start_index = pos + len(keyword)
    # Safely get the next 24 characters (or fewer if the string is short)
    next_24_chars = page_text[start_index : start_index + 24]
    print("Latest estimate:", next_24_chars)
else:
    print("No 'Latest estimate:' found in the text.")



########## TOP NEWS ##############

import requests
from bs4 import BeautifulSoup
from datetime import datetime
import openai

# ✅ Set up the client with your API key
#client = openai.OpenAI(api_key="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")  # Keep your actual key secure

# ✅ Fetch Yahoo Finance headlines
def fetch_yahoo_finance_news():
    url = "https://finance.yahoo.com"
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    headlines = []
    for item in soup.select("h3 a[href^='/news/']"):
        title = item.get_text(strip=True)
        if title:
            headlines.append(title)
    return headlines

# ✅ Fetch CNBC headlines
def fetch_cnbc_top_news():
    url = "https://www.cnbc.com/world/?region=world"
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'html.parser')

    headlines = []
    for item in soup.find_all("a", class_="LatestNews-headline"):
        title = item.get_text(strip=True)
        if title:
            headlines.append(title)
    return headlines

# ✅ Combine and limit headlines
def get_top_news_headlines():
    return (fetch_yahoo_finance_news() + fetch_cnbc_top_news())[:12]

top_headlines = get_top_news_headlines()
today = datetime.now().strftime("%B %d, %Y")

if top_headlines:
    top_news = f"📰 Top Finance & Macro Headlines as of {today}:\n\n"
    top_news += "\n".join(f"• {headline}" for headline in top_headlines)
else:
    top_news = "❗ No headlines were retrieved."

top_news_items = top_news.strip().split('\n')
top_news_html = (
    f"<div style='font-weight:600; color:#30415f; margin-bottom:8px;'>{top_news_items[0]}</div>" +
    ''.join(f"<li>{item.lstrip('• ').strip()}</li>" for item in top_news_items[1:] if item.strip())
)


######### #####################################

def graph_performance_with_width(data, title, width, height):
    # Color palettes
    full_palette = [
        "#30415f", "#f3a712", "#87b1a1", "#5ac5fe",
        "#a8c686", "#a0a197", "#e4572e", "#2337C6",
        "#B7B1B0", "#778BA5", "#990000"
    ]
    simp_palette = ["#30415f", "#DDDDDD", "#DDDDDD", "#DDDDDD"]
    
    fig = go.Figure()
    
    # Add traces depending on Series or DataFrame
    if isinstance(data, pd.Series):
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            mode='lines',
            name=data.name or "Series",
            line=dict(color=full_palette[0], width=2)
        ))
    elif isinstance(data, pd.DataFrame):
        use_full_colors = data.shape[1] >= 4
        palette = full_palette if use_full_colors else simp_palette
        for i, col in enumerate(data.columns):
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[col],
                mode='lines',
                name=str(col),
                line=dict(color=palette[i % len(palette)], width=2)
            ))
    
    # Apply layout
    fig.update_layout(
        title=title,
        xaxis_title='',
        yaxis_title='Price',
        template='plotly_white',
        hovermode='x unified',
        font=dict(family="Montserrat, sans-serif"),
        title_font=dict(family="Montserrat, sans-serif", size=22),
        legend_font=dict(family="Montserrat, sans-serif"),
        width=width,
        height=height,
        xaxis=dict(gridcolor="#ECECEC", linecolor="#ECECEC"),
        yaxis=dict(
            side="left",
            title="Price",
            titlefont=dict(color="black"),
            tickfont=dict(color="black"),
            gridcolor="#ECECEC",
            linecolor="#ECECEC",
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(
            orientation="h",
            y=-0.075,
            x=0.5,
            xanchor="center"
        )
    )
    
    return fig

def timeseriesplotting(valuation_df, valuation_metric, country):
    metric_field = LABEL_TO_FIELD.get(valuation_metric)
    if not metric_field:
        raise ValueError(f"No mapping found for '{valuation_metric}'")

    country_df = valuation_df[country]

    if metric_field not in country_df.columns:
        raise KeyError(f"'{metric_field}' not found in {country}'s columns: {country_df.columns.tolist()}")

    data = country_df[metric_field].dropna()
    mean_val, std_val = data.median(), data.std()

    fig = px.line(data, width=1450, height=600, title=f'{valuation_metric} : {country}')
    fig.update_traces(line=dict(color='#30415f'))

    for offset, dash, name in [(0, 'solid', 'Mean'), 
                               (+1, 'dot', '+ 1 Std'), 
                               (-1, 'dot', '- 1 Std'), 
                               (+2, 'dot', '+ 2 Std'), 
                               (-2, 'dot', '- 2 Std')]:
        fig.add_trace(go.Scatter(
            x=data.index,
            y=[mean_val + (offset * std_val)] * len(data),
            mode='lines',
            name=name,
            line=dict(dash=dash, color='grey')
        ))

    fig.update_layout(
        font=dict(family="Montserrat", size=13),
        title=dict(text=f'<b><span style="color:black;">{valuation_metric}</span> : {country}</b>', font=dict(size=16)),
        plot_bgcolor='white'
    )
    fig.update_xaxes(tickangle=45, title_text="", tickfont=dict(size=10))
    fig.update_yaxes(title_text=f'{valuation_metric}')

    return fig

def per_valuation_plotter(name_list, valuation_list, valuation_metric, winsorized_add_composite):
    rows = math.ceil(len(name_list) / 3)
    fig = make_subplots(rows=rows, cols=3, subplot_titles=[f"{valuation_metric} - {j}" for j in name_list])

    color_map = {
        'Forward PE': 'darkcyan', 'Price to Book': 'coral', 'CAPE': 'blue',
        'EV/Trailing EBITDA': 'red', 'Price to Sales': 'purple', 'Valuation Composite': 'green'
    }
    title_color = color_map.get(valuation_metric, 'black')

    for idx, region in enumerate(name_list):
        row, col = divmod(idx, 3)
        row += 1
        col += 1
        fig_piece = timeseriesplotting(winsorized_add_composite, valuation_metric, region)
        for trace in fig_piece.data:
            trace.showlegend = False
            fig.add_trace(trace, row=row, col=col)

        fig.layout.annotations[idx].update(
            text=f'<b><span style="color:{title_color};">{valuation_metric}</span> : {region}</b>',
            font=dict(size=16, family='Montserrat')
        )

    fig.update_layout(
        height=400 * rows, width=1500,
        font=dict(family="Montserrat", size=13), plot_bgcolor='white'
    )
    fig.update_xaxes(tickangle=45, tickfont=dict(size=10))
    fig.update_yaxes(title_text=valuation_metric)
    return fig

def clean_data_after_bloomberg(df, region_names, val_metrics):
    # Step 1: Detect and convert flat columns like "MXWO Index_PX_TO_BOOK_RATIO"
    if not isinstance(df.columns, pd.MultiIndex):
        try:
            tickers, metrics = zip(*[col.split('_', 1) for col in df.columns])
        except ValueError as e:
            raise ValueError(f"Column parsing failed: {e} — check if columns look like 'ticker_field'")
        df.columns = pd.MultiIndex.from_arrays([tickers, metrics], names=["Ticker", "Metric"])

    # Step 2: Map tickers to readable region/factor names
    unique_tickers = df.columns.get_level_values(0).unique()
    if len(unique_tickers) != len(region_names):
        raise ValueError(f"Mismatch: {len(unique_tickers)} tickers vs {len(region_names)} region_names. Please check.")

    ticker_to_name = dict(zip(unique_tickers, region_names))
    df.columns = pd.MultiIndex.from_tuples([
        (ticker_to_name[ticker], metric) for ticker, metric in df.columns
    ], names=["Region", "Metric"])

    # Step 3: Convert index to Month-Year format
    df.index = pd.to_datetime(df.index)
    df.index = df.index.strftime('%b-%Y')

    # Step 4: Winsorize
    df_wins = winsorize_df(df, 0.01, 0.99)

    # Step 5: Add Valuation Composite
    for region in df_wins.columns.levels[0]:
        sub_df = df_wins[region]
        z_scores = (sub_df - sub_df.mean()) / sub_df.std()
        if 'Forward PE' in z_scores.columns:
            z_scores = z_scores.drop(columns=['Forward PE'])
        composite = z_scores.mean(axis=1)
        df_wins[(region, 'Valuation Composite')] = composite

    # Step 6: Add Valuation Composite to the list (if not already)
    if 'Valuation Composite' not in val_metrics:
        val_metrics = val_metrics + ['Valuation Composite']

    # Step 7: Replace zeroes with NaN for cleanliness
    df_wins.replace(0, np.nan, inplace=True)

    return df_wins, val_metrics, z_scores

def generate_graphs(values_composite, names, prefix):
    for valuation_metric in values_composite[1]:
        var_name = f"{prefix}_{valuation_metric.replace(' ', '_').replace('/', '_')}"  # Fix naming
        graph_list = [plot(timeseriesplotting(values_composite[0], valuation_metric, name), 
                           output_type='div', include_plotlyjs='cdn') for name in names]

        globals()[var_name] = graph_list  # Directly create variables
        print(f"{var_name} = {graph_list}")  # Print for visibility

def graph_performance(data, title):
    # Color palettes
    full_palette = [
        "#30415f", "#f3a712", "#87b1a1", "#5ac5fe",
        "#a8c686", "#a0a197", "#e4572e", "#2337C6",
        "#B7B1B0", "#778BA5", "#990000"
    ]
    simp_palette = ["#30415f", "#DDDDDD", "#DDDDDD", "#DDDDDD"]
    
    fig = go.Figure()
    
    # Add traces depending on Series or DataFrame
    if isinstance(data, pd.Series):
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            mode='lines',
            name=data.name or "Series",
            line=dict(color=full_palette[0], width=2)
        ))
    elif isinstance(data, pd.DataFrame):
        use_full_colors = data.shape[1] >= 4
        palette = full_palette if use_full_colors else simp_palette
        for i, col in enumerate(data.columns):
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[col],
                mode='lines',
                name=str(col),
                line=dict(color=palette[i % len(palette)], width=2)
            ))
    
    # Apply layout
    fig.update_layout(
        title=title,
        xaxis_title='',
        yaxis_title='Price',
        template='plotly_white',
        hovermode='x unified',
        font=dict(family="Montserrat, sans-serif"),
        title_font=dict(family="Montserrat, sans-serif", size=22),
        legend_font=dict(family="Montserrat, sans-serif"),
        width=1100,
        height=600,
        xaxis=dict(gridcolor="#ECECEC", linecolor="#ECECEC"),
        yaxis=dict(
            side="left",
            title="Price",
            titlefont=dict(color="black"),
            tickfont=dict(color="black"),
            gridcolor="#ECECEC",
            linecolor="#ECECEC",
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(
            orientation="h",
            y=-0.075,
            x=0.5,
            xanchor="center"
        )
    )
    
    return fig

def legend_further_down_graph_performance(data, title):
    # Color palettes
    full_palette = [
        "#30415f", "#f3a712", "#87b1a1", "#5ac5fe",
        "#a8c686", "#a0a197", "#e4572e", "#2337C6",
        "#B7B1B0", "#778BA5", "#990000"
    ]
    simp_palette = ["#30415f", "#DDDDDD", "#DDDDDD", "#DDDDDD"]

    fig = go.Figure()

    # Add traces depending on Series or DataFrame
    if isinstance(data, pd.Series):
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            mode='lines',
            name=data.name or "Series",
            line=dict(color=full_palette[0], width=2)
        ))
    elif isinstance(data, pd.DataFrame):
        use_full_colors = data.shape[1] >= 4
        palette = full_palette if use_full_colors else simp_palette
        for i, col in enumerate(data.columns):
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[col],
                mode='lines',
                name=str(col),
                line=dict(color=palette[i % len(palette)], width=2)
            ))

    # Apply layout
    fig.update_layout(
        title=title,
        xaxis_title='',
        yaxis_title='Price',
        template='plotly_white',
        hovermode='x unified',
        font=dict(family="Montserrat, sans-serif"),
        title_font=dict(family="Montserrat, sans-serif", size=22),
        legend_font=dict(family="Montserrat, sans-serif"),
        width=1100,
        height=600,
        xaxis=dict(gridcolor="#ECECEC", linecolor="#ECECEC"),
        yaxis=dict(
            side="left",
            title="Price",
            titlefont=dict(color="black"),
            tickfont=dict(color="black"),
            gridcolor="#ECECEC",
            linecolor="#ECECEC",
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(
            orientation="h",
            y=-0.475,
            x=0.5,
            xanchor="center"
        )
    )

    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def simp_graph_performance(data, title):
    # Color palettes
    full_palette = [
        "#30415f", "#f3a712", "#87b1a1", "#5ac5fe",
        "#a8c686", "#a0a197", "#e4572e", "#2337C6",
        "#B7B1B0", "#778BA5", "#990000"
    ]
    simp_palette = ["#30415f", "#DDDDDD", "#DDDDDD", "#DDDDDD"]

    fig = go.Figure()

    # Add traces depending on Series or DataFrame
    if isinstance(data, pd.Series):
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            mode='lines',
            name=data.name or "Series",
            line=dict(color=full_palette[0], width=2)
        ))
    elif isinstance(data, pd.DataFrame):
        use_full_colors = data.shape[1] >= 4
        palette = full_palette if use_full_colors else simp_palette
        for i, col in enumerate(data.columns):
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[col],
                mode='lines',
                name=str(col),
                line=dict(color=palette[i % len(palette)], width=2)
            ))

    # Apply layout
    fig.update_layout(
        title=title,
        xaxis_title='',
        yaxis_title='Price',
        template='plotly_white',
        hovermode='x unified',
        font=dict(family="Montserrat, sans-serif"),
        title_font=dict(family="Montserrat, sans-serif", size=22),
        legend_font=dict(family="Montserrat, sans-serif"),
        width=1100,
        height=600,
        xaxis=dict(gridcolor="#ECECEC", linecolor="#ECECEC"),
        yaxis=dict(
            side="left",
            title="Price",
            titlefont=dict(color="black"),
            tickfont=dict(color="black"),
            gridcolor="#ECECEC",
            linecolor="#ECECEC",
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(
            orientation="h",
            y=-0.075,
            x=0.5,
            xanchor="center"
        )
    )

    return fig.to_html(full_html=False, include_plotlyjs='cdn')

def cross_sectional_current_table_maker(data_tuple, return_df=False):
    df, val_metrics = data_tuple[0], data_tuple[1]

    # Take the last row of the MultiIndex DataFrame
    df_last_row = df.iloc[-1].reset_index()  # Columns: ['Region', 'Metric', value]

    # Pivot the table so rows are 'Region', columns are metrics
    df_pivot = df_last_row.pivot(index='Region', columns='Metric', values=df.iloc[-1].name).reset_index()

    # Decide naming based on first region name
    first_label = df_pivot['Region'].iloc[0]
    if first_label in ['Value', 'Growth', 'Quality', 'Small', 'Large', 'Low Volatility', 'Enhanced Value']:
        df_pivot = df_pivot.rename(columns={'Region': 'Global Factors'})
    else:
        df_pivot = df_pivot.rename(columns={'Region': 'Region'})

    # Round for display
    df_pivot = df_pivot.round(2)

    # Rename columns to match metric names
    metric_field_to_name = dict(zip(df.columns.levels[1], val_metrics))
    df_pivot = df_pivot.rename(columns=metric_field_to_name)

    # Sort by 'Forward PE' if available
    if 'Forward PE' in df_pivot.columns:
        df_pivot = df_pivot.sort_values(by='Forward PE', ascending=False)

    # Define CSS styles
    styles = [
        {'selector': 'td:hover', 'props': [('background-color', '#30415f')]},
        {'selector': 'th:not(.index_name)', 'props': [('background-color', '#30415f'), ('color', 'white'), ('text-align', 'center')]},
        {'selector': 'td', 'props': [('font-size', '14px'), ('text-align', 'center'), ('width', '120px'), ('border', '1px solid #ddd')]},
        {'selector': 'th', 'props': [('text-align', 'left'), ('border', '1px solid #ddd')]},
        {'selector': 'table', 'props': [('border-collapse', 'collapse')]}
    ]

    # Generate styled HTML
    styled_table = (
        df_pivot.style
        .set_table_styles(styles)
        .format(precision=1)
        .background_gradient(
            subset=['Valuation Composite'] if 'Valuation Composite' in df_pivot.columns else [],
            cmap='RdYlGn_r',
            axis=0
        )
        .hide_index()
        .to_html()
    )

    if return_df:
        return df_pivot, styled_table
    return styled_table

def winsorize_df(df, lower=0.01, upper=0.99):
    return df.apply(lambda x: np.clip(x, np.percentile(x.dropna(), lower * 100), np.percentile(x.dropna(), upper * 100)) if np.issubdtype(x.dtype, np.number) else x)

def process_index(raw_df, lower=0.01, upper=0.99):
    # Winsorize and calculate stats
    winsorized = winsorize_df(raw_df, lower, upper)
    
    # Calculate column-wise statistics
    medians = winsorized.median()
    stds = winsorized.std()
    
    # Normalize using column stats
    normalized = (winsorized - medians) / stds
    mean_series = normalized.mean(axis=1)
    
    # Create final DataFrame with bounds
    return pd.DataFrame({
        'z_score': mean_series,
        'mean': mean_series.median(),  # Average of column medians
        'upper_bound': mean_series.median()+1 ,
        'lower_bound': mean_series.median()-1
    })

def calc_return_1m_to_10y(sector_df, sector_names, cmap="RdYlGn"):
    sector_df.columns = sector_names
    onemonth = (sector_df.iloc[-1] / sector_df.iloc[-2]) - 1
    threemonth = (sector_df.iloc[-1] / sector_df.iloc[-4]) - 1
    sixmonth = (sector_df.iloc[-1] / sector_df.iloc[-7]) - 1
    oneyr = (sector_df.iloc[-1] / sector_df.iloc[-13]) - 1
    threeyr = ((sector_df.iloc[-1] / sector_df.iloc[-37]) ** (1/3)) - 1
    fiveyr = ((sector_df.iloc[-1] / sector_df.iloc[-61]) ** (1/5)) - 1
    tenyr = ((sector_df.iloc[-1] / sector_df.iloc[-121]) ** (1/10)) - 1

    returns_dict = {
        '1 Month': onemonth,
        '3 Months': threemonth,
        '6 Months': sixmonth,
        '1 Year': oneyr,
        '3 Years': threeyr,
        '5 Years': fiveyr,
        '10 Years': tenyr
    }

    returns_df = pd.DataFrame(returns_dict, index=sector_df.columns).applymap(lambda x: f"{x*100:.2f}%")
    return returns_df




class BaseAnalytics:
    def fetch_and_cache(self, blp, ticker, fields, start_date, table_name, freq, **kwargs):
        try:
            cached = read_cache(table_name)
            last_date = cached.index.max()
            
            # When adding new tickers, we need to check if they exist in cache
            # If not, start from the original start_date for those tickers
            if isinstance(ticker, list):
                # For multiple tickers, we need to handle new vs existing separately
                # Let's just re-fetch everything if we detect new tickers
                # This is simpler and ensures all data is captured
                
                # Get ticker names from cache columns (remove field suffix)
                if isinstance(cached.columns, pd.MultiIndex):
                    cached_tickers = set([col[0] for col in cached.columns])
                else:
                    cached_tickers = set([col.split('_')[0] for col in cached.columns if '_' in col])
                
                requested_tickers = set(ticker)
                new_tickers = requested_tickers - cached_tickers
                
                if new_tickers:
                    print(f"New tickers detected: {new_tickers}")
                    print(f"Re-fetching all data from {start_date}")
                    fetch_start = start_date
                else:
                    fetch_start = (last_date + BDay(1)).strftime('%Y-%m-%d')
            else:
                # Single ticker - check if it exists in cache
                ticker_pattern = f"{ticker}_"
                if any(ticker_pattern in str(col) for col in cached.columns):
                    fetch_start = (last_date + BDay(1)).strftime('%Y-%m-%d')
                else:
                    print(f"New ticker detected: {ticker}")
                    print(f"Fetching from {start_date}")
                    fetch_start = start_date
                    
        except Exception as e:
            print(f"Cache read failed or table doesn't exist for {table_name}. Reason: {e}")
            fetch_start = start_date

        print(f"Fetching {ticker} from {fetch_start} → freq={freq}")
        new_data = blp.bdh(ticker, fields, fetch_start, Per=freq, **kwargs)

        if new_data.empty:
            print(f"No new data returned for {ticker}")
            return read_cache(table_name)

        new_data.index.name = "date"

        # --- SMART COLUMN HANDLING ---
        if isinstance(new_data.columns, pd.MultiIndex):
            # Flatten multiindex columns
            new_data.columns = [f"{t}_{f}" for t, f in new_data.columns]
        elif isinstance(ticker, str) and len(fields) == 1:
            # Single ticker, single field: rename to simple 'value'
            new_data.columns = ['value']
        elif isinstance(ticker, list) and len(fields) == 1:
            # Multiple tickers, single field
            new_data.columns = [f"{t}_{fields[0]}" for t in ticker]
        elif isinstance(ticker, str) and len(fields) > 1:
            # Single ticker, multiple fields
            new_data.columns = [f"{ticker}_{f}" for f in fields]
        elif isinstance(ticker, list) and len(fields) > 1:
            # Not expected — multi-ticker, multi-field needs multiindex flattening
            raise ValueError("Unexpected shape: multi-ticker and multi-field without MultiIndex columns")
        else:
            # Fallback: convert whatever columns exist into strings
            new_data.columns = [str(col) for col in new_data.columns]

        return append_to_cache(table_name, new_data)


# class BaseAnalytics:
#     def fetch_and_cache(self, blp, ticker, fields, start_date, table_name, freq, **kwargs):
#         try:
#             cached = read_cache(table_name)
#             last_date = cached.index.max()
#             fetch_start = (last_date + BDay(1)).strftime('%Y-%m-%d')
#         except Exception as e:
#             print(f"Cache read failed or table doesn't exist for {table_name}. Reason: {e}")
#             fetch_start = start_date

#         print(f"Fetching {ticker} from {fetch_start} → freq={freq}")
#         new_data = blp.bdh(ticker, fields, fetch_start, Per=freq, **kwargs)

#         if new_data.empty:
#             print(f"No new data returned for {ticker}")
#             return read_cache(table_name)

#         new_data.index.name = "date"

#         # --- SMART COLUMN HANDLING ---
#         if isinstance(new_data.columns, pd.MultiIndex):
#             # Flatten multiindex columns
#             new_data.columns = [f"{t}_{f}" for t, f in new_data.columns]
#         elif isinstance(ticker, str) and len(fields) == 1:
#             # Single ticker, single field: rename to simple 'value'
#             new_data.columns = ['value']
#         elif isinstance(ticker, list) and len(fields) == 1:
#             # Multiple tickers, single field
#             new_data.columns = [f"{t}_{fields[0]}" for t in ticker]
#         elif isinstance(ticker, str) and len(fields) > 1:
#             # Single ticker, multiple fields
#             new_data.columns = [f"{ticker}_{f}" for f in fields]
#         elif isinstance(ticker, list) and len(fields) > 1:
#             # Not expected — multi-ticker, multi-field needs multiindex flattening
#             raise ValueError("Unexpected shape: multi-ticker and multi-field without MultiIndex columns")
#         else:
#             # Fallback: convert whatever columns exist into strings
#             new_data.columns = [str(col) for col in new_data.columns]

#         return append_to_cache(table_name, new_data)
    







########################
########################
########################
# JB TIPS, FUTURES
########################
import plotly.io as pyo
import plotly.express as px
import plotly.graph_objects as go

class BondMarketCharts(BaseAnalytics):
    def __init__(self, blp):
        self.blp = blp
        self.font = "Montserrat"

    def build_rate_futures_chart(self) -> str:
        tickers = ['US0ANM DEC2025 Index', 'AU0ANM DEC2025 Index', 'EZ0BNM DEC2025 Index']
        labels = ['US', 'AU', 'EU']
        colors = ["#020035", "#4682B4", "orange"]

        dfs = []
        for ticker in tickers:
            label = ticker.replace(" ", "_").replace(".", "")
            df = self.fetch_and_cache(self.blp, ticker, ['px_last'], '2024-01-31', f"{label}_futures", freq='D')
            dfs.append(df)

        combined = [df.iloc[:, 0] for df in dfs]  # assume single column
        data = combined[0].to_frame(labels[0])
        for col, name in zip(combined[1:], labels[1:]):
            data[name] = col

        fig = px.line(-data.iloc[20:], color_discrete_sequence=colors)
        fig.update_layout(
            font_family=self.font,
            title={"text": "Number of Rate Cuts by December 2025", "font": {"size": 22}},
            xaxis_title="",
            yaxis_title="Number of Cuts/Hikes",
            yaxis=dict(side="right", titlefont=dict(color="black"), tickfont=dict(color="black"),
                       gridcolor="lightgray", linecolor="gray"),
            xaxis=dict(gridcolor="lightgray", linecolor="gray"),
            plot_bgcolor="white",
            paper_bgcolor="white",
            width=950,
            height=600,
            legend=dict(orientation="h", y=-0.075, x=0.5, xanchor="center")
        )
        return pyo.to_html(fig, full_html=False, include_plotlyjs='cdn')

    def build_yield_decomp_chart(self) -> str:
        tickers = ['GTII10 Govt', 'USGG10YR Index', 'USGGBE10 Index']
        labels = ['US Real Yld 10Y', 'US Nominal Yld 10Y', 'US 10Y Breakeven Inflation']
        colors = {
            'US 10Y Breakeven Inflation': '#D7A96D',
            'US Real Yld 10Y': '#4682B4',
            'US Nominal Yld 10Y': '#020035'
        }

        dfs = []
        for ticker in tickers:
            label = ticker.replace(" ", "_").replace(".", "")
            df = self.fetch_and_cache(self.blp, ticker, ['px_last'], '2014-11-01', f"{label}_yields", freq='D')
            dfs.append(df.iloc[:, 0])

        df = pd.concat(dfs, axis=1)
        df.columns = labels

        fig = go.Figure()

        # Layer 1: Breakeven fill area
        fig.add_trace(go.Scatter(
            x=df.index, y=df['US 10Y Breakeven Inflation'],
            fill='tozeroy',
            name='10Y Breakeven',
            mode='lines',
            line=dict(color=colors['US 10Y Breakeven Inflation'], width=1.2),
            stackgroup='one',
            hovertemplate='Breakeven: %{y:.2f}%<extra></extra>'
        ))

        # Layer 2: Real yield area fill (stacked on breakeven)
        fig.add_trace(go.Scatter(
            x=df.index, y=df['US Real Yld 10Y'],
            fill='tonexty',
            name='10Y Real Yield',
            mode='lines',
            line=dict(color=colors['US Real Yld 10Y'], width=1.2),
            stackgroup='one',
            hovertemplate='Real Yield: %{y:.2f}%<extra></extra>'
        ))

        # Layer 3: Nominal yield as bold line (overlaid)
        fig.add_trace(go.Scatter(
            x=df.index, y=df['US Nominal Yld 10Y'],
            name='10Y Nominal Yield',
            mode='lines',
            line=dict(color=colors['US Nominal Yld 10Y'], width=3.5),
            hovertemplate='Nominal Yield: %{y:.2f}%<extra></extra>'
        ))

        fig.update_layout(
            width=1200,
            height=700,
            title=dict(text='US 10Y Yields: Real, Nominal, and Breakeven Inflation', font=dict(size=24), x=0.5),
            font=dict(family=self.font, size=13),
            legend=dict(
                orientation="h", yanchor="bottom", y=-0.25,
                xanchor="center", x=0.5,
                font=dict(size=13)
            ),
            xaxis_title="Date",
            yaxis_title="Yield (%)",
            template="plotly_white",
            yaxis=dict(rangemode="tozero"),
            hovermode="x unified"
        )
        return pyo.to_html(fig, full_html=False, include_plotlyjs='cdn')

    def build_tips_chart(self) -> str:
        tickers = ['GTII10 Govt', 'GTGBPII10Y Govt', 'GTAUDII10Y Govt', 'GTDEMII10Y Govt']
        labels = ['US', 'UK', 'AU', 'GER']
        dfs = []
        for ticker in tickers:
            label = ticker.replace(" ", "_").replace(".", "")
            df = self.fetch_and_cache(self.blp, ticker, ['px_last'], '2007-08-01', f"{label}_tips", freq='W')
            dfs.append(df.iloc[:, 0])

        df = pd.concat(dfs, axis=1)
        df.columns = labels
        return graph_performance(df, "Inflation Linked Bonds")

chart_builder = BondMarketCharts(blp)
rate_futures_html = chart_builder.build_rate_futures_chart()
ten_10y_decomp_html = chart_builder.build_yield_decomp_chart()
tips_html = chart_builder.build_tips_chart()



########################
########################
########################
# MAX YIELDS
########################
import plotly.io as pyo

class YieldCurveVisualizer(BaseAnalytics):
    """Class for generating and visualizing yield curves and spread charts"""

    # Class constants
    TICKERS = {
        'US': ['USGG3M Index', 'USGG6M Index', 'USGG12M Index', 'USGG2YR Index', 'USGG3Y Index',
               'USGG5YR Index', 'USGG7Y Index', 'USGG10Y Index', 'USGG20Y Index', 'USGG30Y Index'],
        'EU': ['GTEUR3M Govt', 'GTEUR6M Govt', 'GTEUR1Y Govt', 'GTEUR2Y Govt', 'GTEUR3Y Govt',
               'GTEUR5Y Govt', 'GTEUR7Y Govt', 'GTEUR10Y Govt', 'GTEUR20Y Govt', 'GTEUR30Y Govt'],
        'AU': ['GTAUD3M Govt', 'GTAUD1Y Govt', 'GTAUD2Y Govt', 'GTAUD3Y Govt', 'GTAUD5Y Govt',
               'GTAUD7Y Govt', 'GTAUD10Y Govt', 'GTAUD20Y Govt', 'GTAUD30Y Govt']
    }

    MATURITY_LABELS = {
        'US': ['3m', '6m', '12m', '2y', '3y', '5y', '7y', '10y', '20y', '30y'],
        'EU': ['3m', '6m', '12m', '2y', '3y', '5y', '7y', '10y', '20y', '30y'],
        'AU': ['3m', '12m', '2y', '3y', '5y', '7y', '10y', '20y', '30y']
    }

    SPREAD_INDICES = {
        'Aus_comp': {'ticker': 'BACM0 Index', 'field': 'OAS_SPREAD_MID', 'title': 'Ausbond Composite 0+Yr Spread'},
        'Aus_cred': {'ticker': 'BACR0 Index', 'field': 'OAS_SPREAD_MID', 'title': 'Ausbond Credit 0+Yr Spread'},
        'Aus_FRN': {'ticker': 'BAFRN0 Index', 'field': 'OAS_SPREAD_MID', 'title': 'Ausbond Credit FRN 0+Yr Spread'},
        'US_corp': {'ticker': 'LUACOAS Index', 'field': 'PX_LAST', 'title': 'US Agg Corporate OAS Spread'},
        'US_cred': {'ticker': 'LUCROAS Index', 'field': 'PX_LAST', 'title': 'US Agg Credit OAS Spread'}
    }

    def __init__(self, blp_client):
        self.blp = blp_client
        
        # Handle the business day logic
        today = datetime.today()
        last_business_day = today - timedelta(days=1)
        
        # If today is Monday, adjust to use Friday (3 days ago)
        if today.weekday() == 0:  # Monday is 0
            last_business_day = today - timedelta(days=3)  # Use Friday
            
        self.today = last_business_day
        self.charts = {}
        self.time_periods = {
            'Today': last_business_day,
            'ThreeDays': last_business_day - timedelta(days=3),
            'Week': last_business_day - timedelta(days=7),
            'Month': last_business_day - timedelta(days=30)
        }

        self.curves = {}
        self.global_curves = {}
        self.hedged_curves = {}

    def create_line_chart(self, df_long, x_col, y_col, color_col, title, width=900, height=500, dash_styles=None):
        unique_lines = df_long[color_col].unique()
        fig = px.line(df_long, x=x_col, y=y_col, color=color_col, title=title, width=width, height=height)

        # Styling all traces
        for i, trace in enumerate(fig.data):
            trace.line.width = 2
            trace.marker.line = dict(width=0)

            if trace.name == 'Today':
                trace.line.width = 3
                trace.line.color = '#30415f'
                trace.marker.color = '#30415f'
                trace.line.dash = 'solid'
            elif dash_styles:
                dash_options = dash_styles if len(dash_styles) >= len(unique_lines) else ['dash', 'dot', 'dashdot']
                trace.line.dash = dash_options[i % len(dash_options)]

        fig.update_layout(
            font=dict(family="Montserrat", size=13, color="#30415f"),
            plot_bgcolor='white',
            paper_bgcolor='white',
            xaxis=dict(showgrid=False, linecolor="lightgray"),
            yaxis=dict(showgrid=False, linecolor="lightgray"),
            legend_title_text=color_col,
            legend=dict(
                font=dict(family="Montserrat", color="#30415f"),
                orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5
            ),
            template="plotly_white",
            margin=dict(t=60, b=60)
        )

        fig.update_traces(hovertemplate=f"%{{x}}<br>{y_col}: %{{y:.2f}}")

        return fig



    def fetch_yield_curve_data(self, region):
        curves = {}
        for period_name, period_date in self.time_periods.items():
            curve = []
            for ticker in self.TICKERS[region]:
                table_name = f"{region}_{ticker.replace(' ', '_')}_{period_name}_curve"
                df = self.fetch_and_cache(self.blp, ticker, ['PX_LAST'], '2022-01-01', table_name, freq='D')
                if df.empty:
                    curve.append(np.nan)
                else:
                    df = df.dropna()
                    df.index = pd.to_datetime(df.index)
                    closest_date = df.index[np.argmin(np.abs((df.index - period_date).days))]
                    curve.append(df.loc[closest_date].values[0])
            curves[period_name] = pd.DataFrame([curve], columns=self.MATURITY_LABELS[region])

        df = pd.concat(curves.values(), axis=0)
        df.index = curves.keys()

        if region == 'AU' and '6m' not in df.columns:
            df.insert(1, '6m', (df['3m'] + df['12m']) / 2)

        return df

    def process_spread_data(self, index_name):
        info = self.SPREAD_INDICES[index_name]
        table_name = f"{index_name}_spread"
        df = self.fetch_and_cache(self.blp, info['ticker'], [info['field']], '2015-01-01', table_name, freq='D')
        col = df.columns[0]
        df['Average'] = df[col].mean()
        df['+1 STD'] = df[col].mean() + df[col].std()
        df['-1 STD'] = df[col].mean() - df[col].std()
        df = df.reset_index().rename(columns={'index': 'date'})
        return df.melt(id_vars='date', var_name='Legend', value_name='OAS_Spread')

    def generate_individual_curves(self):
        for region in ['US', 'EU', 'AU']:
            curve_df = self.fetch_yield_curve_data(region)
            self.curves[region] = curve_df
            df = curve_df.transpose().reset_index().rename(columns={'index': 'Maturity'})
            df_long = df.melt(id_vars='Maturity', var_name='TimeFrame', value_name='Yield')
            fig = self.create_line_chart(df_long, 'Maturity', 'Yield', 'TimeFrame', f'{region} Yield Curve {date.today()}', dash_styles=['solid', 'dash', 'dash', 'dash'])
            self.charts[f'{region}_chart_html'] = pyo.to_html(fig, full_html=False, include_plotlyjs='cdn')

    def generate_spread_charts(self):
        for name in self.SPREAD_INDICES:
            df_long = self.process_spread_data(name)
            fig = self.create_line_chart(df_long, 'date', 'OAS_Spread', 'Legend', f"{self.SPREAD_INDICES[name]['title']} {date.today()}", dash_styles=['solid', 'dash', 'dash', 'dash'])
            self.charts[f'{name}_chart_html'] = pyo.to_html(fig, full_html=False, include_plotlyjs='cdn')
            

    def fetch_global_curves(self):
        for region in ['US', 'EU', 'AU']:
            curve = []
            for ticker in self.TICKERS[region]:
                table_name = f"{region}_{ticker.replace(' ', '_')}_globalcurve"
                df = self.fetch_and_cache(self.blp, ticker, ['PX_LAST'], '2022-01-01', table_name, freq='D')
                curve.append(df.iloc[-1, 0])
            df_region = pd.DataFrame([curve], columns=self.MATURITY_LABELS[region], index=[f'{region} Yield Curve'])
            if region == 'AU' and '6m' not in df_region.columns:
                df_region.insert(1, '6m', (df_region['3m'] + df_region['12m']) / 2)
            self.global_curves[region] = df_region

    def generate_global_comparison(self):
        self.fetch_global_curves()
        global_df = pd.concat(self.global_curves.values(), axis=0, join='outer')
        global_transposed = global_df.transpose()
        df_global = global_transposed.reset_index().rename(columns={'index': 'Maturity'})
        df_global_long = df_global.melt(id_vars='Maturity', var_name='Curve', value_name='Yield')
        fig = self.create_line_chart(df_global_long, 'Maturity', 'Yield', 'Curve', f'Global Yield Curves {date.today()}')
        for trace in fig.data:
            trace.line.dash = 'solid'
        self.charts['Global_chart_html'] = pyo.to_html(fig, full_html=False, include_plotlyjs='cdn')

    def generate_aud_hedged_comparison(self):
        fx_tickers = {
            'AUDUSDspot': 'AUDUSD Curncy',
            'AUDEURspot': 'AUDEUR Curncy',
            'EURAUDspot': 'EURAUD Curncy',
            'AUDUSDforward': 'AUD1M BGN Curncy',
            'EURAUDforward': 'ADEU1M BGN Curncy'
        }
        fx_data = {}
        for label, ticker in fx_tickers.items():
            table_name = f"{label}_fx"
            df = self.fetch_and_cache(self.blp, ticker, ['PX_LAST'], '2023-04-01', table_name, freq='D')
            fx_data[label] = df.iloc[-1, 0]

        AUDUSDforward = (fx_data['AUDUSDforward'] * 12) / 10000
        EURAUDforward = (fx_data['EURAUDforward'] * 12) / 10000
        AUDEURimplied = 1 / (fx_data['EURAUDspot'] + EURAUDforward)
        AUDEURforward = AUDEURimplied - fx_data['AUDEURspot']

        if not self.global_curves:
            self.fetch_global_curves()

        us_hedged = {}
        for col in self.global_curves['US'].columns:
            val = (self.global_curves['US'][col] / 100) - AUDUSDforward / (1 + (self.global_curves['US'][col] / 100)) * fx_data['AUDUSDspot']
            us_hedged[col] = val
        self.hedged_curves['US'] = pd.DataFrame(us_hedged, index=['US']).mul(100).round(3)

        eu_hedged = {}
        for col in self.global_curves['EU'].columns:
            val = (self.global_curves['EU'][col] / 100) - AUDEURforward / (1 + (self.global_curves['EU'][col] / 100)) * fx_data['AUDEURspot']
            eu_hedged[col] = val
        self.hedged_curves['EU'] = pd.DataFrame(eu_hedged, index=['EU']).mul(100).round(3)

        self.hedged_curves['AU'] = self.global_curves['AU']

        hedged_df = pd.concat(self.hedged_curves.values(), axis=0, join='outer')
        hedged_transposed = hedged_df.transpose()
        df_hedged = hedged_transposed.reset_index().rename(columns={'index': 'Maturity'})
        df_hedged_long = df_hedged.melt(id_vars='Maturity', var_name='Curve', value_name='Yield')
        fig = self.create_line_chart(df_hedged_long, 'Maturity', 'Yield', 'Curve', f'Global Yield Curves (AUD Hedged) {date.today()}')
        for trace in fig.data:
            trace.line.dash = 'solid'
        self.charts['Globalhedged_chart_html'] = pyo.to_html(fig, full_html=False, include_plotlyjs='cdn')


    def generate_all_charts(self):
        """Generate all charts in one call"""
        self.generate_individual_curves()
        self.generate_spread_charts()
        self.generate_global_comparison()
        self.generate_aud_hedged_comparison()
        return self.charts

def run_yield_curve_analysis(blp):
    """Run the full yield curve analysis using the Bloomberg API"""
    visualizer = YieldCurveVisualizer(blp)
    charts = visualizer.generate_all_charts()

    return charts, visualizer

charts, visualizer = run_yield_curve_analysis(blp)
US_chart_html = charts['US_chart_html']
EU_chart_html = charts['EU_chart_html']
AU_chart_html = charts['AU_chart_html']
Aus_comp_chart_html = charts['Aus_comp_chart_html']
Aus_cred_chart_html = charts['Aus_cred_chart_html']
Aus_FRN_chart_html = charts['Aus_FRN_chart_html']
US_corp_chart_html = charts['US_corp_chart_html']
US_cred_chart_html = charts['US_cred_chart_html']
Global_chart_html = charts['Global_chart_html']
Globalhedged_chart_html = charts['Globalhedged_chart_html']





import base64
def make_dashboard_download(df, title, filename):
    csv = df.to_csv(index=True)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'data:text/csv;base64,{b64}'

    return f"""
    <div style="margin-bottom: 16px;">
        <h4 style="font-family: Montserrat, sans-serif; font-weight: 500; font-size: 15px; color: #30415f;">
            {title}
        </h4>
        <a href="{href}" download="{filename}" 
           style="display:inline-block; background-color:#30415f; color:white; font-size:13px;
                  padding: 6px 14px; border-radius: 6px; text-decoration: none;">
            ⬇️ Download CSV
        </a>
    </div>
    """

download_blocks = []

# Global yield curves (unhedged)
global_df = pd.concat(visualizer.global_curves.values(), keys=visualizer.global_curves.keys())
download_blocks.append(make_dashboard_download(global_df.T, "Global Yield Curves", "global_yields.csv"))

# AUD Hedged curves
hedged_df = pd.concat(visualizer.hedged_curves.values(), keys=visualizer.hedged_curves.keys())
download_blocks.append(make_dashboard_download(hedged_df.T, "Global Yield Curves (AUD Hedged)", "global_yields_aud_hedged.csv"))

# Credit Spreads
for spread_key in ['Aus_comp', 'Aus_cred', 'Aus_FRN', 'US_corp', 'US_cred']:
    spread_df = visualizer.process_spread_data(spread_key)
    spread_title = f"{visualizer.SPREAD_INDICES[spread_key]['title']} Spread History"
    filename = f"{spread_key}_spread.csv"
    download_blocks.append(make_dashboard_download(spread_df, spread_title, filename))


tips_df = chart_builder.fetch_and_cache(
    chart_builder.blp,
    ['GTII10 Govt', 'GTGBPII10Y Govt', 'GTAUDII10Y Govt', 'GTDEMII10Y Govt'],
    ['px_last'],
    '2007-08-01',
    'TIPS_all',
    freq='W'
)
tips_df.columns = ['US', 'UK', 'AU', 'GER']
download_blocks.append(make_dashboard_download(tips_df, "Inflation Linked Bonds (TIPS)", "inflation_linked_bonds.csv"))

yield_curve_downloads_html = "\n".join(download_blocks)





########################
########################
########################
# DAILY MARKET
########################

from pandas.tseries.offsets import BDay, DateOffset

class DailyorWeeklyStuff(BaseAnalytics):
    def __init__(self, blp):
        self.blp = blp


    def styled_dashboard_table(self, df, title):
        return f"""
        <h3 style="font-family: Montserrat, sans-serif; font-weight: 700; font-size: 18px; color: #30415f; margin-top: 30px;">
            {title}
        </h3>
        <div style="
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 6px rgba(0,0,0,0.05);
            padding: 12px;
            margin-bottom: 20px;
            overflow-x: auto;
            font-family: Montserrat, sans-serif;
            font-size: 13px;
        ">
            <table style="border-collapse: collapse; width: 100%;">
                <thead>
                    <tr>
                        <th style="text-align: left; padding: 8px; background-color: #30415f; color: white;">Name</th>
                        {''.join([f'<th style="text-align: center; padding: 8px; background-color: #f0f0f0;">{col}</th>' for col in df.columns])}
                    </tr>
                </thead>
                <tbody>
                    {''.join([
                        f"<tr><td style='padding: 8px; font-weight: bold; color: #30415f;'>{idx}</td>" +
                        ''.join([f"<td style='padding: 8px; text-align: center;'>{val}</td>" for val in row]) +
                        "</tr>"
                        for idx, row in df.iterrows()
                    ])}
                </tbody>
            </table>
        </div>
        """


    def get_weekly_valuations(self) -> dict:
        tickers = {
            'SPX Index': ['PX_TO_BOOK_RATIO', 'LONG_TERM_PRICE_EARNINGS_RATIO', 'CURRENT_EV_TO_T12M_EBITDA', 'PX_TO_SALES_RATIO'],
            'SXXP Index': ['PX_TO_BOOK_RATIO', 'LONG_TERM_PRICE_EARNINGS_RATIO', 'CURRENT_EV_TO_T12M_EBITDA', 'PX_TO_SALES_RATIO'],
            'NKY Index': ['PX_TO_BOOK_RATIO', 'LONG_TERM_PRICE_EARNINGS_RATIO', 'CURRENT_EV_TO_T12M_EBITDA', 'PX_TO_SALES_RATIO'],
            'AS51 Index': ['PX_TO_BOOK_RATIO', 'LONG_TERM_PRICE_EARNINGS_RATIO', 'PX_TO_SALES_RATIO'],
            'MXEF Index': ['PX_TO_BOOK_RATIO', 'LONG_TERM_PRICE_EARNINGS_RATIO', 'PX_TO_SALES_RATIO']
        }

        today_str = datetime.now().strftime("%Y-%m-%d")
        data = {}

        for ticker, fields in tickers.items():
            table_name = f"{ticker.replace(' ', '_')}_weekly_vals"

            try:
                df = self.fetch_and_cache(self.blp, ticker, fields, '1997-05-10', table_name, freq='W')
                df_latest = self.fetch_and_cache(self.blp, ticker, fields, today_str, table_name, freq='D')

                # Merge and sort, only keep new daily data not in weekly cache
                if df_latest is not None and not df_latest.empty:
                    df_latest = df_latest[~df_latest.index.isin(df.index)]
                    combined = pd.concat([df, df_latest]).sort_index()
                else:
                    combined = df

                if combined.empty:
                    print(f"[Warning] Skipping {ticker}: No data available after merging.")
                    continue

                data[ticker] = process_index(combined)

            except Exception as e:
                print(f"[Error] Skipping {ticker} due to issue: {e}")
                continue

        # Ensure all required indices are present before creating combined
        required_indices = ['NKY Index', 'SXXP Index', 'SPX Index', 'AS51 Index', 'MXEF Index']
        if all(idx in data for idx in required_indices):
            combined = pd.concat([
                data['NKY Index'],
                data['SXXP Index'],
                data['SPX Index'],
                data['AS51 Index'],
                data['MXEF Index']
            ], axis=1, join='inner', keys=['Nikkei', 'Stoxx 600', 'SP500', 'ASX 200', 'MSCI EM']).xs('z_score', axis=1, level=1)
        else:
            combined = pd.DataFrame()
            print("[Warning] Not all regional indices available for combined chart.")

        return {
            'sp500': graph_performance_with_width(data.get('SPX Index', pd.DataFrame()), "S&P 500 Valuation Z-Scores", 800, 400),
            'eur': graph_performance_with_width(data.get('SXXP Index', pd.DataFrame()), "Stoxx 600 Valuation Z-Scores", 800, 400),
            'nky': graph_performance_with_width(data.get('NKY Index', pd.DataFrame()), "Nikkei Valuation Z-Scores", 800, 400),
            'asx': graph_performance_with_width(data.get('AS51 Index', pd.DataFrame()), "ASX 200 Valuation Z-Scores", 800, 400),
            'em': graph_performance_with_width(data.get('MXEF Index', pd.DataFrame()), "MSCI EM Valuation Z-Scores", 800, 400),
            'combined': graph_performance_with_width(combined, "Regional Composite Valuation Z-Scores", 800, 400)
        }

    
    # def get_daily_market_watch(self, asset_path='temp_shortened.csv') -> tuple[dict, pd.DataFrame, dict]:
    #     today = pd.Timestamp.today().normalize()
    #     while today.weekday() >= 5:
    #         today -= timedelta(days=1)

    #     yesterday = today - timedelta(days=1)
    #     while yesterday.weekday() >= 5:
    #         yesterday -= timedelta(days=1)

    #     anchor_date = yesterday

    #     asset = pd.read_csv(asset_path, index_col=0).drop_duplicates()
    #     assets = asset.index.to_list()

    #     table_name = "market_watch_tot_return"
    #     raw = self.fetch_and_cache(self.blp, assets, ['tot_return_index_gross_dvds'], '2010-01-31', table_name, freq='D')

    #     # Fix: only drop a level if columns are MultiIndex
    #     if isinstance(raw.columns, pd.MultiIndex):
    #         raw = raw.droplevel(1, axis=1)

    #     raw.index = pd.to_datetime(raw.index)


    #     names = self.blp.bdp(assets, 'long_comp_name')
    #     types = self.blp.bdp(assets, 'SECURITY_TYP')

    #     raw_with_names = raw.copy()

    #     lookback_periods = {
    #         '1d': BDay(1), '3d': BDay(3), '1w': BDay(5),
    #         '1m': DateOffset(months=1), '3m': DateOffset(months=3),
    #         '6m': DateOffset(months=6), '1y': DateOffset(years=1),
    #         '3y': DateOffset(years=3), '5y': DateOffset(years=5),
    #         '7y': DateOffset(years=7), '10y': DateOffset(years=10)
    #     }

    #     def closest_date(target, available):
    #         return available[np.argmin(np.abs((available - target).days))]

    #     available_dates = raw.index
    #     lookback_dates = {
    #         label: closest_date(anchor_date - offset, available_dates)
    #         for label, offset in lookback_periods.items()
    #     }

    #     returns_dict = {}
    #     for label, ref_date in lookback_dates.items():
    #         current = raw.loc[anchor_date]
    #         past = raw.loc[ref_date]
    #         n_days = (anchor_date - ref_date).days

    #         if n_days > 365:
    #             n_years = n_days / 365
    #             returns_dict[label] = (current / past) ** (1 / n_years) - 1
    #         else:
    #             returns_dict[label] = (current / past) - 1

    #     ret_df = pd.DataFrame(returns_dict).round(4).applymap(lambda x: f"{x*100:.2f}%")
    #     ret_df['Name'] = names
    #     ret_df['Asset Class'] = types
    #     df = ret_df.set_index('Name')

    #     raw_with_names.columns = df.index
    #     raw_with_names = raw_with_names.ffill()

    #     equity = df[df['Asset Class'] == 'Equity Index'].drop('Asset Class', axis=1)
    #     debt = df[df['Asset Class'] == 'Fixed Income Index'].drop('Asset Class', axis=1)
    #     other = df[df['Asset Class'].isin(['Index', 'Commodity Index'])].drop('Asset Class', axis=1)

    #     tables = {
    #             'all': self.styled_dashboard_table(df, "All Asset Classes"),
    #             'equity': self.styled_dashboard_table(equity, "Equity Indices"),
    #             'debt': self.styled_dashboard_table(debt, "Fixed Income Indices"),
    #             'other': self.styled_dashboard_table(other, "Other Indices")
    #         }

    #     charts_for_reig = {'equity': {}, 'debt': {}, 'other': {}}
    #     name_map = pd.Series(names['long_comp_name'])
    #     type_map = pd.Series(types['security_typ'])

    #     for label, ref_date in lookback_dates.items():
    #         if label in ['1d', '3d', '1w']:
    #             continue

    #         sliced = raw_with_names.loc[ref_date:anchor_date]
    #         rebased = sliced / sliced.iloc[0] * 100

    #         equity_names = name_map[type_map == 'Equity Index'].values
    #         debt_names = name_map[type_map == 'Fixed Income Index'].values
    #         other_names = name_map[type_map.isin(['Index', 'Commodity Index'])].values

    #         if len(set(equity_names) & set(rebased.columns)) > 0:
    #             charts_for_reig['equity'][label] = graph_performance(rebased[equity_names], f"Equity Indices Rebased to 100 – {label}")
    #         if len(set(debt_names) & set(rebased.columns)) > 0:
    #             charts_for_reig['debt'][label] = graph_performance(rebased[debt_names], f"Fixed Income Indices Rebased to 100 – {label}")
    #         if len(set(other_names) & set(rebased.columns)) > 0:
    #             charts_for_reig['other'][label] = graph_performance(rebased[other_names], f"Other Indices Rebased to 100 – {label}")

    #     return tables, raw, charts_for_reig

    def get_stock_bond_corr(self) -> str:
        country_bond_and_stock_pairs = ['luattruu Index', 'spx index']
        table_name = 'tot_returns'  # <- fixed here
        raw_df = self.fetch_and_cache(
            self.blp,
            country_bond_and_stock_pairs,
            ['tot_return_index_gross_dvds'],
            '2000-07-31',
            table_name,
            freq='D'
        ) #.droplevel(1, axis=1)

        corr = raw_df.dropna()
        corr_pct = (corr / corr.shift()) - 1
        corr_pct.columns = ['US Bonds','US Stocks']
        rolling = corr_pct['US Bonds'].rolling(90).corr(corr_pct['US Stocks']).dropna()

        return simp_graph_performance(rolling, "90 Day Correlation (MSCI ACWI AND GLOBAL AGG)")


    def get_factor_performance(self) -> dict:
        tickers = ['MXWO000V Index','MXWO000G Index','MXWOSC Index','MXWOLC Index','M1WOMOM Index','M1WOQU Index','M1WOMVOL Index','NU748615 Index','M1WOEV Index']
        names = ['Value','Growth','Small','Large','Momentum','Quality','Min Vol','Quality Smalls','Enhanced Value']

        table_name = "factor_tot_return"
        df = self.fetch_and_cache(self.blp, tickers, ['tot_return_index_gross_dvds'], '2023-07-31', table_name, freq='D') #.droplevel(1, axis=1)
        df.columns = names
        df.index = pd.to_datetime(df.index)

        returns = df.pct_change()
        ytd = returns[returns.index.year == datetime.today().year]
        cumulative = (1 + returns).cumprod()
        ytd_cumulative = (1 + ytd).cumprod()

        return {
            'full': graph_performance_with_width(cumulative.dropna(), "Global Factor Long only Performance", 800, 400),
            'ytd': graph_performance_with_width(ytd_cumulative, "Global Factor Long only YTD Performance", 800, 400)
        }
    

        # ---------- SECTOR TABLES ----------
    def get_sector_performance_tables(self) -> dict:
        def load_sector_data(tickers):
            table_name = f"sectors_{tickers[0].split()[0].lower()}_perf"
            return self.fetch_and_cache(
                self.blp,
                tickers,
                ['tot_return_index_gross_dvds'],
                '2014-07-31',
                table_name,
                freq='M'
            ) #.droplevel(1, axis=1)

        sector_names = ['Mat', 'En', 'Fin', 'HC', 'CS', 'CD', 'IT', 'Real Est', 'Util', 'Comm', 'Indus']
        regions = {
            'us': ['s5matr Index', 's5enrs index', 's5finl index', 's5hlth index', 's5cond index', 's5cons index', 's5inft index', 's5rlst index', 's5util index', 's5tels index', 's5indu index'],
            'au': ['AS51MATL Index', 'as51engy index', 'as51fin index', 'as51hc index', 'as51cond index', 'as51cons index', 'as51it index', 'as51prop index', 'as51util index', 'as51tele index', 'as51indu index'],
            'jp': ['MXJP0MT Index', 'MXJP0EN Index', 'MXJP0FN Index', 'MXJP0HC Index', 'MXJP0CD Index', 'MXJP0CS Index', 'MXJP0IT Index', 'MXJP0RL Index', 'MXJP0UT Index', 'MXJP0TC Index', 'MXJP0IN Index'],
            'uk': ['MXGB0MT Index', 'MXGB0EN Index', 'MXGB0FN Index', 'MXGB0HC Index', 'MXGB0CD Index', 'MXGB0CS Index', 'MXGB0IT Index', 'MXGB0RL Index', 'MXGB0UT Index', 'MXGB0TC Index', 'MXGB0IN Index'],
            'eu': ['MXEU0MT Index', 'MXEU0EN Index', 'MXEU0FN Index', 'MXEU0HC Index', 'MXEU0CD Index', 'MXEU0CS Index', 'MXEU0IT Index', 'MXEU0RE Index', 'MXEU0UT Index', 'MXEU0TC Index', 'MXEU0IN Index'],
        }

        tables = {
                region: self.styled_dashboard_table(
                    calc_return_1m_to_10y(load_sector_data(tickers), sector_names),
                    f"{region.upper()} Sector Performance"
                )
                for region, tickers in regions.items()
            }

        return tables
    

    def get_all_asset_class_z_scores(self) -> str:
        today = pd.Timestamp.today()
        prev_month = today - pd.DateOffset(months=1)
        start_date = (today - pd.DateOffset(years=25)).strftime('%Y-%m-%d')
        end_date = today.strftime('%Y-%m-%d')

        current_label = today.strftime('%b %Y')
        prev_label = prev_month.strftime('%b %Y')

        invert_fields = ['yield_to_worst', 'OAS_SPREAD_MID', 'px_last']

        groups = {
            "Bonds": [
                {"name": "US Treasuries", "ticker": "LGTRTRUU Index", "field": "yield_to_worst"},
                {"name": "US HY OAS", "ticker": "LF98OAS Index", "field": "px_last"},
                {"name": "US Inv Grade Credit OAS", "ticker": "LUCROAS Index", "field": "px_last"},
                {"name": "AUS Corp Bond Comp", "ticker": "BACR0 Index", "field": "OAS_SPREAD_MID"},
                {"name": "Ausbond Credit FRN", "ticker": "BAFRN0 Index", "field": "OAS_SPREAD_MID"},
                {"name": "Euro IG OAS", "ticker": "LECPOAS Index", "field": "px_last"}
            ],
            "Equities": [
                {"name": "MSCI World", "ticker": "MXWO Index", "field": "LONG_TERM_PRICE_EARNINGS_RATIO"},
                {"name": "ASX 200", "ticker": "AS51 Index", "field": "LONG_TERM_PRICE_EARNINGS_RATIO"},
                {"name": "MSCI Europe", "ticker": "MXEU Index", "field": "LONG_TERM_PRICE_EARNINGS_RATIO"},
                {"name": "MSCI EM", "ticker": "MXEF Index", "field": "LONG_TERM_PRICE_EARNINGS_RATIO"},
                {"name": "MSCI World Small Cap", "ticker": "MXWOSC Index", "field": "LONG_TERM_PRICE_EARNINGS_RATIO"}
            ],
            "Real Assets": [
                {"name": "ASX 200 A-Reit", "ticker": "AS51PROP Index", "field": "px_to_tang_bv_per_sh"},
                {"name": "FTSE EORA/NAREIT Dev", "ticker": "ENGL Index", "field": "px_to_tang_bv_per_sh"},
                {"name": "S&P Global Infra", "ticker": "SPGTIND Index", "field": "px_to_tang_bv_per_sh"}
            ]
        }

        assets = []
        for group, lst in groups.items():
            for asset in lst:
                asset['group'] = group
                assets.append(asset)

        def get_zscore(df, target_date):
            if df.empty:
                return None, None
            values_winsor = winsorize(df['value'], limits=[0.01, 0.01])
            mean_val = values_winsor.mean()
            std_val = values_winsor.std()
            df['z_score'] = (values_winsor - mean_val) / std_val
            try:
                idx = df.index.get_loc(target_date, method='nearest')
                return df.iloc[idx]['z_score'], df.index[0].strftime('%b %d, %Y')
            except Exception:
                return None, df.index[0].strftime('%b %d, %Y')

        results = []
        for asset in assets:
            table_name = f"zscore_{asset['ticker'].replace(' ', '_')}_{asset['field']}"
            df = self.fetch_and_cache(self.blp, asset['ticker'], [asset['field']], start_date, table_name, freq='D')

            # ✅ Fix: enforce single-column and rename to 'value'
            df = df.iloc[:, [0]]
            df.columns = ['value']
            df.dropna(inplace=True)

            z_current, first_valid = get_zscore(df.copy(), today)
            z_prev, _ = get_zscore(df.copy(), prev_month)

            if asset['field'] in invert_fields:
                if z_current is not None: z_current *= -1
                if z_prev is not None: z_prev *= -1
                label_field = f"Inverted {asset['field']}"
            else:
                label_field = asset['field']

            results.append({
                "name": asset['name'],
                "field": label_field,
                "group": asset['group'],
                "z_current": z_current,
                "z_prev": z_prev,
                "first_valid": first_valid
            })

        zscore_df = pd.DataFrame(results)
        fig = go.Figure()

        for i, row in zscore_df.iterrows():
            x0, x1 = i - 0.4, i + 0.4
            for y0, y1, color in [(2, 3, "gainsboro"), (-3, -2, "gainsboro"), (1, 2, "lightgray"), (-2, -1, "lightgray"), (-1, 1, "gray")]:
                fig.add_shape(type="rect", x0=x0, x1=x1, y0=y0, y1=y1, fillcolor=color, opacity=0.2 if 'gain' in color else 0.3, layer="below", line_width=0)

        for i, row in zscore_df.iterrows():
            if row['z_current'] is not None:
                fig.add_trace(go.Scatter(
                    x=[row['name']], y=[row['z_current']],
                    mode='markers+text',
                    name=current_label if i == 0 else None,
                    marker=dict(symbol='circle', size=14, color="#30415f"),
                    text=[f"{row['z_current']:.2f}"],
                    textposition='top center',
                    showlegend=i == 0,
                    textfont=dict(family="Montserrat")
                ))

            if row['z_prev'] is not None:
                fig.add_trace(go.Scatter(
                    x=[row['name']], y=[row['z_prev']],
                    mode='markers',
                    name=prev_label if i == 0 else None,
                    marker=dict(symbol='triangle-down', size=14, color="#a8c686"),
                    showlegend=i == 0
                ))

        fig.update_layout(
            title={'text': 'Asset Class Valuations (Z-scores based on 25-year average valuation measures)', 'font': {'family': 'Montserrat', 'size': 18}},
            yaxis=dict(title='Z-score', range=[-3.5, 3], zeroline=True, zerolinewidth=2, zerolinecolor='black', showgrid=False, tickfont=dict(family="Montserrat")),
            xaxis=dict(
                tickvals=zscore_df['name'].tolist(),
                ticktext=[f"{row['name']}<br><span style='font-size:11px;color:gray'>{row['field']}<br>{row['first_valid']}</span>" for _, row in zscore_df.iterrows()],
                showgrid=False,
                tickfont=dict(family="Montserrat")
            ),
            plot_bgcolor='white',
            showlegend=True,
            legend=dict(orientation="h", yanchor="top", y=-0.2, xanchor="center", x=0.5, font=dict(family="Montserrat")),
            height=720,
            width=max(900, 150 + 100 * len(zscore_df)),
            margin=dict(t=80, b=160),
            font=dict(family="Montserrat")
        )

        return plot(fig, output_type='div', include_plotlyjs='cdn')
    
model = DailyorWeeklyStuff(blp)

# dmw_tables, raw, charts_for_reig = model.get_daily_market_watch()
# equity_styled = dmw_tables['equity']
# debt_styled = dmw_tables['debt']
# other_styled = dmw_tables['other']
weekly_valuation_charts = model.get_weekly_valuations()
factor_charts = model.get_factor_performance()
graph_for_factor_equity_ytd = factor_charts['ytd']
graph_for_factor_equity = factor_charts['full']
corr_chart = model.get_stock_bond_corr()
sector_tables = model.get_sector_performance_tables()
aashna_all_asset_class_z_score_valuations_html = model.get_all_asset_class_z_scores()
# equity_charts_html = "".join(
#     f"<h3 style='text-align:center'>{label.upper()}</h3>{charts_for_reig['equity'][label]}"
#     for label in charts_for_reig['equity']
# )
# debt_charts_html = "".join(
#     f"<h3 style='text-align:center'>{label.upper()}</h3>{charts_for_reig['debt'][label]}"
#     for label in charts_for_reig['debt']
# )
# other_charts_html = "".join(
#     f"<h3 style='text-align:center'>{label.upper()}</h3>{charts_for_reig['other'][label]}"
#     for label in charts_for_reig['other']
# )




########################
########################
########################
# MACRO
########################

import plotly.express as px
from plotly.offline import plot

class MacroAnalytics(BaseAnalytics):
    def __init__(self, blp):
        self.blp = blp

    def get_economic_surprise_index(self):
        tickers = ['CESIUSD Index', 'CESIEUR Index', 'CESIAUD Index', 'CESICNY Index']
        df = self.fetch_and_cache(self.blp, tickers, ['px_last'], '2021-11-01', 'economic_surprise_monthly', freq='M') #.droplevel(1, axis=1)
        df.columns = ['US', 'EU', 'AU', 'China']
        return graph_performance(df, "Citi Economic Surprise Indices")

    def get_weekly_economic_surprise(self):
        tickers = ['GTII10 Govt', 'CESIEUR Index', 'CESIAUD Index', 'GTAUDII10Y Govt']
        df = self.fetch_and_cache(self.blp, tickers, ['px_last'], '2021-11-01', 'economic_surprise_weekly', freq='W') #.droplevel(1, axis=1)
        df.columns = ['US', 'EU', 'AU', 'China']
        return graph_performance(df, "Citi Economic Surprise Indices")

    def get_gdp_forecast_2025(self):
        tickers = ['ECGDUS 25 Index', 'ECGDAU 25 Index', 'ECGDGB 25 Index', 'ECGDEU 25 Index', 'ECGDCN 25 Index',
                   'ECGDJP 25 Index', 'ECGDKR 25 Index', 'ECGDBR 25 Index', 'ECGDIN 25 Index']
        names = ['USA', 'AUS', 'GBR', 'EUR', 'CHN', 'JAP', 'KOR', 'BRA', 'IND']
        df = self.fetch_and_cache(self.blp, tickers, ['px_last'], '2023-09-01', 'gdp_forecast_2025', freq='W') #.droplevel(1, axis=1)
        df.columns = names
        return graph_performance(df, "Bloomberg Consensus Real GDP Forecast YoY 2025")

    def get_manufacturing_pmi_chart(self):
        tickers = ['NAPMPMI Index','MPMIAUMA Index','MPMIGBMA Index','MPMIEZMA Index','CPMINDX Index',
                   'MPMIJPMA Index','MPMIKRMA Index','MPMIBRMA Index','MPMIINMA Index']
        names = ['USA','AUS','GBR','EUR','CHN','JAP','KOR','BRA','IND']
        df = self.fetch_and_cache(self.blp, tickers, ['px_last'], '2020-10-10', 'manufacturing_pmi', freq='M') #.droplevel(1, axis=1).dropna()
        df.index = pd.to_datetime(df.index)
        df.columns = names

        graph_df = pd.concat([df.iloc[-1], df.iloc[-12]], axis=1)
        graph_df.columns = ['Current', '1 Year Ago']

        fig = px.bar(
            graph_df, x=graph_df.index, y=['Current', '1 Year Ago'],
            title="Manufacturing PMI: Current vs 1 Year Ago",
            labels={"value": "PMI Index", "index": ""},
            barmode='group',
            color_discrete_sequence=['#30415f', '#669bbc']
        )

        fig.update_layout(
            font_family="Montserrat",
            title={"font": {"size": 22}},
            yaxis=dict(title="PMI Index", titlefont=dict(color="black"), tickfont=dict(color="black"), gridcolor="lightgray"),
            plot_bgcolor="white",
            paper_bgcolor="white",
            width=950,
            height=600,
            legend=dict(orientation="h", y=-0.075, x=0.5, xanchor="center")
        )
        return plot(fig, output_type='div', include_plotlyjs='cdn')

    def get_leading_indicator_zscore_chart(self):
        df = self.fetch_and_cache(self.blp, 'LEI YOY Index', ['px_last'], '1960-01-01', 'leading_indicator_yoy', freq='M')
        df_z = (df - df.median()) / df.std()
        return simp_graph_performance(df_z, "US Leading Economic Indicator YoY") #simp_graph_performance(df_z.droplevel(1, axis=1), "US Leading Economic Indicator YoY")

    def get_real_gdp_table(self):
        tickers = ['GDP CYOY Index', 'AUNAGDPY Index', 'UKGRABIY Index', 'EUGNEMUY Index',
                   'CNGDPYOY Index','JGDPNSAQ Index','KOGDPYOY Index','BZGDYOY% Index','IGQREGDY Index']
        names = ['USA','AUS','GBR','EUR','CHN','JAP','KOR','BRA','IND']
        df = self.fetch_and_cache(self.blp, tickers, ['px_last'], '2020-10-10', 'real_gdp_table', freq='Q').dropna() #.droplevel(1, axis=1).dropna()
        df.index = pd.to_datetime(df.index)
        latest = pd.concat([df.iloc[-1], df.iloc[-5]], axis=1)
        latest.index = names
        latest.columns = ['Current', '1 Year Ago']

        return (
            latest.style
            .format(precision=2)
            .set_table_styles([
                {'selector': 'td:hover', 'props': [('background-color', '#30415f')]},
                {'selector': 'th:not(.index_name)', 'props': [('background-color', '#30415f'), ('color', 'white'), ('text-align', 'center')]},
                {'selector': 'td', 'props': [('font-size', '14px'), ('text-align', 'center'), ('width', '80px'), ('border', '1px solid #ddd')]},
                {'selector': 'th', 'props': [('text-align', 'left'), ('border', '1px solid #ddd'), ('width', '80px')]},
                {'selector': 'table', 'props': [('border-collapse', 'collapse')]}
            ])
            .to_html(table_attrs={'id': 'marketWatchTable', 'class': 'display'})
        )

macro = MacroAnalytics(blp)

eco_surpris_df_html = macro.get_economic_surprise_index()
gdp_consensus_html = macro.get_gdp_forecast_2025()
eco_surpris_weekly_html = macro.get_weekly_economic_surprise()
manufac = macro.get_manufacturing_pmi_chart()
lei_z_table = macro.get_leading_indicator_zscore_chart()
real_gdp_table = macro.get_real_gdp_table()

########################
########################
########################
########################
# TECHNICALS
########################

class TechnicalAnalytics(BaseAnalytics):
    def __init__(self, blp):
        self.blp = blp
        self.start_date = '2005-06-01'
        self.tickers = ['AS51 INDEX', 'spx index', 'nky index', 'mxeu index', 'mxef index',
                        'UKX index', 'shcomp index', 'KOSPI Index', 'NDX Index', 'spw index', 'mxwd index']
        self.names = ['ASX200', 'S&P 500', 'Nikkei 225', 'MSCI Europe', 'MSCI EM',
                      'FTSE 100', 'Shanghai Composite', 'KOSPI Index', 'Nasdaq Comp', 'S&P 500 Equal Weight', 'MSCI ACWI']

    def get_major_index_data(self):
        table_name = "major_indices_prices"
        df = self.fetch_and_cache(self.blp, self.tickers, ['px_last'], self.start_date, table_name, freq='D') #.droplevel(1, axis=1)
        df.columns = self.names
        return df.ffill()

    def plot_moving_averages(self, dataframe):
        graph_html = {}
        ma_specs = {
            '50d_MA': {'color': 'silver', 'dash': 'dot'},
            '200d_MA': {'color': 'steelblue', 'dash': 'dot'}
        }
        for index_name in dataframe.columns:
            temp_df = dataframe[[index_name]].copy()
            temp_df['50d_MA'] = temp_df[index_name].rolling(window=50).mean()
            temp_df['200d_MA'] = temp_df[index_name].rolling(window=200).mean()

            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=temp_df.index,
                y=temp_df[index_name],
                mode='lines',
                name=f'{index_name} Price',
                line=dict(color='#30415f', width=2)
            ))
            for ma_type, style in ma_specs.items():
                fig.add_trace(go.Scatter(
                    x=temp_df.index,
                    y=temp_df[ma_type],
                    mode='lines',
                    name=ma_type,
                    line=dict(color=style['color'], dash=style['dash'], width=2)
                ))

            fig.update_layout(
                title=f'{index_name} Moving Averages',
                xaxis_title='Date',
                yaxis_title='Price',
                template='plotly_white',
                hovermode='x unified',
                font=dict(family="Montserrat, sans-serif"),
                title_font=dict(family="Montserrat, sans-serif"),
                legend_font=dict(family="Montserrat, sans-serif"),
                width=1400,
                height=700
            )
            fig.update_xaxes(title_font=dict(family="Montserrat, sans-serif"))
            fig.update_yaxes(title_font=dict(family="Montserrat, sans-serif"))

            graph_html[index_name] = fig.to_html(full_html=False, include_plotlyjs='cdn')

        return graph_html

    def get_cross_asset_vol(self):
        vol_tickers = ['VIX Index', 'MOVE Index', 'CVIX Index', 'BCOM Index']
        table_name = "cross_asset_vol"
        df = self.fetch_and_cache(self.blp, vol_tickers, 'px_last', '1990-12-12', table_name, freq='D') #.droplevel(1, axis=1)

        df['Comm_ret'] = np.log(df['BCOM Index_px_last'] / df['BCOM Index_px_last'].shift())
        df['Commodity Realized Vol (21D)'] = df['Comm_ret'].rolling(window=21).std() * math.sqrt(252)
        df = df.drop(['Comm_ret', 'BCOM Index_px_last'], axis=1)
        df = df.rename(columns={'CVIX Index': 'FX Volatility'})

        return graph_performance(df, 'Cross Asset Volatility')

    def get_technical_signals(self):
        flds_list = [
            'PCT_MEMB_ABOVE_MOV_AVG_200D',
            'PCT_MEMB_PX_GT_50D_MOV_AVG',
            'PCT_MEMB_WITH_14D_RSI_GT_70'
        ]
        tickers = self.tickers

        day200 = self.fetch_and_cache(self.blp, tickers, [flds_list[0]], '2021-12-12', 'pct_above_200d', freq='D') #.droplevel(1, axis=1)
        day50  = self.fetch_and_cache(self.blp, tickers, [flds_list[1]], '2021-12-12', 'pct_above_50d', freq='D') #.droplevel(1, axis=1)
        rsi70  = self.fetch_and_cache(self.blp, tickers, [flds_list[2]], '2021-12-12', 'pct_rsi_gt_70', freq='D') #.droplevel(1, axis=1)

        return (
            graph_performance(day200, '% of Members above their 200D MA'),
            graph_performance(day50, '% of Members above their 50D MA'),
            graph_performance(rsi70, '% of Members above 70 RSI')
        )
    
    
ta = TechnicalAnalytics(blp)

index_data = ta.get_major_index_data()
technicals_graphs_html = ta.plot_moving_averages(index_data)
cross_asset_vol_chart = ta.get_cross_asset_vol()

day200, day50, rsi70 = ta.get_technical_signals()



########## VALUATIONS #################
########## VALUATIONS #################
########## VALUATIONS #################
########## VALUATIONS #################
########## VALUATIONS #################
########## VALUATIONS #################

class ConsolidatedValuationAnalytics(BaseAnalytics):
    def __init__(self, blp):
        # Initialize the parent class
        super().__init__()
        
        self.blp = blp
        self.font = "Montserrat"
        
        # Core configs
        self.frequency = 'M'
        self.start_date = '1997-05-10'
        self.aus_start_date = '2003-05-10'
        self.lower = 0.01
        self.upper = 0.99
        
        # Mapping for metric labels to fields
        self.LABEL_TO_FIELD = {
            'Forward PE': 'BEST_PE_RATIO',
            'Price to Book': 'PX_TO_BOOK_RATIO',
            'CAPE': 'LONG_TERM_PRICE_EARNINGS_RATIO',
            'EV/Trailing EBITDA': 'CURRENT_EV_TO_T12M_EBITDA',
            'Price to Sales': 'PX_TO_SALES_RATIO',
        }
        
        # Configuration dictionaries for each market category
        self.market_configs = {
            'regional': {
                'tickers': ['MXWO Index', 'SPX Index', 'SPW Index', 'NKY Index', 'AS51 Index', 'AS38 Index', 
                            'MXEF Index', 'SXXP Index', 'MXKR Index', 'MXCN Index', 'UKX Index', 'MXWOU Index'],
                'names': ['World', 'US', 'US Equal Weight', 'Japan', 'Australia', 'Aussie Smalls', 
                          'EM', 'Europe', 'Korea', 'China', 'UK', 'World ex US'],
                'metric_fields': ['BEST_PE_RATIO', 'PX_TO_BOOK_RATIO', 'LONG_TERM_PRICE_EARNINGS_RATIO', 
                                  'CURRENT_EV_TO_T12M_EBITDA', 'PX_TO_SALES_RATIO'],
                'metric_names': ['Forward PE', 'Price to Book', 'CAPE', 'EV/Trailing EBITDA', 'Price to Sales'],
                'cache_prefix': 'regional',
                'earnings_tickers': ['MXWO Index', 'SPX Index', 'SPW Index', 'NKY Index', 'AS51 Index', 
                                    'MXEF Index', 'SXXP Index', 'MXKR Index', 'MXCN Index', 'UKX Index'],
                'earnings_names': ['World', 'US', 'US_Equal_Weight', 'Japan', 'Australia', 
                                  'Emerging Markets', 'Europe', 'Korea', 'China', 'UK']
            },
            'factor': {
                'tickers': ['MXWO000V Index', 'M1WOEV Index', 'MXWO000G Index', 'M1WOQU Index', 
                           'MXWOSC Index', 'MXWOLC Index', 'M1WOMVOL Index'],
                'names': ['Value', 'Enhanced Value', 'Growth', 'Quality', 'Small', 'Large', 'Low Volatility'],
                'metric_fields': ['BEST_PE_RATIO', 'PX_TO_BOOK_RATIO', 'LONG_TERM_PRICE_EARNINGS_RATIO', 
                                  'CURRENT_EV_TO_T12M_EBITDA', 'PX_TO_SALES_RATIO'],
                'metric_names': ['Forward PE', 'Price to Book', 'CAPE', 'EV/Trailing EBITDA', 'Price to Sales'],
                'cache_prefix': 'factor',
                'earnings_tickers': ['MXWO000V Index', 'MXWO000G Index', 'M1WOQU Index', 
                                    'MXWOSC Index', 'MXWOLC Index', 'M1WOMVOL Index'],
                'earnings_names': ['Value', 'Growth', 'Quality', 'Small', 'Large', 'Low_Volatility']
            },
            'aussie': {
                'tickers': ['AS51BANX Index', 'AS45 Index', 'MVMVWTRG Index', 'AS51MATL Index', 'AS51 Index'],
                'names': ['Banks', 'Resources', 'Equal Weight', 'Materials', 'ASX 200'],
                'metric_fields': ['BEST_PE_RATIO', 'PX_TO_BOOK_RATIO', 'LONG_TERM_PRICE_EARNINGS_RATIO', 'PX_TO_SALES_RATIO'],
                'metric_names': ['Forward PE', 'Price to Book', 'CAPE', 'Price to Sales'],
                'cache_prefix': 'aussie',
                'earnings_tickers': ['AS51BANX Index', 'AS45 Index', 'MVMVWTRG Index', 'AS51MATL Index', 'AS52 Index'],
                'earnings_names': ['Banks', 'Resources', 'Equal Weight', 'Materials', 'ASX 200']
            },
            'sector': {
                'regions': {
                    'us': {
                        'tickers': ['s5matr Index', 's5enrs index', 's5finl index', 's5hlth index', 's5cond index', 
                                   's5cons index', 's5inft index', 's5rlst index', 's5util index', 's5tels index', 's5indu index'],
                        'cache_prefix': 'us_sector'
                    },
                    'au': {
                        'tickers': ['AS51MATL Index', 'as51engy index', 'as51fin index', 'as51hc index', 'as51cond index', 
                                   'as51cons index', 'as51it index', 'as51prop index', 'as51util index', 'as51tele index', 'as51indu index'],
                        'cache_prefix': 'au_sector'
                    },
                    'jp': {
                        'tickers': ['MXJP0MT Index', 'MXJP0EN Index', 'MXJP0FN Index', 'MXJP0HC Index', 'MXJP0CD Index', 
                                   'MXJP0CS Index', 'MXJP0IT Index', 'MXJP0RL Index', 'MXJP0UT Index', 'MXJP0TC Index', 'MXJP0IN Index'],
                        'cache_prefix': 'jp_sector'
                    },
                    'uk': {
                        'tickers': ['MXGB0MT Index', 'MXGB0EN Index', 'MXGB0FN Index', 'MXGB0HC Index', 'MXGB0CD Index', 
                                   'MXGB0CS Index', 'MXGB0IT Index', 'MXGB0RL Index', 'MXGB0UT Index', 'MXGB0TC Index', 'MXGB0IN Index'],
                        'cache_prefix': 'uk_sector'
                    },
                    'eu': {
                        'tickers': ['MXEU0MT Index', 'MXEU0EN Index', 'MXEU0FN Index', 'MXEU0HC Index', 'MXEU0CD Index', 
                                   'MXEU0CS Index', 'MXEU0IT Index', 'MXEU0RE Index', 'MXEU0UT Index', 'MXEU0TC Index', 'MXEU0IN Index'],
                        'cache_prefix': 'eu_sector'
                    }
                },
                'names': ['Material', 'Energy', 'Financials', 'Healthcare', 'Consumer Discret', 'Consumer Staples', 
                         'Info Tech', 'Real Estate', 'Utilities', 'Communication Serv', 'Industrials'],
                'metric_fields': ['BEST_PE_RATIO', 'PX_TO_BOOK_RATIO', 'LONG_TERM_PRICE_EARNINGS_RATIO', 
                                 'CURRENT_EV_TO_T12M_EBITDA', 'PX_TO_SALES_RATIO'],
                'metric_names': ['Forward PE', 'Price to Book', 'CAPE', 'EV/Trailing EBITDA', 'Price to Sales']
            }
        }

    def winsorize_df(self, df, lower=0.01, upper=0.99):
    
        if isinstance(df.columns, pd.MultiIndex):
            result = df.copy()
            
            for region in df.columns.levels[0]:
                for metric in df.columns.levels[1]:
                    series = df[(region, metric)].dropna()
                    if len(series) > 0:
                        lo = series.quantile(lower)
                        hi = series.quantile(upper)
                        result[(region, metric)] = np.clip(series, lo, hi)
            
            return result
        else:
            # Handle case where columns are not MultiIndex
            result = df.copy()
            for col in df.columns:
                series = df[col].dropna()
                if len(series) > 0:
                    lo = series.quantile(lower)
                    hi = series.quantile(upper)
                    result[col] = np.clip(series, lo, hi)
            
            return result

    def clean_data_after_bloomberg(self, df, region_names, val_metrics):
        if df.empty or df.isna().all().all():
            raise ValueError("No valuation data returned. DataFrame is empty or entirely NaN.")

        if not isinstance(df.columns, pd.MultiIndex):
            try:
                tickers, metrics = zip(*[col.split('_', 1) for col in df.columns])
            except ValueError as e:
                raise ValueError(f"Column parsing failed: {e} — check if columns look like 'ticker_field'")
            df.columns = pd.MultiIndex.from_arrays([tickers, metrics], names=["Ticker", "Metric"])

        # Step 2: Map tickers to region names
        unique_tickers = df.columns.get_level_values(0).unique()
        if len(unique_tickers) != len(region_names):
            raise ValueError(f"Mismatch: {len(unique_tickers)} tickers vs {len(region_names)} region_names. Please check.")
        ticker_to_name = dict(zip(unique_tickers, region_names))
        df.columns = pd.MultiIndex.from_tuples([
            (ticker_to_name[ticker], metric) for ticker, metric in df.columns
        ], names=["Region", "Metric"])

        # Step 3: Convert index to month-year string
        df.index = pd.to_datetime(df.index)
        df.index = df.index.strftime('%b-%Y')

        # Step 4: Winsorize
        df_wins = self.winsorize_df(df, self.lower, self.upper)

        # Step 5: Add Valuation Composite
        for region in df_wins.columns.levels[0]:
            sub_df = df_wins[region]
            z_scores = (sub_df - sub_df.mean()) / sub_df.std()
            if 'Forward PE' in z_scores.columns:
                z_scores = z_scores.drop(columns=['Forward PE'])  # Optional
            composite = z_scores.mean(axis=1)
            df_wins[(region, 'Valuation Composite')] = composite

        if 'Valuation Composite' not in val_metrics:
            val_metrics = val_metrics + ['Valuation Composite']

        # Step 6: Replace zeros with NaN
        df_wins.replace(0, np.nan, inplace=True)

        # Step 7: Build z-score DataFrame (excluding Valuation Composite)
        z_data = {}
        for region in region_names:
            sub_df = df_wins[region]
            z_scores = (sub_df.drop(columns=['Valuation Composite'], errors='ignore') - sub_df.mean()) / sub_df.std()
            z_scores = z_scores.round(3)
            if 'Valuation Composite' in sub_df.columns:
                z_scores['Valuation Composite'] = sub_df['Valuation Composite'].round(3)
            z_data[region] = z_scores

        z_score_df = pd.concat(z_data, axis=1)
        z_score_df.columns.names = ['Region', 'Metric']

        # Step 8: Extract [-1], [-2], [-13] snapshots
        field_to_label = {v: k for k, v in self.LABEL_TO_FIELD.items()}

        def get_snapshot(i):
            snap = z_score_df.iloc[i].unstack(level=0)
            snap.index.name = 'Metric'
            snap.columns.name = 'Region'
            return snap.rename(index=field_to_label).round(3)

        latest_z = get_snapshot(-1)
        prev_month_z = get_snapshot(-2) if len(z_score_df) >= 2 else None
        year_ago_z = get_snapshot(-13) if len(z_score_df) >= 13 else None

        return df_wins.round(3), val_metrics, z_score_df, latest_z, prev_month_z, year_ago_z
        
    def timeseriesplotting(self, valuation_df, valuation_metric, country):
        """Generate time series plot for a specific country and valuation metric"""
        import plotly.express as px
        import plotly.graph_objects as go
        
        metric_field = self.LABEL_TO_FIELD.get(valuation_metric, valuation_metric)
        if valuation_metric == 'Valuation Composite':
            metric_field = valuation_metric
        
        country_df = valuation_df[country]

        if metric_field not in country_df.columns:
            raise KeyError(f"'{metric_field}' not found in {country}'s columns: {country_df.columns.tolist()}")

        data = country_df[metric_field].dropna()
        mean_val, std_val = data.median(), data.std()

        fig = px.line(data, width=1450, height=600, title=f'{valuation_metric} : {country}')
        fig.update_traces(line=dict(color='#30415f'))

        for offset, dash, name in [(0, 'solid', 'Mean'), 
                                   (+1, 'dot', '+ 1 Std'), 
                                   (-1, 'dot', '- 1 Std'), 
                                   (+2, 'dot', '+ 2 Std'), 
                                   (-2, 'dot', '- 2 Std')]:
            fig.add_trace(go.Scatter(
                x=data.index,
                y=[mean_val + (offset * std_val)] * len(data),
                mode='lines',
                name=name,
                line=dict(dash=dash, color='grey')
            ))

        fig.update_layout(
            font=dict(family="Montserrat", size=13),
            title=dict(text=f'<b><span style="color:black;">{valuation_metric}</span> : {country}</b>', font=dict(size=16)),
            plot_bgcolor='white'
        )
        fig.update_xaxes(tickangle=45, title_text="", tickfont=dict(size=10))
        fig.update_yaxes(title_text=f'{valuation_metric}')

        return fig

    def per_valuation_plotter(self, name_list, valuation_list, valuation_metric, winsorized_add_composite):
        """Plot valuation metric for multiple regions in a subplot grid"""
        import math
        import plotly.graph_objects as go
        from plotly.subplots import make_subplots
        
        rows = math.ceil(len(name_list) / 3)
        fig = make_subplots(rows=rows, cols=3, subplot_titles=[f"{valuation_metric} - {j}" for j in name_list])

        color_map = {
            'Forward PE': 'darkcyan', 'Price to Book': 'coral', 'CAPE': 'blue',
            'EV/Trailing EBITDA': 'red', 'Price to Sales': 'purple', 'Valuation Composite': 'green'
        }
        title_color = color_map.get(valuation_metric, 'black')

        for idx, region in enumerate(name_list):
            row, col = divmod(idx, 3)
            row += 1
            col += 1
            fig_piece = self.timeseriesplotting(winsorized_add_composite, valuation_metric, region)
            for trace in fig_piece.data:
                trace.showlegend = False
                fig.add_trace(trace, row=row, col=col)

            # Update subplot title if it exists
            if idx < len(fig.layout.annotations):
                fig.layout.annotations[idx].update(
                    text=f'<b><span style="color:{title_color};">{valuation_metric}</span> : {region}</b>',
                    font=dict(size=16, family='Montserrat')
                )

        fig.update_layout(
            height=400 * rows, width=1500,
            font=dict(family="Montserrat", size=13), plot_bgcolor='white'
        )
        fig.update_xaxes(tickangle=45, tickfont=dict(size=10))
        fig.update_yaxes(title_text=valuation_metric)
        return fig
        
    def graph_performance(self, data, title):
        """Create performance graph for time series data"""
        import plotly.graph_objects as go
        import pandas as pd
        
        # Color palettes
        full_palette = [
            "#30415f", "#f3a712", "#87b1a1", "#5ac5fe",
            "#a8c686", "#a0a197", "#e4572e", "#2337C6",
            "#B7B1B0", "#778BA5", "#990000"
        ]
        simp_palette = ["#30415f", "#DDDDDD", "#DDDDDD", "#DDDDDD"]
        
        fig = go.Figure()
        
        # Add traces depending on Series or DataFrame
        if isinstance(data, pd.Series):
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data.values,
                mode='lines',
                name=data.name or "Series",
                line=dict(color=full_palette[0], width=2)
            ))
        elif isinstance(data, pd.DataFrame):
            use_full_colors = data.shape[1] >= 4
            palette = full_palette if use_full_colors else simp_palette
            for i, col in enumerate(data.columns):
                fig.add_trace(go.Scatter(
                    x=data.index,
                    y=data[col],
                    mode='lines',
                    name=str(col),
                    line=dict(color=palette[i % len(palette)], width=2)
                ))
        
        # Apply layout
        fig.update_layout(
            title=title,
            xaxis_title='',
            yaxis_title='Price',
            template='plotly_white',
            hovermode='x unified',
            font=dict(family="Montserrat, sans-serif"),
            title_font=dict(family="Montserrat, sans-serif", size=22),
            legend_font=dict(family="Montserrat, sans-serif"),
            width=1100,
            height=600,
            xaxis=dict(gridcolor="#ECECEC", linecolor="#ECECEC"),
            yaxis=dict(
                side="left",
                title="Price",
                titlefont=dict(color="black"),
                tickfont=dict(color="black"),
                gridcolor="#ECECEC",
                linecolor="#ECECEC",
            ),
            plot_bgcolor="white",
            paper_bgcolor="white",
            legend=dict(
                orientation="h",
                y=-0.075,
                x=0.5,
                xanchor="center"
            )
        )
        
        return fig
        
    def cross_sectional_current_table_maker(self, data, return_df=False):
        field_to_label = {v: k for k, v in self.LABEL_TO_FIELD.items()}

        def format_df(series):
            df = pd.DataFrame(series).unstack(level=0)  # (Metric, Region) → (Region as columns)
            df.index = [
                field_to_label.get(metric, metric) if metric != 'Valuation Composite' else 'Valuation Composite'
                for metric in df.index
            ]
            df.index.name = 'Metric'
            df = df.T  # Flip so Region becomes index
            return df.round(2)

        current = data[0].iloc[-1]
        month_ago = data[0].iloc[-2] if len(data[0]) > 1 else None

        current_df = format_df(current)
        month_ago_df = format_df(month_ago) if month_ago is not None else None

        def style_html_table(df, title):
            html = f"""
            <h3 style="font-family: Montserrat, sans-serif; font-weight: 700; font-size: 18px; color: #30415f; margin-top: 30px;">
                {title}
            </h3>
            <div style="
                background-color: white;
                border-radius: 8px;
                box-shadow: 0 2px 6px rgba(0,0,0,0.05);
                padding: 12px;
                margin-bottom: 20px;
                overflow-x: auto;
                font-family: Montserrat, sans-serif;
                font-size: 13px;
            ">
            <table style="border-collapse: collapse; width: 100%;">
                <thead>
                    <tr>
                        <th style="text-align: left; padding: 8px; background-color: #30415f; color: white;">Region</th>
                        {''.join([f'<th style="text-align: center; padding: 8px; background-color: #f0f0f0;">{col}</th>' for col in df.columns])}
                    </tr>
                </thead>
                <tbody>
                    {''.join([
                        f"<tr><td style='padding: 8px; font-weight: bold; color: #30415f;'>{idx}</td>" +
                        ''.join([f"<td style='padding: 8px; text-align: center;'>{val:.2f}" if isinstance(val, float) else f"<td>{val}</td>" for val in row]) +
                        "</tr>"
                        for idx, row in df.iterrows()
                    ])}
                </tbody>
            </table>
            </div>
            """
            return html

        html = style_html_table(current_df, "Current Valuations")
        if month_ago_df is not None:
            html += style_html_table(month_ago_df, "Valuations 1 Month Ago")

        if return_df:
            return (current_df, month_ago_df), html
        else:
            return html


    def get_valuation_data(self, market_type, region=None):
        """Fetch valuation data for the specified market type"""
        if market_type not in self.market_configs:
            raise ValueError(f"Invalid market type: {market_type}. Must be one of {list(self.market_configs.keys())}")
        
        # Special handling for sector data which is organized by region
        if market_type == 'sector':
            if not region:
                raise ValueError("Region must be specified for sector data")
            
            if region not in self.market_configs[market_type]['regions']:
                raise ValueError(f"Invalid region: {region}. Must be one of {list(self.market_configs[market_type]['regions'].keys())}")
            
            config = self.market_configs[market_type]['regions'][region]
            names = self.market_configs[market_type]['names']
            metric_fields = self.market_configs[market_type]['metric_fields']
            metric_names = self.market_configs[market_type]['metric_names']
            cache_prefix = config['cache_prefix']
            tickers = config['tickers']
            
            # Use appropriate start date based on region
            start_date = self.aus_start_date if region == 'au' else self.start_date
            
        else:
            config = self.market_configs[market_type]
            names = config['names']
            metric_fields = config['metric_fields']
            metric_names = config['metric_names']
            cache_prefix = config['cache_prefix']
            tickers = config['tickers']
            
            # Use appropriate start date based on market type
            start_date = self.aus_start_date if market_type == 'aussie' else self.start_date
        
        raw = self.fetch_and_cache(
            self.blp, 
            tickers, 
            metric_fields, 
            start_date, 
            f"{cache_prefix}_valuation_data", 
            freq=self.frequency
        )
        
        return self.clean_data_after_bloomberg(raw, names, metric_names)

    def get_earnings_revisions(self, market_type, region=None):
        """Get earnings revisions for the specified market type"""
        from datetime import datetime, timedelta
        import pandas as pd
        
        if market_type not in self.market_configs:
            raise ValueError(f"Invalid market type: {market_type}. Must be one of {list(self.market_configs.keys())}")
        
        # Special handling for sector data which is organized by region
        if market_type == 'sector':
            if not region:
                raise ValueError("Region must be specified for sector data")
            
            if region not in self.market_configs[market_type]['regions']:
                raise ValueError(f"Invalid region: {region}. Must be one of {list(self.market_configs[market_type]['regions'].keys())}")
            
            config = self.market_configs[market_type]['regions'][region]
            names = self.market_configs[market_type]['names']
            cache_prefix = config['cache_prefix']
            tickers = config['tickers']
            
            # For sector data, use the same tickers for earnings as for valuation
            earnings_tickers = tickers
            earnings_names = names
            
        else:
            config = self.market_configs[market_type]
            cache_prefix = config['cache_prefix']
            
            # Check if earnings tickers and names are available for this market type
            if 'earnings_tickers' not in config or 'earnings_names' not in config:
                return {
                    'fig': None,
                    'html': f"<p>Earnings revisions data not available for {market_type}</p>",
                    'data': None
                }
            
            earnings_tickers = config['earnings_tickers']
            earnings_names = config['earnings_names']
        
        start_date = (datetime.today() - timedelta(days=365)).strftime("%Y-%m-%d")
        
        try:
            fwd = self.fetch_and_cache(
                self.blp, 
                earnings_tickers, 
                ['INDX_WEIGHTED_EST_ERN'], 
                start_date, 
                f"{cache_prefix}_earnings_fwd", 
                freq='D',
                BEST_FPERIOD_OVERRIDE='BF'
            )
            
            realized = self.fetch_and_cache(
                self.blp, 
                earnings_tickers, 
                ['t12_eps_aggte'], 
                start_date, 
                f"{cache_prefix}_earnings_realized", 
                freq='D'
            )
            
            # Handle column structure based on fetch_and_cache return format
            if isinstance(fwd.columns, pd.MultiIndex):
                fwd = fwd.droplevel(1, axis=1)
                realized = realized.droplevel(1, axis=1)
            
            # Ensure the number of columns matches the number of names
            if len(fwd.columns) == len(earnings_names):
                fwd.columns = earnings_names
            else:
                # In case of mismatch, use default column names
                fwd.columns = [f"Series {i+1}" for i in range(len(fwd.columns))]
                
            if len(realized.columns) == len(earnings_names):
                realized.columns = earnings_names
            else:
                # In case of mismatch, use default column names
                realized.columns = [f"Series {i+1}" for i in range(len(realized.columns))]
            
            revisions = (((fwd / realized) - 1) * 100).round(2)
            
            # Handle possible NaN values from division
            revisions = revisions.fillna(0)
            
            title = f'{market_type.capitalize()} '
            if region:
                title += f'{region.upper()} '
            title += 'Blended 4Q Forward Earnings Revisions (%)'
            
            fig = self.graph_performance(revisions, title)
            
            return {
                'fig': fig,
                'html': fig.to_html(include_plotlyjs=False, full_html=False),
                'data': revisions
            }
            
        except Exception as e:
            # Return a graceful error if data fetching fails
            return {
                'fig': None,
                'html': f"<p>Error retrieving earnings revisions data: {str(e)}</p>",
                'data': None
            }

    def get_valuation_charts(self, market_type, data, region=None):
        """Generate valuation charts for the specified market type"""
        if market_type not in self.market_configs:
            raise ValueError(f"Invalid market type: {market_type}. Must be one of {list(self.market_configs.keys())}")
        
        # Special handling for sector data which is organized by region
        if market_type == 'sector':
            if not region:
                raise ValueError("Region must be specified for sector data")
            
            if region not in self.market_configs[market_type]['regions']:
                raise ValueError(f"Invalid region: {region}. Must be one of {list(self.market_configs[market_type]['regions'].keys())}")
            
            names = self.market_configs[market_type]['names']
            metric_names = self.market_configs[market_type]['metric_names']
            
        else:
            config = self.market_configs[market_type]
            names = config['names']
            metric_names = config['metric_names']
        
        try:
            figs = {
                metric: self.per_valuation_plotter(names, metric_names, metric, data[0])
                for metric in metric_names + ['Valuation Composite']
            }
            
            html = {
                metric: figs[metric].to_html(include_plotlyjs=False, full_html=False)
                for metric in figs
            }
            
            return {
                'figs': figs,
                'html': html
            }
        except Exception as e:
            # Return a graceful error if chart generation fails
            return {
                'figs': {},
                'html': {
                    'error': f"<p>Error generating valuation charts: {str(e)}</p>"
                }
            }

    def get_valuation_tables(self, market_type, data, region=None):
        """Generate simple valuation tables for the specified market type"""
        if market_type not in self.market_configs:
            raise ValueError(f"Invalid market type: {market_type}. Must be one of {list(self.market_configs.keys())}")
        
        try:
            df, html = self.cross_sectional_current_table_maker(data, return_df=True)
            return {
                'df': df,
                'html': html
            }
        except Exception as e:
            # Return a graceful error if table generation fails
            return {
                'df': None,
                'html': f"<p>Error generating valuation tables: {str(e)}</p>"
            }
        
    def get_cross_sectional_time_series(self, regional_data, factor_data):
        """Generate cross-sectional time series analysis with individual and combined charts"""

        long = ['Value', 'Small', 'EM', 'World ex US', 'US Equal Weight']
        short = ['Growth', 'Large', 'World', 'US', 'US']
        metric = 'Valuation Composite'

        # Construct the cross-sectional spread DataFrame
        xs_df = pd.DataFrame({
            'Global Value / Growth': factor_data[0][long[0]][metric] - factor_data[0][short[0]][metric],
            'Small / Large': factor_data[0][long[1]][metric] - factor_data[0][short[1]][metric],
            'EM / DM': regional_data[0][long[2]][metric] - regional_data[0][short[2]][metric],
            'Intl ex US / US': regional_data[0][long[3]][metric] - regional_data[0][short[3]][metric],
            'S&P Equal / S&P 500': regional_data[0][long[4]][metric] - regional_data[0][short[4]][metric]
        }, index=factor_data[0].index)

        # Combined chart with all spreads
        fig_all = self.graph_performance(xs_df, 'Cross-Sectional Valuation Composite Spreads')

        # Individual charts
        individual_htmls = {}
        for col in xs_df.columns:
            fig_single = self.graph_performance(xs_df[[col]], f"{col} Valuation Spread")
            individual_htmls[col] = fig_single.to_html(include_plotlyjs=False, full_html=False)

        return {
            'fig': fig_all,
            'html': fig_all.to_html(include_plotlyjs=False, full_html=False),
            'individual_charts': individual_htmls,
            'data': xs_df
        }

    def get_complete_market_analysis(self, market_type, region=None):
        """Get complete analysis for a market type: valuations, charts, and earnings revisions"""
        try:
            valuation_data = self.get_valuation_data(market_type, region)
            valuation_charts = self.get_valuation_charts(market_type, valuation_data, region)
            valuation_tables = self.get_valuation_tables(market_type, valuation_data, region)  
            earnings_revisions = self.get_earnings_revisions(market_type, region)
            
            return {
                'data': valuation_data,
                'charts': valuation_charts,
                'tables': valuation_tables,
                'earnings_revisions': earnings_revisions
            }
        except Exception as e:
            return {
                'data': None,
                'charts': {'html': {'error': f"<p>Error in analysis: {str(e)}</p>"}, 'figs': {}},
                'tables': {'html': f"<p>Error in analysis: {str(e)}</p>", 'df': None},
                'earnings_revisions': {'html': f"<p>Error in analysis: {str(e)}</p>", 'data': None}
            }
    
    def get_all_market_analysis(self):
        """Get complete analysis for all market types"""
        results = {}
        
        # Get data for each market type
        for market_type in ['regional', 'factor', 'aussie']:
            results[market_type] = self.get_complete_market_analysis(market_type)
        
        # Add sector analysis for each region
        results['sector'] = {}
        for region in self.market_configs['sector']['regions'].keys():
            try:
                results['sector'][region] = self.get_complete_market_analysis('sector', region)
            except Exception as e:
                # Handle any errors gracefully
                results['sector'][region] = {
                    'data': None,
                    'charts': {'html': {'error': f"<p>Error in {region} sector analysis: {str(e)}</p>"}},
                    'tables': {'html': f"<p>Error in {region} sector analysis: {str(e)}</p>"},
                    'earnings_revisions': {'html': f"<p>Error in {region} sector analysis: {str(e)}</p>", 'data': None}
                }
        
        # Add cross-sectional analysis
        results['cross_sectional'] = self.get_cross_sectional_time_series(
            results['regional']['data'], 
            results['factor']['data']
        )
        
        return results

    def reits(self):
        raw = self.fetch_and_cache(
            self.blp, 
            ticker=['ENGL Index','SPGTIND Index','FDCICUN Index','AS51PROP Index','MVMVATRG Index'], 
            fields=['PX_TO_TANG_BV_PER_SH','eqy_dvd_yld_12m'], 
            start_date='2001-01-01', 
            table_name='reits_valuation_data', 
            freq=self.frequency
        )

        # Rebuild MultiIndex
        raw.columns = pd.MultiIndex.from_tuples(
            [tuple(col.split('_', 1)) for col in raw.columns],
            names=["Ticker", "Field"]
        )

        reit_names = {
            'ENGL Index': 'Global REITs',
            'SPGTIND Index': 'S&P Infra',
            'FDCICUN Index': 'FTSE Global Core Infra',
            'AS51PROP Index': 'ASX 200 REITs',
            'MVMVATRG Index': 'MVA Index (10% Cap)'
        }

        # Extract and rename
        pnta = raw.xs('PX_TO_TANG_BV_PER_SH', axis=1, level=1).rename(columns=reit_names)
        div = raw.xs('eqy_dvd_yld_12m', axis=1, level=1).rename(columns=reit_names)

        def add_bands(df, label):
            """Return DataFrame with original series, median and ±1 std dev as extra columns"""
            banded = pd.DataFrame(index=df.index)
            series = df[label]
            banded[label] = series
            banded['Median'] = series.median()
            banded['+1 Std Dev'] = series.mean() + series.std()
            banded['-1 Std Dev'] = series.mean() - series.std()
            return banded

        def build_fig(df, title, main_series_label):
            """Customized chart with gray bands and Montserrat styling"""
            import plotly.graph_objects as go

            fig = go.Figure()

            # Main series: #30415f
            fig.add_trace(go.Scatter(
                x=df.index, y=df[main_series_label],
                mode='lines', name=main_series_label,
                line=dict(color='#30415f', width=2)
            ))

            # Median: solid gray
            fig.add_trace(go.Scatter(
                x=df.index, y=df['Median'],
                mode='lines', name='Median',
                line=dict(color='gray', dash='solid', width=1.5)
            ))

            # +1 Std Dev: dotted gray
            fig.add_trace(go.Scatter(
                x=df.index, y=df['+1 Std Dev'],
                mode='lines', name='+1 Std Dev',
                line=dict(color='gray', dash='dot', width=1.2)
            ))

            # -1 Std Dev: dotted gray
            fig.add_trace(go.Scatter(
                x=df.index, y=df['-1 Std Dev'],
                mode='lines', name='-1 Std Dev',
                line=dict(color='gray', dash='dot', width=1.2)
            ))

            fig.update_layout(
                title=title,
                font_family='Montserrat',
                plot_bgcolor='white',
                paper_bgcolor='white',
                width=1250,
                height=500,
                margin=dict(l=40, r=40, t=40, b=40),
                legend_title_text=''
            )

            return fig

        # Generate charts
        pnta_figs = {col: build_fig(add_bands(pnta, col), f"{col} - Price to Tangible Book", col) for col in pnta.columns}
        div_figs = {col: build_fig(add_bands(div, col), f"{col} - 12M Dividend Yield", col) for col in div.columns}

        return {
            'figs': {
                'pnta': pnta_figs,
                'dividend_yield': div_figs
            },
            'html': {
                'pnta': {k: v.to_html(include_plotlyjs=False, full_html=False) for k, v in pnta_figs.items()},
                'dividend_yield': {k: v.to_html(include_plotlyjs=False, full_html=False) for k, v in div_figs.items()}
            },
            'data': {
                'pnta': pnta,
                'dividend_yield': div
            }
        }

cva = ConsolidatedValuationAnalytics(blp)

reits_valuations = cva.reits()
reits_html = cva.reits()['html']  # Contains 'pnta' and 'dividend_yield'

# Option 1: Get all analyses at once
all_analyses = cva.get_all_market_analysis()


################### TRAFFIC FLASH #######################
zscore_snapshots = []
first_dates = {}
types = {}

monthly_changes = []
yearly_changes = []

# Main markets
for mtype in ['regional', 'factor', 'aussie']:
    data = all_analyses[mtype]['data']
    if data:
        current = data[3]
        prev_month = data[4]
        prev_year = data[5]

        if current is not None:
            zscore_snapshots.append(current)
            monthly_changes.append(current - prev_month)
            yearly_changes.append(current - prev_year)

            for col in current.columns:
                first_valid = data[2][col].first_valid_index()
                first_dates[col] = first_valid
                types[col] = mtype.capitalize()

# Sector markets
region_map = {
    'us': 'US', 'au': 'Australia', 'jp': 'Japan',
    'uk': 'UK', 'eu': 'Europe'
}

for region_key, sector_entry in all_analyses['sector'].items():
    data = sector_entry['data']
    if data:
        region_label = region_map.get(region_key.lower(), region_key.upper())
        current = data[3]
        prev_month = data[4]
        prev_year = data[5]

        if current is not None:
            # Rename current
            renamed_current = current.copy()
            renamed_current.columns = [f"{region_label} {col}" for col in current.columns]
            zscore_snapshots.append(renamed_current)

            # Do the renaming first for prev_month and prev_year to align safely
            if prev_month is not None:
                renamed_prev_month = prev_month.copy()
                renamed_prev_month.columns = renamed_current.columns
                renamed_month = renamed_current - renamed_prev_month
                monthly_changes.append(renamed_month)

            if prev_year is not None:
                renamed_prev_year = prev_year.copy()
                renamed_prev_year.columns = renamed_current.columns
                renamed_year = renamed_current - renamed_prev_year
                yearly_changes.append(renamed_year)

            for col in renamed_current.columns:
                original_col = col.split(' ', 1)[1]
                matching_cols = [c for c in data[2].columns if f"{original_col}" in str(c)]
                if matching_cols:
                    first_valid = data[2][matching_cols[0]].first_valid_index()
                    first_dates[col] = first_valid
                types[col] = 'Sector'


# Combine and clean
combined_latest_z = pd.concat(zscore_snapshots, axis=1).round(3)
combined_latest_z = combined_latest_z.loc[:, ~combined_latest_z.columns.duplicated()]

combined_z_1m_change = pd.concat(monthly_changes, axis=1).round(3)
combined_z_1m_change = combined_z_1m_change.loc[:, ~combined_z_1m_change.columns.duplicated()].T

combined_z_12m_change = pd.concat(yearly_changes, axis=1).round(3)
combined_z_12m_change = combined_z_12m_change.loc[:, ~combined_z_12m_change.columns.duplicated()].T

# Create metadata rows
first_date_row = pd.Series({col: first_dates.get(col, "N/A") for col in combined_latest_z.columns}, name='First Value')
type_row = pd.Series({col: types.get(col, "Unknown") for col in combined_latest_z.columns}, name='Type')

# Final wide DataFrame with metadata at the bottom
combined_latest_z_with_meta = pd.concat(
    [combined_latest_z, first_date_row.to_frame().T, type_row.to_frame().T]
).T

metrics = [
    "Forward PE", 
    "EV/Trailing EBITDA", 
    "CAPE", 
    "Price to Book", 
    "Price to Sales", 
    "Valuation Composite"
]

# Create dictionary to store new dataframes
combined_dfs = {}

for metric in metrics:
    # Create new dataframe starting with index from the latest data
    new_df = pd.DataFrame(index=combined_latest_z_with_meta.index)
    
    # Add the current value for this metric
    new_df[f"{metric}"] = combined_latest_z_with_meta[metric]
    
    # Add the 1-month change
    new_df["1m Change"] = combined_z_1m_change[metric]
    
    # Add the 12-month change
    new_df["1Y Change"] = combined_z_12m_change[metric]
    
    # Add the First Value and Type columns
    new_df["First Value"] = combined_latest_z_with_meta["First Value"]
    new_df["Type"] = combined_latest_z_with_meta["Type"]
    
    # Store in dictionary
    combined_dfs[f"{metric}"] = new_df.sort_values(by=new_df.columns[0])


def make_filtered_val_tables(combined_dfs, lower_thresh=-0.5, upper_thresh=0.5):
    def build_table_block(df, title, highlight_idx):
        styled_df = (
            df.style
            .apply(lambda x: [
                "background-color: #dceeff" if x.name in highlight_idx else ""
            ] + [""] * (len(x) - 1), axis=1)
            .format({col: "{:.2f}" for col in df.select_dtypes(include='number').columns}, na_rep="N/A")
        )

        return f"""
        <div style="
            flex: 1;
            min-width: 30%;
            max-width: 32%;
            padding: 12px;
            margin-bottom: 16px;
            background-color: #fff;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.06);
            font-size: 13px;
        ">
            <h4 style="font-family: Montserrat, sans-serif; font-weight: 600; font-size: 16px; margin-bottom: 8px;">
                {title}
            </h4>
            {styled_df.to_html()}
        </div>
        """

    def build_no_data_block(title, message):
        return f"""
        <div style="
            flex: 1;
            min-width: 30%;
            max-width: 32%;
            padding: 12px;
            margin-bottom: 16px;
            background-color: #f9f9f9;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.04);
            font-size: 13px;
            color: #666;
        ">
            <h4 style="font-family: Montserrat, sans-serif; font-weight: 600; font-size: 16px; margin-bottom: 8px;">
                <a href="#" onclick="showContent('charts_{title.replace(' ', '_')}')" style="color: #30415f; text-decoration: none;">
                    {title}
                </a>
            </h4>
            <p>{message}</p>
        </div>
        """

    cheap_tables = []
    expensive_tables = []

    metric_order = ['Valuation Composite'] + sorted(k for k in combined_dfs if k != 'Valuation Composite')

    for metric in metric_order:
        if metric not in combined_dfs:
            continue

        df = combined_dfs[metric]
        if df.shape[1] == 0:
            continue

        df_numeric = df.copy()
        for col in df_numeric.columns[:3]:
            df_numeric[col] = pd.to_numeric(df_numeric[col], errors='coerce')

        # Cheap
        cheap_df = df_numeric[df_numeric.iloc[:, 0] < lower_thresh]
        if not cheap_df.empty:
            top5_cheap = cheap_df.nsmallest(5, cheap_df.columns[0]).index
            cheap_tables.append(build_table_block(cheap_df, metric, top5_cheap))
        elif metric == 'Valuation Composite':
            cheap_tables.append(build_no_data_block("Valuation Composite", "No assets 1std under"))

        # Expensive
        expensive_df = df_numeric[df_numeric.iloc[:, 0] > upper_thresh]
        if not expensive_df.empty:
            top5_expensive = expensive_df.nlargest(5, expensive_df.columns[0]).index
            expensive_tables.append(build_table_block(expensive_df, metric, top5_expensive))
        elif metric == 'Valuation Composite':
            expensive_tables.append(build_no_data_block("Valuation Composite", "No assets 1std over"))

    html_wrapper = lambda blocks, title: f"""
    <h2 style="font-family: Montserrat, sans-serif; font-weight: 700; font-size: 20px; margin-top: 30px;">
        {title}
    </h2>
    <div style="display: flex; flex-wrap: wrap; gap: 16px; justify-content: space-between; font-size: 13px;">
        {''.join(blocks)}
    </div>
    """

    cheap_html = html_wrapper(cheap_tables, f"Assets Below ({lower_thresh}) Std")
    expensive_html = html_wrapper(expensive_tables, f"Assets Above ({upper_thresh}) Std")

    return cheap_html, expensive_html


traffic_alert_cheap = make_filtered_val_tables(combined_dfs, -1, 1)[0]
traffic_alert_expensive = make_filtered_val_tables(combined_dfs, -1, 1)[1]

def process_opportunities(combined_dfs, lower_thresh):
    # Track cheap assets (excluding Valuation Composite)
    cheap_assets_tracker = {}

    for metric, df in combined_dfs.items():
        if metric == "Valuation Composite":
            continue
        df_numeric = df.copy()
        df_numeric.iloc[:, :3] = df_numeric.iloc[:, :3].apply(pd.to_numeric, errors='coerce')
        cheap_df = df_numeric[df_numeric.iloc[:, 0] < lower_thresh]
        for asset in cheap_df.index:
            if asset not in cheap_assets_tracker:
                cheap_assets_tracker[asset] = []
            cheap_assets_tracker[asset].append(metric)

    # Filter those appearing in 3+ metrics
    qualified_assets = {a: m for a, m in cheap_assets_tracker.items() if len(m) >= 3}

    # Build opportunities DataFrame
    opportunities = []
    for asset in qualified_assets:
        row = {}
        include = False
        for metric in combined_dfs:
            if asset in combined_dfs[metric].index:
                row[f"{metric}"] = combined_dfs[metric].loc[asset, metric]
                row[f"{metric} 1Y Change"] = combined_dfs[metric].loc[asset, "1Y Change"]
                if metric != "Valuation Composite" and combined_dfs[metric].loc[asset, "1Y Change"] > 0:
                    include = True
        if include:
            row["# Cheap Flags"] = len(qualified_assets[asset])
            row["Appears In"] = ", ".join(qualified_assets[asset])
            opportunities.append(pd.Series(row, name=asset))

    return pd.DataFrame(opportunities)

oppurtunities = process_opportunities(combined_dfs, lower_thresh=-1)
oppurtunities = oppurtunities[['Valuation Composite','Valuation Composite 1Y Change','# Cheap Flags','Appears In']]
oppurtunities = oppurtunities.T

def build_opportunities_html(df):
    metrics = ["Forward PE", "EV/Trailing EBITDA", "CAPE", "Price to Book", "Price to Sales", "Valuation Composite"]

    rows_html = ""
    for idx, row in df.iterrows():
        metric_cells = []
        for col, val in zip(row.index, row.values):
            if isinstance(val, float):
                cell_val = f"{val:.3f}"
            else:
                cell_val = val

            # Add hyperlink to the metric name (first column)
            if col == df.columns[0] and idx in metrics:
                link_id = f"charts_{idx.replace(' ', '_')}"
                metric_cells.append(
                    f"<td style='padding: 8px; text-align: center;'>"
                    f"<a href='#' onclick=\"showContent('{link_id}')\" style='color:#30415f; font-weight:bold;'>{cell_val}</a>"
                    f"</td>"
                )
            else:
                metric_cells.append(f"<td style='padding: 8px; text-align: center;'>{cell_val}</td>")

        rows_html += (
                f"<tr><td style='padding: 8px; font-weight: bold; color: #30415f;'>"
                f"<a href='#' onclick=\"showContent('charts_{idx.replace(' ', '_')}')\" "
                f"style='color:#30415f; font-weight:bold; text-decoration:none;'>{idx}</a></td>"
                f"{''.join(metric_cells)}</tr>"
            )

    return f"""
    <div style="
        max-width: 700px;
        background-color: white;
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.05);
        padding: 16px;
        font-family: Montserrat, sans-serif;
        font-size: 13px;
        margin-bottom: 24px;
    ">
        <div style="font-weight: 600; font-size: 16px; color: #30415f; margin-bottom: 12px;">
            Valuation Opportunities
        </div>
        <table style="border-collapse: collapse; width: 100%;">
            <thead>
                <tr>
                    <th style="text-align: left; padding: 8px; background-color: #30415f; color: white;">Metric</th>
                    {"".join([
                        f'<th style="text-align: center; padding: 8px; background-color: #f0f0f0;">{col}</th>'
                        for col in df.columns
                    ])}
                </tr>
            </thead>
            <tbody>
                {rows_html}
            </tbody>
        </table>
    </div>
    """



opps_html = build_opportunities_html(oppurtunities)

################### TRAFFIC FLASH #######################

# Access specific components
regional_forward_pe_html = all_analyses['regional']['charts']['html']['Forward PE'] #figs instead of html #fig for earnings reviisons
factor_composite_fig = all_analyses['factor']['charts']['figs']['Valuation Composite']
aussie_earnings = all_analyses['aussie']['earnings_revisions']['data']
cross_sectional = all_analyses['cross_sectional']['data']

# Option 2: Get analysis for a specific market
regional_analysis = cva.get_complete_market_analysis('regional')
aussie_analysis = cva.get_complete_market_analysis('aussie')
factor_analysis = cva.get_complete_market_analysis('factor')

# Access components directly for a specific market
regional_tables = cva.get_valuation_tables('regional', cva.get_valuation_data('regional'))
factor_charts = cva.get_valuation_charts('factor', cva.get_valuation_data('factor'))
aussie_earnings = cva.get_earnings_revisions('aussie')

# Get cross-sectional analysis separately
regional_data = cva.get_valuation_data('regional')
factor_data = cva.get_valuation_data('factor')
cross_sectional = cva.get_cross_sectional_time_series(regional_data, factor_data)


#Valuations
rei_matrix = all_analyses['regional']['tables']['html']
rei_earn = all_analyses['regional']['earnings_revisions']['html']
rei_ts = all_analyses['regional']['charts']['html']

fac_matrix = all_analyses['factor']['tables']['html']
fac_earn = all_analyses['factor']['earnings_revisions']['html']
fac_ts = all_analyses['factor']['charts']['html']

au_matrix = all_analyses['aussie']['tables']['html']
au_earn = all_analyses['aussie']['earnings_revisions']['html']
au_ts = all_analyses['aussie']['charts']['html']

# Access sector data for each region
us_sector_matrix = all_analyses['sector']['us']['tables']['html']
us_sector_earn = all_analyses['sector']['us']['earnings_revisions']['html']
us_sector_ts = all_analyses['sector']['us']['charts']['html']

jp_sector_matrix = all_analyses['sector']['jp']['tables']['html']
jp_sector_earn = all_analyses['sector']['jp']['earnings_revisions']['html']
jp_sector_ts = all_analyses['sector']['jp']['charts']['html']

eu_sector_matrix = all_analyses['sector']['eu']['tables']['html']
eu_sector_earn = all_analyses['sector']['eu']['earnings_revisions']['html']
eu_sector_ts = all_analyses['sector']['eu']['charts']['html']

au_sector_matrix = all_analyses['sector']['au']['tables']['html']
au_sector_earn = all_analyses['sector']['au']['earnings_revisions']['html']
au_sector_ts = all_analyses['sector']['au']['charts']['html']

uk_sector_matrix = all_analyses['sector']['uk']['tables']['html']
uk_sector_earn = all_analyses['sector']['uk']['earnings_revisions']['html']
uk_sector_ts = all_analyses['sector']['uk']['charts']['html']

xs_gap = all_analyses['cross_sectional']['html']
for title, html in all_analyses['cross_sectional']['individual_charts'].items():
    xs_gap += f"<hr><h3>{title}</h3>{html}"











def graph_performance1(data, title):
    import plotly.graph_objects as go

    full_palette = [
        "#30415f", "#f3a712", "#87b1a1", "#5ac5fe",
        "#a8c686", "#a0a197", "#e4572e", "#2337C6",
        "#B7B1B0", "#778BA5", "#990000"
    ]
    simp_palette = ["#30415f", "#DDDDDD", "#DDDDDD", "#DDDDDD"]

    fig = go.Figure()

    if isinstance(data, pd.Series):
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            mode='lines',
            name=str(data.name) if data.name else "Series",
            line=dict(color=full_palette[0], width=2),
            hovertemplate='%{y:.2f}<extra></extra>'
        ))
    elif isinstance(data, pd.DataFrame):
        use_full_colors = data.shape[1] >= 4
        palette = full_palette if use_full_colors else simp_palette
        for i, col in enumerate(data.columns):
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[col],
                mode='lines',
                name=str(col),
                line=dict(color=palette[i % len(palette)], width=2),
                hovertemplate='%{y:.2f}<extra></extra>'
            ))

    fig.update_layout(
        title=f"<span style='font-size:14px; font-weight:600; color:#30415f'>{title}</span>",
        margin=dict(l=40, r=20, t=50, b=40),
        template='plotly_white',
        hovermode='x unified',
        font=dict(family="Montserrat, sans-serif", size=13),
        xaxis=dict(
            title='',
            tickangle=-45,
            tickfont=dict(size=11),
            gridcolor="#ECECEC",
            linecolor="#ECECEC"
        ),
        yaxis=dict(
            title='',
            tickfont=dict(size=11),
            gridcolor="#ECECEC",
            linecolor="#ECECEC"
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        width=420,
        height=320,
        showlegend=False
    )

    return fig

# Build time series data with consistent naming
timeseries_data = {}
region_map = {'us': 'US', 'au': 'Australia', 'jp': 'Japan', 'uk': 'UK', 'eu': 'Europe'}

# Sector time series
for region_key, sector_entry in all_analyses['sector'].items():
    data = sector_entry.get('data')
    if data:
        region_label = region_map.get(region_key.lower(), region_key.upper())
        for col in data[0].columns:
            timeseries_data[f"{region_label} {col}"] = data[0][col]
        for col in data[2].columns:
            timeseries_data[f"{region_label} {col}"] = data[2][col]

# Regional / Factor / Aussie time series (no prefix)
for group in ['regional', 'factor', 'aussie']:
    data = all_analyses.get(group, {}).get('data')
    if data:
        for col in data[0].columns:
            timeseries_data[col] = data[0][col]


valuation_composite_assets = set(process_opportunities(combined_dfs, -1).index)

metric_charts = {}

for metric, df in combined_dfs.items():
    if metric != "Valuation Composite":
        chart_dict = {
            asset: graph_performance1(timeseries_data[asset], title=asset).to_html(include_plotlyjs=False, full_html=False)
            for asset in df[df[metric] < -1].index if asset in timeseries_data
        }
        metric_charts[metric] = chart_dict

vc_charts = {}
for asset in valuation_composite_assets:
    parts = asset.split(" ", 1)
    if len(parts) < 2:
        continue  # skip malformed names
    region, sector = parts

    matched_keys = [
        k for k in timeseries_data
        if region in k and sector in k
    ]

    if matched_keys:
        for match in matched_keys:
            chart_html = graph_performance1(timeseries_data[match], title=str(match)).to_html(include_plotlyjs=False, full_html=False)

            # Use match as the key to keep it unique if multiple per asset
            vc_charts[match] = chart_html


metric_charts["Valuation Composite"] = vc_charts

chart_pages_html = ""  # ✅ Do this only once at the top

for metric, asset_charts in metric_charts.items():
    asset_blocks = "".join([
        f"""
        <div style='
            flex: 1;
            min-width: 30%;
            max-width: 31%;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 1px 4px rgba(0,0,0,0.05);
            padding: 10px;
            margin: 8px;
        '>{chart}</div>""" for chart in asset_charts.values()
    ])
    
    chart_pages_html += f"""
    <div id="charts_{metric.replace(' ', '_')}" class="content-pane" style="display:none;">
        <h2 style="font-family: Montserrat; font-weight: 700; font-size: 20px;">Cheap Assets – {metric}</h2>
        <div style="display: flex; flex-wrap: wrap; justify-content: space-between;">
            {asset_blocks if asset_blocks else "<p>No charts available.</p>"}
        </div>
        <div style="margin-top: 20px;">
            <button onclick="showContent('dmww0')" class="btn btn-secondary">← Back</button>
        </div>
    </div>
    """







import base64

def make_dashboard_download_button_only(df, title, filename):
    csv = df.to_csv(index=True)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'data:text/csv;base64,{b64}'

    return f"""
    <div style="margin-bottom: 18px;">
        <span style="font-family: Montserrat, sans-serif; font-weight: 500; font-size: 14px; color: #30415f; margin-right: 10px;">
            {title}
        </span>
        <a href="{href}" download="{filename}" 
           style="display:inline-block; background-color:#30415f; color:white; font-size:13px;
                  padding: 6px 14px; border-radius: 6px; text-decoration: none;">
            ⬇️ Download CSV
        </a>
    </div>
    """

dashboard_buttons = []

for category, content in all_analyses.items():
    if category == 'cross_sectional':
        continue

    if category == 'sector':
        for region_key, region_content in content.items():
            if region_content.get('data') and isinstance(region_content['data'], (list, tuple)):
                df = region_content['data'][0]
                title = f"Sector Valuations – {region_key.upper()}"
                filename = f"sector_{region_key}_valuations.csv"
                dashboard_buttons.append(make_dashboard_download_button_only(df, title, filename))

    elif category == 'cross_sectional' and 'data' in content:
        df = content['data']
        title = "Valuation Spreads – Cross-Sectional"
        filename = "cross_sectional_spreads.csv"
        dashboard_buttons.append(make_dashboard_download_button_only(df, title, filename))
        
    else:
        if content.get('data') and isinstance(content['data'], (list, tuple)):
            df = content['data'][0]
            title = f"{category.capitalize()} Valuations"
            filename = f"{category}_valuations.csv"
            dashboard_buttons.append(make_dashboard_download_button_only(df, title, filename))

valuation_download_buttons_html = "\n".join(dashboard_buttons)






#Recession
u = ['LEI YOY Index','NFCIINDX Index','USGG2YR Index','FEDL01 Index','NAPMNEWO Index','NHSPSTOT Index', 'USRINDEX Index']
steadywinter = blp.bdh(u, ['px_last'], start_date='1960-01-01', Per ='M').droplevel(1, axis=1)
steadywinter.index = pd.to_datetime(steadywinter.index)  # ensure datetime index
steadywinter.index = steadywinter.index + pd.offsets.MonthEnd(0)

# Step 1: Add MonthEnd label for grouping
steadywinter['Month'] = steadywinter.index.to_period('M')

# Step 2: Apply forward fill within each group (month), per column
steadywinter = (
    steadywinter.groupby('Month')
    .apply(lambda group: group.ffill().bfill())  # both directions to ensure fill within group
    .reset_index(drop=True)  # remove multiindex created by groupby
)

# Step 3: Reassign correct datetime index (rebuild it from Month)
steadywinter['Date'] = steadywinter['Month'].dt.to_timestamp('M')  # convert back to month end
steadywinter = steadywinter.set_index('Date').drop(columns='Month')
steadywinter = steadywinter[~steadywinter.index.duplicated(keep='first')]
steadywinter.columns = ['LEI YOY','NFCI','2Y','Fed Funds','ISM New Orders','Housing Starts', 'US Recession']
recession = steadywinter['US Recession']

# FEATURE ENGINEERING
steadywinter['2Y-FedFunds'] = (steadywinter['2Y'] - steadywinter['Fed Funds'])
steadywinter['Housing Starts YoY'] = ((steadywinter['Housing Starts'] / steadywinter['Housing Starts'].shift(12))-1)*100 

rec_indicator = steadywinter.drop(['2Y','Fed Funds','Housing Starts','US Recession'],axis=1)

#PARAMS FOR MODEL:
params = [-0.192,1.414,-0.0247,-1.79,-0.0313]
dict_for_model = dict(zip(rec_indicator.columns.to_list(), params))

rec_indicator['Logit'] = (
    rec_indicator['LEI YOY'] * dict_for_model['LEI YOY'] +
    rec_indicator['NFCI'] * dict_for_model['NFCI'] +
    rec_indicator['ISM New Orders'] * dict_for_model['ISM New Orders'] +
    rec_indicator['2Y-FedFunds'] * dict_for_model['2Y-FedFunds'] +
    rec_indicator['Housing Starts YoY'] * dict_for_model['Housing Starts YoY']
)

rec_indicator['Exponent'] = np.exp(rec_indicator['Logit'])
rec_indicator['Probability'] = (rec_indicator['Exponent'] / (1 + rec_indicator['Exponent'])).clip(upper=0.99999999999)
# rec_indicator['Log-Likelihood'] = rec_indicator['E604'] * np.log(rec_indicator['O604']) + (1 - rec_indicator['E604']) * np.log(1 - rec_indicator['O604'])


final_recession_df = rec_indicator[['Probability']]
final_recession_df['Recession'] = recession
final_recession_df = final_recession_df.dropna()

import plotly.graph_objects as go

latest_val = final_recession_df['Probability'].iloc[-1]
latest_date = final_recession_df.index[-1].strftime('%b %Y')
latest_pct = f"{latest_val:.1%}"

fig = go.Figure()

# Shade recession periods (gray bands)
recession_mask = final_recession_df['Recession'] == 1
in_recession = False
start_date = None

for date, is_rec in final_recession_df['Recession'].items():
    if is_rec and not in_recession:
        start_date = date
        in_recession = True
    elif not is_rec and in_recession:
        fig.add_shape(
            type="rect",
            x0=start_date,
            x1=date,
            y0=0,
            y1=1,
            fillcolor="lightgray",
            opacity=0.5,
            layer="below",
            line_width=0
        )
        in_recession = False

# Handle case where last period is still a recession
if in_recession:
    fig.add_shape(
        type="rect",
        x0=start_date,
        x1=final_recession_df.index[-1],
        y0=0,
        y1=1.5,
        fillcolor="lightgray",
        opacity=0.5,
        layer="below",
        line_width=0
    )

# Add recession probability line
fig.add_trace(go.Scatter(
    x=final_recession_df.index,
    y=final_recession_df["Probability"],
    mode="lines",
    name="Probability of Recession",
    line=dict(color="#30415f", width=2)
))

# Update layout
fig.update_layout(
    title=dict(
        text="US Recession Probability in the next 12-months",
        font=dict(family="Montserrat", size=20),
        x=0.5
    ),
    annotations=[
        dict(
            text=f"<span style='font-size:13px;'>Current ({latest_date}) = <b>{latest_pct}</b></span>",
            xref="paper", yref="paper",
            x=0.5, y=1.07,
            showarrow=False,
            font=dict(family="Montserrat", color="gray"),
            align="center"
        )
    ],

    yaxis=dict(
        range=[0, 1],
        title="",
        tickformat=".0%",  # 👈 This makes it display 0%–100%
        ticksuffix="",
    ),
    xaxis_title="",
    plot_bgcolor='white',
    paper_bgcolor='white',
    font=dict(family="Montserrat"),
    showlegend=True,
    width=1200,
    height=500,
    margin=dict(t=60, l=50, r=50, b=80),
    legend=dict(
        orientation="h",
        yanchor="top",
        y=-0.1,
        xanchor="center",
        x=0.5,
        font=dict(family="Montserrat", size=12)
    ),
    template="plotly_white"
)

recession_prob_chart_official = fig.to_html(include_plotlyjs=False, full_html=False)

usable_graphs = rec_indicator.drop(['Logit','Exponent','Probability'], axis=1).copy()
usable_graphs['LEI YOY'] = usable_graphs['LEI YOY'] / 100
usable_graphs['Housing Starts YoY'] = usable_graphs['Housing Starts YoY'] / 100

def graph_performance_for_macro(data, title):
    # Color palettes
    full_palette = [
        "#30415f", "#f3a712", "#87b1a1", "#5ac5fe",
        "#a8c686", "#a0a197", "#e4572e", "#2337C6",
        "#B7B1B0", "#778BA5", "#990000"
    ]
    simp_palette = ["#30415f", "#DDDDDD", "#DDDDDD", "#DDDDDD"]
    
    fig = go.Figure()
    
    # Add traces depending on Series or DataFrame
    if isinstance(data, pd.Series):
        fig.add_trace(go.Scatter(
            x=data.index,
            y=data.values,
            mode='lines',
            name=data.name or "Series",
            line=dict(color=full_palette[0], width=2)
        ))
    elif isinstance(data, pd.DataFrame):
        use_full_colors = data.shape[1] >= 4
        palette = full_palette if use_full_colors else simp_palette
        for i, col in enumerate(data.columns):
            fig.add_trace(go.Scatter(
                x=data.index,
                y=data[col],
                mode='lines',
                name=str(col),
                line=dict(color=palette[i % len(palette)], width=2)
            ))
    
    # Apply layout
    fig.update_layout(
        title=title,
        xaxis_title='',
        yaxis_title='Price',
        template='plotly_white',
        hovermode='x unified',
        font=dict(family="Montserrat, sans-serif"),
        title_font=dict(family="Montserrat, sans-serif", size=14),
        legend_font=dict(family="Montserrat, sans-serif"),
        width=850,
        height=400,
        xaxis=dict(gridcolor="#ECECEC", linecolor="#ECECEC"),
        yaxis=dict(
            side="left",
            title="Price",
            titlefont=dict(color="black"),
            tickfont=dict(color="black"),
            gridcolor="#ECECEC",
            linecolor="#ECECEC",
        ),
        plot_bgcolor="white",
        paper_bgcolor="white",
        legend=dict(
            orientation="h",
            y=-0.075,
            x=0.5,
            xanchor="center"
        )
    )
    
    return fig

# Dashboard-style wrapper for each chart
def make_indicator_chart_html(series, title):
    chart = graph_performance_for_macro(series, title=title)
    chart_html = chart.to_html(include_plotlyjs=False, full_html=False)  # ✅ convert to HTML
    return f"""
    <div style="
        flex: 1;
        min-width: 300px;
        max-width: 600px;
        background-color: white;
        border-radius: 8px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.06);
        padding: 12px;
        margin: 10px;
        font-family: Montserrat, sans-serif;
    ">
        {chart_html}
    </div>
    """

# Generate all chart blocks
dashboard_blocks = [
    make_indicator_chart_html(usable_graphs[col], col)
    for col in usable_graphs.columns[:5]  # Assuming we have at least 6 indicators
]

# Split into two rows of 3 charts each
first_row_blocks = dashboard_blocks[:3]
second_row_blocks = dashboard_blocks[3:5]

# Wrap all in a flex layout with two distinct rows
macro_indicators_html = f"""
<h2 style="font-family: Montserrat, sans-serif; font-weight: 700; font-size: 20px; margin-top: 30px;">
    Macro Recession Indicators – Key Charts
</h2>

<!-- First Row -->
<div style="display: flex; flex-wrap: wrap; justify-content: space-between; margin-bottom: 15px;">
    {''.join(first_row_blocks)}
</div>

<!-- Second Row -->
<div style="display: flex; flex-wrap: wrap; justify-content: space-between;">
    {''.join(second_row_blocks)}
</div>
"""




threshold = 0.60

# Step 1: Pull LEI YOY
lei_yoy = steadywinter['LEI YOY'].copy()
macro_regime = pd.DataFrame(index=lei_yoy.index)
macro_regime['LEI YOY'] = lei_yoy

# Step 2: Compute Z-score
mean = lei_yoy.mean()
std = lei_yoy.std()
macro_regime['Mean'] = mean
macro_regime['LEI YOY Z'] = (lei_yoy - mean) / std

# Step 3: Define ±1 std threshold lines
macro_regime['+1 STD'] = mean + std
macro_regime['-1 STD'] = mean - std

# Step 4: Classify regime
macro_regime['Regime'] = None
for i in macro_regime.index:
    z = macro_regime.at[i, 'LEI YOY Z']
    if z > 1:
        macro_regime.at[i, 'Regime'] = 'Acceleration'
    elif z < -1:
        macro_regime.at[i, 'Regime'] = 'Deceleration'
    else:
        # Inherit previous if not the first row
        prev_idx = macro_regime.index.get_loc(i) - 1
        if prev_idx >= 0:
            macro_regime.at[i, 'Regime'] = macro_regime.iloc[prev_idx]['Regime']

# Step 5: Forward fill any remaining None values just in case
macro_regime['Regime'] = macro_regime['Regime'].ffill()

macro_regime['Probability'] = final_recession_df['Probability']

def classify_phase(row):
    if row['Regime'] == 'Acceleration' and row['Probability'] < threshold:
        return 'Expansion'
    elif row['Regime'] == 'Deceleration' and row['Probability'] < threshold:
        return 'Slowdown'
    elif row['Regime'] == 'Deceleration' and row['Probability'] >= threshold:
        return 'Contraction'
    elif row['Regime'] == 'Acceleration' and row['Probability'] >= threshold:
        return 'Recovery'
    else:
        return 'Unknown'  # fallback

macro_regime['Macro Regime'] = macro_regime.apply(classify_phase, axis=1)

coincident = blp.bdh(tickers = 'COI YOY  Index', flds='px_last',start_date='1960-01-31', Per='M').droplevel(1,axis=1)/100
coincident.columns = ['Coincident']

macro_regime['Coincident'] = coincident

df = macro_regime.reset_index().dropna(subset=['Coincident', 'Macro Regime']).dropna()

# Plot scatter-style regime dots
fig = px.scatter(
    df,
    x='Date',
    y='Coincident',
    color='Macro Regime',
    color_discrete_map={
        'Contraction': '#fdd835',  # Yellow
        'Expansion': '#ef6c00',    # Orange
        'Recovery': '#3949ab',     # Indigo
        'Slowdown': '#6a1b9a',     # Purple
        'Unknown': '#bdbdbd'       # Gray
    },
    title="Innova Asset Management - Macro Economic Cycle Indicator",
    labels={'Coincident': 'Coincident Index'},
    width=1100,
    height=500
)

# Layout and formatting
fig.update_layout(
    template='plotly_white',
    font=dict(family='Montserrat'),
    legend=dict(orientation='h', y=-0.2, x=0.5, xanchor='center'),
    margin=dict(t=60, l=40, r=40, b=70)
)

latest_regime = df['Macro Regime'].iloc[-1]

macro_regime_official = fig.to_html(include_plotlyjs=False, full_html=False)



html_template = f"""
<!DOCTYPE html>
<html>
<head>
  <meta charset="UTF-8">
  <title>Innova Asset Mangagement Internal Dashboard</title>
  <!-- Google Fonts -->
  <link href="https://fonts.googleapis.com/css?family=Montserrat:400,700&display=swap" rel="stylesheet">
  <!-- Bootstrap CSS -->
  <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/css/bootstrap.min.css">
  <style>
    /* Base styles */
    body {{
      margin: 0;
      font-family: 'Montserrat', sans-serif;
    }}
    /* Header Section */
    .site-header {{
      width: 100%;
      box-shadow: 0 2px 4px rgba(0,0,0,0.1);
    }}
    .palette {{
      display: flex;
    }}
    .palette .color-block {{
      flex: 1;
      height: 8px;
    }}
    .header-title-container {{
      background-color: #30415f;
      padding: 10px;
      text-align: center;
      color: #fff;
      position: relative;
    }}
    /* Home button styling */
    .home-button {{
      position: absolute;
      right: 20px;
      top: 10px;
    }}
    /* Main container for sidebar + content */
    .main-container {{
      display: flex;
      height: calc(100vh - 60px); /* Adjust based on header height */
    }}
    .sidebar {{
      width: 250px;
      background-color: #f5f5f5;
      padding: 10px;
      border-right: 1px solid #ddd;
      overflow-y: auto;
    }}
    .content {{
      flex-grow: 1;
      padding: 20px;
      overflow-y: auto;
    }}
    /* First-level tabs styling for sidebar (unchanged) */
    #sidebar-menu > a.list-group-item {{
      background-color: #30415f;
      color: #fff;
      font-weight: bold;
      border: none;
    }}
    #sidebar-menu > a.list-group-item:hover {{
      background-color: #30415f; 
    }}
    /* Sub-tabs remain default */
    .list-group-item {{
      cursor: pointer;
    }}
    /* Image styling */

    .content-pane div.js-plotly-plot {{
        min-height: 400px;
      }}
      
    .content-pane img {{
      display: block;
      margin: 20px auto; /* Adds spacing between images */
      max-width: 100%; /* Ensures images don’t overflow */
    }}

    /* Graph styling: center divs, canvases, iframes, and svgs inside .content-pane */
    .content-pane div,
    .content-pane canvas,
    .center-table {{
      margin: 0 auto;
      width: fit-content;
    }}

    .content-pane iframe,
    .content-pane svg {{
      display: block;
      margin: 20px auto; /* Centers the element */
      max-width: 100%;   /* Prevents overflow */
    }}

    /* Horizontal navigation tabs styling */
    .main-nav {{
      margin-bottom: 0;
    }}
    .nav-tabs .nav-link {{
      color: #30415f;  /* Changed text color */
    }}


    /* Dropdown styling */
    .graph-selector {{
      display: block;
      width: 100%;
      max-width: 500px;
      margin: 20px auto;
      padding: 8px 12px;
      font-size: 16px;
      border: 1px solid #ccc;
      border-radius: 4px;
      background-color: white;
      font-family: 'Montserrat', sans-serif;
    }}
    
    /* Graph container */
    .graph-container {{
      margin-top: 30px;
    }}

  
    .sector-table {{
      margin: 0 auto;
      text-align: center;
    }}
    .sector-table th, .sector-table td {{
      text-align: center;
      padding: 8px;
    }}
    </style>

  </style>
</head>
<body>
  <!-- Header Section with Palette and Title -->
  <header class="site-header">
    <div class="palette">
      <div class="color-block" style="background-color: #30415f;"></div>
      <div class="color-block" style="background-color: #30415f;"></div>
    </div>
    <div class="header-title-container">
      <h1>Innova Asset Mangagement Internal Dashboard</h1>
      <a href="#" class="btn btn-light home-button" onclick="showContent('landing')">Home</a>
    </div>
  </header>

  <!-- Main Tabs Horizontal Navigation -->
  <nav class="main-nav">
    <ul class="nav nav-tabs">
      <li class="nav-item">
        <a class="nav-link" data-main-tab="innova" href="#" onclick="showMainTab('innova')">Innova SMA Analytics</a>
      </li>
      <li class="nav-item">
        <a class="nav-link" data-main-tab="vf" href="#" onclick="showMainTab('vf')">Equity Valuations</a>
      </li>
      <li class="nav-item">
        <a class="nav-link" data-main-tab="macro" href="#" onclick="showMainTab('macro')">Macroeconomic</a>
      </li>
      <li class="nav-item">
        <a class="nav-link" data-main-tab="tech" href="#" onclick="showMainTab('tech')">Technicals / Sentiment / Risk Aversion</a>
      </li>
      <li class="nav-item">
        <a class="nav-link" data-main-tab="misc" href="#" onclick="showMainTab('misc')">Bond Futures and Yield Curve</a>
      </li>
    </ul>
  </nav>
  
  <!-- Main Container: Sidebar (Subtabs) + Content Area -->
  <div class="main-container">
    <!-- Sidebar for Subtabs -->
    <div class="sidebar" id="subtab-sidebar">
      <!-- Innova SMA Analytics Subtabs -->
      <div class="subtabs" id="subtabs-innova" style="display: none;">
        <div class="list-group">
          <a class="list-group-item" onclick="showContent('dmww0')">Cheap Traffic</a>
          <a class="list-group-item" onclick="showContent('dmww99')">Expensive Traffic</a>
          <a class="list-group-item" onclick="showContent('dmww1')">Multi-Asset Valuations</a>
          <a class="list-group-item" onclick="showContent('dmww09')">Performance & Daily Val / Misc</a>
          <a class="list-group-item" onclick="showContent('dmww3')">Target Spreadsheets</a>
          <a class="list-group-item" onclick="showContent('dmww4')">Innova Charts 2025</a>
        </div>
      </div>
      <!-- Equity Valuations Subtabs -->
      <div class="subtabs" id="subtabs-vf" style="display: none;">
        <div class="list-group">
          <a class="list-group-item" onclick="showContent('vf8')">Aussie</a>
          <a class="list-group-item" onclick="showContent('vf0')">Regional</a>
          <a class="list-group-item" onclick="showContent('vf6')">Factor</a>
          <a class="list-group-item" onclick="showContent('vf99')">US Sector</a>
          <a class="list-group-item" onclick="showContent('vf99_jp')">Japan Sector</a>
          <a class="list-group-item" onclick="showContent('vf99_eu')">EU Sector</a>
          <a class="list-group-item" onclick="showContent('vf99_au')">Australian Sector</a>
          <a class="list-group-item" onclick="showContent('vf7')">Cross-Sectional</a>
          <a class="list-group-item" onclick="showContent('vf77')">Real Assets</a>
          <a class="list-group-item" onclick="showContent('vf778')">Downloadable Data</a>
        </div>
      </div>
      <!-- Macroeconomic Subtabs -->
      <div class="subtabs" id="subtabs-macro" style="display: none;">
        <div class="list-group">
          <a class="list-group-item" onclick="showContent('macro5')">Recession Probability</a>
          <a class="list-group-item" onclick="showContent('macro1')">Macroeconomic Regime</a>
          <a class="list-group-item" onclick="showContent('macro2')">Growth</a>
          <a class="list-group-item" onclick="showContent('macro3')">Eco Surprise</a>
          <a class="list-group-item" onclick="showContent('macro6')">US Stock Bond Correlation</a>
        </div>
      </div>
      <!-- Technicals / Sentiment / Risk Aversion Subtabs -->
      <div class="subtabs" id="subtabs-tech" style="display: none;">
        <div class="list-group">
          <a class="list-group-item" onclick="showContent('tech1')">Moving Day Average Graphs</a>
          <a class="list-group-item" onclick="showContent('tech2')">Table of Index Technicals</a>
          <a class="list-group-item" onclick="showContent('tech3')">Cross Asset Vol</a>
          <a class="list-group-item" onclick="showContent('tech4')">Net Flows</a>
          <a class="list-group-item" onclick="showContent('tech5')">Fear / Greed Type Indicators</a>
        </div>
      </div>
      <!-- Bond Futures and Yield Curve Subtabs -->
      <div class="subtabs" id="subtabs-misc" style="display: none;">
        <div class="list-group">
          <a class="list-group-item" onclick="showContent('misc1')">Rate Cut/Hike Pricing</a>
          <a class="list-group-item" onclick="showContent('misc2')">Yield Curves</a>
          <a class="list-group-item" onclick="showContent('misc3')">AU Credit Spreads</a>
          <a class="list-group-item" onclick="showContent('misc4')">Global Credit Spreads</a>
          <a class="list-group-item" onclick="showContent('misc5')">TIPS</a>
        </div>
      </div>
    </div>
    
    <!-- Content Area for Charts -->
    <div class="content" id="content-area">
      <!-- Landing Page -->
      <div id="landing" class="content-pane">
        <div style="display: flex; justify-content: space-between; align-items: flex-start; gap: 32px; padding-left: 20px;">

          <!-- LEFT COLUMN -->
          <div style="flex: 1; max-width: 700px;">
            
            <!-- Welcome Section -->
            <div style="background-color: white; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.05); padding: 16px; font-family: Montserrat, sans-serif; font-size: 13px; margin-bottom: 24px;">
              <div style="font-weight: 600; font-size: 16px; color: #30415f; margin-bottom: 12px;">
                Welcome Innova Team
              </div>
              <p style="margin-top: 0;">
                Please use the tabs above to navigate the different sections. Below are some relevant weekly updates as of <strong>{today_date}</strong> in macro/markets:
              </p>
              <ul style="padding-left: 18px; margin-top: 0;">
                <li><a href="https://www.atlantafed.org/cqer/research/gdpnow" target="_blank">Latest GDP Nowcast is {next_24_chars}</a></li>
                <li><a href="https://www.cnbc.com/finance/" target="_blank">CNBC Finance Top Headlines for {today_date}</a></li>
                <li><a href="https://tradingeconomics.com/calendar" target="_blank">Economic Calendar/Releases as of {today_date}</a></li>
              </ul>

            </div>

            <!-- Valuation Opportunities -->
            {opps_html}

            <!-- Regime Note (aligned properly under Valuation block) -->
            <div style="
                max-width: 700px;
                background-color: white;
                border-radius: 8px;
                box-shadow: 0 2px 8px rgba(0,0,0,0.05);
                padding: 10px 16px;
                font-family: Montserrat, sans-serif;
                font-size: 14px;
                color: #444;
                margin-top: -10px;
                margin-bottom: 24px;
            ">
                As of {today_date} the Macroeconomic Regime is in: <strong style="color: #30415f;">{latest_regime}</strong> (threshold = 60%)
            </div>

      </div>
          <!-- RIGHT COLUMN -->
          <div style="width: 350px;">
            <div style="background-color: white; border-radius: 8px; box-shadow: 0 2px 8px rgba(0,0,0,0.05); padding: 16px; font-family: Montserrat, sans-serif; font-size: 13px;">
              <div style="font-weight: 600; font-size: 16px; color: #30415f; margin-bottom: 12px;">
                Top News of the Week
              </div>
              <ul style="padding-left: 18px; margin: 0;">
                {top_news_html}
              </ul>
            </div>
          </div>

        </div>
      </div>


      <!-- Innova SMA Analytics Content Panes -->
      <div id="dmww0" class="content-pane" style="display:none;">
        <div>{traffic_alert_cheap}</div>
      </div>
      <div id="dmww99" class="content-pane" style="display:none;">
        <div>{traffic_alert_expensive}</div>
      </div>
      <div id="dmww1" class="content-pane" style="display:none;">
        <div>{aashna_all_asset_class_z_score_valuations_html}</div>
      </div>
      <div id="dmww09" class="content-pane" style="display:none;">
        <div style="
            display: flex;
            flex-wrap: wrap;
            justify-content: space-between;
            gap: 24px;
        ">
          <!-- Top Left: YTD Factor Performance -->
          <div style="
              flex: 1;
              min-width: 48%;
              background-color: white;
              border-radius: 8px;
              box-shadow: 0 2px 8px rgba(0,0,0,0.05);
              padding: 16px;
              font-family: Montserrat, sans-serif;
              font-size: 13px;
          ">
            <h3 style="color: #30415f; font-weight: 600;">YTD Factor Performance</h3>
            {graph_for_factor_equity_ytd.to_html(include_plotlyjs=False, full_html=False)}
          </div>

          <!-- Top Right: Full Factor Performance -->
          <div style="
              flex: 1;
              min-width: 48%;
              background-color: white;
              border-radius: 8px;
              box-shadow: 0 2px 8px rgba(0,0,0,0.05);
              padding: 16px;
              font-family: Montserrat, sans-serif;
              font-size: 13px;
          ">
            <h3 style="color: #30415f; font-weight: 600;">Full Factor Performance</h3>
            {graph_for_factor_equity.to_html(include_plotlyjs=False, full_html=False)}
          </div>

          <!-- Bottom Left: Sector Performance Tables -->
          <div style="
              flex: 1;
              min-width: 48%;
              background-color: white;
              border-radius: 8px;
              box-shadow: 0 2px 8px rgba(0,0,0,0.05);
              padding: 16px;
              font-family: Montserrat, sans-serif;
              font-size: 13px;
          ">
            <h3 style="color: #30415f; font-weight: 600;">Sector Performance Tables</h3>
            {sector_tables}
          </div>

          <!-- Bottom Right: Weekly Valuation Charts -->
          <div style="
              flex: 1;
              min-width: 48%;
              background-color: white;
              border-radius: 8px;
              box-shadow: 0 2px 8px rgba(0,0,0,0.05);
              padding: 16px;
              font-family: Montserrat, sans-serif;
              font-size: 13px;
          ">
            <h3 style="color: #30415f; font-weight: 600;">Weekly Valuation Charts</h3>
            {weekly_valuation_charts['combined'].to_html(include_plotlyjs=False, full_html=False)}
            {weekly_valuation_charts['sp500'].to_html(include_plotlyjs=False, full_html=False)}
            {weekly_valuation_charts['nky'].to_html(include_plotlyjs=False, full_html=False)}
            {weekly_valuation_charts['eur'].to_html(include_plotlyjs=False, full_html=False)}
            {weekly_valuation_charts['asx'].to_html(include_plotlyjs=False, full_html=False)}
            {weekly_valuation_charts['em'].to_html(include_plotlyjs=False, full_html=False)}
          </div>
        </div>
      </div>

      <div id="dmww3" class="content-pane" style="display:none;">
        <div>{Funda}</div>
        <div>{Fla}</div>
        <div>{Trad}</div>
        <div>{Cfs_fc}</div>
      </div>
      <div id="dmww4" class="content-pane" style="display:none;">
        <div>{basecase}</div>
        <div>{eco_surprise}</div>
        <div>{concentration}</div>
        <div>{cape_chart}</div>
        <div>{returns_2022}</div>
        <div>{crsp_dimensional}</div>
        <div>{region_positioning}</div>
      </div>
     
      <!-- Equity Valuations Content Panes -->
      <div id="vf8" class="content-pane" style="display:none;">
        <div>{au_matrix}</div>
        <div>{au_earn}</div>

        <select id="ausValueSelector" class="graph-selector" onchange="showAusValuation(this.value)">
        <option value="">-- Select a valuation metric --</option>
        <option value="aus_forward_pe">Forward PE</option>
        <option value="aus_cape">CAPE Ratio</option>
        <option value="aus_price_to_book">Price to Book</option>
        <option value="aus_price_to_sales">Price to Sales</option>
        <option value="aus_valuation_composite">Valuation Composite</option>
        </select>

        <div id="graph-container" class="graph-container">
        <div id="valuation-default" style="text-align: center; padding: 40px;">
            <p>Please select a valuation metric from the dropdown above</p>
        </div>
        <div id="aus_forward_pe" style="display: none;">{au_ts['Forward PE']}</div>
        <div id="aus_cape" style="display: none;">{au_ts['CAPE']}</div>
        <div id="aus_price_to_book" style="display: none;">{au_ts['Price to Book']}</div>
        <div id="aus_price_to_sales" style="display: none;">{au_ts['Price to Sales']}</div>
        <div id="aus_valuation_composite" style="display: none;">{au_ts['Valuation Composite']}</div>
        </div>
      </div>

      <div id="vf0" class="content-pane" style="display:none;">
        <div class="center-table">{rei_matrix}</div>
        <div class="center-table">{rei_earn}</div>

        <select id="reigValueSelector" class="graph-selector" onchange="reigAusValuation(this.value)">
          <option value="">-- Select a valuation metric --</option>
          <option value="_forward_pe">Forward PE</option>
          <option value="_cape">CAPE Ratio</option>
          <option value="_price_to_book">Price to Book</option>
          <option value="_price_to_sales">Price to Sales</option>
          <option value="_valuation_composite">Valuation Composite</option>
        </select>

        <div id="graph-container" class="graph-container">
          <div id="valuation-default" style="text-align: center; padding: 40px;">
            <p>Please select a valuation metric from the dropdown above</p>
          </div>
          <div id="_forward_pe" style="display: none;">{rei_ts['Forward PE']}</div>
          <div id="_cape" style="display: none;">{rei_ts['CAPE']}</div>
          <div id="_price_to_book" style="display: none;">{rei_ts['Price to Book']}</div>
          <div id="_price_to_sales" style="display: none;">{rei_ts['Price to Sales']}</div>
          <div id="_valuation_composite" style="display: none;">{rei_ts['Valuation Composite']}</div>

        </div>
      </div>

      <div id="vf6" class="content-pane" style="display:none;">
        <div class="center-table">{fac_matrix}</div>
        <div class="center-table">{fac_earn}</div>
        <select id="ValueSelector" class="graph-selector" onchange="showFactorValuation(this.value)">
          <option value="">-- Select a valuation metric --</option>
          <option value="factor_forward_pe">Forward PE</option>
          <option value="factor_cape">CAPE Ratio</option>
          <option value="factor_price_to_book">Price to Book</option>
          <option value="factor_price_to_sales">Price to Sales</option>
          <option value="factor_valuation_composite">Valuation Composite</option>
        </select>

        <div id="graph-container" class="graph-container">
          <div id="valuation-default" style="text-align: center; padding: 40px;">
            <p>Please select a valuation metric from the dropdown above</p>
          </div>
          <div id="factor_forward_pe" style="display: none;">{fac_ts['Forward PE']}</div>
          <div id="factor_cape" style="display: none;">{fac_ts['CAPE']}</div>
          <div id="factor_price_to_book" style="display: none;">{fac_ts['Price to Book']}</div>
          <div id="factor_price_to_sales" style="display: none;">{fac_ts['Price to Sales']}</div>
          <div id="factor_valuation_composite" style="display: none;">{fac_ts['Valuation Composite']}</div>
          </div>
      </div>

      <div id="vf99" class="content-pane" style="display:none;">
        <div class="center-table">{us_sector_matrix}</div>
        <div class="center-table">{us_sector_earn}</div>
        <select id="ValueSelectorUS" class="graph-selector" onchange="showSectorValuation(this.value)">
          <option value="">-- Select a valuation metric --</option>
          <option value="sector_forward_pe">Forward PE</option>
          <option value="sector_cape">CAPE Ratio</option>
          <option value="sector_price_to_book">Price to Book</option>
          <option value="sector_price_to_sales">Price to Sales</option>
          <option value="sector_valuation_composite">Valuation Composite</option>
        </select>

        <div id="graph-container" class="graph-container">
          <div id="valuation-default" style="text-align: center; padding: 40px;">
            <p>Please select a valuation metric from the dropdown above</p>
          </div>
          <div id="sector_forward_pe" style="display: none;">{us_sector_ts['Forward PE']}</div>
          <div id="sector_cape" style="display: none;">{us_sector_ts['CAPE']}</div>
          <div id="sector_price_to_book" style="display: none;">{us_sector_ts['Price to Book']}</div>
          <div id="sector_price_to_sales" style="display: none;">{us_sector_ts['Price to Sales']}</div>
          <div id="sector_valuation_composite" style="display: none;">{us_sector_ts['Valuation Composite']}</div>
        </div>
      </div>
      <!-- JP Sector Content Pane -->
      <div id="vf99_jp" class="content-pane" style="display:none;">
        <div class="center-table">{jp_sector_earn}</div>
        <select id="ValueSelectorJP" class="graph-selector" onchange="showSectorValuationJP(this.value)">
          <option value="">-- Select a valuation metric --</option>
          <option value="jp_forward_pe">Forward PE</option>
          <option value="jp_cape">CAPE Ratio</option>
          <option value="jp_price_to_book">Price to Book</option>
          <option value="jp_price_to_sales">Price to Sales</option>
          <option value="jp_valuation_composite">Valuation Composite</option>
        </select>

        <div id="graph-container" class="graph-container">
          <div id="valuation-default" style="text-align: center; padding: 40px;">
            <p>Please select a valuation metric from the dropdown above</p>
          </div>
          <div id="jp_forward_pe" style="display: none;">{jp_sector_ts['Forward PE']}</div>
          <div id="jp_cape" style="display: none;">{jp_sector_ts['CAPE']}</div>
          <div id="jp_price_to_book" style="display: none;">{jp_sector_ts['Price to Book']}</div>
          <div id="jp_price_to_sales" style="display: none;">{jp_sector_ts['Price to Sales']}</div>
          <div id="jp_valuation_composite" style="display: none;">{jp_sector_ts['Valuation Composite']}</div>
        </div>
      </div>

      <!-- EU Sector Content Pane -->
      <div id="vf99_eu" class="content-pane" style="display:none;">
        <div class="center-table">{eu_sector_earn}</div>
        <select id="ValueSelectorEU" class="graph-selector" onchange="showSectorValuationEU(this.value)">
          <option value="">-- Select a valuation metric --</option>
          <option value="eu_forward_pe">Forward PE</option>
          <option value="eu_cape">CAPE Ratio</option>
          <option value="eu_price_to_book">Price to Book</option>
          <option value="eu_price_to_sales">Price to Sales</option>
          <option value="eu_valuation_composite">Valuation Composite</option>
        </select>

        <div id="graph-container" class="graph-container">
          <div id="valuation-default" style="text-align: center; padding: 40px;">
            <p>Please select a valuation metric from the dropdown above</p>
          </div>
          <div id="eu_forward_pe" style="display: none;">{eu_sector_ts['Forward PE']}</div>
          <div id="eu_cape" style="display: none;">{eu_sector_ts['CAPE']}</div>
          <div id="eu_price_to_book" style="display: none;">{eu_sector_ts['Price to Book']}</div>
          <div id="eu_price_to_sales" style="display: none;">{eu_sector_ts['Price to Sales']}</div>
          <div id="eu_valuation_composite" style="display: none;">{eu_sector_ts['Valuation Composite']}</div>
        </div>
      </div>

      <!-- AU Sector Content Pane -->
      <div id="vf99_au" class="content-pane" style="display:none;">
        <div class="center-table">{au_sector_earn}</div>
        <select id="ValueSelectorAU" class="graph-selector" onchange="showSectorValuationAU(this.value)">
          <option value="">-- Select a valuation metric --</option>
          <option value="au_forward_pe">Forward PE</option>
          <option value="au_cape">CAPE Ratio</option>
          <option value="au_price_to_book">Price to Book</option>
          <option value="au_price_to_sales">Price to Sales</option>
          <option value="au_valuation_composite">Valuation Composite</option>
        </select>

        <div id="graph-container" class="graph-container">
          <div id="valuation-default" style="text-align: center; padding: 40px;">
            <p>Please select a valuation metric from the dropdown above</p>
          </div>
          <div id="au_forward_pe" style="display: none;">{au_sector_ts['Forward PE']}</div>
          <div id="au_cape" style="display: none;">{au_sector_ts['CAPE']}</div>
          <div id="au_price_to_book" style="display: none;">{au_sector_ts['Price to Book']}</div>
          <div id="au_price_to_sales" style="display: none;">{au_sector_ts['Price to Sales']}</div>
          <div id="au_valuation_composite" style="display: none;">{au_sector_ts['Valuation Composite']}</div>
        </div>
      </div>

      <div id="vf7" class="content-pane" style="display:none;">
        <h2>Cross-Sectional Gap Table</h2>
        <div>{xs_gap}</div>
      </div>
      
      <div id="vf77" class="content-pane" style="display:none;">
      <div>
      {reits_html['pnta']['Global REITs']}
      {reits_html['pnta']['S&P Infra']}
      {reits_html['pnta']['FTSE Global Core Infra']}
      {reits_html['pnta']['ASX 200 REITs']}
      {reits_html['pnta']['MVA Index (10% Cap)']}

      {reits_html['dividend_yield']['Global REITs']}
      {reits_html['dividend_yield']['S&P Infra']}
      {reits_html['dividend_yield']['FTSE Global Core Infra']}
      {reits_html['dividend_yield']['ASX 200 REITs']}
      {reits_html['dividend_yield']['MVA Index (10% Cap)']}
    </div>
  </div>
      <div id="vf778" class="content-pane" style="display:none;">
        <div>
          <h3 style="margin-top:20px; font-family: Montserrat, sans-serif; color: #30415f;">Valuation Data Downloads</h3>
          {valuation_download_buttons_html}
          
          <h3 style="margin-top:30px; font-family: Montserrat, sans-serif; color: #30415f;">Yield Curve & Credit Spread Downloads</h3>
          {yield_curve_downloads_html}
        </div>
      </div>

      <!-- Macroeconomic Content Panes -->
      <div id="macro1" class="content-pane" style="display:none;">
        <div style="
            max-width: 750px;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            padding: 18px 20px;
            font-family: Montserrat, sans-serif;
            font-size: 14px;
            margin-bottom: 24px;
        ">
          <div style="font-weight: 600; font-size: 16px; color: #30415f; margin-bottom: 6px;">
            Current Macro Regime
          </div>
          <div style="font-size: 14px; color: #444;">
            As of latest data: <strong style="color: #30415f;">{latest_regime}</strong> (threshold = 60%)
          </div>
        </div>

        <div style="margin-top: 20px;">
          {macro_regime_official}
        </div>
      </div>

      <div id="macro2" class="content-pane" style="display:none;">
        {gdp_consensus_html.to_html(include_plotlyjs=False, full_html=False)}
      </div>
      <div id="macro3" class="content-pane" style="display:none;">
        {eco_surpris_df_html.to_html(include_plotlyjs=False, full_html=False)}
      </div>
      <div id="macro5" class="content-pane" style="display:none;">
        <div style="
            max-width: 750px;
            background-color: white;
            border-radius: 8px;
            box-shadow: 0 2px 8px rgba(0,0,0,0.05);
            padding: 18px 20px;
            font-family: Montserrat, sans-serif;
            font-size: 14px;
            margin-bottom: 24px;
        ">
          <div style="font-weight: 600; font-size: 16px; color: #30415f; margin-bottom: 6px;">
            Recession Probability
          </div>
          <div style="font-size: 14px; color: #444;">
            Based on macro indicators using a logit model and a recession probability threshold of <strong>70%</strong>.
          </div>
        </div>

        <!-- Chart -->
        <div style="margin-top: 10px;">
          {recession_prob_chart_official}
        </div>

        <!-- Macro Indicator Dashboard -->
        <div style="margin-top: 30px;">
          {macro_indicators_html}
        </div>
      </div>
      <div id="macro6" class="content-pane" style="display:none;">
        {corr_chart}
      </div>
      
      <!-- Technicals / Sentiment / Risk Aversion Content Panes -->
      <div id="tech1" class="content-pane" style="display:none;">
        {technicals_graphs_html}
      </div>
      <div id="tech2" class="content-pane" style="display:none;">
        {day200.to_html(include_plotlyjs=False, full_html=False)}
        {day50.to_html(include_plotlyjs=False, full_html=False)}
        {rsi70.to_html(include_plotlyjs=False, full_html=False)} 
      </div>
      <div id="tech3" class="content-pane" style="display:none;">
        {cross_asset_vol_chart.to_html(include_plotlyjs=False, full_html=False)}
      </div>
      <div id="tech4" class="content-pane" style="display:none;">
      </div>
      <div id="tech5" class="content-pane" style="display:none;">
      </div>
      
      <!-- Bond Futures and Yield Curve Content Panes -->
      <div id="misc1" class="content-pane" style="display:none;">
        <div>{rate_futures_html}</div>
        <div>{ten_10y_decomp_html}</div>  
      </div>
      <div id="misc2" class="content-pane" style="display:none;">
        <div>{US_chart_html}</div>
        <div>{AU_chart_html}</div>
        <div>{EU_chart_html}</div>
        <div>{Globalhedged_chart_html}</div>
        <div>{Global_chart_html}</div>
      </div>
      <div id="misc3" class="content-pane" style="display:none;">
        <div>{Aus_comp_chart_html}</div>
        <div>{Aus_cred_chart_html}</div>
        <div>{Aus_FRN_chart_html}</div>
      </div>
      <div id="misc4" class="content-pane" style="display:none;">
        <div>{US_corp_chart_html}</div>
        <div>{US_cred_chart_html}</div>
      </div>
      <div id="misc5" class="content-pane" style="display:none;">
        {tips_html.to_html(include_plotlyjs=False, full_html=False)}
      </div>
    </div>
  {chart_pages_html}
  </div>
  
  <!-- jQuery and Bootstrap JS -->
  <script src="https://code.jquery.com/jquery-3.3.1.slim.min.js"></script>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/popper.js/1.14.7/umd/popper.min.js"></script>
  <script src="https://stackpath.bootstrapcdn.com/bootstrap/4.3.1/js/bootstrap.min.js"></script>
  <script src="https://cdn.plot.ly/plotly-latest.min.js"></script> <!-- Add Plotly CDN here -->
  <script>

      function showSectorValuationJP(metricId) {{
      showValuationDropdown(metricId, [
        "jp_forward_pe", "jp_cape", "jp_price_to_book", "jp_price_to_sales", "jp_valuation_composite"
      ]);
    }}

    function showSectorValuationEU(metricId) {{
      showValuationDropdown(metricId, [
        "eu_forward_pe", "eu_cape", "eu_price_to_book", "eu_price_to_sales", "eu_valuation_composite"
      ]);
    }}

    function showSectorValuationAU(metricId) {{
      showValuationDropdown(metricId, [
        "au_forward_pe", "au_cape", "au_price_to_book", "au_price_to_sales", "au_valuation_composite"
      ]);
    }}



    function showFactorValuation(metricId) {{
      showValuationDropdown(metricId, [
        "factor_forward_pe", "factor_cape", "factor_price_to_book", "factor_price_to_sales", "factor_valuation_composite"
      ]);
    }}

    function showSectorValuation(metricId) {{
      showValuationDropdown(metricId, [
        "sector_forward_pe", "sector_cape", "sector_price_to_book", "sector_price_to_sales", "sector_valuation_composite"
      ]);
    }}


    function showValuationDropdown(metricId, ids) {{
      ids.forEach(id => {{
        const el = document.getElementById(id);
        if (el) el.style.display = "none";
      }});

      const selected = document.getElementById(metricId);
      const defaultMsg = document.getElementById("valuation-default");
      if (selected) {{
        selected.style.display = "block";
        if (defaultMsg) defaultMsg.style.display = "none";
      }} else {{
        if (defaultMsg) defaultMsg.style.display = "block";
      }}

      if (selected) {{
        setTimeout(() => {{
          const graphs = selected.querySelectorAll('.js-plotly-plot');
          graphs.forEach(graph => {{
            if (typeof Plotly !== 'undefined') {{
              Plotly.Plots.resize(graph);
            }}
          }});
        }}, 600);
      }}
    }}

    function showAusValuation(metricId) {{
      showValuationDropdown(metricId, [
        "aus_forward_pe", "aus_cape", "aus_price_to_book", "aus_price_to_sales", "aus_valuation_composite"
      ]);
    }}

    function reigAusValuation(metricId) {{
      showValuationDropdown(metricId, [
        "_forward_pe", "_cape", "_price_to_book", "_price_to_sales", "_valuation_composite"
      ]);
    }}

    function showValuation(metricId) {{
      showValuationDropdown(metricId, [
        "factor_forward_pe", "factor_cape", "factor_price_to_book", "factor_price_to_sales", "factor_valuation_composite"
      ]);
    }}


  // Function to display the corresponding subtab menu based on the selected main tab
  function showMainTab(tabId) {{
    // Hide all subtabs first
    var subtabs = document.getElementsByClassName('subtabs');
    for (var i = 0; i < subtabs.length; i++) {{
      subtabs[i].style.display = 'none';
    }}

    // Show current tab’s subtab section
    var currentSubtab = document.getElementById('subtabs-' + tabId);
    if (currentSubtab) {{
      currentSubtab.style.display = 'block';
    }}

    // Automatically open the first subtab under the selected main tab
    const firstSubtab = document.querySelector(`#subtabs-${{tabId}} .list-group-item`);
    if (firstSubtab && firstSubtab.getAttribute('onclick')) {{
      const onclickContentId = firstSubtab.getAttribute('onclick').match(/showContent\\('(.+?)'\\)/);
      if (onclickContentId && onclickContentId[1]) {{
        showContent(onclickContentId[1]);
      }}
    }}

    // Tab highlighting logic
    var mainTabLinks = document.querySelectorAll('.nav-tabs .nav-link');
    for (var i = 0; i < mainTabLinks.length; i++) {{
      mainTabLinks[i].classList.remove('active');
    }}

    var activeLink = document.querySelector('.nav-tabs .nav-link[data-main-tab="{{' + tabId + '}}"]');
    if (activeLink) {{
      activeLink.classList.add('active');
    }}
  }}

  document.addEventListener("DOMContentLoaded", function () {{
    showMainTab('innova');
    showContent('landing');
  }});

  
  function showContent(contentId) {{
    var panes = document.getElementsByClassName('content-pane');
    for (var i = 0; i < panes.length; i++) {{
      panes[i].style.display = 'none';
    }}

    var el = document.getElementById(contentId);
    if (el) {{
      el.style.display = 'block';

      // Resize Plotly charts after showing the content
      setTimeout(function () {{
        var plotlyGraphs = el.querySelectorAll('.js-plotly-plot');
        for (var j = 0; j < plotlyGraphs.length; j++) {{
          if (typeof Plotly !== 'undefined') {{
            Plotly.Plots.resize(plotlyGraphs[j]);
          }}
        }}
      }}, 600);
    }}
  }}

</script>
  
</body>
</html>
"""
# Write the HTML to a file
with open("index.html", "w", encoding='utf-8') as file:
    file.write(html_template)