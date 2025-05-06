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
import yfinance as yf
import math
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
from sql_cache_utils import read_cache, write_cache, append_to_cache

class ValuationAnalytics:
    def __init__(self, blp):
        self.blp = blp
        self.frequency = 'M'
        self.start_date = '2020-05-10'
        self.valuation_metric_list = ['BEST_PE_RATIO', 'PX_TO_BOOK_RATIO', 'LONG_TERM_PRICE_EARNINGS_RATIO', 'CURRENT_EV_TO_T12M_EBITDA', 'PX_TO_SALES_RATIO']
        self.regional_list = ['SPX Index', 'MXWO Index']

    def fetch_and_cache(self, ticker, fields, start_date, table_name, freq="M", **kwargs):
        try:
            cached = read_cache(table_name)
            last_date = cached.index.max()
            fetch_start = (last_date + pd.Timedelta(days=1)).strftime('%Y-%m-%d')
        except:
            fetch_start = start_date

        new_data = self.blp.bdh(ticker, fields, fetch_start, Per=freq, **kwargs)
        if new_data.empty:
            print(f"No new data for {ticker}")
            return read_cache(table_name)

        new_data.index.name = "date"
        new_data.columns = [f"{ticker}_{field}" for field in fields]
        return append_to_cache(table_name, new_data)

    def get_monthly_valuation_data(self):
        dfs = []
        for ticker in self.regional_list:
            table_name = f"{ticker.replace(' ', '_').replace('.', '')}_monthly_vals"
            df = self.fetch_and_cache(ticker, self.valuation_metric_list, self.start_date, table_name, freq=self.frequency)
            dfs.append(df)
        return pd.concat(dfs, axis=1)

va = ValuationAnalytics(blp)
regional_data = va.get_monthly_valuation_data()
print(regional_data.tail())



regional_data.to_csv("output/monthly_valuations_snapshot.csv")
