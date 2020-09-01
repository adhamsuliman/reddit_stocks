import pandas as pd
import numpy as np 

from datetime import datetime, timezone, timedelta
import json 
import itertools
from nltk.tokenize import word_tokenize
from nltk.tag import pos_tag
import praw
from prawcore.exceptions import Forbidden
import re 
import time
from tqdm import tqdm
import yfinance as yf
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()

from df2gspread import df2gspread as d2g
import gspread
import gspread_dataframe as gd
from gspread_pandas import Spread

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# Create Functions
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
def preprocess(sent):
    sent = word_tokenize(sent)
    sent = pos_tag(sent)
    nouns = [word for (word, pos) in sent if pos in ['NNP']]
    return nouns

# Identify if a ticker exists in text 
def regex(nouns):
    companies = []
    for i in nouns:
        try:
            company = re.search('[A-Z]{1,4}',i.strip()).group(0)
            if (len(company) > 1) and (company not in ['AI','DD','CDC','PE',\
                'CEO','ATH','IPO','IMO','GDP','WFH','USA','IRL','EV','USD','IRS','NEW']):
                companies.append(company)
        except:
            pass
    companies = (list(set(companies)))
    return companies

# Download tocker data from the start of 2020-01-01 to the most recent date
def stock_price(tickers):
    ticker_list = []
    for i in tickers:
        df_yf = yf.download(i,'2020-01-01',datetime.today().date())
        df_yf['ticker'] = i
        ticker_list.append(df_yf)

# Find the date of the most recent weekday
def prev_weekday(adate):
    adate -= timedelta(days=1)
    while adate.weekday() > 4: # Mon-Fri are 0-4
        adate -= timedelta(days=1)
    return str(adate)

# Use vader sentiment to identify how positive a comment is
def sentiment(df):
    df['tik_sentiment'] = df.tik_comment.apply(lambda x: analyzer.polarity_scores(text = x))
    df['pos'] = df['tik_sentiment'].apply(lambda x: x['pos'])
    df['neg'] = df['tik_sentiment'].apply(lambda x: x['neg'])

# Find difference in between dates utilizing only weekdays. This number is used in calculating daily average return 
def date_diff(date):
    s = pd.date_range(date, datetime.today()-timedelta(days=1), freq='D').to_series()
    d = s.dt.weekday.reset_index()
    d.columns = ['date','bus_day']
    num_days = d[d['bus_day'].isin(range(5))].shape[0]
    return num_days          

# Read in data from google sheets
def read_gsheet(key, sheet):
    gc = gspread.service_account()
    sh = gc.open_by_key(key)
    worksheet = sh.worksheet(sheet)
    df = pd.DataFrame(worksheet.get_all_records())
    df['tik_time'] = df['tik_time'].apply(lambda x: datetime.strptime(x[:10],r'%Y-%m-%d'))
    return df

# Calculate ticker data for each unique comment. 
def ticker_data(df):
    # Get ticker data 
    tickers = list(df.tik.values)
    x = yf.download(tickers, '2020-01-01', datetime.today().date(),'close')
    df_ticker_close, df_ticker_volume = x['Adj Close'], x['Volume']

    # Apply ticker data to reddit dataframe
    df['tik_today'] = 0
    df['tik_day_of_comment'] = 0
    for i in range(df.shape[0]):
        try:
            df_ticker1 = df_ticker_close[df.loc[i,'tik']].copy()
            df.loc[i,'tik_today'] = df_ticker1[prev_weekday(datetime.today().date())]
            df.loc[i,'tik_day_of_comment'] = df_ticker1[prev_weekday(df.loc[i,'tik_time'])]
        except:
            df.loc[i,'tik_today'] = 0.00069
            df.loc[i,'tik_day_of_comment'] = 0.00069

    # Filter out stocks we couldn't find values for 
    df_1 = df[(df.tik_today > .001) | (df.tik_day_of_comment > .001)]
    df_1 = df_1.reset_index(drop = True)

    return df_1

# Get stats for each individual comment
def extract_stats(df):
    # Equations to find how stocks performed
    df['tik_change'] = df['tik_today']/df['tik_day_of_comment']
    df['tik_change_normalized'] = df.apply(lambda x: ((x.tik_today - x.tik_day_of_comment) / np.round(x.tik_day_of_comment,6)/ date_diff(x.tik_time)), axis = 1)
    return df

# Join the initial scrape df with user dataframe
def combine_scrape_user(df_scrape, df_user):
    # Combine both the scrape and user dataframes
    common_columns = df_user.columns
    df_combined = pd.concat([df_scrape[common_columns], df_user[common_columns]]).reset_index(drop = True)
    df_combined_1 = df_combined.drop_duplicates(subset = common_columns[:4], keep = "first").reset_index(drop = True).copy()

    # Calculate stats by user 
    df_combined_1['tik_author'] = df_combined_1['tik_author'].apply(lambda x: str(x))
    df_grouped = df_combined_1.groupby(['tik_author'], as_index = False).agg({'tik_change_normalized':['mean','count']})

    # Join the stats by user to oringinal dataframe
    df_combined_2 = df_combined_1.merge(df_grouped, on = 'tik_author',how = 'inner',suffixes = ('','_group')).copy()
    
    #Identify first redditor
    df_combined_2['tik_time_2'] = df_combined_2['tik_time'].apply(lambda x: int(x.strftime(r"%Y%m%d")))
    df_combined_2['first_redditor'] = df_combined_2.groupby('tik')['tik_time_2'].rank(method = 'first')
    df_combined_2.drop(columns=[ 'tik_time_2'],inplace = True)
    
    # renaming of columns
    df_combined_2.columns = list(common_columns) + ['tik_change_normalized_mean','tik_change_normalized_count','first_redditor']
    
    # Drop any uplicates that may be found
    df_combined_2.drop_duplicates(['tik_time','tik_author','tik','tik_comment'], inplace = True)    
    
    return df_combined_2 


