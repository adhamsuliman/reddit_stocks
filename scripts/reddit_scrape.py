# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# Import Modules
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
import pandas as pd
import numpy as np 

import configparser 
from datetime import datetime, timezone, timedelta
import gspread_dataframe as gd
from gspread_pandas import Spread
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

# Call functions file
from reddit_functions import *
from subreddit_class import subreddit


# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# Call main
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
if __name__ == "__main__":
    # set use_gsheet = False if you would like to use excel instead of google sheets. 
    subreddit = subreddit()
    subreddit.main(['stocks','investing','StockMarket'], use_gsheet = False)

