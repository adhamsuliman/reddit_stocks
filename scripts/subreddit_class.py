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
from prawcore.exceptions import Forbidden, ResponseException
import re 
import time
from tqdm import tqdm
import yfinance as yf
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
analyzer = SentimentIntensityAnalyzer()

# Call functions file
from reddit_functions import *

# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# Define Class
# XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX
class subreddit:
    # Create reddit api
    def __init__(self):       
        config = configparser.ConfigParser()
        config.read('../reddit_api/config_reddit.ini')

        # Initiate connection to reddit
        self.reddit = praw.Reddit(client_id= config.get('reddit','client_id'),\
                        client_secret = config.get('reddit','client_secret'),
                        user_agent=config.get('reddit','user_agent'),\
                        username=config.get('reddit','username'),\
                        password=config.get('reddit','password'))
        
        # Initiate connection to google drive 
        self.key = config.get('google', 'api_key')

        self.ticker = pd.read_csv('../data/ticker.csv')

    
    def subreddit_scrape(self, subreddits, df_scrape_legacy):
        post_body, post_score, post_id, post_time =  [], [], [], []
        tik_name, tik, tik_author, tik_score, tik_comment, tik_time, tik_sentiment = [], [], [], [], [], [], []
        tik_pos, tik_neg, tik_compound = [], [], []
        ticker = self.ticker

        if df_scrape_legacy is None:
            for sub in subreddits:
                subreddit_1 = self.reddit.subreddit(sub).top(limit = 1000)
                for submission in subreddit_1: 
                    submission.comments.replace_more(limit=0)
                    for comments in submission.comments.list():
                        # create filters for reasons why you wouodln't include the comment where the ticker as identified
                        # Currently filtering out mods and making sure upvotes are larger than 3
                        if comments.author != 'AutoModerator' and int(comments.score) > 3:    
                            comments_body = comments.body
                            companies = preprocess(comments_body)
                            companies = regex(companies)
                            for i in companies:
                                try:
                                    # add comment elements here
                                    tik_name.append(ticker[ticker.stock == i.upper()]['name_1'].values[0])
                                    tik.append(i)
                                    tik_author.append(comments.author)
                                    tik_comment.append(comments.body)
                                    tik_score.append(comments.score)
                                    tik_time.append(datetime.fromtimestamp(comments.created).date())
                                    tik_sentiment.append(analyzer.polarity_scores(text = comments.body))
                                    sentiment = analyzer.polarity_scores(text = comments.body)
                                    tik_pos.append(sentiment['pos'])
                                    tik_neg.append(sentiment['neg'])
                                    tik_compound.append(sentiment['compound'])

                                    # add post elements here 
                                    post_time.append(datetime.fromtimestamp(submission.created).date())
                                    post_id.append(submission.id)
                                    post_score.append(submission.score)
                                except:
                                    pass
        else:
            for sub in subreddits:
                subreddit_1 = self.reddit.subreddit(sub).top("year",limit = 1000)
                for submission in subreddit_1: 
                    if submission.id not in df_scrape_legacy.post_id.values:
                        submission.comments.replace_more(limit=0)
                        for comments in submission.comments.list():
                            # create filters for reasons why you wouodln't include the comment where the ticker as identified
                            # Currently filtering out mods and making sure upvotes are larger than 3
                            if comments.author != 'AutoModerator' and int(comments.score) > 3:    
                                comments_body = comments.body
                                companies = preprocess(comments_body)
                                companies = regex(companies)
                                for i in companies:
                                    try:
                                        # add comment elements here
                                        tik_name.append(ticker[ticker.stock == i.upper()]['name_1'].values[0])
                                        tik.append(i)
                                        tik_author.append(comments.author)
                                        tik_comment.append(comments.body)
                                        tik_score.append(comments.score)
                                        tik_time.append(datetime.fromtimestamp(comments.created).date())
                                        tik_sentiment.append(analyzer.polarity_scores(text = comments.body))
                                        sentiment = analyzer.polarity_scores(text = comments.body)
                                        tik_pos.append(sentiment['pos'])
                                        tik_neg.append(sentiment['neg'])
                                        tik_compound.append(sentiment['compound'])

                                        # add post elements here 
                                        post_time.append(datetime.fromtimestamp(submission.created).date())
                                        post_id.append(submission.id)
                                        post_score.append(submission.score)
                                    except:
                                        pass

        df = pd.DataFrame(list(zip(post_id, post_score, post_time, tik_author, tik, tik_score, tik_compound,\
            tik_name, tik_pos, tik_neg, tik_comment, tik_time)), columns= [ 'post_id', 'post_score', 'post_time', \
            'tik_author','tik', 'tik_score', 'tik_compound','tik_name', 'tik_pos','tik_neg', 'tik_comment', 'tik_time'])
        df.drop_duplicates(['tik_time','tik_author','tik','tik_score','tik_comment'], inplace = True)
        
        if isinstance(df_scrape_legacy, pd.DataFrame):
            cols = df.columns
            df_1 = pd.concat([df, df_scrape_legacy[cols]]).reset_index(drop = True).drop_duplicates()
        else:
            df_1 = df

        # Found tik_author that was an empty string. deleting rows with '' as value. 
        df_1 = df_1.replace('', np.nan).dropna().reset_index(drop=True)
        
        return df_1
  
    def user_scrape(self,df, df_user_scrape_legacy ):
        users = df.tik_author.unique()
        tik_name, tik, tik_author, tik_score, tik_comment, tik_time, tik_sentiment = [], [], [], [], [], [], []
        tik_pos, tik_neg, tik_compound = [], [], []
        ticker = self.ticker

        
        if df_user_scrape_legacy is None:
            for u in users:
                try:
                    for comment in self.reddit.redditor(str(u)).comments.top(limit=100): 
                        if (str(comment.subreddit).lower() in ['stocks','investing','stockmarket']) and (comment.score > 3):
                            comments_body = comment.body
                            companies = preprocess(comments_body)
                            companies = regex(companies)
                            if len(companies) > 0:
                                for i in companies:
                                    try:
                                        # add comment elements here
                                        tik_name.append(ticker[ticker.stock == i.upper()]['name_1'].values[0])
                                        tik.append(i)
                                        tik_author.append(comment.author)
                                        tik_comment.append(comment.body)
                                        tik_score.append(comment.score)
                                        tik_time.append(datetime.fromtimestamp(comment.created).date())
                                        sentiment = analyzer.polarity_scores(text = comment.body)
                                        tik_pos.append(sentiment['pos'])
                                        tik_neg.append(sentiment['neg'])
                                        tik_compound.append(sentiment['compound'])
                                    except:
                                        pass
                except (Forbidden, AssertionError) as e:
                    print(e)
                except ResponseException as e:
                    print(e)
        else:
            for u in users:
                try:
                    if u in df_user_scrape_legacy.tik_author.values:
                        # Find the most recent time a user commented and convert it to utc time 
                        df_user_scrape_legacy_copy = df_user_scrape_legacy[df_user_scrape_legacy.tik_author == u].copy()
                        x = df_user_scrape_legacy_copy.groupby('tik_author')['tik_time'].max()
                        most_recent_post = pd.to_datetime(str(x.values[0]))
                        for comment in self.reddit.redditor(str(u)).comments.new(limit=100): 
                            tik_time_comm = datetime.fromtimestamp(comment.created).date()
                            if (str(comment.subreddit).lower() in ['stocks','investing','stockmarket']) and \
                            (comment.score > 3) and (tik_time_comm > most_recent_post):
                                comments_body = comment.body
                                companies = preprocess(comments_body)
                                companies = regex(companies)
                                if len(companies) > 0:
                                    for i in companies:
                                        try:
                                            # add comment elements here
                                            tik_name.append(ticker[ticker.stock == i.upper()]['name_1'].values[0])
                                            tik.append(i)
                                            tik_author.append(comment.author)
                                            tik_comment.append(comment.body)
                                            tik_score.append(comment.score)
                                            tik_time.append(datetime.fromtimestamp(comment.created).date())
                                            sentiment = analyzer.polarity_scores(text = comment.body)
                                            tik_pos.append(sentiment['pos'])
                                            tik_neg.append(sentiment['neg'])
                                            tik_compound.append(sentiment['compound'])
                                        except:
                                            pass
                    else:
                        for comment in self.reddit.redditor(str(u)).comments.top(limit=100): 
                            if (str(comment.subreddit).lower() in ['stocks','investing','stockmarket']) and (comment.score > 3):
                                comments_body = comment.body
                                companies = preprocess(comments_body)
                                companies = regex(companies)
                                if len(companies) > 0:
                                    for i in companies:
                                        try:
                                            # add comment elements here
                                            tik_name.append(ticker[ticker.stock == i.upper()]['name_1'].values[0])
                                            tik.append(i)
                                            tik_author.append(comment.author)
                                            tik_comment.append(comment.body)
                                            tik_score.append(comment.score)
                                            tik_time.append(datetime.fromtimestamp(comment.created).date())
                                            sentiment = analyzer.polarity_scores(text = comment.body)
                                            tik_pos.append(sentiment['pos'])
                                            tik_neg.append(sentiment['neg'])
                                            tik_compound.append(sentiment['compound'])
                                        except:
                                            pass
                except (Forbidden, AssertionError) as e:
                    print(e)
                except ResponseException as e:
                    print(e)    



        df_1 = pd.DataFrame(list(zip(tik_time,tik_author, tik, tik_score,tik_name,tik_pos, tik_neg, tik_compound, tik_comment,)), \
            columns= ['tik_time','tik_author','tik', 'tik_score', 'tik_name', 'tik_pos', 'tik_neg', 'tik_compound', 'tik_comment'])
        df_1.drop_duplicates(['tik_time','tik_author','tik','tik_score','tik_comment'], inplace = True)
        
        # Concat new dataframe with legacy dataframe 
        if isinstance(df_user_scrape_legacy, pd.DataFrame):
            cols = df_1.columns
            df_user_final = pd.concat([df_1, df_user_scrape_legacy[cols]]).reset_index(drop = True).drop_duplicates()
        else:
            df_user_final = df_1
        
        df_user_final = df_user_final.replace('', np.nan).dropna().reset_index(drop=True)

        return df_user_final


    def combine_scrape_user(df_scrape, df_user):
        # Combine both the scrape and user dataframes
        common_columns = df_user.columns
        df_combined = pd.concat([df_scrape[common_columns], df_user[common_columns]]).reset_index(drop = True)
        df_combined_1 = df_combined.drop_duplicates(subset = common_columns[:4], keep = "first").reset_index(drop = True).copy()
        
        # DD and CDC have been very common so let's remove them
        df_combined_1 = df_combined_1[~df_combined_1.tik.isin(['DD','CDC','PE','CEO','ATH','IPO','IMO','GDP','WFH','USA','IRL','EV','USD','IRS','CBO','PER','HUGE', 'TA'])]    
        
        # Calculate stats by user 
        #df_combined_1['tik_author'] = df_combined_1['tik_author'].apply(lambda x: str(x))
        #df_grouped = df_combined_1.groupby(['tik_author'], as_index = False).agg({'tick_change_normalized':['mean','count']})

        # Join the stats by user to oringinal dataframe
        #df_combined_2 = df_combined_1.merge(df_grouped, on = 'tik_author',how = 'inner',suffixes = ('','_group')).copy()

        #Identify first redditor
        df_combined_1['tik_time_2'] = df_combined_1['tik_time'].apply(lambda x: int(x.strftime(r"%Y%m%d")))
        df_combined_1['first_redditor'] = df_combined_1.groupby('tik')['tik_time_2'].rank(method = 'first')
        df_combined_1.drop(columns=[ 'tik_time_2'],inplace = True)

        # renaming of columns
        df_combined_1.columns = list(common_columns) + ['first_redditor']

        # Drop any uplicates that may be found
        df_combined_1.drop_duplicates(['tik_time','tik_author','tik','tik_comment'], inplace = True)    

        return df_combined_1

    def main(self, subreddit1, use_gsheet = True):
        # Read Gsheets or excel 
        if use_gsheet == True:
            df_scrape_legacy = read_gsheet(self.key, sheet = "Subreddit_Scrape")
            df_user_scrape_legacy = read_gsheet(self.key, sheet = 'User_Scrape')
            print("Using Gsheets")
        else:
            try:
                df_scrape_legacy = pd.read_csv('../data/subreddit_scrape.csv')
                df_user_scrape_legacy = pd.read_csv('../data/user_scrape.csv')
                print("Using Local Excel Files")
            except:
                df_scrape_legacy = None
                df_user_scrape_legacy = None
                print('No prior files found')

        # Scrape main new posts
        df_scrape = self.subreddit_scrape(subreddits = subreddit1, df_scrape_legacy = df_scrape_legacy)

        df_ticker = ticker_data(df_scrape)
        df_scrape_final = extract_stats(df_ticker)
        
        # Scrape for any user 
        
        df_user = self.user_scrape(df = df_scrape_final, df_user_scrape_legacy = df_user_scrape_legacy)
        df_ticker_user = ticker_data(df_user)
        df_user_final = extract_stats(df_ticker_user)
        
        # Combine initial scrape with user scrape 
        df_combined = combine_scrape_user(df_scrape_final, df_user_final)


        if use_gsheet == True:
            # Initiate connection to google drive
            spread = Spread('user_data')
            # Send combined data
            spread.df_to_sheet(df_combined, index=False, sheet='Subreddit_and_User', replace=True)
            # Send subreddit scrape
            spread.df_to_sheet(df_scrape_final, index=False, sheet='Subreddit_Scrape', replace=True)
            # Send user scrape 
            spread.df_to_sheet(df_user_final, index=False, sheet='User_Scrape', replace=True)
        
        else:
            # Send combined data
            df_combined.to_csv('../data/df_combined.csv', index = False)
            # Send subreddit scrape
            df_scrape_final.to_csv('../data/subreddit_scrape.csv', index = False)
            # Send user scrape 
            df_user_final.to_csv('../data/subreddit_scrape.csv', index = False)
        
        print(f'DONE! {datetime.now().date()}')


