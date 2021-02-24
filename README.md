# Reddit Scraper For Stock Picks
## Summary
This repository contains python scripts which will scrape r/Stocks, r/Investing, and r/StockMarket. The goal is to scrape comments where there has been a stock ticker identified within the comment. The goal is to find redditors who consistently suggest stocks which provide high returns.

## Medium Article
I wrote a medium article which explains the inspiration behind this project. The article can be found in the link below:
https://medium.com/@adhamsuliman/redditors-vs-the-market-a0e0833081c1

## How to get up and running
#### Reddit Application
In order to use this repo, users must create a reddit application. Felippe Rodrigues wrote a great article on how to go about getting those credentials. The link to that article is provided below: 

https://www.storybench.org/how-to-scrape-reddit-with-python/

#### Python Module Requirements
All the libraries used in this example can be installed using pip with the requirements.txt file included. Open any terminal or command prompt and type in the following line. Use conda install if working with a conda environment. 
```python
pip install requirements.txt
```

## Current Processing Methodology
1. Scrape Reddit 
    1. Scrape new posts for r/stocks, r/investing, r/stockmarket.
    1. If a new user is identified when scraping, scrape all of that user's comments. 
    1. Use Named Entity Recogntion (**NER**) to identify stocks within comments
    1. Use vader sentiment to determine how positive a comment is. 

1. Find stock prices
    1. Use yfinance to pull data for all stocks found.
    2. Calculate the return if a redditor had bought one share the day that he/she posted the initial commment. 
    3. Calculate the average daily return by comment where only the number of weekdays are taken into consideration.
    4. Find the average daily return by user.
      

## Dashboard
There is a dashboard which looks at this data which can be found in the following link. 
https://datastudio.google.com/reporting/7d5c20ed-b6a6-4162-81ba-062bf869c5c0/page/z9ScB

### Page 1
 The table on the top right list redditors with the number of comments found with a ticker in it and the average daily return of all their picks since the day of each comment. There are multiple filters that can be used for the table on the bottom half of the page. The filters available include by date range, by redditor, by stock, and by comment positivity. The positivity score was pulled using the vader sentiment module which was trained on a large twitter corpus.    
![page_1](images/page_1.png)

### Page 2
This line graph looks at ticker mention by day. Users can filter by date range, ticker, redditor, and comment positivity. 
![page_2](images/page_2.png)

### Page 3
This table aims to identify who was the first redditor to mention a certain ticker. A disclaimer to note is that the scraper is only run on the top comments for 2020, and it has been scraping all new posts on r/stocks, r/investing, and r/stockmarket since late August.
![page_3](images/page_3.png)