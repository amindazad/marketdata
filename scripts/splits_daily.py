import os
import pandas as pd
import numpy as np
import re
import time
import itertools
from datetime import datetime as dt
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
from data_utils import *

# Load settings, launch browser and make directory
settings = utils_get_settings()
safe_make_dir(settings['yfinance_splits'])

# Load Market Calendar and loop through the dates
market_cal = pd.read_csv(settings['processed_path'] + 'market_calendar.csv')
# Slicing to only this year
market_cal_1 = market_cal[(market_cal['year'] == dt.now().year) & (market_cal['month'] == dt.now().month)]
missed_dates = []

browser = launch_firefox_browser('', DeleteContents=False)
for marketdate in market_cal_1['market_date']:
    try:
        # Go to the url
        url = 'https://finance.yahoo.com/calendar/splits?day=' + marketdate
        browser.get(url)
        # Find the table
        tbl = browser.find_element_by_xpath(
            '/html/body/div[1]/div/div/div[1]/div/div[2]/div/div/div[7]/div/div/div/div/div[1]/table'
        ).get_attribute(
            'outerHTML'
        )
        dfs = pd.read_html(tbl, header=0)
        # Save as csv
        df = dfs[0].to_csv(
            settings['yfinance_splits']+'yfinance_ipo_{}.csv'.format(marketdate), index=False
        )
        print('Scrapped Splits', marketdate)
        browser.implicitly_wait(3)
    except:
        print(marketdate, 'Not Found')
        missed_dates.append(marketdate)
        pass
browser.close()

missed_dates_df = pd.DataFrame(missed_dates)

# Create a dataframe to append all csvs
df = pd.DataFrame()
# Loop through csv files and append them to df
for ipofile in os.listdir(settings['yfinance_splits']):
    if ipofile.endswith('csv') & ipofile.startswith('yfinance'):
        print('appending', ipofile)
        df = df.append(pd.read_csv(settings['yfinance_splits'] + ipofile))
        # delete the appended csv
        # os.remove(settings['yfinance_splits'] + ipofile)

print('Adding previous splits date ...')
x = df.groupby('Symbol')['Payable on'].apply(lambda x: pd.Series(list(x))).unstack()
dfx = df.merge(x, on='Symbol', how='outer')
dfx = dfx.drop(columns=[0], axis=1)
dfx.rename(columns = {1: 'previous_split_date'}, inplace = True)

dfx['previous_split_date'] = pd.to_datetime(dfx['previous_split_date'])
dfx['previous_split_date'] = dfx['previous_split_date'].apply(lambda x: x.strftime('%Y-%m-%d') if pd.notnull(x) else "")

dfx['ratio_from'] = dfx['Ratio'].apply(lambda x: x.split('-', 1))
dfx['ratio_to'] = dfx['ratio_from'].apply(lambda x: x[1] if len(x) == 2 else x)
dfx['ratio_from'] = dfx['ratio_from'].apply(lambda x: x[0])

# Save df as a csv
dfx.to_csv(settings['yfinance_splits'] + 'splits.csv', index=False)
missed_dates_df.to_csv(settings['yfinance_splits'] + 'splits_missed_dates.csv', index=False)
print('Done')
