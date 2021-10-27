import os
import pandas as pd
from datetime import datetime as dt
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
from data_utils import *

# Load settings, launch browser and make directory
settings = utils_get_settings()
safe_make_dir(settings['ipo_yfinance_exchanges'])

# Load Market Calendar and loop through the dates
market_cal = pd.read_csv(settings['processed_path'] + 'market_calendar.csv')
# Slicing to only this year and next year dates
# market_cal = market_cal[(market_cal['year'] == dt.now().year) & (market_cal['month'] == dt.now().month)]
missed_dates = []

year = dt.today().year
year_list = list(range(year, year-10, -1))
month_list = list(range(1, 13))

for month in month_list:
    for year in year_list:

        market_cal_1 = market_cal[
            (market_cal['year'] == year)
            & (market_cal['month'] == month)
        ]

        browser = launch_firefox_browser('', DeleteContents=False)

        for marketdate in market_cal_1['market_date']:
            try:
                # Go to the url
                url = 'https://finance.yahoo.com/calendar/ipo?day=' + marketdate
                browser.get(url)
                # Find the table
                tbl = browser.find_element_by_xpath('/html/body/div[1]/div/div/div[1]/div/div[2]/div/div/div[7]/div/div/div/div').get_attribute('outerHTML')
                dfs = pd.read_html(tbl, header=0)
                # Save as csv
                df = dfs[0].to_csv(settings['ipo_yfinance_exchanges']+'yfinance_ipo_{}.csv'.format(marketdate), index=False)
                print('Scrapped', marketdate)
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
for ipofile in os.listdir(settings['ipo_yfinance_exchanges']):
    if ipofile.endswith('csv') & ipofile.startswith('yfinance'):
        print('appending', ipofile)
        df = df.append(pd.read_csv(settings['ipo_yfinance_exchanges'] + ipofile))
        # delete the appended csv
        # os.remove(settings['ipo_yfinance_exchanges'] + ipofile)

# Save df as a csv
df.to_csv(settings['ipo_yfinance_exchanges'] + 'ipo_exchanges.csv', index=False)
missed_dates_df.to_csv(settings['ipo_yfinance_exchanges'] + 'missed_dates.csv', index=False)

# Patching IPO exchanges to final IPO primary file
# Reading ipo exchanges into pandas and dropping Shares column
ipoex_df = pd.read_csv(settings['ipo_yfinance_exchanges'] + 'ipo_exchanges.csv')
ipoex_df = ipoex_df.drop(columns=['Shares'], axis=1)
# Reading final ipo pimary into pandas
ipoprimary_df = pd.read_csv(settings['ipo_boutique_final_primary'] + 'final_ipo_df_primary.csv')

print('Patching IPO Exchanges...')
# Merge ipo exchanges and ipo primary data frames
df = ipoprimary_df.merge(ipoex_df, how='left', left_on='SYMBOL', right_on='Symbol')
# Drop unwanted columns
df = df.drop(columns=['Symbol', 'Company', 'Date', 'Price Range', 'Price', 'Currency', 'Actions'], axis=1)
# Rename all nasdaq features as Q and NYSE as N
df = df.replace(['NasdaqGM','NasdaqGS', 'Nasdaq', 'NasdaqCM', 'NASDQ'], 'Q')
df = df.replace('NYSE', 'N')
# Save to the final ipo primary file
df.to_csv(settings['ipo_boutique_final_primary'] + 'final_ipo_df_primary.csv', index=False)

print('Patching IPO Exchanges Done')

