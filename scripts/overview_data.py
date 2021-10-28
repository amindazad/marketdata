import os
import time
import pandas as pd
import requests
import datetime as dt
from data_utils import *
from dotenv import load_dotenv

# define the countdown func.
def countdown(t):
    while t:
        mins, secs = divmod(t, 60)
        timer = '{:02d}:{:02d}'.format(mins, secs)
        print(timer, end="\r")
        time.sleep(1)
        t -= 1

# Load settings, launch browser and make directory
settings = utils_get_settings()
safe_make_dir(settings['alpha_advantage'])

# Credentials
load_dotenv()
API_KEY = os.getenv("ALPHA_ADVANTAGE_KEY")

symbols_all = pd.read_csv(settings['processed_path'] + 'symbols_all.csv')
symbol_list = symbols_all['symbol'].to_list()
symbol_chunks = [symbol_list[x:x+145] for x in range(0, len(symbol_list), 145)]

failed_symbols = []
for i in symbol_chunks:
    print('Waiting 1 minute to get over API call limit')
    countdown(60)
    for symbol in i:
        url = 'https://www.alphavantage.co/query?function=OVERVIEW&symbol={}&apikey={}'.format(symbol, API_KEY)
        r = requests.get(url)
        data = r.json()
        df = pd.DataFrame.from_dict(pd.json_normalize(data), orient='columns')
        if df.empty:
            print('--> No data for', symbol)
            failed_symbols.append(symbol)
        else:        
            df.to_csv(settings['alpha_advantage'] + 'overview_{}.csv'.format(symbol), index = False)
            print('saved', symbol, 'data')

    df_failed = pd.DataFrame(failed_symbols)
    df_failed.to_csv(settings['alpha_advantage'] + 'failed_symbols.csv', index = False)

# Create a dataframe to append all csvs
df = pd.DataFrame()
# Loop through csv files and append them to df
for overviewfile in os.listdir(settings['alpha_advantage']):
    if overviewfile.endswith('csv') & overviewfile.startswith('overview'):
        if os.stat(settings['alpha_advantage'] + overviewfile).st_size > 250:
            df = df.append(pd.read_csv(settings['alpha_advantage'] + overviewfile))
            print(os.stat(settings['alpha_advantage'] + overviewfile).st_size)
            # delete the appended csv
            # os.remove(settings['yfinance_splits'] + ipofile)
# Save df as a csv
df.sort_values(by=['Symbol'])
df.to_csv(settings['alpha_advantage'] + 'overview_all_{}.csv'.format(str(dt.date.today())), index=False)