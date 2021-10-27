import pandas as pd
import numpy as np
import datetime as dt
import yfinance 
from data_utils import *
from datetime import datetime as dt

# Load settings, launch browser and make directory
settings = utils_get_settings()

symbols_all = pd.read_csv(settings['processed_path'] + 'symbols_all.csv')
symbol_list = symbols_all['symbol'].to_list()

data = {'symbol' : [],
        # 'ex' : [],
        'float_shares' : [],
        'outstanding_shares' : []
        # 'shares_out_pct' : [],
        # 'institutions_pct' : [],
        # 'shares_short' : [],
        # 'short_interest':[],
        # 'short_last_month' : [],
        # 'pct_short_float': [],
        # 'market_cap' : [],
        # 'book_value' : [],
        # 'sector' : [],
        # 'industry':[],
        # 'last_split' : [],
        # 'last_split_ratio' : [],
        # 'open_interest':[],
        # 'payout_ratio':[],
        # 'net_income_to_common' : [],
        # 'high_52w' : [],
        # 'low_52w' : [],
        # 'volume_24hr': [],
        # 'volume_10d' : [],
        # 'volume_regular_market' : [],
        # 'total_assets':[],
        # 'PE_trailing' : [],
        # 'avg_dividend_yield_5y' : [],
        # 'risk_rating' : [],
        # 'overal_rating': []
        }

failed_symbols = []

for symbol in symbol_list:
  print('trying', symbol)
  try:
      ticker = yfinance.Ticker(symbol)
      data['symbol'].append(symbol)
      # data['ex'].append(ticker.info['exchange'])
      data['float_shares'].append(ticker.info['floatShares'])
      data['outstanding_shares'].append(ticker.info['sharesOutstanding'])
      # data['shares_out_pct'].append(ticker.info['sharesPercentSharesOut'])
      # data['institutions_pct'].append(ticker.info['heldPercentInstitutions'])
      # data['shares_short'].append(ticker.info['sharesShort'])
      # data['short_interest'].append(ticker.info['dateShortInterest'])
      # data['pct_short_float'].append(ticker.info['shortPercentOfFloat'])
      # data['short_last_month'].append(ticker.info['sharesShortPriorMonth'])
      # data['market_cap'].append(ticker.info['marketCap'])
      # data['book_value'].append(ticker.info['bookValue'])
      # data['sector'].append(ticker.info['sector'])
      # data['industry'].append(ticker.info['industry'])
      # data['last_split'].append(ticker.info['lastSplitDate'])
      # data['last_split_ratio'].append(ticker.info['lastSplitFactor'])
      # data['open_interest'].append(ticker.info['openInterest'])
      # data['payout_ratio'].append(ticker.info['payoutRatio'])
      # data['net_income_to_common'].append(ticker.info['netIncomeToCommon'])
      # data['high_52w'].append(ticker.info['fiftyTwoWeekHigh'])
      # data['low_52w'].append(ticker.info['fiftyTwoWeekLow'])
      # data['volume_24hr'].append(ticker.info['volume24Hr'])
      # data['volume_10d'].append(ticker.info['averageVolume10days'])
      # data['volume_regular_market'].append(ticker.info['regularMarketVolume'])
      # data['total_assets'].append(ticker.info['totalAssets'])
      # data['PE_trailing'].append(ticker.info['trailingPE'])
      # data['avg_dividend_yield_5y'].append(ticker.info['fiveYearAvgDividendYield'])
      # data['risk_rating'].append(ticker.info['morningStarRiskRating'])
      # data['overal_rating'].append(ticker.info['morningStarOverallRating'])

      print('Pulled', symbol, 'data')
  except:
    failed_symbols.append(symbol)
    pass

df = pd.DataFrame(data)
df.to_csv(
    settings['yfinance_data'] 
    + 'yfinance_{}.csv'.format(
        str(dt.date.today())
    ), index=False
)

df_failed = pd.DataFrame(failed_symbols)
df_failed.to_csv(settings['yfinance_data'] 
    + 'failed_{}.csv'.format(
        str(dt.date.today())
    ), index=False)