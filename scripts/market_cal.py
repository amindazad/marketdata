import sys
import math
import pandas as pd
from datetime import time
from pytz import timezone
from pprint import pprint
from datetime import datetime
import pandas_market_calendars as mcal
from data_utils import *

log_banner('GENERATE MARKET CALENDAR')
utils_create_dirs()
settings = utils_get_settings()
newyork_timezone = timezone("US/Eastern")
nyse = mcal.get_calendar('NYSE')


# * Each market will close early at 1:00 p.m. (1:15 p.m. for eligible options) on Monday, July 3, 2023. 
# Crossing Session orders will be accepted beginning at 1:00 p.m. for continuous executions 
# until 1:30 p.m. on these dates, and NYSE American Equities, NYSE Arca Equities, NYSE Chicago, 
# and NYSE National late trading sessions will close at 5:00 pm. All times are Eastern Time.

# ** Each market will close early at 1:00 p.m. (1:15 p.m. for eligible options) on Friday, November 26, 2021, 
# Friday, November 25, 2022, and Friday, November 24, 2023 (the day after Thanksgiving). 
# Crossing Session orders will be accepted beginning at 1:00 p.m. for continuous executions until 1:30 p.m. 
# on these dates, and NYSE American Equities, NYSE Arca Equities, NYSE Chicago, and NYSE National late 
# trading sessions will close at 5:00 pm. All times are Eastern Time.

half_days = ['2020-11-27','2020-12-24','2021-11-26','2022-11-25','2023-11-24','2023-07-03',]

#holidays = nyse.holidays()

# https://www.nyse.com/markets/hours-calendars
holidays = ['2020-01-01','2020-01-20','2020-02-17','2020-04-10',
            '2020-05-25','2020-07-03','2020-09-07','2020-11-26',
            '2020-12-25',
    
            '2021-01-01','2021-01-18','2021-02-15','2021-04-02',
            '2021-05-31','2021-07-05','2021-09-06','2021-11-25',
            '2021-12-24',
            
            '2022-01-17','2022-02-21','2022-04-15','2022-05-30',
            '2022-07-04','2022-09-05','2022-11-24','2022-12-26',
            
            '2023-01-02','2023-01-16','2023-02-20','2023-04-07',
            '2023-05-29','2023-07-04','2023-09-04','2023-11-12',
            '2023-12-25']
            
#==============================================================================
#==============================================================================
# GENERATE LIST OF OPTIONS EXPIRATION DATES FOR KTG LIBRARY
#==============================================================================
#==============================================================================
# if True:
#     rows = nyse.schedule(start_date='2015-01-01', end_date='2030-01-01').to_dict('records')
#     market_dates = [r['market_open'].astimezone(newyork_timezone).strftime('%Y-%m-%d') for r in rows]    
#     options_days = []
#     for year in range(2015,2030):
#         for month in range(1,13):
#             options_expiration_day = 0
#             # get all fridays in this month
#             fridays = []
#             first_day_dt = datetime(year,month,1)
#             for dday in range(0,32):
#                 day_dt = first_day_dt + timedelta(days=dday)
#                 if(day_dt.weekday()==4): fridays.append(day_dt)
#             # check if market is open on 3rd friday 
#             friday_3rd_date_str = fridays[2].strftime('%Y-%m-%d')
#             if(friday_3rd_date_str in market_dates):
#                 options_days.append(fridays[2].strftime('%Y-%m-%d'))
#             else:
#                 # the market is not open that friday.  use thursday.
#                 thursday = (fridays[2] - timedelta(days=1))
#                 options_days.append(thursday.strftime('%Y-%m-%d'))
#     pprint(options_days)

# sys.exit()
#==============================================================================                    
#==============================================================================
#==============================================================================

rows = nyse.schedule(start_date=settings['start_date'], end_date=settings['end_date']).to_dict('records')
#pprint(rows)
#sys.exit()

dates_market_is_open = [r['market_open'].astimezone(newyork_timezone).strftime('%Y-%m-%d') for r in rows]

# calculate all the basics
for ir,r in enumerate(rows):
    ny_market_open = r['market_open'].astimezone(newyork_timezone)
    ny_market_close = r['market_close'].astimezone(newyork_timezone)
    ny_midnight = newyork_timezone.localize(datetime.combine(ny_market_open.date(), time(0, 0)), is_dst=None)

    r['market_date'] = ny_market_open.strftime('%Y-%m-%d')
    r['market_open'] = ny_market_open.strftime('%H:%M:%S')
    r['market_close'] = ny_market_close.strftime('%H:%M:%S')
    r['market_date_ts'] = ny_midnight.timestamp()
    r['market_open_ts'] = ny_market_open.timestamp()
    r['market_close_ts'] = ny_market_close.timestamp()

    r['day'] = ny_market_open.day
    r['month'] = ny_market_open.month
    r['year'] = ny_market_open.year
    r['day_of_week_str'] = ny_market_open.strftime('%A')
    r['day_of_week'] = ny_market_open.weekday()    # 0-Mon, 4-Fri
    r['half_day'] = int((r['market_close_ts']-r['market_open_ts']) < (4*3600))

# fill out special situations
for ir,r in enumerate(rows):
    # decide if this is earnings season
    if True:
        m = r['month']
        d = r['day']
        r['is_earnings_season'] =(int)((m== 1 and d>15) or (m== 2 and d<15) or
                                       (m== 4 and d>15) or (m== 5 and d<15) or
                                       (m== 7 and d>15) or (m== 8 and d<15) or
                                       (m==10 and d>15) or (m==11 and d<15));    
    # decide if this is last trading day of the month
    if True:
        r['is_last_trading_of_month'] = 0
        if(ir<len(rows)-1):
            if rows[ir+1]['month'] != rows[ir]['month']:
                r['is_last_trading_of_month'] = 1
    # decide if this is an options expiration friday
    if True:
        r['is_options_expiration'] = 0

        # get all fridays in this month
        fridays = []
        first_day_dt = datetime(r['year'],r['month'],1)
        for dday in range(0,32):
            day_dt = first_day_dt + timedelta(days=dday)
            if(day_dt.weekday()==4): fridays.append(day_dt)
        # check if market is open on 3rd friday 
        friday_3rd_date_str = fridays[2].strftime('%Y-%m-%d')
        if(friday_3rd_date_str in dates_market_is_open):
            # is this date that friday?
            if(r['market_date'] == friday_3rd_date_str):
                r['is_options_expiration'] = 1
        else:
            # the market is not open that friday.  use thursday.
            thursday_date_str = (fridays[2] - timedelta(days=1)).strftime('%Y-%m-%d')
            if(r['market_date'] == thursday_date_str):
                r['is_options_expiration'] = 1

    #r['first_day'] = first_day_dt
    #r['last_day_of_first_week'] = first_day_dt.day + (7 - (1 + first_day_dt.weekday()))
    pprint(r)
    #if(r['day_of_week']==)
    #r['week_of_month'] = (ny_market_open.isocalendar()[1] - ny_market_open.replace(day=1).isocalendar()[1] + 1)
    #last_day_of_week_of_month = ny_market_open.day + (7 - (1 + ny_market_open.weekday()))
    #r['week_of_month'] =  int(math.ceil(last_day_of_week_of_month/7.0))
    #print(r['market_day'],(r['market_open_ts'] - r['market_day_ts'])/3600,(r['market_close_ts'] - r['market_day_ts'])/3600)

print('Saving Market Calendar')
df = pd.DataFrame(rows)
df.to_csv(settings['market_calendar_path'],index=False)

# create market directories
print('Creating market date directories')
market_dates = load_market_dates()
print(market_dates[:5])
print(market_dates[-5:])
#for d in market_dates:

    #directory = settings['processed_path']+d
    #if not os.path.exists(directory):
        #os.makedirs(directory)
        
    #directory = settings['bod_path']+d
    #if not os.path.exists(directory):
        #os.makedirs(directory)

successful_exit('Market Calendar Generated')