import os
import gc
import sys
import glob
import json
import shutil
import random
import dateutil
import requests
import pandas as pd
import numpy as np
from pprint import pprint
from datetime import datetime, date, timedelta
from time import sleep
import smtplib, imaplib, email
import platform

def log(s):
    print(s)
    # TODO: route to the tests's log file

def log_bar():
    log('='*80)
    sys.stdout.flush()

def log_tab(s):
    log('='*20+'>  '+s)
    sys.stdout.flush()

def log_banner(s):
    log('')
    log_bar()
    l = len(s)
    i = (80-l)//2
    w1 = i-1
    w2 = 80-1-i-l
    log('='+' '*w1+s+' '*w2+'=')
    log_bar()
    sys.stdout.flush()


def successful_exit(s):
    log_banner('SUCCESS - '+s)
    sys.stdout.flush()
    sys.exit()

def error_exit(title,s):
    log_banner('ERROR - '+title)
    log(s)
    sys.stdout.flush()
    sys.exit(-1)


def safe_make_dir(dir_path):
    if dir_path=='': return
    os.makedirs(dir_path,exist_ok=True)

def safe_delete_dir_contents(dir_path):
    fns = glob.glob(dir_path+'/*')
    #print(fns)
    for fn in fns:
        if os.path.isfile(fn):
            os.remove(fn)
        else:
            shutil.rmtree(fn)

def is_dir_empty(dir_path):
    fns = glob.glob(dir_path+'/*')
    return len(fns)==0


def utils_get_settings(): 
    
    todays_date = date.today()
    
    s = {}
    
    if is_mac():
        s['root_path'] =                    './'
        s['bod_data_repo'] =                './'
    else:
        s['root_path'] =                    'C:/marketdata/'
        s['bod_data_repo'] =                'C:/bod_data/'
        s['x_drive_datafeed'] =             'X:/datafeed/'

    s['raw_path'] =                         s['root_path']+'data/raw/'
    s['processed_path'] =                   s['root_path']+'data/processed/'
    s['bod_path'] =                         s['root_path']+'data/bod/'
    s['bod_github'] =                       s['root_path']+'data/bod_github/'
    s['bod_github_datafeed'] =              s['root_path']+'data/bod_github_datafeed/'
    s['temp_path'] =                        s['root_path']+'data/temp/'
    s['raw_quandl_path'] =                  s['raw_path']+'quandl_eod_historical/'
    s['market_calendar_path'] =             s['processed_path'] + 'market_calendar.csv'
    s['shs'] =                              s['raw_path']+'shortsqueeze/'
    s['quantum'] =                          s['raw_path']+'quantum/'
    s['thefly_ccalls'] =                    s['raw_path']+'conference_calls/'
    s['whs_historical_csv'] =               s['raw_path']+'whs/historical/csv/'
    s['whs_historical_json'] =              s['raw_path']+'whs/historical/json/'
    s['ipo_boutique_historical'] =          s['raw_path']+'ipo_boutique/historial/'
    s['ipo_boutique_secondary'] =           s['raw_path']+'ipo_boutique/secondary/'
    s['ipo_boutique_final_primary'] =       s['raw_path']+'ipo_boutique/final/primary/'
    s['ipo_boutique_final_secondary'] =     s['raw_path']+'ipo_boutique/final/secondary/'
    s['ipo_yfinance_exchanges'] =           s['raw_path']+'ipo_exchanges/'
    s['yfinance_splits'] =                  s['raw_path']+'stock_splits/'
    s['yfinance_econ_events'] =             s['raw_path']+'econ_events/'
    s['yfinance_data']  =                   s['raw_path']+'yfinance/'
    s['takion_path'] =                      s['raw_path']+'takion_exchange/'
    
    s['symbols_path'] =                     s['processed_path'] + 'symbols_all.csv'
    s['symbols_path2'] =                    s['processed_path'] + 'quandl_symbol_lists/XXX_symbols.csv'
    s['earnings_report_dates_path'] =       s['raw_path'] + 'earnings_report_dates.csv'

    s['start_date'] =                       '2015-01-01'
    s['end_date'] =                         todays_date + timedelta(days = 60)

    s['symbol_exclude_path'] =              s['raw_path']+'SymbolExcludeList.csv'

    s['briefing_user'] =                    "aesirtradingstrategies@gmail.com"
    s['briefing_pswd'] =                    "Briefing1"
    s['briefing_url_login'] =               "https://www.briefing.com/Login/subscriber.aspx"
    s['briefing_url_logout'] =              "https://www.briefing.com/Login/Logout.aspx"
    s['briefing_url_data_confcalls'] =      "https://www.briefing.com/InPlayEq/EarningsEvents/EarningsConferenceCalls.htm"
    s['briefing_url_data_earnings'] =       "https://www.briefing.com/DisplayArticle/ArchivePage.aspx?PageId=-8007&ArchiveDate="#+May-03-2018
    s['briefing_url_archive_news'] =        "https://www.briefing.com/InPlayEq/InPlay/InPlayViewArchive.htm?ArchiveDate="#+May-13-2020"
    s['briefing_url_live_news'] =           "https://www.briefing.com/InPlayEq/InPlay/InPlay.htm"
    s['thefly_url_ccalls'] =                "https://thefly.com//events.php?fecha="#+May-03-2021

    return s

def utils_create_dirs():
    s = utils_get_settings()
    safe_make_dir(s['raw_path'])
    safe_make_dir(s['processed_path'])
    safe_make_dir(s['bod_path'])
    safe_make_dir(s['temp_path'])
    safe_make_dir(s['raw_quandl_path'])

def utils_init():
    utils_create_dirs()

def kill_all_firefox():
    os.system('taskkill /f /im  firefox.exe')
    log_banner('FIREFOX PROCESSES KILLED')

def load_market_calendar():
    settings = utils_get_settings()
    df = pd.read_csv(settings['market_calendar_path'])
    rows = df.to_dict('records')
    rows.sort(key = lambda x:x['market_date'])
    return rows

def load_market_dates():
    rows = load_market_calendar()
    dates = [r['market_date'] for r in rows]
    return dates

def load_symbol_exclude_list():
    settings = utils_get_settings()
    df = pd.read_csv(settings['symbol_exclude_path'])
    df['symbol'] = [str(x).upper() for x in df['symbol']]
    rows = df.to_dict('records')
    rows.sort(key = lambda x:x['symbol'])
    exclude_list = [r['symbol'] for r in rows]
    print('-------- SYMBOL EXCLUDE LIST --------')
    print(exclude_list)
    return exclude_list

def load_ccall_date_times():
    settings = utils_get_settings()
    df = pd.read_csv(settings['tier2_ccall_date_path'],index_col=None)
    df = df.astype(str)
    df['symbol'] = [x.upper() for x in df['symbol']]
    rows = df.to_dict('records')
    return rows

def load_earning_report_dates():
    settings = utils_get_settings()
    rows = pd.read_csv(settings['earnings_report_dates_path'],index_col=False).to_dict('records')
    return rows


def briefing_login(browser=None, logout_other_sessions=True):
    settings = utils_get_settings()
    # Navigate to login page
    log("Logging in to Briefing")
    browser.get(settings['briefing_url_login'])


    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    print('user: (aesirtradingstrategies@gmail.com)')
    print('pwd:  (Briefing1)')
    print('!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!')
    input('===> LOG INTO BRIEFING AND THEN HIT ENTER <===')

    # Get entry fields
    # username_box = browser.find_element_by_id("username")
    # password_box = browser.find_element_by_id("password")
    # # Supply username & password
    # username_box.send_keys(settings['briefing_user'])
    # password_box.send_keys(settings['briefing_pswd'])
    # # Press submit button
    # #browser.find_element_by_id("_buttonLogin").click()
    # browser.find_element_by_type("submit").click()
    
    # it's showing this popup option for some reason?
    # maybe because of the trial?
    try:
        sleep(2)
        browser.find_element_by_id("_buttonLoginProductPick").click()
    except:
        print('_buttonLoginProductPick wasnt present!')

        
    # Confirm logging out other browsers (if necessary)
    if logout_other_sessions: # only 1 browser can access briefing at a time
        try:
            browser.find_element_by_id("_deleteLiveSessions").click()
            log("Logging out other sessions")
        except: pass

def briefing_logout(browser=None):
    settings = utils_get_settings()
    browser.get(settings['briefing_url_logout'])


    # safe_make_dir(settings['shs_historical'])

    # profile = webdriver.FirefoxProfile()
    # profile.set_preference(
    #     "browser.download.folderList",
    #     2)  # Not to use default Downloads directory

    # profile.set_preference(
    #     "browser.download.manager.showWhenStarting",
    #     False)  # Sets the directory for downloads

    # profile.set_preference(
    #     "browser.download.dir",
    #     '/Users/amin/Desktop/Sherpa/AesirData/data/raw/shortsqueeze_historical'
    #     )  # Automatically download the mentioned file types

    # profile.set_preference(
    #     "browser.helperApps.neverAsk.saveToDisk",
    #     "application/octet-stream,text/csv,application/csv,text/comma-separated-values,text/json,application/json,application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")

    # try:
    #     browser = webdriver.Firefox(
    #         executable_path=settings['root_path']+'scripts/geckodriver',
    #         firefox_profile=profile
    #         )  # Lunch Firefox using selenium.webdriver
    # except:
    #     browser = webdriver.Firefox(
    #         executable_path=settings['root_path']+'scripts/geckodriver.exe',
    #         firefox_profile=profile
    #         )  # Lunch Firefox using selenium.webdriver

def launch_firefox_browser(DownloadDir='',DeleteContents=True):

    print('launch_firefox_browser: (',DownloadDir,')')

    # firefox is super picky about this path
    if is_mac() is False:
        if DownloadDir != '':
            DownloadDir = DownloadDir.replace('/','\\')
            if (DownloadDir[-1] != '\\') and (DownloadDir[-1]!='/'):
                DownloadDir += '\\'
            print('setting download directory (',DownloadDir,')')
    else:
        DownloadDir = '/Users/amin/Desktop/AesirData/data/raw/shortsqueeze'
        
    if DeleteContents and (DownloadDir!=''):
        fns = glob.glob(DownloadDir+'*')
        for fn in fns:
            try:
                if os.path.isfile(fn): os.remove(fn)
            except:
                pass

    try:
        safe_make_dir(DownloadDir)

        from selenium import webdriver
        from selenium.webdriver.common.keys import Keys

        settings = utils_get_settings()

        profile = webdriver.FirefoxProfile()
        profile.set_preference('browser.download.manager.showWhenStarting', False)

        if DownloadDir != '':
            profile.set_preference('browser.download.dir', DownloadDir)
            profile.set_preference('browser.download.useDownloadDir', True)
            profile.set_preference('browser.download.folderList', 2) # custom location



        # for new filetype that keeps bringing up open\save option
        # got to website manually and download the file...tell the dialog to always save
        # Now in firefox, new tab, "about:support"
        # go to Application basics
        # find Profile Directory...open directory
        # open handlers.json
        # look for the new addition related to your file type
        # copy below:
        file_types = "application/download,application/octet-stream,text/csv,application/xlsx,application/csv,text/comma-separated-values,text/json,application/json,application/vnd.ms-excel,application/excel"
        file_types += ",application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        profile.set_preference('browser.helperApps.neverAsk.saveToDisk', file_types)    
        profile.set_preference('browser.helperApps.neverAsk.openFile', file_types)    


        try:
            browser = webdriver.Firefox(
                executable_path=settings['root_path']+'scripts/geckodriver.exe', # Windows
                firefox_profile=profile
                )  # Launch Firefox using selenium.webdriver
        except:
            browser = webdriver.Firefox(
                executable_path=settings['root_path']+'scripts/geckodriver', # MAC
                firefox_profile=profile
                )  # Launch Firefox using selenium.webdriver

        log('Browser Created!')
        log(browser.get_window_size())
    except:
        print('FAILED LAUNCHING FIREFOX BROWSER')
        sys.exit(-1)
        
    return browser

def is_mac():
    if platform.system() == "Darwin":
        return True
    else:
        return False