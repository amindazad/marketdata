import os
import glob
import pandas as pd
from datetime import datetime as dt 
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support.ui import Select
from dotenv import load_dotenv
from data_utils import *

def ipo_boutique_data():
    """
    Returns: Saved IPO data CSV files from 
    ipoboutique.com
    File Directory: data/raw/ipoboutique
    """
    # Load directory settings from data_utils
    settings = utils_get_settings()
    # Safe make directories
    safe_make_dir(settings['ipo_boutique_historical'])
    safe_make_dir(settings['ipo_boutique_secondary'])
    # Launch firefox for data scrapping
    browser = launch_firefox_browser(
        settings['ipo_boutique_historical'], DeleteContents=False)
    # Define years to loop through
    year = dt.today().year
    ipo_years = list(range(year, year-8, -1))
    # Scrapping primary issue data for ipo_years
    for ipo_year in ipo_years:
        browser.get(
            'http://www.ipoboutique.com/cgi/pricings.php?year={}'.format(ipo_year))
        tbl = browser.find_element_by_xpath(
            "/html/body/div[2]/table").get_attribute('outerHTML')
        dfs = pd.read_html(tbl, header=0)
        df = dfs[0].to_csv(settings['ipo_boutique_historical'] +
                           'ipo_boutique_{}.csv'.format(ipo_year), index=False)
        
        print('--> Saved {} IPO Primary Data'.format(ipo_year))
    # Scrapping Secondary issue data for ipo years
    for ipo_secondary_year in ipo_years:
        browser.get(
            'http://www.ipoboutique.com/cgi/secondaries-stock-track-record.php?y={}'.format(ipo_secondary_year))
        tbl = browser.find_element_by_xpath(
            "/html/body/div[2]/div[4]/table").get_attribute('outerHTML')
        ipo_df = pd.read_html(tbl, header=0)
        ipo_df[0].to_csv(settings['ipo_boutique_secondary'] +
                         'ipo_boutique_secondary_{}.csv'.format(ipo_secondary_year), index=False)

        ipo_df = pd.read_csv(settings['ipo_boutique_secondary'] +
                         'ipo_boutique_secondary_{}.csv'.format(ipo_secondary_year))

        # Lower column names
        ipo_df.columns = ipo_df.columns.str.lower()
        # Standardize column names
        ipo_df.columns = ipo_df.columns.str.replace(".", " ", regex=False)
        ipo_df.columns = ipo_df.columns.str.replace("%", "pct")
        ipo_df.columns = ipo_df.columns.str.replace(" ", "_")
        ipo_df = ipo_df.rename(columns={'pct\xa0gainissue\xa0vs_open': 'pct_gainissue_vs_open',
                                        'pct\xa0gainissue\xa0vs_high': 'pct_gainissue_vs_high'})
        # Convert to datetime format
        ipo_df['issued'] = ipo_df['issued'].astype('str')
        ipo_df['issued'] = ipo_df['issued'].apply(
            lambda x: x+'/{}'.format(ipo_secondary_year))
        ipo_df['issued'] = pd.to_datetime(ipo_df['issued'])
        # Clean cell contents
        ipo_df['pct_gainissue_vs_open'] = ipo_df['pct_gainissue_vs_open'].astype(
            'str')
        ipo_df['pct_gainissue_vs_open'] = ipo_df['pct_gainissue_vs_open'].apply(
            lambda x: float(x.strip('%')))

        ipo_df['pct_gainissue_vs_high'] = ipo_df['pct_gainissue_vs_high'].astype(
            'str')
        ipo_df['pct_gainissue_vs_high'] = ipo_df['pct_gainissue_vs_high'].apply(
            lambda x: float(x.strip('%')))
        # Save as csv        
        ipo_df.to_csv(settings['ipo_boutique_secondary'] +
                         'ipo_boutique_secondary_{}.csv'.format(ipo_secondary_year), index=False)

        print('--> Saved {} IPO Secondary Data'.format(ipo_secondary_year))

    browser.close()

def ipo_final_files():
    """
    Concatinate ipo files into one final file
    """
    # Load settings from data_utils
    settings = utils_get_settings()
    
    # Make ipo_boutique_final folder
    safe_make_dir(settings['ipo_boutique_final_primary'])
    safe_make_dir(settings['ipo_boutique_final_secondary'])

    # Loop through all primary files and make a single final file
    all_files_primary = glob.glob(settings['ipo_boutique_historical'] + "/*.csv")
    li_primary = []
    for filename in all_files_primary:
        df = pd.read_csv(filename, index_col=None, header=0)
        li_primary.append(df)

    frame_primary = pd.concat(li_primary, axis=0, ignore_index=True)

    frame_primary.to_csv(settings['ipo_boutique_final_primary'] +
                    "final_ipo_df_primary.csv", index=False)

    # Loop through all secondary files and make a single final file
    all_files_sec = glob.glob(settings['ipo_boutique_secondary'] + "/*.csv")
    li_sec = []
    for filename in all_files_sec:
        df = pd.read_csv(filename, index_col=None, header=0)
        li_sec.append(df)

    frame_sec = pd.concat(li_sec, axis=0, ignore_index=True)

    frame_sec.to_csv(settings['ipo_boutique_final_secondary'] +
                    "final_ipo_df_secondary.csv", index=False)

print('--> IPO Boutique Data Srcapping Started')
ipo_boutique_data()
ipo_final_files()
print('--> IPO Boutique Data Srcapping Done!!!')
