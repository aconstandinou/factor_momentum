# -*- coding: utf-8 -*-
"""
Created on Sat Oct 27 11:14:49 2018

@author: antonio constandinou
"""


import psycopg2
import datetime
import seaborn
import pandas as pd
import os
import functools
import matplotlib.pyplot as plt

import common_methods as cm

def backtest_momentum(ticker_dict_by_year, conn):
    """
    return a dictionary where each key is our year, value is list of average returns of stocks
    args:
        ticker_dict: dictionary of keys [year] and values [list of tickers to hold in portfolio]
        conn: a Postgres DB connection object
    returns:
        dictionary where each key is our year, value is list of average returns of stocks
    """    
    annual_collector = {}
    
    for key, value in ticker_dict_by_year.items():
        # find the last trading day for our years range
        year_start = key - 1
        year = key
        print("Working on {} momentum portfolio".format(year))
        last_tr_day_start = cm.fetch_last_day_mth(year_start, conn)
        last_tr_day_end = cm.fetch_last_day_mth(year, conn)
        mth = 12
         
        trd_start_dt = datetime.date(year_start,mth,last_tr_day_start)
        trd_end_dt = datetime.date(year,mth,last_tr_day_end)
        # need to convert list of tickers to tuple of tickers
        tuple_ticker_values = tuple(value)
        year_data = cm.load_df_stock_data_array(tuple_ticker_values, trd_start_dt, trd_end_dt, conn)
        
        
        for ticker_data in year_data:
            ticker = ticker_data.columns[1]
            # annual return
            """DEBUG THIS HERE"""
            annual_return = [(ticker_data[ticker].iloc[-1] - ticker_data[ticker].iloc[0]) / ticker_data[ticker].iloc[0]]
            print('Ticker {} annual return {}'.format(ticker, annual_return))

            if year not in annual_collector:
                annual_collector[year] = annual_return
            else:
                annual_collector[year] = annual_collector[year] + annual_return
    
    return annual_collector


def main():
    # main function to isolate pairs
    skip_etfs = True
    db_credential_info_p = "\\" + "database_info.txt"
    
    # create our instance variables for host, username, password and database name
    db_host, db_user, db_password, db_name = cm.load_db_credential_info(db_credential_info_p)
    conn = psycopg2.connect(host=db_host,database=db_name, user=db_user, password=db_password)
    
    # original
    year_array = list(range(2004, 2015))
    # year_array = list(range(2004, 2006))
    # collect each year's stocks to hold. key = year, values = list of tickers
    ticker_dict_by_year = {}
    
    for yr in year_array:
        # create a pairs file for each one year chunk in our range
        year = yr
        end_year = year + 1
        # find the last trading day for our years range
        last_tr_day_start = cm.fetch_last_day_mth(year, conn)
        last_tr_day_end = cm.fetch_last_day_mth(end_year, conn)
        
        # date range to pull data from
        start_dt = datetime.date(year,12,last_tr_day_start)
        end_dt = datetime.date(end_year,12,last_tr_day_end)
        start_dt_str = start_dt.strftime("%Y%m%d")
        end_dt_str = end_dt.strftime("%Y%m%d")
        
        # list of stocks and their sector
        list_of_stocks = cm.load_db_tickers_sectors(start_dt, conn)
        # dict: key = sector with values = array of all tickers pertaining to a sector
        sector_dict = cm.build_dict_of_arrays(list_of_stocks)
        
        for sector, ticker_arr in sector_dict.items():
            if skip_etfs and sector != "ETF":
                # for next_year's portfolio
                next_year = end_year + 1
                data_array_of_dfs = cm.load_df_stock_data_array(ticker_arr, start_dt, end_dt, conn)
                merged_data = cm.data_array_merge(data_array_of_dfs)
                return_data_series = (merged_data.iloc[-1] - merged_data.iloc[0]) / merged_data.iloc[0]
                top_five = return_data_series.nlargest(5).index.tolist()
                if next_year not in ticker_dict_by_year:
                    ticker_dict_by_year[next_year] = top_five
                else:
                    ticker_dict_by_year[next_year] = ticker_dict_by_year[next_year] + top_five
                print("Done {}: {}".format(end_year, sector))
    
    # annual returns of all stocks per year
    portfolio_performance = backtest_momentum(ticker_dict_by_year, conn)
    # file name to output
    f_name = "factor_momentum_annual_results" + ".txt"
    # let's start outputting data
    file_to_write = open(f_name, 'w')
    
    for year, returns_arr in portfolio_performance.items():
        str_rtns_list = ','.join(str(e) for e in returns_arr)
        file_to_write.write('{},{}\n'.format(year,str_rtns_list))
            
    print("LETS CHECK PERFORMANCE")
    
if __name__ == "__main__":
    main()