import datetime as dt
import os
import re
from functools import partial
from multiprocessing import Pool, cpu_count

import pandas as pd
import pandas_datareader.data as web
import requests_cache
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

pd.set_option('display.max_columns', 100)
pd.set_option('display.width', 10000)

driver = None


def scrape_investing(df_stock, from_date, to_date, symbol):
    try:
        df = investpy.get_stock_historical_data(stock=df_stock.loc[symbol, 'Investing'], country='thailand', from_date=from_date, to_date=to_date, as_json=False, order='ascending')
        df.drop(labels='Currency', axis=1, inplace=True)
        df.sort_index(ascending=False, inplace=True)
        df.to_csv(r'data/investing/' + df_stock.loc[symbol, 'Filename'] + '.csv')
        return symbol
    except Exception as e:
        print('\nError scrape investing.com:', symbol, 'Error nessage:', str(e))
        return None


def scrape_jitta(df_stock, symbol):
    global driver

    try:
        if driver is None:
            driver = webdriver.Chrome(executable_path='chromedriver.exe')

            # Get logged-in cookies
            login_url = 'https://accounts.jitta.com/login'
            driver.get(login_url)
            driver.find_element_by_css_selector('input[name="email"]').send_keys('paroonk@hotmail.com')
            driver.find_element_by_css_selector('input[name="password"]').send_keys('bjm816438')
            driver.find_element_by_xpath('//button[text()="เข้าสู่ระบบ"]').click()
            wait = WebDriverWait(driver, 60)
            element = wait.until(EC.visibility_of_element_located((By.XPATH, '//input[@placeholder="Search on Jitta"]')))

        df = pd.DataFrame()

        # Get factsheet (annual)
        jitta_factsheet = 'https://www.jitta.com/stock/bkk:' + df_stock.loc[symbol, 'Jitta'].lower() + '/factsheet'
        driver.get(jitta_factsheet)
        wait = WebDriverWait(driver, 60)
        element = wait.until(EC.visibility_of_element_located((By.XPATH, '//div[contains(@class, "FactsheetTable__TableContainer")]')))
        get_data_to_df(driver, df)

        # Get factsheet (quarter)
        driver.find_element_by_xpath('//button[text()="QUARTER"]').click()
        wait = WebDriverWait(driver, 60)
        element = wait.until(EC.visibility_of_element_located((By.XPATH, '//div[contains(@class, "FactsheetTable__TableContainer")]')))
        get_data_to_df(driver, df)

        df.replace('- -', '', inplace=True)
        df.to_csv(r'data/jitta/' + df_stock.loc[symbol, 'Filename'] + '.csv')
        return symbol

    except Exception as e:
        print('\nError scrape jitta.com:', symbol, 'Error message:', str(e))
        return None


def get_data_to_df(driver, df):
    soup = BeautifulSoup(driver.page_source, features='lxml')
    table = soup.body.find('div', class_=re.compile('FactsheetTable__TableContainer'))
    div_list = [div for div in table]
    data_list = []
    index_list = []
    for i, div in enumerate(div_list):
        # Get column name
        if i == 0:
            column_list = [col.get_text() for col in div.div.findAll('div', recursive=False)[:-1]]
            column_list = list(filter(None, column_list))
            n_column = len(column_list)
        # Get value and label name
        else:
            for row in div.findAll('div', recursive=False):
                if re.compile('FactsheetTableRow__RowContainer').search(row.attrs['class'][0]) is not None:
                    row_data = [data.get_text() for data in row.div.findAll('div', recursive=False)]
                    data_list.append(row_data[:n_column])
                    index_list.append(row_data[-1])
    df[column_list] = pd.DataFrame(data_list, index=index_list)
    return df


def close_driver(i):
    global driver
    if driver is not None:
        driver.close()


if __name__ == "__main__":
    # Reformat SET Hist Data
    df_set = pd.read_csv('data/SET Index Historical Data.csv')
    if df_set.columns.values.tolist() == ['Date', 'Price', 'Open', 'High', 'Low', 'Vol.', 'Change %']:
        df_set.rename(columns={'Price': 'Close', 'Vol.': 'Volume'}, inplace=True)
        df_set = df_set.loc[:, ['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
        df_set.loc[:, 'Date'] = pd.to_datetime(df_set.loc[:, 'Date'])
        df_set.loc[:, 'Volume'] = df_set.loc[:, 'Volume'].str.replace('.', ',').str.replace('B', '0,000,000').str.replace('M', '0,000')
        df_set.to_csv('data/SET Index Historical Data.csv', index=False)

    # Prepare data folder and thread
    file_name = 'stock_list.xlsx'
    sheet_name = 'StockList'

    df_stock = pd.read_excel(file_name, sheet_name=sheet_name, index_col=0)
    df_stock.index = [str(text).upper() for text in df_stock.index]
    df_stock.loc[['TRUE'], ['Investing', 'Yahoo', 'Jitta', 'Filename']] = ['TRUE', 'TRUE', 'TRUE', 'TRUE']

    thread = 4
    thread = thread if thread < cpu_count() else cpu_count()
    pool = Pool(processes=thread)

    # Scrape investing.com
    symbol_list = df_stock.loc[investing_filter].index.tolist()
    from_date = '01/01/2000'
    to_date = (dt.datetime.now() - dt.timedelta(days=1)).strftime('%d/%m/%Y')

    try:
        if len(symbol_list) > 0:
            for symbol in tqdm(pool.imap_unordered(partial(scrape_yahoo, df_stock, start, end), symbol_list), total=len(symbol_list)):
                pass
    except Exception as e:
        print('\nError Yahoo Mainloop:', str(e))
    finally:
        print('\nFinished scrape finance.yahoo.com')

    # Scrape jitta.com
    symbol_list = df_stock.loc[jitta_filter].index.tolist()
    try:
        if len(symbol_list) > 0:
            for symbol in tqdm(pool.imap_unordered(partial(scrape_jitta, df_stock), symbol_list), total=len(symbol_list)):
                pass
    except Exception as e:
        print('\nError Jitta Mainloop:', str(e))
    finally:
        for _ in pool.imap_unordered(close_driver, range(thread)):
            pass
        print('\nFinished scrape jitta.com')

    print('\nFinished')

import pyfolio as pf
pf.create_returns_tear_sheet()