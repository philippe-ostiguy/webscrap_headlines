#!/usr/local/bin/python3.7
# -*- coding: utf-8 -*-
###############################################################################
#
#  The MIT License (MIT)
#  Copyright (c) 2021 Philippe Ostiguy
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
#  EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
#  MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
#  IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
#  DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR
#  OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE
#  OR OTHER DEALINGS IN THE SOFTWARE.
###############################################################################

"""It's the module for web scraping. The module uses different sources and APIs for web-scraping"""

import os
import requests
from datetime import datetime
from dateutil.relativedelta import relativedelta
from pathlib import Path
import json
from decouple import config
from langdetect import detect
import time
import sqlite3
import bs4 as bs

def delta_date(start_date,end_date):
    """Function that returns the number of days between 2 dates """

    return abs((datetime.strptime(start_date, "%Y-%m-%d") - datetime.strptime(end_date, "%Y-%m-%d")).days)

def get_tickers():
    """Method that gets the stock symbols from companies listed in the S&P 500

    Return
    ------
    `tickers` : list
        S&P 500 company symbols
    """
    resp = requests.get('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find_all('table')[0]  # Grab the first table

    tickers = []
    for row in table.findAll('tr')[1:]:
        ticker = row.findAll('td')[0].text.strip('\n')
        tickers.append(ticker)

    return tickers

class Init():
    """Class that initializes global value for the module. It also use general method to initialize value.
     """

    def __init__(self):
        """Built-in method to inialize the global values for the module

        Attributes
        -----------
        `self.start.date` : str
            start date of the training period. Must be within the last year for the free version of FinHub. Format
            must be "YYYY-mm-dd"
        `self.end_date` : str
            end date of the training period. Format must be "YYYY-mm-dd"
        `self.ticker` : list
            tickers on which we want to perform the test. Can be one ticker in form of a list as well as a list
            of tickers like the s&p 500.
        `self.db_name` : str
            name of the sqlite3 database
        `self.dir_path` : str
            directory where the data are saved. It takes into account the `self.start_date` and `self.end_date`
        `self.start_date_` : datetime object
            same thing as `start_date` but as a datetime object
        `self.end_date_` : datetime object
            same thing as `start_date` but as a datetime object
        """

        #initialize value here
        self.start_date = "2020-10-22"
        self.end_date = "2021-02-22"
        self.tickers = ['AMZN']


        self.db_name = 'financial_data'
        self.dir_path = os.path.dirname(os.path.realpath(__file__)) + '/output/' + self.start_date + '_' + \
                        self.end_date + '/'
        Path(self.dir_path).mkdir(parents=True, exist_ok=True) #create new path if it doesn't exist
        self.start_date_ = datetime.strptime(self.start_date, "%Y-%m-%d")  #datetime object
        self.end_date_ = datetime.strptime(self.end_date, "%Y-%m-%d")    #datetime object
        self.delta_date = abs((self.end_date_ - self.start_date_).days) #number of days between 2 dates

        try:
            self.start_date_ > self.end_date_
        except:
            print("'start_date' is after 'end_date'")

        t = (datetime.now()- relativedelta(years=1))
        d= datetime.strptime(self.start_date, "%Y-%m-%d")

        if (datetime.strptime(self.start_date, "%Y-%m-%d") <= (datetime.now()- relativedelta(years=1))) :
            raise Exception("'start_date' is older than 1 year. It doesn't work with the free version of FinHub")

class FinnHub():
    """Class to make API calls to FinnHub"""

    def __init__(self,start_date,end_date,start_date_,end_date_,tickers,dir_path,db_name):
        """ Class constructor

        Parameters
        ----------
        `start_date` : str
            Start date of the request. Must be within 1 year from now for must request
            with the free version of FinHub
        `end_date` : str
            End date of the request.
        `start_date_` : datetime object
            Same thing as `start_date` but as a datetime object
        `end_date_` : datetime object
             Same thing as `start_date` but as a datetime object
        `ticker` : str
            Ticker symbol
        `db_name` : str
            Name of the sqlite database
        `dir_path` : str
            Directory  where our data will be stored

        Attributes
        ----------
        `self.max_call` : int
            maximum api calls per minute for the finhub API
        `self.time_sleep` : int
            seconds to sleep before making a new API call. Default is 60 seconds as the maximum number of API calls is
            per minute
        `self.nb_request` : int
            nb of request made so far. Set to 0 in constructor `__init__` as we may loop through ticker
            and want to avoid the variable to reset to 0 when exiting the wrapper `iterate_day()` (which could generate
            an error)
        `self.finhub_key` : str
            finhub unique API key. Get yours here : https://finnhub.io/
        `self.db_name : str
            default file name for the sql database
        """

        #Initialize attributes values here
        self.max_call = 60
        self.time_sleep = 60
        self.nb_request = 0
        self.finhub_key = config('FINHUB_KEY')
        self.news_header = ['category', 'datetime','headline','id','image','related','source','summary','url']
        self.start_date = start_date
        self.end_date = end_date
        self.tickers = tickers
        self.ticker_request = tickers #different value because ticker like 'ALL' (All State) can generate error in SQLite
                                    #database
        self.dir_path = dir_path
        self.db_name = db_name
        self.js_data = []

        self.start_date_ = start_date_ #datetime object
        self.end_date_ = end_date_ #datetime object

        #call the methods to access historical financial headlines
        #tickers = get_tickers() #get_tickers is to get tickers from all the companies listedin the s&p 500

        for ticker_ in self.tickers:
            self.js_data.clear()
            self.ticker = ticker_ + '_'
            self.ticker_request = ticker_
            self.req_new()
            self.create_table()
            self.clean_table()
            self.lang_review()

    def init_sql(func):
        """ Decorator that open the sql database, save it and close it. The operation are between the opening and
        saving of the file"""

        def wrapper_(self):
            conn_ = sqlite3.connect(self.dir_path + self.db_name + '.db')
            c = conn_.cursor()
            func(self,conn_,c)
            conn_.commit()
            conn_.close()
        return wrapper_

    @init_sql
    def clean_table(self,conn_,c):
        """Method that clean the database using sqlite3

        Parameters
        ----------
        `conn_` : database object
            Connection object that represents the database
        `c` : database object
            Cursor object
        """

        #remove NULL entry (row) from headline column
        c.execute(f" DELETE FROM {self.ticker} WHERE {self.news_header[2]} IS NULL OR "
                  f"trim({self.news_header[2]}) = '';")
        # remove NULL value from datetime
        c.execute(f" DELETE FROM {self.ticker} WHERE {self.news_header[1]} IS NULL OR "
                  f"trim({self.news_header[1]}) = '';")

        #removes duplicate entries (row)
        c.execute(f" DELETE FROM {self.ticker} WHERE rowid NOT IN (select MIN(rowid)"
                  f"FROM {self.ticker} GROUP BY {self.news_header[2]})")

    @init_sql
    def create_table(self,conn_,c):
        """ Method that creates a table in SQLite database. It creates the table  in `self.dir_path` and write
        the data in it

        Parameters
        ----------
        `conn_` : database object
            Connection object that represents the database
        `c` : database object
            Cursor object
        """

        #create table if it does not exist
        c.execute(f'drop table if exists {self.ticker}')
        conn_.commit()
        c.execute(f"CREATE TABLE IF NOT EXISTS {self.ticker} ({self.news_header[0]})")
        conn_.commit()

        #add columns to the table if the columns don't exist
        for header_ in range(len(self.news_header)-1):
            c.execute(f"alter table {self.ticker} add column '%s' " % self.news_header[header_+1])
            conn_.commit()

        iteration = 0
        for data_ in self.js_data:
            iteration +=1
            try :
                c.execute(f'insert into {self.ticker} values (?,?,?,?,?,?,?,?,?)',[data_[self.news_header[0]],
                          data_[self.news_header[1]],data_[self.news_header[2]],data_[self.news_header[3]],
                        data_[self.news_header[4]],data_[self.news_header[5]],data_[self.news_header[6]],
                          data_[self.news_header[7]],data_[self.news_header[8]]])
            except:
                print(f"Error at the {iteration}th ieration")

            conn_.commit()

    def iterate_day(func):
        """ Decorator that makes the API call on FinHub each days between the `self.start_date`
        and `self.end_date` """

        def wrapper_(self):
            delta_date_ = delta_date(self.start_date,self.end_date)
            date_ = self.start_date
            date_obj = self.start_date_

            for item in range(delta_date_ + 1):
                self.nb_request +=1
                func(self,date_)
                date_obj = date_obj + relativedelta(days=1)
                date_  = date_obj.strftime("%Y-%m-%d")
                if self.nb_request == (self.max_call-1):
                    time.sleep(self.time_sleep)
                    self.nb_request=0
        return wrapper_

    @init_sql
    def lang_review(self,conn_,c):
        """ Methods that delete non-english entries based on the 'headline' column in a SQLlite3 db

        Parameters
        ----------
        `conn_` : database object
            Connection object that represents the database
        `c` : database object
            Cursor object
        """

        list_ = []
        c.execute(f" SELECT {self.news_header[2]} FROM {self.ticker}")

        #check for non-english headlines
        for item_ in c:
            if detect(item_[0]) != 'en':
                list_.append(item_[0])

        #delete non-english entries (rows)
            query = f"DELETE FROM {self.ticker} where {self.news_header[2]} in ({','.join(['?']*len(list_))})"
            c.execute(query, list_)

    @iterate_day
    def req_new(self,date_):
        """ Method that makes news request(s) to the Finnhub API"""

        request_ = requests.get('https://finnhub.io/api/v1/company-news?symbol=' + self.ticker_request + '&from=' +
                                date_ + '&to=' + date_ + '&token=' + self.finhub_key)
        self.js_data += request_.json()


init_ = Init()

finhub = FinnHub(start_date=init_.start_date, end_date=init_.end_date,start_date_=init_.start_date_ ,
                end_date_ =init_.end_date_, tickers=init_.tickers, dir_path =init_.dir_path,db_name=init_.db_name)