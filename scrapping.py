import urllib.request, urllib.parse, urllib.error
from bs4 import BeautifulSoup
import re
import string
from urllib.request import urlopen
import pandas as pd
from fuzzywuzzy import fuzz

class get_dataset:

    def dataset(season, mode):
        url = f'https://www.basketball-reference.com/leagues/NBA_{season}_{mode}.html'
        table_web = BeautifulSoup(urlopen(url), 'html.parser').findAll('table')

        df = pd.read_html(str(table_html))[0]
        df = df.drop(df[df.Player == 'Player'].index) # drop row contains table header
        df = df.drop('Rk', axis=1) # drop Rk columns
        df.Player = df.Player.str.replace('*','',regex=True) # remove asterisk on player's name
        df.insert(0,'Season',season) # insert season column
        df = df.apply(pd.to_numeric, errors='coerce').fillna(df) # convert non string values to numeric