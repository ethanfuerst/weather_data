#%%
import pandas as pd
import datetime as dt
from datetime import date, timedelta, datetime
import requests
import urllib.request, json, os, itertools, threading, time, sys


#%%
def call_api():
    api_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/locations?datasetid=GHCND&locationcategoryid=ZIP' + '&units=standard&limit=1000&offset=3000'

    f = open('noaa_token.txt', 'r')
    token = f.read()

    headers = {'token': token}

    response = requests.get(api_url, headers=headers)
    
    if response.status_code == 200:
        data = json.loads(response.content.decode('utf-8'))
        return data
    else:
        return None

# %%
data = call_api()
df = pd.DataFrame(data['results'])

# %%

# Make a class to search in NCDC database?

class NCDC:
    def __init__(self, zip_code, token=None):

        # If token is not specified it is read from noaa_token.txt
        if token == None:
            f = open('noaa_token.txt', 'r')
            self.token = f.read()
        else:
            self.token = token
        
        # Get location data from zip code
        self.zip_code = zip_code
        self.base_url = 'https://www.ncdc.noaa.gov/cdo-web/api/v2/data?datasetid=GHCND&limit=1000&locationid=ZIP:' + str(self.zip_code) + '&units=standard'
        # Print how much data is in that location
        return
    
    def call_api(self, url):
        headers = {'token': self.token}
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception('Error')
        else:
            return json.loads(response.content.decode('utf-8'))
    
    def get_day(self, day):
        '''
        Gets one day of weather
        day is in format yyyy-mm-dd
        '''
        # self.call_api(self.base_url + '&startdate=' + day + '&enddate=' + day + '&datatypeid=TAVG')
        # df = df[df['datatype'].isin(['TMAX', 'TMIN', 'TAVG'])].pivot('date','datatype', 'value').copy()
        return self.call_api(self.base_url + '&startdate=' + day + '&enddate=' + day + '&datatypeid=TAVG')
    
    def get_range(self, startdate, enddate):
        '''
        Gets range of weather between dates
        dates are in format yyyy-mm-dd
        '''
        df = self.call_api(self.base_url + '&startdate=' + startdate + '&enddate=' + enddate)
        return df
    
    def get_year(self, year):
        '''
        Gets year worth of weather
        '''
        df = self.call_api(self.base_url + '&startdate=' + year + '-01-01&enddate=' + year + '-12-31')
        return df


# %%
