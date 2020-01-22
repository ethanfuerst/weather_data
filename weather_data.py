import pandas as pd
import datetime as dt
from datetime import date, timedelta, datetime
import urllib.request, json, os, itertools, threading, time, sys

class IDNotFoundError(Exception):
    pass

def get_weather(lat, long, dark_sky_id, sdate=date.today() - timedelta(days=365), window=1, edate=date.today()):
    '''
    This method returns a dataframe that contains the high temp, low temp, moving average high and moving average low for a range of days.
    The dataframe returned will contain all data from the startdate through the day before the enddate.

    Parameters:


    lat (float, required)
        The latitude of where you want data from

    long (float, required)
        The longitude of where you want data from

    id (string, required)
        The ID for darksky API.
        For more info, visit https://darksky.net/
        
    sdate (datetime.date, default: today - 1 year)
        The startdate of the interval
        Must be a datetime.date object
    
    window (integer, default: 1)
        The moving window for the moving average column
    
    edate (datetime.date, default: today)
        The enddate of the interval
        Must be a datetime.date object
    '''

    if type(sdate) != date or type(edate) != date:
        raise TypeError("Both sdate and edate must by datetime.date type.")
    if type(lat) != float or type(long) != float:
        raise TypeError("Both lat and long must be floats.")
    if type(window) != int:
        raise TypeError("The window variable must be an integer.")
    try:
        test_api = 'https://api.darksky.net/forecast/' + str(dark_sky_id)+ '/30.266666,-97.73333,1579435200?exclude=flags,hourly'
        with urllib.request.urlopen(test_api) as url:
            data = json.loads(url.read().decode())
    except:
        raise IDNotFoundError("Not a valid Dark Sky API ID. See https://darksky.net/dev for more information")
    
    # creating a list for each day in the range
    # using unix time because that's what the darksky api uses
    # each date will give me the unix time at 6pm for that day
    # the daily_high and daily_low are the parameters that I will use to see how my mpg changes
    delta = edate - sdate       # as timedelta
    date_list = []
    for i in range(window * -1, delta.days + 1):
        day = sdate + timedelta(days=i)
        date_list.append(int((day - dt.date(1970,1,1)).total_seconds()) + 43200)

    # creating a list for each api call
    api_list = []
    for i in date_list:
        api_list.append('https://api.darksky.net/forecast/'+str(dark_sky_id)+'/'+str(lat)+','+str(long)+','+str(i)+'?exclude=flags,hourly')

    # getting the high temp and low temp for each day
    dates = []
    daily_high = []
    daily_low = []
    for api in api_list:
        with urllib.request.urlopen(api) as url:
            data = json.loads(url.read().decode())
            # keeping day in yyyymmdd format, just like in other dataframes
            dates.append(datetime.fromtimestamp(data["currently"]["time"]).strftime("20%y/%m/%d"))
            daily_high.append(data["daily"]["data"][0]["temperatureHigh"])
            daily_low.append(data["daily"]["data"][0]["temperatureLow"])

    # putting all the lists in to a dateframe
    df_weather = pd.DataFrame({'date': dates, 'daily_high': daily_high, 'daily_low': daily_low})
    if window != 1:
        df_weather['high_mov_avg'] = df_weather['daily_high'].rolling(window=window).mean().round(3)
        df_weather['low_mov_avg'] = df_weather['daily_low'].rolling(window=window).mean().round(3)

    # need to drop the records before the sdate
    drop_list = [i for i in range(window)]
    df_weather = df_weather.drop(drop_list)
    # and then reset the index
    df_weather.reset_index(drop=True, inplace=True)

    return df_weather
