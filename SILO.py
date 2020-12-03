
import requests
from datetime import datetime
import numpy as np
import pandas as pd
import re
import logging
import sys

'''
https://legacy.longpaddock.qld.gov.au/cgi-bin/silo/PatchedPointDataset.php?format=Alldata&station=070042&start=18890101&finish=20200814&username=uuuuuuuuu&password=ppppppp

https://legacy.longpaddock.qld.gov.au/cgi-bin/silo/PatchedPointDataset.php?
format=Alldata&
station=070042&
start=18890101&
finish=20200814&
username=uuuuuuuu&
password=pppppppp

'''
#username = 'NSWINDUSTRY'
#password = 'INVESTMENT'

baseurl = 'https://www.longpaddock.qld.gov.au/cgi-bin/silo/PatchedPointDataset.php?'

''' also works
username = 'SILO1518' 
password = 'HUITW2Q6'
baseurl = 'https://legacy.longpaddock.qld.gov.au/cgi-bin/silo/PatchedPointDataset.php?'
'''
firstrowofdata = 53
writeresponse = False


class SILO:
    """
    Legacy SILO Access class, because for some reason the perfectly functional
    new SILO has been decomissioned...
    """

    def __init__(self):
        """
        Parameters
        ----------
        usr : str
            The account username for access to the legacy SILO API
        pwd : str
            The password for the account
        """
        #----------------------------------
        filename='password_SILO.txt'
        with open(filename) as f:
            userpass = [x.strip().split(':', 1) for x in f]
        username = userpass[0][0]
        password = userpass[0][1]
        #----------------------------------

        self.usr = username
        self.pwd = password
        self.baseurl = baseurl

    def get_data(self, station_no, start, finish):
        """
        get data

        station_no : int
            The SILO station number
        start : int
            The start time formatted as yyyymmdd ie: 20180101
        finish : int
            The end time formatted as yyyymmdd ie: 20180101

        """
        payload = {'format': 'Alldata',
                   'station': station_no,
                   'start': start,
                   'finish': finish,
                   'username': self.usr,
                   'password': self.pwd}

        #logging.info(['Calling SILO ', self.baseurl, str(payload)])
        logging.info(['Calling SILO ', self.baseurl])
        response = requests.get(self.baseurl, params=payload)
        logging.info(response.url)

        # if needed for analysis
        if writeresponse:
            f = open("SILO.response.txt", "w")
            # f.write(response.url+'\n')
            f.write(response.text)
            f.close()

        '''
        catch this error
        'Please email silo@des.qld.gov.au that this error occurred,
        and please include the date and (Brisbane) time when the error occurred'
        '''

        if re.search("that this error occurred", response.text):
            # die.
            logging.error('SILO ERROR')
            logging.error(response.text)
            logging.error('experience: check dates')

            sys.exit("SILO ERROR - see log")

        return self._create_data_frame(response.text)

    def _create_data_frame(self, data):
        """create data frame"""
        # read string into a list and remove unused header lines
        lines = data.splitlines()

        # drop the first 52/53 lines - header info for humans
        used_lines = lines[firstrowofdata:]

        temp = []
        for i in [row for row in used_lines if row]:
           # print(i)
            temp.append(i.split())

        date = []
        rain = []
        mlake = []
        for i in [row for row in temp if row]:
            date.append(i[0])
            rain.append(i[7])
            mlake.append(i[18])

        arr_dt = np.array(date)
        arr_rain = np.array(rain)
        arr_mlake = np.array(mlake)

        time_stamps = pd.to_datetime(arr_dt)

        df_rain = pd.DataFrame(data={
            'timestamp': time_stamps,
            'value': arr_rain,
            'quality': np.ones(len(arr_rain))
        })
        # set index
        df_rain.set_index('timestamp', inplace=True)

        df_mlake = pd.DataFrame(data={
            'timestamp': time_stamps,
            'value': arr_mlake,
            'quality': np.ones(len(arr_mlake))
        })
        # set index
        df_mlake.set_index('timestamp', inplace=True)

        return (df_rain, df_mlake)
