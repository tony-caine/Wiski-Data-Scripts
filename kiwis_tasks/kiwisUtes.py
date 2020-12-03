import pandas as pd
import numpy as np

import requests
from io import StringIO

import sys
import os

from collections import defaultdict

# intranet server - 2020-08-14
base_url = 'http://amaprdwiskweb1b:8081/KiWIS/KiWIS?'


class kiwisUtes:

    def __init__(self):
        self.base_url = base_url

    def _tsvalues_csv2df(self, csvresponse):
        df = pd.read_csv(StringIO(csvresponse), sep=';', header=None, names=['key', 'values'])

        # start by making df for first timeseries
        ts = df.iloc[0]['values']
        numrows = int(df.iloc[1]['values'])
        di = 3  # assuming data starts here - ie minimal metadata
        df1 = pd.DataFrame({'dates': df.loc[di:di+numrows-1, 'key'], ts: df.loc[di:di+numrows-1, 'values']})
        df1[ts] = df1[ts].astype(float)
        # then add(join) the rest of timeseries on df at time till done
        i = di+numrows
        maxi = len(df.index)  # total number of rows
        while i < maxi:
            ts = df.iloc[i]['values']
            numrows = int(df.iloc[i+1]['values'])
            di = i+3  # di is index to start of data rows
            df_next = pd.DataFrame({'dates': df.loc[di:di+numrows-1, 'key'], ts: df.loc[di:di+numrows-1, 'values']})
            df_next[ts] = df_next[ts].astype(float)
            # join on dates then release index ready for next itteration
            df1 = df1.set_index('dates').join(df_next.set_index('dates'), how='outer')
            df1 = df1.reset_index()
            i = i+numrows+3
        return df1

    def z_csv2df(self, csvresponse):
        df = pd.read_csv(StringIO(csvresponse), sep=';', header=None, names=['key', 'values'])
        df2 = pd.DataFrame(index=df['key'].drop_duplicates())
        uniqlen = df2.shape[0]
        # make a wide df from vertical
        ts = df.iloc[0]['values']
        numrows = int(df.iloc[1]['values'])
        df_col = pd.DataFrame({'dates': df.loc[3:2+numrows, 'key'], ts: df.loc[3:2+numrows, 'values']})
        pd.concat([df2, df_col])
        df_col.set_index('dates')
        df.loc[1:numrows, 'values']

        for i in range(0, df.shape[0], uniqlen):
            df2[i] = df.loc[i:i+uniqlen-1, 'values'].values
        # keep just the data
        df2.drop(index=['#ts_id', '#rows', '#Timestamp'], inplace=True)
        # and convert the strings to numberic so can manipulate numbers
        for i in range(df2.shape[1]):
            df2.iloc[:, i] = df2.iloc[:, i].astype(float)

        return (df2)

    def get_station_list(self, site, station, wateraccount ):
        '''
        Given site and wateraccount return a df of stations.
        ''' 
        payload = {
            'service': 'kisters',
            'type': 'queryServices',
            'request': 'getStationList',
            'datasource': '0',
            'site_no':site,
            'station_no':station,
            'returnfields': 'station_name,station_no,site_name,ca_sta',
            'ca_sta_returnfields':'W_Acct',
            'format': 'html'
        }

        try:
            self.response = requests.get(self.base_url, params=payload)
            html = self.response.text
            listdf = pd.read_html(html)  # read_html returns a list of df's.
            df = listdf[0]  # just want first
        except:
            e = sys.exc_info()[0]
            print("Exception: %s" % str(e))
            sys.exit()   
        headers = [s.strip() for s in df.iloc[0]]
        df.columns = headers
        df.drop(axis=0, index=0, inplace=True)
        df.dropna(inplace=True)

        flags = df['W_Acct']==wateraccount
        df1=df[flags]

        stations=df1['station_no']
        return stations               

    def get_ts_list(self, requested_ts_path):
        """
        get the timeseries for the specified ts_path
        and return a dataframe
        """
        payload = {
            'service': 'kisters',
            'type': 'queryServices',
            'request': 'getTimeseriesList',
            'datasource': '0',
            'returnfields': 'ts_path,site_no,station_no,stationparameter_no,ts_shortname',
            'format': 'html',
            'ts_path': requested_ts_path
        }

        try:
            self.response = requests.get(self.base_url, params=payload)
            html = self.response.text
            listdf = pd.read_html(html)  # read_html returns a list of df's.
            df = listdf[0]  # just want first
        except:
            e = sys.exc_info()[0]
            print("Exception: %s" % str(e))
            sys.exit()

        headers = [s.strip() for s in df.iloc[0]]
        df.columns = headers
        df.drop(axis=0, index=0, inplace=True)
        df.dropna(inplace=True)
        return df

    def get_tsvalues_list_ts_paths(self,  startdate, enddate, list_ts_paths, outputpath, filename):
        """
        get the timeseries values given a list of ts_path's
        and a date start and end
        and return a dataframe
        """
        df = pd.DataFrame()

        for ts_path in list_ts_paths:
            payload = {
                'service': 'kisters',
                'type': 'queryServices',
                'datasource': '0',
                'request': 'getTimeseriesValues',
                'format': 'csv',
                'ts_path': ts_path,
                'from': startdate,
                'to': enddate,
                'dateformat': 'yyyy-MM-dd',
                'metadata': 'true',
                'md_returnfields': 'ts_path'
            }

            try:
                self.response = requests.get(self.base_url, params=payload)
                csv = self.response.text
            except:
                e = sys.exc_info()[0]
                print("Exception: %s" % str(e))
                sys.exit()

            df_i = self._tsvalues_csv2df(csv)
            # join on dates
            if df.empty:
                df = df_i
            else:
                df = df.set_index('dates').join(df_i.set_index('dates'), how='outer')

            #df = pd.concat([df, df_i], axis=1, sort=False)
        # next ts_path
        # now some final minipulation and exports of intermedite products

        raw_pandas_table_filename = os.path.join(outputpath,   filename+'_raw.csv')
        sums_table_filename = os.path.join(outputpath,   filename+'_sums.csv')
        #
        df.to_csv(raw_pandas_table_filename)
        #
        if not df.index.name =='dates':
            df.set_index('dates', inplace=True)
        df2 = df.sum(axis=1)
        df2 = df2.reset_index()
        df2.rename(columns={0: 'values'}, inplace=True)
        df2.to_csv(sums_table_filename)

        return df2


    def get_tsvalues_path_plus_stations(self, startdate, enddate, paths, stations, outputpath, filename):
        """
        get the timeseries values given a list of ts_path's
        and a date start and end
        and return a dataframe of results

        filter results by the stations series
        """
        raw_pandas_table_filename = os.path.join(outputpath,   filename+'_raw.csv')
        sums_table_filename = os.path.join(outputpath,   filename+'_sums.csv')
        #

        df=self.get_tsvalues(startdate, enddate, paths)

        #df.to_csv(raw_pandas_table_filename)
        #
        if not df.index.name =='dates':
            df.set_index('dates', inplace=True)



        df2 = df.sum(axis=1)
        df2 = df2.reset_index()
        df2.rename(columns={0: 'values'}, inplace=True)
        df2.to_csv(sums_table_filename)

        return df2

    def get_tsvalues(self, startdate, enddate, list_ts_paths ):
        #take a list if ts_path's, get ts values for each, append (columns) to data frame
        df = pd.DataFrame()
        for ts_path in list_ts_paths:
            payload = {
                'service': 'kisters',
                'type': 'queryServices',
                'datasource': '0',
                'request': 'getTimeseriesValues',
                'format': 'csv',
                'ts_path': ts_path,
                'from': startdate,
                'to': enddate,
                'dateformat': 'yyyy-MM-dd',
                'metadata': 'true',
                'md_returnfields': 'ts_path'
            }

            try:
                self.response = requests.get(self.base_url, params=payload)
                csv = self.response.text
            except:
                e = sys.exc_info()[0]
                print("Exception: %s" % str(e))
                sys.exit()

            df_i = self._tsvalues_csv2df(csv)
            # join on dates
            if df.empty:
                df = df_i
            else:
                df = df.set_index('dates').join(df_i.set_index('dates'), how='outer')
            #
        # next ts_path
        return (df)

    def get_ts_lists_by_W_Acct(self, dsvol_path):
        payload = {
                'service':'kisters',
                'type':'queryServices',
                'datasource':'0',
                'request':'getTimeseriesList',
                'format':'csv',
                'ts_path':dsvol_path,
#                'returnfields':'ts_id,station_name,station_no,stationparameter_name,stationparameter_no,ts_name,ts_shortname,ca_sta',
                'returnfields':'ts_id,station_name,station_no,ca_sta',
                'metadata':'true',
                'ca_sta_returnfields':'W_Acct'
            }
        try:
            self.response = requests.get(self.base_url, params=payload)
            #self.response.url
            #self.response.statuscode
            csv = self.response.text
        except:
            e = sys.exc_info()[0]
            print("Exception: %s" % str(e))
            sys.exit()

        #load as dataframe
        df = pd.read_csv(StringIO(csv), sep=';', header=0)
        df = df[['ts_id','W_Acct']]

        #convert to a dictionary of 'ts_id1,ts_id2,...'
        dict = {}
        for i, row in df.iterrows():
            key = row['W_Acct']
            ts=row['ts_id']
            if key not in dict:
                dict[key] = str(ts)
            else:
                dict[key]=dict[key]+','+str(ts)
        return dict
        
        
    def get_tsvalues_ts_list(self, startdate, enddate, ts_list, filename_raw, filename_sums ):
    #take a single ts_list ('91234,1235,12456'), get ts values to data frame

        payload = {
            'service': 'kisters',
            'type': 'queryServices',
            'datasource': '0',
            'request': 'getTimeseriesValues',
            'format': 'csv',
            'ts_id': ts_list,
            'from': startdate,
            'to': enddate,
            'dateformat': 'yyyy-MM-dd',
            'metadata': 'true',
            'md_returnfields': 'ts_path'
        }

        try:
            self.response = requests.get(self.base_url, params=payload)
            #self.response.url
            #self.response.statuscode
            csv = self.response.text
        except:
            e = sys.exc_info()[0]
            print("Exception: %s" % str(e))
            sys.exit()

        df = self._tsvalues_csv2df(csv)
        df.to_csv(filename_raw)
        #
        if not df.index.name =='dates':
            df.set_index('dates', inplace=True)
        df2 = df.sum(axis=1)
        df2 = df2.reset_index()
        df2.rename(columns={0: 'values'}, inplace=True)
        df2.to_csv(filename_sums)

        return df2
