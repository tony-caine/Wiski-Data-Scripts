import pandas as pd
import requests
import sys

# intranet server - 2020-08-14
base_url = 'http://amaprdwiskweb1b:8081/KiWIS/KiWIS?'


class kiwis_get_ts_list:

    def __init__(self):
        self.base_url = base_url

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
            listdf = pd.read_html(html)  # read_html returns a list of df'd
            df = listdf[0]
        except:
            e = sys.exc_info()[0]
            print("Exception: %s" % str(e))
            sys.exit()

        headers = [s.strip() for s in df.iloc[0]]
        df.columns = headers
        df.drop(axis=0, index=0, inplace=True)
        df.dropna(inplace=True)
        return df
