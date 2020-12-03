import requests
import pandas as pd
from bs4 import BeautifulSoup as bs
import datetime

base_url = 'http://www.bom.gov.au/waterdata/services?'


class BOM():
    """
    Class used to access the Water Data Online XML web service
    """

    def __init__(self):
        """
        Parameters
        ----------
        base_url : str
        """
        self.base_url = base_url

    def _get(self, payload):
        """
        Perform the request and get the data
        """
        response = requests.get(self.base_url, params=payload)
        return response

    def content_to_dataframe( self, contentXml,valueName):
        soup = bs(contentXml, "lxml")
        # process each observation (daily value) in the response
        points = soup.find_all('wml2:point')
        
        row=[]
        for point in points:
            datestr = point.find('wml2:time').get_text().strip()
            datestr=datestr[:10] #get the base date
            dt=datetime.datetime.strptime(datestr,'%Y-%m-%d')
            value = point.find('wml2:value').get_text().strip()
            rowvalues=[dt,value]
            row.append(rowvalues)
        df=pd.DataFrame(row,columns=['datetime',valueName])
        df.set_index(keys=['datetime'],inplace=True)
        return df


    def get_storage_level(self, FeatureofInterest, temporalFilter ):
        """ 
        """

        payload = {
            'service': 'SOS', 
            'request': 'getobservation',
            'datasource': 0, 
            'version': '2.0',
            'featureOfInterest': FeatureofInterest,
            'procedure': 'http://bom.gov.au/waterdata/services/tstypes/Pat7_C_B_1_DailyMean',
            'observedProperty': 'http://bom.gov.au/waterdata/services/parameters/Storage Level',
            'temporalFilter': temporalFilter
        }

        response = self._get(payload)
        df=self.content_to_dataframe(  response.content, 'level')
        return df

    def get_storage_volume(self, FeatureofInterest, temporalFilter ):
        """ 
        """
        payload = {
            'service': 'SOS', 
            'request': 'getobservation',
            'datasource': 0, 
            'version': '2.0',
            'featureOfInterest': FeatureofInterest,
            'procedure': 'http://bom.gov.au/waterdata/services/tstypes/Pat6_C_B_1_DailyMean',
            'observedProperty': 'http://bom.gov.au/waterdata/services/parameters/Storage Volume',
            'temporalFilter': temporalFilter
        }

        response = self._get(payload)
        df=self.content_to_dataframe(  response.content, 'volume')
        return df


    def get_sites_level_volume(self, bomid, startdate, enddate):
        FeatureofInterest = 'http://bom.gov.au/waterdata/services/stations/{id}'.format( id=bomid)
        temporalFilter = 'om:phenomenonTime,{start}/{end}'.format(start=startdate, end=enddate)


        df_level=self.get_storage_level( FeatureofInterest, temporalFilter )
        df_volume=self.get_storage_volume( FeatureofInterest, temporalFilter )

        df = df_level.join(df_volume, how='outer')
        df_level.columns = ['value']
        df_volume.columns= ['value']
        return (df, df_level, df_volume)
