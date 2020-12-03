import requests

base_url = 'https://wraxml.water.nsw.gov.au/wra/main/xmlService?'


class wra_xml_service():
    """
    Class used to access the Water Resource Accounting Reporter XML web service
    """
    # password = pswd

    def __init__(self):
        """
        Parameters
        ----------
        base_url : str
            Base url of the web service
        """
        self.base_url = base_url

    def _get(self, payload):
        """
        Perform the request and get the data
        """
        response = requests.get(self.base_url, params=payload)
        return response

    def get_water_account(self, wsid, wystart):
        """
        Get water account information, as dict. 
        """

        payload = {'dfAction': 'getWaterAccount', 'dfWSId': wsid,
                   'dfWYStart': wystart, 'dfPswd': 'pswd'}
        response = self._get(payload)

        return response.content

    def get_water_account_env(self, wsid, wystart):
        """
        Get ENV water account information, as dict. 
        """

        payload = {'dfAction': 'getEWWaterAccount', 'dfWSId': wsid,
                   'dfWYStart': wystart, 'dfPswd': 'pswd'}
        response = self._get(payload)

        return response.content