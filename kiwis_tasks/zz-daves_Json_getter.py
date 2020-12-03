import json
import requests


def main():
    ST = '2017-07-01'
    EN = '2018-06-30'
    ID = 'AccessLicenceAccounts/11982*/*Usage/FYear.Total'

    URL = (
        'http://amaprdwiskweb1b:8081/KiWIS/KiWIS?datasource=0'
        '&service=kisters'
        '&type=queryServices'
        '&request=gettimeseriesvalues'
        '&datasource=0'
        '&format=json'
        # '&ts_id={0}'
        '&ts_path={0}'
        # '&from=2018-10-01&to=2018-12-31'
        '&from={1}&to={2}'
        # '&metadata=true'
        # '&md_returnfields=station_name'
        '&header=true'
        '&kiUser=&kiPassword='.format(ID, ST, EN))

    # Get data from WISKI
    r = requests.get(URL)

    # parse reply
    js = json.loads(r.text)

    # sum licence values
    LU = 0
    for item in js:
        print(item)
       # print(item['data'][0])
        LU += item['data'][0][1]
    print(LU)


if __name__ == "__main__":
    main()
