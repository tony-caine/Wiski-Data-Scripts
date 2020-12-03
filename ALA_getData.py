
import os
from bs4 import BeautifulSoup as bs
import pandas as pd
from collections import defaultdict
import logging

#

# from simplified from kisters DataAccess
from wra_xml_service import wra_xml_service as wra
# use xlwings to load xl sheets to dataframe
from xl2df import xl2df
# get timeseries and importnumbers from kiwis
from kiwis_get_ts_list import kiwis_get_ts_list as kiwis_ts
# does as it says for a dictionary of list'ed data
from zrxp_writer import zrxp_writer as zrxp

startyear = 2015
endyear = 2020

# get the config for the WaterSources and the StationNumbers from work in Excel file
# get the ImportNumbers from kiwis
xlWbFullPath = r'ALA_config.xlsx'
waterSourcesWorksheet = 'watersources'
alaInfoWorksheet = 'AccessLicenceInfo'
outputPath = r'D:\Kisters\_Import_Hot_Folder\_PRE_IMPORT'
zrxpfile = os.path.join(outputPath,'ALA_data.zrxp')
 

#ALA_number = 'AccessLicenceAccounts'
#TS_Number = 'FYear.Total'


def getWaterSourcesDict():
    print('loading watersources')
    watersources = xl2df(xlWbFullPath, waterSourcesWorksheet)
    # move the sheets into dictionaries for lookuup purposes
    # will itterate through the watersources
    dfWatersources = watersources.df
    dfWatersources['wsid'] = dfWatersources['wsid'] .apply(
        lambda x: str(int(x)))
    dfWatersources['startyear'] = dfWatersources['startyear'] .apply(
        lambda x: int(x))
    #dfWatersources.drop(['WSrc', 'WsShortName', 'split'], axis='columns')
    mappingWatersources = {}
    # if the skip column is set to 'yes' then ignore for this loop
    dfWatersources = dfWatersources[dfWatersources['skip'] != 'yes']
    for i, row in dfWatersources.iterrows():
        key = row['wsid'].strip()
        year = row['startyear']
        name = row['name'].strip()
        mappingWatersources[key] = (year, name)

    logging.info('dfWatersources info. ' + str(i) + ' rows loaded')
    dfWatersources.info()
    return mappingWatersources


def getAlaStationsDict():
    print(' loading watersource station mappings')
    alainfo = xl2df(xlWbFullPath, alaInfoWorksheet)

    # the station numbers are stored, indexed by (WaterSource,FullCategory)
    dfAlainfo = alainfo.df
    #dfAlainfo.drop(['WSId', 'SiteName', 'SiteNumber', 'StationName'], axis='columns')
    mappingAlaStations = {}
    for i, row in dfAlainfo.iterrows():
        watersource = row['WaterSource'].strip()
        fullcategory = row['FullCategory'].strip()
        stationnumber = row['StationNumber'].strip()
        key = (watersource, fullcategory)
        mappingAlaStations[key] = stationnumber
    # print( mappingAlaStations[(	'BELUBULA REGULATED RIVER WATER SOURCE',	'SUPPLEMENTARY WATER')	])
    # print( mappingAlaStations[(	'BELUBULA REGULATED RIVER WATER SOURCE',	'DOMESTIC AND STOCK[DOMESTIC]')])

    logging.info('dfAlainfo info. ' + str(i) + ' rows loaded')
    dfAlainfo.info()
    return mappingAlaStations


def getAlaImportNumbersDict():
    print(' loading timeseries importnumber mappings via kiwis')
    # the import numbers can be indexed by the (station_no, stationparameter_no)
    # get the current wiski timeseries for the AccessLicenceAccounts
    ts_path_ala = 'AccessLicenceAccounts/*/*/*'
    ala_ts = kiwis_ts()
    dftimeseries = ala_ts.get_ts_list(ts_path_ala)
    # headers': 'ts_path,site_no,station_no,stationparameter_no,ts_shortname'
    mappingAlaImportNumber = {}
    for i, row in dftimeseries.iterrows():
        # because beautifulsoup lower cases all xml keys
        stationnumber = row['station_no'].strip()
        parameternumber = row['stationparameter_no'].lower().strip()
        ts_path = row['ts_path']
        key = (stationnumber, parameternumber)
        mappingAlaImportNumber[key] = ts_path
    '''
    # debugging - what's in the dict
    f = open('mappingAlaImportNumber.csv', 'w')
    f.write('stationNo, parameterNo, importNo, key\n')
    for key in mappingAlaImportNumber:
        stationNo, parameterNo = key
        importnumber = mappingAlaImportNumber[key]
        f.write(stationNo + ',' + parameterNo + ',' +
                mappingAlaImportNumber[key] + ',' + str(key)+'\n')
    f.close()
    '''

    logging.info('dftimeseries info. ' + str(i) + ' rows loaded')
    dftimeseries.info()
    return mappingAlaImportNumber


def main():
    logging.basicConfig(filename='ALA_getData.log',
                        level=logging.INFO, filemode='w')

    mappingWatersources = getWaterSourcesDict()
    mappingAlaStations = getAlaStationsDict()
    mappingAlaImportNumber = getAlaImportNumbersDict()

    # use the Access Licence Accounts configuration work in alaInfoWorksheet.
    # contains
    # a list of watersources to process
    # and the mapping from WSId and Category to the Wiski Station Number

    web = wra()  # get the AccessLicenceAccount data from WRAXML web service
    # store the accumulated data in zrxpdata dict.
    # write zrxp file(s) at end.
    zrxpdata = defaultdict(list)
    for wsid in mappingWatersources:
        (startyear, name) = mappingWatersources[wsid]
        print('Watersource: ' + str(([wsid, name, startyear])))
        logging.info('Watersource: ' + str(([wsid, name, startyear])))
        #
        for y in range(startyear, endyear+1):
            year = '01-july-'+str(y)  # needed for web service call
            dateOfValue = (y, 7, 1)  # needed for the zrxp data row
            print('year: ' + str(y))
            logging.info('year: ' + str(y))
            response = web.get_water_account(wsid, year)
            soup = bs(response, "lxml")
            # process each account in the response
            accounts = soup.find_all('wacc')
            for account in accounts:
                wsname = account.find('ws').get_text().strip()
                category = account.find('category').get_text().strip()
                try:
                    stationnumber = mappingAlaStations[(wsname, category)]
                    #print('StationNumber: ' + str([wsname, category, stationnumber]))
                    logging.info('StationNumber: ' +
                                 str([wsname, category, stationnumber]))
                except KeyError:
                    logging.error('Error: StationNumber key notFound: ' +
                                  str([wsname, category]))
                    continue
                #
                # deal with xml that not needed
                #
                # there is no GS for WS=14681 even though it's in the XML
                # so skip it
                if stationnumber == '14681-GS':
                    continue
                # this is the list of subcat's to exclude
                notOfInterest = {'wystart': 1, 'wyend': 1,
                                 'wtype': 1, 'ws': 1, 'category': 1}

                for cell in account:
                    subcat = cell.name.strip()
                    value = cell.text.strip()
                    if subcat in notOfInterest:
                        continue
                    importnumberkey = (stationnumber, subcat)
                    try:
                        importnumber = mappingAlaImportNumber[importnumberkey]
                        # cache the data here to accumulate all years before writing
                        zrxpdata[importnumber].append((dateOfValue, value))
                    except KeyError:
                        logging.error('Error: Parameter key notFound: ' +
                                      str(importnumberkey))
                # end - for subcategories of account
            # end - for account
        # end - for years
    # end - for watersources
    print(['writing zrxp file', zrxpfile ])
    z = zrxp()
    z.write_wra_dict2zrxp( zrxpfile, zrxpdata)


if __name__ == "__main__":
    main()
