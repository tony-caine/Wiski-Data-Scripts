
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

startyear = 2000
endyear = 2019

# get the config for the WaterSources and the StationNumbers from work in Excel file
# get the ImportNumbers from kiwis
xlWbFullPath = r'ALA_config.xlsx'
waterSourcesWorksheet = 'watersources'

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


def main():
    logging.basicConfig(filename='ALA_getData.log',
                        level=logging.INFO, filemode='w')

    mappingWatersources = getWaterSourcesDict()

    with open("analysis.csv", "w") as file:
        file.write("WSid, Watersource,Category")
        web = wra()  # get the AccessLicenceAccount data from WRAXML web service
        for wsid in mappingWatersources:
            (startyear, name) = mappingWatersources[wsid]
            print('Watersource: ' + str(([wsid, name, startyear])))
            logging.info('Watersource: ' + str(([wsid, name, startyear])))
            #
            for y in range(startyear, endyear+1):
                year = '01-july-'+str(y)  # needed for web service call
                print('year: ' + str(y))
                logging.info('year: ' + str(y))
                response = web.get_water_account_env(wsid, year)
                soup = bs(response, "lxml")
                # process each account in the response
                accounts = soup.find_all('wacc')
                for account in accounts:
                    wsname = account.find('ws').get_text().strip()
                    category = account.find('category').get_text().strip()
                    
                    file.write( wsid +','+ wsname +','+ category +'\n')
                    #
                    # deal with xml that not needed
                    # this is the list of subcat's to exclude
                    notOfInterest = {'wystart': 1, 'wyend': 1,
                                    'wtype': 1, 'ws': 1, 'category': 1}

                    for cell in account:
                        subcat = cell.name.strip()
                        if subcat in notOfInterest:
                            continue

                        #print( subcat)

                    # end - for subcategories of account
                # end - for account
            # end - for years
        # end - for watersources


if __name__ == "__main__":
    main()
