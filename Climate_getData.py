
import os
import pandas as pd
import logging

#
from SILO import SILO as SILO
# use xlwings to load xl sheets to dataframe
from xl2df import xl2df
# get timeseries and importnumbers from kiwis
from kiwis_get_ts_list import kiwis_get_ts_list as kiwis_ts
# does as it says for a dictionary of list'ed data
from zrxp_writer import zrxp_writer as zrxp

startdate = '18900101'
enddate = '20210101'

# get the config for the WaterSources and the StationNumbers from work in Excel file
xlWbFullPath = 'Climate_config.xlsx'
ClimateStationsWorksheet = 'climate stations'

#outputPath = r'D:\Kisters\_Import_Hot_Folder\_Import_PRE_IMPORT'
outputpath = r'D:\Kisters\scripts\out-silo'


def getClimateStationDict(fullpath, worksheet):
    print('loading ClimateStations list to process')
    # only process those in list that are wanted ie. not marked to skip
    stations = xl2df(fullpath, worksheet)
    # move the sheet into dictionaries for lookuup purposes
    df = stations.df
    df['StationNumber'] = df['StationNumber'] .apply(
        lambda x: str(int(x)))
    mappingClimateStationDict = {}
    # if the skip column is set to 'yes' then ignore for this loop
    df = df[df['skip'] != 'yes']
    for i, row in df.iterrows():
        key = row['StationNumber'].strip()
        name = row['StationName']
        account = row['WaterAccount'].strip()
        mappingClimateStationDict[key] = (name, account)

    logging.info('stations df info. ' + str(i) + ' rows loaded')
    df.info()
    return mappingClimateStationDict


def getClimateImportNumbersDict():
    print(' loading timeseries importnumber mappings via kiwis')

    # get the current wiski timeseries for the ClimateStations Original Daily Data
    ts_path_climate = 'ClimateStations/*/*/Day.CmdTotal.O'
    kiwis = kiwis_ts()
    df = kiwis.get_ts_list(ts_path_climate)
    # headers': 'ts_path,site_no,station_no,stationparameter_no,ts_shortname'
    mappingImportNumbers = {}
    for i, row in df.iterrows():
        # because beautifulsoup lower cases all xml keys
        stationnumber = row['station_no'].strip()
        parameternumber = row['stationparameter_no'].lower().strip()
        ts_path = row['ts_path']
        key = (stationnumber, parameternumber)
        mappingImportNumbers[key] = ts_path

    logging.info('df info. ' + str(i) + ' rows loaded')
    df.info()
    return mappingImportNumbers


def main():
    logging.basicConfig(filename='Climate_getData.log',
                        level=logging.INFO, filemode='w')

    mappingClimateStationDict = getClimateStationDict(
        xlWbFullPath, ClimateStationsWorksheet)
    mappingImportNumbers = getClimateImportNumbersDict()

    silo = SILO()
    z = zrxp()
    for Station in mappingClimateStationDict:
        (name, account) = mappingClimateStationDict[Station]
        print('Station: ' + str([Station, name, account]))
        logging.info('Station: ' + str([Station, name, account]))
        #
        (df_rain, df_mlake) = silo.get_data(Station, startdate, enddate)
        logging.info('have SILO data')
        #df_rain.info()
        #df_mlake.info()
        #

        rainImportNumber = mappingImportNumbers[(Station, 'rain')]
        evapImportNumber = mappingImportNumbers[(Station, 'evap')]
        z.write_silo_dfs2zrxp(outputpath, Station, 'rain', rainImportNumber, df_rain)
        z.write_silo_dfs2zrxp(outputpath,  Station, 'evap', evapImportNumber, df_mlake)

    # end of stations
    print('done')


if __name__ == "__main__":
    main()
