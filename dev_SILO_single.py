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

startdate = '20190101'
enddate = '20210101'

# get the config for the WaterSources and the stationNumbers from work in Excel file
xlWbFullPath = 'Climate_config.xlsx'
ClimateStationsWorksheet = 'climate stations'

#outputPath = r'D:\Kisters\_Import_Hot_Folder\_Import_PRE_IMPORT'
outputpath = r'C:\_data\wiski\Scripts\out-silo'


def main():
    logging.basicConfig(filename='Climate_getData.log',
                        level=logging.INFO, filemode='w')

    silo = SILO()
    z = zrxp()

#    station, name, account = ('73007', 'Burrinjuck Dam', 'Bidgee')
    station, name, account = ('55041', 'Nundle Post Office', 'Peel')

    print('station: ' + str([station, name, account]))
    logging.info('station: ' + str([station, name, account]))
    #
    (df_rain, df_mlake) = silo.get_data(station, startdate, enddate)
    logging.info(['have SILO data', station, name])
    df_rain.info()
    df_mlake.info()
    #

    rainImportNumber = 'dummy1'
    evapImportNumber = 'dummy2'

    z.write_silo_dfs2zrxp(outputpath, station, 'rain', rainImportNumber, df_rain)
    z.write_silo_dfs2zrxp(outputpath,  station, 'evap', evapImportNumber, df_mlake)

    print('done')
    logging.info('done')


if __name__ == "__main__":
    main()
