
import pandas as pd
from datetime import datetime

from kiwisUtes import kiwisUtes as kiwis
# does as it says for a dictionary of list'ed data
from zrxp_writer import zrxp_writer as zrxp


def main():

    sitefilter='StreamStations'
    stationfilter='419*'
    wateraccount='Peel'
    paths = ['StreamStations/419*/DSLoss/Day.CmdTotal.P']
    startdate = '2010-01-01'
    enddate = '2019-12-31'
    input_number = 'RiverBalance/15101/TotRivEvapVol/Day.Cmd.O'
    outputpath = r'D:\Kisters\scripts\kiwis_tasks\outputs'
    filename = 'StreamStations_419_DSLoss'

    '''
    #prefixing todays date on filename's base
    datetime_str=datetime.today().strftime('%Y-%m-%d')
    filename=datetime_str+'_'+filename
    '''   
    web = kiwis()
    #get the stations in the wateraccount - filter by site and station
    stations = web.get_station_list( sitefilter, stationfilter, wateraccount )
    df_data = web.get_tsvalues_path_plus_stations(startdate, enddate, paths, stations, outputpath, filename)

    print(df_data)
    '''
    z = zrxp()
    z.write_kiwis_date_value_dfs2zrxp(outputpath, filename, input_number, df)
    '''

if __name__ == '__main__':  # single underscore
    main()
