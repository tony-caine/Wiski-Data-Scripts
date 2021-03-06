
import pandas as pd
from datetime import datetime

from kiwisUtes import kiwisUtes as kiwis
# does as it says for a dictionary of list'ed data
from zrxp_writer import zrxp_writer as zrxp


def main():

    paths = ['StreamStations/410*/DSLoss/Day.CmdTotal.P']
    start = '2010-01-01'
    end = '2019-12-31'
    input_number = 'RiverBalance/11982/TotRivEvapVol/Day.Cmd.O'
    outputpath = r'D:\Kisters\scripts\kiwis_tasks\outputs'
    filename = 'StreamStations_410_DSLoss'

    #prefixing todays date on filename's base
    datetime_str=datetime.today().strftime('%Y-%m-%d')
    filename=datetime_str+'_'+filename
   
    web = kiwis()
    df = web.get_tsvalues_list_ts_paths(start, end, paths,outputpath,filename )

    z = zrxp()
    z.write_kiwis_date_value_dfs2zrxp(outputpath, filename, input_number, df)


if __name__ == '__main__':  # single underscore
    main()
