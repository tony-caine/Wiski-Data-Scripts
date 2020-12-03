
import pandas as pd
from datetime import datetime
import os

from kiwisUtes import kiwisUtes as kiwis
# does as it says for a dictionary of list'ed data
from zrxp_writer import zrxp_writer as zrxp

from wraUtes import Utes

# to allow moving all paths are set in the localPaths class
from localPaths import Paths


def main():

    dsvol_path = r'StreamStations/*/DSVolume/Day.Cmd.O'
    start = '2010-01-01'
    end = '2020-12-31'

    #outputpath = r'D:\Kisters\scripts\kiwis_tasks\outputs'
    outputpath = Paths.outputpath()

    # prefixing todays date on filename's base
    datetime_str = datetime.today().strftime('%Y-%m-%d')

    web = kiwis()
    z = zrxp()
    utes = Utes()

    dict_W_Acct = web.get_ts_lists_by_W_Acct(dsvol_path)

    for WaterAccount, ts_list in dict_W_Acct.items():
        water_account_number = utes.wateraccount_to_catchmentNumber(WaterAccount)
        #
        filename_raw = '{dt}_StreamStations_{wa}_DSVol.raw.csv'.format(dt=datetime_str, wa=water_account_number)
        filename_raw = os.path.join(outputpath, filename_raw)
        filename_sums = '{dt}_StreamStations_{wa}_DSVol.sums.csv'.format(dt=datetime_str, wa=water_account_number)
        filename_sums = os.path.join(outputpath, filename_sums)
        #
        df = web.get_tsvalues_ts_list(start, end, ts_list, filename_raw, filename_sums)
        #
        input_number = 'RiverBalance/{wa}/TotRivDSVol/Day.Cmd.O'.format(wa=water_account_number)
        filename_zrx = '{dt}_StreamStations_{wa}_DSVol.zrxp'.format(dt=datetime_str, wa=water_account_number)
        z.write_kiwis_date_value_dfs2zrxp(outputpath, filename_zrx, input_number, df)


if __name__ == '__main__':  # single underscore
    main()
