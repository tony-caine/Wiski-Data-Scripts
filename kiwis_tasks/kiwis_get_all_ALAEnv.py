
import pandas as pd
from datetime import datetime
import os

from kiwisUtes import kiwisUtes as kiwis
# does as it says for a dictionary of list'ed data
from zrxp_writer import zrxp_writer as zrxp
# to allow moving all paths are set in the localPaths class
from localPaths import Paths


def main():

    outputpath = Paths.outputpath()

    paths = ['AccessLicenceAccountsEnv/*/*/*']
    start = '2009-01-01'
    end = '2020-07-01'
    input_number = '0/0/0/0'  # not real really
    filename = 'ALA_env'

    # prefixing todays date on filename's base
    datetime_str = datetime.today().strftime('%Y-%m-%d')
    filename = datetime_str+'_'+filename

    web = kiwis()
    df = web.get_tsvalues_list_ts_paths(start, end, paths, outputpath, filename)

    outfile = os.path.join(outputpath, 'ala_env.csv')
    df.to_csv(outfile)

    #z = zrxp()
    #z.write_kiwis_date_value_dfs2zrxp(outputpath, filename, input_number, df)


if __name__ == '__main__':  # single underscore
    main()
