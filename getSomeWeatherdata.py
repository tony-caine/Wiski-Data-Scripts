import pandas as pd
import os
from LegacySILO import LegacySILO as SILO

outputpath = r'C:\_data\wiski\Scripts\silo\out'
stations = ['73007', '75175']
startdate = '18900101'
enddate = '20210101'

silo = SILO()
for stn in stations:
    print(stn)
    (df_rain, df_mlake) = silo.get_data(stn, startdate, enddate)
    df_rain.info()
    df_mlake.info()
    #
    zrxpfile = os.path.join(outputpath,   'silo-'+stn+'.zrxp')
    print('writing zrxp file: '+zrxpfile)
    z = zrxp()
