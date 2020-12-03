
import os
import pandas as pd
import logging
from zrxp_writer import zrxp_writer as zrxp

#
from BOM import BOM as BOM

bom=BOM()
z = zrxp()
path='out_bom'

startdate = '1970-01-01'
enddate =  '2020-12-01'
station_id = 'sp-o10814'
(df, df_level, df_volume) = bom.get_sites_level_volume( bomid=station_id, startdate=startdate, enddate=enddate)
df.to_csv(r'out_bom\Yarrawonga-level_volume.csv')
z.write_bom_storage_dfs2zrxp( path, station_id, 'level', r'Storages/sp-010814/Level/Day.Cmd.O', df_level)
z.write_bom_storage_dfs2zrxp( path, station_id, 'volume', r'Storages/sp-010814/Volume/Day.Cmd.O', df_volume)
'''
startdate = '1987-07-01'
enddate =  '2020-12-01'
station_id = 'sp-o11430'
(df, df_level, df_volume)= bom.get_sites_level_volume( bomid=station_id, startdate=startdate, enddate=enddate)
df.to_csv(r'out_bom\Torrumbarry-level_volume.csv')
z.write_bom_storage_dfs2zrxp( path, station_id, 'level', r'Storages/sp-o11430/Level/Day.Cmd.O', df_level)
z.write_bom_storage_dfs2zrxp( path, station_id, 'volume', r'Storages/sp-o11430/Volume/Day.Cmd.O', df_volume)


startdate = '2007-05-17'
enddate =  '2020-12-01'
station_id = 'A4261093'
(df, df_level, df_volume)= bom.get_sites_level_volume( bomid=station_id, startdate=startdate, enddate=enddate)
df.to_csv(r'out_bom\LakeVictoria-level_volume.csv')
z.write_bom_storage_dfs2zrxp( path, station_id, 'level', r'Storages/A4261093/Level/Day.Cmd.O', df_level)
z.write_bom_storage_dfs2zrxp( path, station_id, 'volume', r'Storages/A4261093/Volume/Day.Cmd.O', df_volume)
'''
