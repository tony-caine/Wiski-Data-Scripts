import os
import datetime

'''
#ZRXPVERSION3014.03|*|
#REXCHANGE49002.daily_rain
#LAYOUT(timestamp,value,status)|*|
20180701000000 0.0 1
20180702000000 0.0 1

'''
qualcode = 1


class zrxp_writer:
    '''
    the zrxpdata dict is full set of stations timeseries data
    the key is the import number
    the list of data is tupple ( (y,m,d), value)

    eg of element in zrxpdata
    AccessLicenceAccounts/16801-GS/DYFfit/ \
        FYear.Total[((2017, 7, 1), '0'), ((2018, 7, 1), '0'),
                     ((2019, 7, 1), '52.800000000000004')]

    each set of data needs zrxp header
    then each data element on a row

    '''

    def write_header(self, fh, importnumber):
        fh.write("#ZRXPVERSION3014.03|*|\n")
        fh.write("#REXCHANGE" + importnumber + "\n")
        fh.write("#LAYOUT(timestamp,value,status)|*|\n")

    def write_wra_dict2zrxp(self, file, zrxpdata):
        with open(file, 'w+') as fh:
            for key in zrxpdata:
                self.write_header(fh, key)
                data = zrxpdata[key]
                for ((y, m, d), val) in data:
                    dt = datetime.datetime(y, m, d)
                    datestr = dt.strftime("%Y%m%d%H%M%S")
                    val = float(val)
                    datarow = f'{datestr} {"%.2f" % round(val, 2)} {qualcode}\n'
                    fh.write(datarow)
                fh.write("\n")
            fh.write("\n")
        fh.close()

    def write_silo_dfs2zrxp(self, outputpath, station, typestring, importNumber, df):
        zrxpfile = os.path.join(outputpath,   'silo-'+station+'_'+typestring+'.zrxp')
        with open(zrxpfile, 'w') as fh:
            self.write_header(fh, importNumber)
            for index, row in df.iterrows():
                datestr = index.strftime("%Y%m%d%H%M%S")
                val = float(row['value'])
                quality = str(int(row['quality']))
                datarow = f'{datestr} {"%.2f" % round(val, 2)} {quality}\n'
                fh.write(datarow)
            fh.write("\n")
        fh.close()

    def write_kiwis_date_value_dfs2zrxp(self, outputpath, filename, importNumber, df):
        zrxpfile = os.path.join(outputpath,   filename+'.zrxp')
        with open(zrxpfile, 'w') as fh:
            self.write_header(fh, importNumber)
            for i in df.index:
                date = df['dates'][i]
                (y, m, d) = date.split('-')
                datestr = f'{y}{m}{d}000000'
                val = df['values'][i]
                quality = str(1)
                datarow = f'{datestr} {"%.2f" % round(val, 2)} {quality}\n'
                fh.write(datarow)
            fh.write("\n")
        fh.close()


'''
f = open('zrxpdata.txt', 'w')
for key in zrxpdata:
    f.write('zrxpdata: ' + key)
    f.write(str(zrxpdata[key]))
    f.write('\n')
f.close()
'''
