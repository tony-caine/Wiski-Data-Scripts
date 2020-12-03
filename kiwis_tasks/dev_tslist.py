import pandas as pd
from io import StringIO



def csv2df(csvresponse):
    df = pd.read_csv(StringIO(csvresponse), sep=';', header=None, names=['key', 'values'])

    #start by making df for first timeseries
    ts=df.iloc[0]['values']
    numrows=int(df.iloc[1]['values'])
    di=3 #assuming data starts here - ie minimal metadata
    df1=pd.DataFrame({'dates': df.loc[di:di+numrows-1,'key'], ts:df.loc[di:di+numrows-1,'values']} )
    #then add(join) the rest of timeseries on df at time till done
    i=di+numrows
    maxi=len(df.index) #total number of rows
    while i<maxi:
        ts=df.iloc[i]['values']
        numrows=int(df.iloc[i+1]['values'])
        di=i+3 #di is index to start of data rows
        df_next=pd.DataFrame({'dates': df.loc[di:di+numrows-1,'key'], ts:df.loc[di:di+numrows-1,'values']} )
        #join on dates then release index ready for next itteration
        df1 = df1.set_index('dates').join(df_next.set_index('dates'), how='outer')
        df1=df1.reset_index()
        i=i+numrows+3
    return df1

'''
    df2 = pd.DataFrame(index=df['key'].drop_duplicates())
    uniqlen = df2.shape[0]
    # make a wide df from vertical

    for i in range(0, df.shape[0], uniqlen):
        df2[i] = df.loc[i:i+uniqlen-1, 'values'].values
    # keep just the data
    df2.drop(index=['#ts_id', '#rows', '#Timestamp'], inplace=True)
    # and convert the strings to numberic so can manipulate numbers
    for i in range(df2.shape[1]):
        df2.iloc[:, i] = df2.iloc[:, i].astype(float)

    return (df2)
'''

def main():
    filename =r'D:\Kisters\scripts\kiwis_tasks\kiwis-gettsvalues.csv'
    file = open(filename)
    csvresponse = file.read()
    file.close()
    df=csv2df(csvresponse)
    df.to_csv(r'D:\Kisters\scripts\kiwis_tasks\kiwis-gettsvalues_out.csv')
    print(df)

if __name__ == '__main__':  # single underscore
    main()

