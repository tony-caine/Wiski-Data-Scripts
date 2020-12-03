import xlwings as xw
import pandas as pd

'''
given a full path (including the file name) and a shetname
use the xlwings library to load that worksheet to a dataframe.

assuming the worksheet is clean of white noise and
that there are headings in the top row.

'''


class xl2df:
    def __init__(self, path, sheetname):
        self.path = path
        self.sheetname = sheetname
        self.wb = xw.Book(path)
        self.sht = self.wb.sheets[sheetname]
        self.sheet2dataframe()

    def sheet2dataframe(self):
        '''
        convert the sht into a dataframe
        Assuming there are column names in the top row and
          that they exist in the sht object as first list in x.
        So use the first row as the column names
        todo: what to do if there is white noise in the worksheet
        '''
        x = self.sht.range('A1').options(
            index=False, header=False).expand().value
        self.df = pd.DataFrame(x)
        # remove any white space at beginning or end of names
        # to ensure that don't get headers like ' leadingSpaceTrap '
        headers = [s.strip() for s in x[0]]
        self.df.columns = headers
        self.df.drop(axis=0, index=0, inplace=True)
        # beware ### self.df.dropna(inplace=True)
