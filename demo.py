#import sys
#print(sys.version)
from dbfpy import dbf
dbf.demoCreate('test.dbf')
dbf.demoRead('..\\table1.dbf')
#db = dbf.Dbf('fox_samp.dbf')
