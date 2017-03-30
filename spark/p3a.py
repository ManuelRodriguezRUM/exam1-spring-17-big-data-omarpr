#Code here

import csv

data = sc.textFile('/home/omar/exam1/studentsPR.csv')

dataMap = data.map(lambda x: x.split(','))

filter = dataMap.filter(lambda x: x[2] == '71381').filter(lambda x: x[5] == 'F')

filter.collect()
