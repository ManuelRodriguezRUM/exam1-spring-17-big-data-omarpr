#Code here

import csv

data = sc.textFile('/home/omar/exam1/escuelasPR.csv')

dataMap = data.map(lambda x: x.split(','))

filter = dataMap.filter(lambda x: (x[2] == 'Ponce' or x[2] == 'Cabo Rojo' or x[2] == 'Dorado'))

filter.collect()
