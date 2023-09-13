import pandas as pd
from datetime import datetime as dt
from datetime import timedelta
import numpy as np


class Transform():

    def processTestTbl(testTbl, programTbl, coolingTbl, testPCTbl):
        testTbl1 = pd.merge(testTbl, testPCTbl, on = ['runID','PCID'], how = 'inner')
        testTbl2 = pd.merge(testTbl1, coolingTbl, on = ['testPCID'], how = 'inner')
        testTbl2 = pd.merge(testTbl2, programTbl, on = ['programNo', 'technology'], how = 'left')
        testTbl2 = testTbl2[['testPCID', 'circuitName', 'startDate', 'endDate', 'runStatus',
                             'programNo', 'programName_x', 'pauseStep', 'programAH', 'programTime']]
        testTbl2.columns = ['testPCID', 'circuitName', 'startDate', 'endDate', 'runStatus',
                             'programNo', 'programName', 'pauseStep', 'programAH', 'programTime']
        testTbl2['lastUpdate'] = dt.now()
        testTbl2.replace({np.nan:None}, inplace = True)
        return testTbl2
    
    def processMeasurementTbl(measurementTbl, testPCTbl):
        measurementTbl1 = pd.merge(measurementTbl, testPCTbl, on = ['runID', 'PCID'], how = 'inner')
        measurementTbl1 = measurementTbl1[['testPCID', 'RealTimeLog', 'stepNo', 'voltage', 'temperature', 
                                           'amperage', 'resistance', 'totalWH', 'totalAH', 'elapsedTime']]
        measurementTbl1.sort_values(by = ['testPCID', 'RealTimeLog'], inplace = True)
        measurementTbl1.drop_duplicates(inplace = True)
        return measurementTbl1
    
    def getmaxtimelognew(testTbl):
        maxtimelognew = testTbl['startDate'].max() + timedelta(milliseconds = 200)
        return maxtimelognew

    def getmaxmeasurementtimelognew(measurementTbl):
        maxmeasurementtimelognew = measurementTbl['RealTimeLog'].max()
        return maxmeasurementtimelognew