import pandas as pd
from datetime import datetime as dt
from datetime import timedelta


class PowerconOnGoingTests():

    def __init__(self, mysqlconnection, PCID, loadtorectifierdatabaseinstance):
        self.conn = mysqlconnection
        self.PCID = PCID
        self.loadinstance = loadtorectifierdatabaseinstance

    def UpdateTestStatus(self, unfinishedTest):
        for testPCID, test in unfinishedTest:
            updatedStatus = pd.read_sql(f"SELECT testID as runID, creationdatetime, enddatetime\
                                        from testdetails WHERE testID = {test}", self.conn)
            cursor = self.conn.cursor()
            cursor.execute(f"SELECT MAX(RealTimeLog) FROM programdatalog WHERE testID = {test} and \
                           RealTimeLog > '{dt.now() - timedelta(weeks=1)}'")
            lasttimelog = cursor.fetchall()[0][0]
            cursor.close()
            if lasttimelog == None:
                lasttimelog = updatedStatus.loc[0, 'creationdatetime']
            
            runState = updatedStatus.loc[:0]['enddatetime'].apply(lambda x: 1 if x is not None else 0)[0]
            endDate = updatedStatus.loc[0, 'enddatetime']

            self.loadinstance.updateTestData(runState = runState, testPCID = testPCID, endDate = endDate,
                                             lasttimelog = lasttimelog)
        
class BafosOnGoingTests():
    def __init__(self, accessconnection, PCID, loadtorectifierdatabaseinstance):
        self.conn = accessconnection
        self.PCID = PCID
        self.loadinstance = loadtorectifierdatabaseinstance

    def UpdateTestStatus(self, unfinishedTest):
        for testPCID, test in unfinishedTest:
            updatedStatus = pd.read_sql(f"SELECT ProcessID as runID, StartTime as startDate, EndTime as endDate FROM Process WHERE ProcessID = {test}", self.conn)
            runState = updatedStatus.apply(lambda x: 1 if (dt.now() - x.endDate).total_seconds()/3600 > 8 else 0, axis = 1)
            endDate = updatedStatus.loc[0, 'endDate']
            self.loadinstance.updateTestData(runState = runState, testPCID = testPCID, endDate = endDate,
                                             lasttimelog = endDate)