import pandas as pd
import numpy as np
from datetime import datetime as dt

class PowerconExtractClass:

    def __init__(self, mysqlconnection, PCID, databaseSource) -> None:
        self.conn = mysqlconnection
        self.databaseSource = databaseSource
        self.PCID = PCID


    
    def RTextractTestPCData(self, maxtimelog) -> pd.DataFrame:
        query = f"SELECT testID as runID, CktIDText as circuitName FROM testdetails a \
        LEFT JOIN programinfo b ON a.ProgramID = b.ProgramID LEFT JOIN cktinfo c \
        ON a.CktInfoID = c.CktInfoID WHERE CreationDateTime > '{maxtimelog}' AND \
        TestID >= 24344 AND programName != 'BOOST CHARGE'"
        testPCTbl = pd.read_sql(query, self.conn)
        testPCTbl['PCID'] = self.PCID
        testPCTbl['databaseSource'] = self.databaseSource
        testPCTbl = testPCTbl[['runID', 'circuitName', 'PCID', 'databaseSource']]
        return testPCTbl
    
    def RTextractTestData(self, maxtimelog) -> pd.DataFrame:
        query = f"SELECT testID as runID, CktIDText as circuitName, LEFT(ProgramName, 3) AS programNo, \
            programName, CreationDateTime as startDate, EndDateTime as endDate, CASE \
            WHEN EndDateTime IS NULL THEN 0 ELSE 1 END AS runStatus \
            FROM testdetails a LEFT JOIN programinfo b ON a.ProgramID = b.ProgramID LEFT JOIN cktinfo c \
            ON a.CktInfoID = c.CktInfoID WHERE CreationDateTime > '{maxtimelog}' AND \
            TestID >= 24344 AND programName != 'BOOST CHARGE'"
        testTbl = pd.read_sql(query, self.conn)
        testTbl['PCID'] = self.PCID
        testTbl.replace({np.nan:None}, inplace = True)

        testTbl = testTbl[['runID', 'circuitName', 'programNo', 'programName', 'startDate', 'endDate', 'runStatus', 'PCID']]
        return testTbl
    
    def RTextractMeasurement(self, maxmeasurementtimelog) -> pd.DataFrame:
        query = f"SELECT a.testID as runID, stepNo, Current as amperage, voltage, temperature, resistance,\
            totalAH, totalWH, RealTimeLog, TIMESTAMPDIFF(MICROSECOND, CreationDateTime, RealTimeLog)/3600000000 \
            AS elapsedTime FROM programdatalog a \
            LEFT JOIN testdetails b \
            ON a.testID = b.testID \
            LEFT JOIN nameconfiguration c \
            ON a.Mode = c.NameCfgID \
            WHERE RealTimeLog  > '{maxmeasurementtimelog}' AND a.TestID >= 24344;"
    
        measurementdf = pd.read_sql(query, self.conn)
        measurementdf['PCID'] = self.PCID
        measurementdf['resistance'] = round(measurementdf['resistance'], 2)
        measurementdf['voltage'] = round(measurementdf['voltage'],2)
        measurementdf['amperage'] = round(measurementdf['amperage'],2)
        measurementdf['temperature'] = round(measurementdf['temperature'], 2)
        measurementdf['totalAH'] = round(measurementdf['totalAH'],2)
        measurementdf['totalWH'] = round(measurementdf['totalWH'], 2)


        measurementdf.columns = ['runID', 'stepNo', 'amperage', 'voltage', 'temperature', 
                                       'resistance', 'totalAH', 'totalWH', 'RealTimeLog', 'elapsedTime', 'PCID']
        return measurementdf

class RectifierDBClass:

    def __init__(self, sqlconnection, ipaddress) -> None:
        self.conn = sqlconnection
        cursor = self.conn.cursor()
        cursor.execute("SELECT PCID, databaseSource, directory FROM PCTbl WHERE IPAddress = ?", ipaddress)
        rows = cursor.fetchall()
        cursor.close()
        for row in rows:
            self.PCID = row[0]
            self.databaseSource = row[1]
            self.directory = row[2]
        
    def extractTestPCData(self) -> pd.DataFrame:
        query = f"SELECT testPCID, PCID, runID FROM TestPCTbl \
        WHERE PCID = '{self.PCID}' AND databaseSource = '{self.databaseSource}'"
        testPCTbl = pd.read_sql(query, self.conn)
        return testPCTbl
    
    def extractProgramTbl(self) -> pd.DataFrame:
        programTbl = pd.read_sql("SELECT * FROM programTbl WHERE isActive = 1", self.conn)
        programTbl['programNo'] = programTbl['programNo'].astype(int)
        return programTbl

    def getmaxtimelog(self) -> dt:
        cursor = self.conn.cursor()
        query = "SELECT LastTestQuery FROM PCStatusTbl WHERE PCID = ?"
        cursor.execute(query, self.PCID)
        maxtimelog = cursor.fetchall()[0][0]
        cursor.close()
        return maxtimelog
    
    def getmaxmeasurementtimelog(self) -> dt:
        cursor = self.conn.cursor()
        query = "SELECT LastMeasurementQuery FROM PCStatusTbl WHERE PCID = ?"
        cursor.execute(query, self.PCID)
        maxmeasurementtimelog = cursor.fetchall()[0][0]
        cursor.close()
        return maxmeasurementtimelog
    
    def extractCoolingTbl(self) -> pd.DataFrame:
        coolingTbl = pd.read_sql(f"SELECT a.testPCID, b.technology FROM TestPCTbl a \
                                JOIN PCTbl b ON a.PCID = b.PCID WHERE a.PCID = {self.PCID}", self.conn)
        return coolingTbl
    
    def extractOnGoingTest(self) -> list:
        cursor = self.conn.cursor()
        cursor.execute("SELECT a.TestPCID, runID FROM testTbl a JOIN testPCTbl b\
        ON a.TestPCID = b.TestPCID WHERE runStatus = 0 and PCID = ?", self.PCID)
        unfinishedtest = cursor.fetchall()
        return unfinishedtest
    

class BafosExtractClass():

    def __init__(self, accessconnection, PCID, databaseSource) -> None:
        self.conn = accessconnection
        self.PCID = PCID
        self.databaseSource = databaseSource

    def RTextractTestPCData(self, maxtimelog) -> pd.DataFrame:
        query = f"SELECT ProcessID as runID, circuitName FROM Process WHERE StartTime > '{maxtimelog}' and StartTime IS NOT NULL"
        testPCTbl = pd.read_sql(query, self.conn)
        testPCTbl['PCID'] = self.PCID
        testPCTbl['databaseSource'] = self.databaseSource
        testPCTbl = testPCTbl[['runID', 'circuitName', 'PCID', 'databaseSource']]
        return testPCTbl

        
    def RTextractTestData(self, maxtimelog) -> pd.DataFrame:
       query = f"SELECT ProcessID as runID, circuitName, ProfileIndex as programNo, ProfileName as programName, \
                StartTime as startDate, EndTime as endDate FROM Process WHERE StartTime > '{maxtimelog}'"
       testTbl = pd.read_sql(query, self.conn)
       testTbl['runStatus'] = testTbl.apply(lambda x: 1 if (x.endDate - x.startDate).total_seconds()/3600 > 10 else 0, axis = 1)
       testTbl['PCID'] = self.PCID
       testTbl.replace({np.nan:None}, inplace = True)

       testTbl = testTbl[['runID', 'circuitName', 'programNo', 'programName', 'startDate', 'endDate', 'runStatus', 'PCID']]
       return testTbl
    
    def RTextractMeasurement(self, maxmeasurementtimelog) -> pd.DataFrame:
        query = f"SELECT ProcessID AS runID, Time as RealTimeLog, Step as stepNo, Current as amperage, \
        voltage, temperature, totalAH, totalWH, ElapsedTime/60 AS ElapsedTimeHr FROM Actual WHERE Time > '{maxmeasurementtimelog}'"
    
        measurementdf = pd.read_sql(query, self.conn)
        measurementdf['PCID'] = self.PCID
        measurementdf['resistance'] = measurementdf['voltage']/measurementdf['amperage']
        measurementdf['resistance'] = round(measurementdf['resistance'], 2)
        measurementdf['resistance'] = measurementdf['resistance'].replace([np.inf, -np.inf], 0)
        measurementdf['resistance'].fillna(0, inplace = True)
        measurementdf['resistance'] = round(measurementdf['resistance'], 2)
        measurementdf['voltage'] = round(measurementdf['voltage'],2)
        measurementdf['amperage'] = round(measurementdf['amperage'],2)
        measurementdf['temperature'] = round(measurementdf['temperature'], 2)
        measurementdf['totalAH'] = round(measurementdf['totalAH'],2)
        measurementdf['totalWH'] = round(measurementdf['totalWH'], 2)


        measurementdf = measurementdf[['runID', 'stepNo', 'amperage', 'voltage', 'temperature', 
                                       'resistance', 'totalAH', 'totalWH', 'RealTimeLog', 'ElapsedTimeHr', 'PCID']]
        measurementdf.columns = ['runID', 'stepNo', 'amperage', 'voltage', 'temperature', 
                                       'resistance', 'totalAH', 'totalWH', 'RealTimeLog', 'elapsedTime', 'PCID']
        return measurementdf