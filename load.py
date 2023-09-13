import pandas as pd
from datetime import datetime as dt
import numpy as np

class LoadtoRectifierDatabase():

    def __init__(self, mssqlconnection):
        self.conn = mssqlconnection

    def updateHealthState(self, healthState, PCID):
        cursor = self.conn.cursor()
        if healthState:
            cursor.execute("UPDATE PCStatusTbl SET HealthState = 1, LastUpdate = ? WHERE PCID = ?", dt.now(), PCID)
        else:
            cursor.execute("UPDATE PCStatusTbl SET HealthState = 0, LastUpdate = ? WHERE PCID = ?", dt.now(), PCID)
        self.conn.commit()
        cursor.close()
    
    def updateTestData(self, runState, testPCID, endDate, lasttimelog):
        cursor = self.conn.cursor()
        if runState == 1:
            cursor.execute("UPDATE testTbl SET runstatus = '1', endDate = ?, lastUpdate = ? WHERE testPCID = ?", 
                                  endDate, dt.now(), testPCID)
            print(testPCID)
        elif (dt.now() - lasttimelog).total_seconds()/60 > 600:
            cursor.execute("UPDATE testTbl SET runstatus = '3', endDate = ?, lastUpdate = ? WHERE testPCID = ?", 
                                  lasttimelog, dt.now(), testPCID)
            print(testPCID)
        else:
            cursor.execute("UPDATE testTbl SET lastUpdate = ? WHERE testPCID = ?", 
                           dt.now(), testPCID)

        self.conn.commit()
        cursor.close()

    def updateTimeStamps(self, maxtimelog, maxmeasurementtimelog, PCID):
        query = "UPDATE PCStatusTbl SET LastTestQuery = ?, LastMeasurementQuery = ? WHERE PCID = ?"
        cursor = self.conn.cursor()
        cursor.execute(query, maxtimelog, maxmeasurementtimelog, PCID)
        self.conn.commit()
        cursor.close()

    def loadTestPCData(self, testPCTbl):
        testPCTbl = testPCTbl[['PCID', 'runID', 'circuitName', 'databaseSource']]
        query = "INSERT INTO TestPCTbl (PCID, runID, circuitName, databaseSource) VALUES (?,?,?,?)"
        values = [tuple(row) for row in testPCTbl.values]
        cursor = self.conn.cursor()
        try:
            cursor.executemany(query, values)
            self.conn.commit()
            print(f"     |Inserted {len(values)} rows to TestPCTbl!")
        except:
            print("      Failed to insert rows!")
        finally:
            cursor.close()

    def loadTestData(self, testTbl):
        query = f"INSERT INTO testTbl (testPCID, circuitName, startDate, endDate, runStatus, \
            programNo, programName, pauseStep, programAH, programTime, lastUpdate) \
            VALUES (?,?,?,?,?,?,?,?,?,?,?)"
        
        values = [tuple(row) for row in testTbl.values]
        cursor = self.conn.cursor()
        try:
            cursor.executemany(query, values)
            self.conn.commit()
            print(f"     |Inserted {len(values)} rows to testTbl!")  
        except:
            print("Failed to insert rows!")  
        finally:
            cursor.close()


    def loadMeasurementData(self, measurementTbl):
        query = "INSERT INTO measurementTbl (TestPCID, RealTimeLog, stepNo, voltage, temperature, amperage,\
        resistance, totalWH, totalAH, elapsedTime) VALUES (?,?,?,?,?,?,?,?,?,?)"

        values = [tuple(row) for row in measurementTbl.values]
        cursor = self.conn.cursor()
        try:
            cursor.executemany(query, values)
            self.conn.commit()
            print(f"     |Inserted {len(values)} rows to measurementTbl!")
        except:
            print("Failed to insert rows!")
        finally:
            cursor.close()