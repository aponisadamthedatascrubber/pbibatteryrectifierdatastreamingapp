import ping3
from datetime import datetime as dt
from connection import *
from extract import *
from process import *
from transform import *
from load import *
from constants import POWERCON_PW

def check_ping(host):
    try:
        response_time = ping3.ping(host)
        if response_time is not None:
            return True
        else:
            return False
    except ping3.exceptions.PingError as e:
        return False

BAFOS_CREDENTIALS = ['172.31.2.92']

def RTStreamingBAFOS(BAFOS_CREDENTIALS, TARGET_CREDENTIALS):
    target_conn = databaseconnect.connectSQLServer(server = TARGET_CREDENTIALS['TARGET_HOST'], 
                                                   database = TARGET_CREDENTIALS['TARGET_DB'])
    loadrectifierdatabaseinstance = LoadtoRectifierDatabase(target_conn)


    for ipaddress in BAFOS_CREDENTIALS:
        healthState = check_ping(ipaddress)
        rectifierdbinstance = RectifierDBClass(target_conn, ipaddress)
        PCID = rectifierdbinstance.PCID
        databaseSource = rectifierdbinstance.databaseSource
        directory = rectifierdbinstance.directory
        loadrectifierdatabaseinstance.updateHealthState(healthState, PCID)
        if healthState:
            try:
                src_conn = databaseconnect.connectAccessFile(directory = directory, file_name = databaseSource)
                maxtimelog = rectifierdbinstance.getmaxtimelog()
                bafosextractinstance = BafosExtractClass(src_conn, PCID, databaseSource)
                testPCTbl = bafosextractinstance.RTextractTestPCData(maxtimelog)
                if testPCTbl.shape[0] > 0:
                    loadrectifierdatabaseinstance.loadTestPCData(testPCTbl)
                    testTbl = bafosextractinstance.RTextractTestData(maxtimelog)
                    testPCTbl2 = rectifierdbinstance.extractTestPCData()
                    programTbl = rectifierdbinstance.extractProgramTbl()
                    coolingTbl = rectifierdbinstance.extractCoolingTbl()
                    testTbl2 = Transform.processTestTbl(testTbl = testTbl, programTbl = programTbl,
                                                     coolingTbl = coolingTbl, testPCTbl = testPCTbl2)
                    maxtimelognew = Transform.getmaxtimelognew(testTbl)
                    loadrectifierdatabaseinstance.loadTestData(testTbl2)

                else:
                    testPCTbl2 = rectifierdbinstance.extractTestPCData()
                    maxtimelognew = maxtimelog
                
                maxmeasurementtimelog = rectifierdbinstance.getmaxmeasurementtimelog()
                measurementTbl = bafosextractinstance.RTextractMeasurement(maxmeasurementtimelog)
                if measurementTbl.shape[0] > 0:
                    measurementTbl2 = Transform.processMeasurementTbl(measurementTbl, testPCTbl2)
                    maxmeasurementtimelognew = Transform.getmaxmeasurementtimelognew(measurementTbl2)
                    loadrectifierdatabaseinstance.loadMeasurementData(measurementTbl2)
                else:
                    maxmeasurementtimelognew = maxmeasurementtimelog

                loadrectifierdatabaseinstance.updateTimeStamps(maxtimelognew, maxmeasurementtimelognew, PCID)
                unfinishedTest = rectifierdbinstance.extractOnGoingTest()
                BafosOnGoingTests(src_conn, PCID, loadrectifierdatabaseinstance).UpdateTestStatus(unfinishedTest)
            except:
                print(f"Cannot connect to {ipaddress} at {dt.now()}")
        else:
            pass


def RTStreamingPowerCon(POWERCON_CREDENTIALS, TARGET_CREDENTIALS):
    target_conn = databaseconnect.connectSQLServer(server = TARGET_CREDENTIALS['TARGET_HOST'], 
                                                   database = TARGET_CREDENTIALS['TARGET_DB'])
    loadrectifierdatabaseinstance = LoadtoRectifierDatabase(target_conn)


    for hostPC in POWERCON_CREDENTIALS.keys():
        ipaddress = POWERCON_CREDENTIALS['IP']
        database = POWERCON_CREDENTIALS['db']
        username = POWERCON_CREDENTIALS['user']
        healthState = check_ping(hostPC)
        rectifierdbinstance = RectifierDBClass(target_conn, ipaddress)
        PCID = rectifierdbinstance.PCID
        databaseSource = rectifierdbinstance.databaseSource
        loadrectifierdatabaseinstance.updateHealthState(healthState, PCID)
        if healthState:
            try:
                src_conn = databaseconnect.connectMySQLServer(server = ipaddress, port = 3306, 
                                                              database = database, username = username, password = POWERCON_PW)
                maxtimelog = rectifierdbinstance.getmaxtimelog()
                
                powerconextract = PowerconExtractClass(src_conn, PCID, databaseSource)
                testPCTbl = powerconextract.RTextractTestPCData(maxtimelog)
                if testPCTbl.shape[0] > 0:
                    loadrectifierdatabaseinstance.loadTestPCData(testPCTbl)
                    testTbl = powerconextract.RTextractTestData(maxtimelog)
                    testPCTbl2 = rectifierdbinstance.extractTestPCData()
                    programTbl = rectifierdbinstance.extractProgramTbl()
                    coolingTbl = rectifierdbinstance.extractCoolingTbl()
                    testTbl2 = Transform.processTestTbl(testTbl = testTbl, programTbl = programTbl,
                                                     coolingTbl = coolingTbl, testPCTbl = testPCTbl2)
                    maxtimelognew = Transform.getmaxtimelognew(testTbl)
                    loadrectifierdatabaseinstance.loadTestData(testTbl2)

                else:
                    testPCTbl2 = rectifierdbinstance.extractTestPCData()
                    maxtimelognew = maxtimelog
                
                maxmeasurementtimelog = rectifierdbinstance.getmaxmeasurementtimelog()
                measurementTbl = powerconextract.RTextractMeasurement(maxmeasurementtimelog)
                if measurementTbl.shape[0] > 0:
                    measurementTbl2 = Transform.processMeasurementTbl(measurementTbl, testPCTbl2)
                    maxmeasurementtimelognew = Transform.getmaxmeasurementtimelognew(measurementTbl2)
                    loadrectifierdatabaseinstance.loadMeasurementData(measurementTbl2)
                else:
                    maxmeasurementtimelognew = maxmeasurementtimelog

                loadrectifierdatabaseinstance.updateTimeStamps(maxtimelognew, maxmeasurementtimelognew, PCID)


                unfinishedTest = rectifierdbinstance.extractOnGoingTest()
                PowerconOnGoingTests(src_conn, PCID, loadrectifierdatabaseinstance).UpdateTestStatus(unfinishedTest)
            except:
                print(f"Cannot connect to {hostPC} at {dt.now()}")
        else:
            pass