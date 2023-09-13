CREATE DATABASE rectifierdatabase

CREATE TABLE PCTbl (
    PCID INT IDENTITY(1,1),
    PCName VARCHAR(10),
    technology VARCHAR(20),
    rectifierBrand VARCHAR(20),
    IPAddress VARCHAR(20),
    formationBldg VARCHAR(20),
    formationLine VARCHAR(20),
    databaseSource VARCHAR(20),
    directory VARCHAR(20),
    description VARCHAR(100),
    isActive BIT
    lastUpdate DATETIME
)

CREATE TABLE PCStatusTbl (
    PCID INT,
    status INT,
    lastUpdate DATETIME,
    lastTestQuery DATETIME,
    lastMeasurementQuery DATETIME
)

CREATE TABLE CircuitTbl (
    circuitID INT IDENTITY(1,1),
    PCID INT,
    circuitStandardName VARCHAR(20),
    circuitPlantingName VARCHAR(20),
    condition VARCHAR(20),
    isActive BIT
)

CREATE TABLE ProgramTbl (
    programID INT IDENTITY(1,1)
    programNo INT,
    programName VARCHAR(50),
    pauseStep INT,
    programAH FLOAT,
    programTime FLOAT,
    PPC INT,
    size VARCHAR(10),
    isActive BIT,
    lastUpdate DATETIME,
)

CREATE TABLE testPCTbl (
    testPCID INT IDENTITY(1,1),
    PCID INT,
    runID INT,
    databaseSource VARCHAR(20),
    circuitName VARCHAR(20)
)

CREATE TABLE testTbl (
    testPCID INT,
    circuitID INT,
    startDate DATETIME,
    endDate DATETIME,
    status INT,
    lastUpdate DATETIME,
    programNo VARCHAR(20),
    programAH FLOAT,
    programTime FLOAT
)

CREATE measurementTbl (
    measurementID INT IDENTITY(1,1),
    testPCID INT,
    realtimelog DATETIME,
    stepNo INT,
    voltage FLOAT,
    temperature FLOAT,
    amperage FLOAT,
    resistance FLOAT,
    totalWH FLOAT,
    totalAH FLOAT,
    elapsedTime FLOAT
)