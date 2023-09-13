import pyodbc
import mysql.connector


class databaseconnect():

    @classmethod
    def connectSQLServer(cls, server:str, database:str, username:str = '', password:str = ''):
        try:
            #standard query to connect to MS SQL server
            cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
            #print('Connection successful')
        except:
            print('Connection failed')
        return cnxn
    
    @classmethod
    def connectAccessFile(cls, directory, file_name):
        driver_str = 'Microsoft Access Driver (*.mdb, *.accdb)'

        full_path = directory + file_name
        conn_str = f'DRIVER={driver_str};DBQ={full_path};'
        try:
            #standard query to connect to MS SQL server
            cnxn = pyodbc.connect(conn_str)
            #print('Connection successful')
            return cnxn
        except:
            print('Connection failed')

    @classmethod
    def connectMySQLServer(cls,server:str, port:str, 
                           database:str, username:str = '', password:str = ''):

        # Establish a connection
        try:
            connection = mysql.connector.connect(host=server,port=port,database=database,user=username,password=password)

            if connection.is_connected():
                #print("Connected to MySQL database")
                
                return connection
        except mysql.connector.Error as e:
            print("Error:", e)
