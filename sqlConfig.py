import pypyodbc as pyodbc
import pandas as pd

class SQL_Config:
    def __init__(self, db_host, db_name, db_trusted=True):
        self.db_host = db_host
        self.db_name = db_name
        self.db_trusted = db_trusted
        self.connection_string = 'Driver={SQL Server};Server=' + self.db_host + ';Database=' + self.db_name + ';Trusted_Connection=yes;'
   
    def sqlStatement(self, statement):
        db = pyodbc.connect(self.connection_string)
        return pd.read_sql(statement, db)

    def writeToSQL(self, statement):
        db = pyodbc.connect(self.connection_string)
        cursor = db.cursor()
        cursor.execute(statement)
        db.commit()
        db.close()


