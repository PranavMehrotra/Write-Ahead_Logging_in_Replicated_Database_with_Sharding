import os
import numpy as np
import mysql.connector
import time

class SQLHandler:
    def __init__(self,host='localhost',user='root',password='',db='db_server_database', max_retries=5):
        self.host=host
        self.user=user
        self.password=password
        self.db=db
        self.max_retries = max_retries
        self.connected = False

    def connect(self):
        connected=False
        attemps = 0
        while not connected:
            try:
                print(f"Attempt {attemps+1}/{self.max_retries} to connect to the SQL server...")
                self.mydb = mysql.connector.connect(host=self.host,user=self.user,password=self.password)
                self.Use_database(self.db)
                connected=True
                self.connected = True
                print("MYSQL server connected.")
                return "Connected", 200
            
            except Exception as e:
                if attemps == self.max_retries-1:
                    return f"Error while connecting to the server: {e}", 500
            
            attemps += 1
            # Wait for 0.1 seconds before retrying
            time.sleep(0.1)
        
    
    def disconnect(self):
        connected = True
        while connected:
            try:
                if self.mydb.is_connected():
                    self.mydb.close()
                    print("MYSQL server closed.")
                    connected = False
            except Exception as e:
                print("Error while disconnecting:", e)
                pass
        return "Disconnected", 200
    
    def query(self, sql):
        
        try:
            cursor = self.mydb.cursor()
            cursor.execute(sql)
            res=cursor.fetchall()
            # print(sql, res)
            cursor.close()
            self.mydb.commit()
            return res,200
        except mysql.connector.errors.DataError as e:
            return "DataError: data exception", 400
        except mysql.connector.errors.IntegrityError as e:
            return "IntegrityError: Stud_id already exists", 400
        except mysql.connector.errors.ProgrammingError as e:
            return str(e.msg), 400
        except mysql.connector.Error as e:
            return str(e.msg),400
        except Exception as e:
            return str(e), 500

    def Use_database(self,dbname='db_server_database'):
        try:
            res,status = self.query("SHOW DATABASES")
            if status != 200:
                return res, status
            
            if dbname not in [r[0] for r in res]:
                res,status = self.query(f"CREATE DATABASE {dbname}")
                if status != 200:
                    return res, status
                
            res,status = self.query(f"USE {dbname}")
            if status != 200:
                    return res, status
            return dbname,200
        except Exception as e:
            return e, 500

    def Get_table_rows(self, table_name):
        try:
            res,status = self.query(f"SELECT * FROM {table_name};")
            
            if status != 200:
                    return res, status
            return res,200
        except Exception as e:
            return e, 500

    def Create_table(self, tabname, columns, dtypes, pk: list):
        try:
            # Check if the table already exists
            res,status = self.query("SHOW TABLES;")
            if status != 200:
                return res, status
            col_config = ''
            if tabname not in [r[0] for r in res]:
                # Map data types
                dmap = {'Number': 'INT', 'String': 'VARCHAR(32)'}
                col_config = ', '.join([f"{col} {dmap[dtype]}" for col, dtype in zip(columns, dtypes)])
                
                query_str = ""
                if pk != []:
                    query_str = f"CREATE TABLE {tabname} ({col_config}, PRIMARY KEY ({','.join(pk)}));"
                else:
                    query_str = f"CREATE TABLE {tabname} ({col_config});"


                # Execute SQL query to create the table
                res,status = self.query(query_str)
                if status != 200:
                    return res, status
                
            return tabname,200
        except Exception as e:
            return e, 500

    def Insert(self,table_name,row_str,schema):  
        try:
            res,status = self.query(f"INSERT INTO {table_name} ({schema}) VALUES {row_str};")
            if status != 200:
                return res, status
            
            return f"Successfully inserted {row_str}", 200
        except Exception as e:
            return e, 500
        
    def Delete(self,table_name,col, val):
        try:
            res,status = self.query(f"DELETE FROM {table_name} WHERE `{col}`='{val}';")
            if status != 200:
                return res, status
            return f"Successfully deleted {val} from {table_name}", 200
        except Exception as e:
            return e, 500
        
    def Update(self,table_name,col, val, col2, val2):
        try:
            res,status = self.query(f"UPDATE {table_name} SET `{col2}`='{val2}' WHERE `{col}`='{val}';")
            if status != 200:
                return res, status
            return f"Successfully updated {col2} to {val2} where {col} = {val}", 200
        except Exception as e:
            return e, 500
        
    def Drop_table(self,table_name):
        try:
            res,status = self.query(f"DROP TABLE {table_name};")
            if status != 200:
                return res, status
            return f"Successfully dropped {table_name}", 200
        except Exception as e:
            return e, 500
        
    def Clear_table(self,table_name):
        try:
            res,status = self.query(f"DELETE FROM {table_name};")
            if status != 200:
                return res, status
            return f"Successfully cleared {table_name}", 200
        except Exception as e:
            return e, 500