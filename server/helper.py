import os
import numpy as np
import mysql.connector
import time

class SQLHandler:
    def __init__(self,host='localhost',user='root',password='',db='server_database', max_retries=5):
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
                self.mydb = mysql.connector.connect(host=self.host,user=self.user, password=self.password)
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
            self.mydb.commit()          ## Commit the changes
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

    def Use_database(self,dbname='server_database'):
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

    def Drop_database(self,dbname='server_database'):
        try:
            res=res,status = self.query("SHOW DATABASES")
            if status != 200:
                    return res, status
            
            if dbname in [r[0] for r in res]:
                res,status = self.query(f"DROP DATABASE {dbname}")
                if status != 200:
                    return res, status
            else:
                return f"Database with name {dbname} does not exist in the server",404    
        
            return "Successfully dropped database",200
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

    def Create_table(self, tabname, columns, dtypes):
        try:
            # Check if the table already exists
            res,status = self.query("SHOW TABLES;")
            if status != 200:
                return res, status
            col_config = ''
            if tabname not in [r[0] for r in res]:
                # Map data types
                dmap = {'Number': 'INT', 'String': 'VARCHAR(32)'}
                for c,d in zip(columns,dtypes):
                    if c == columns[0]:
                        col_config+=f", {c} {dmap[d]} UNIQUE"
                    else:
                        col_config+=f", {c} {dmap[d]}"
            
                # Execute SQL query to create the table
                res,status = self.query(f"CREATE TABLE {tabname} (ID INT AUTO_INCREMENT {col_config}, PRIMARY KEY (ID,{columns[0]}));")
                if status != 200:
                    return res, status
                
            return tabname,200
        except Exception as e:
            return e, 500
        
    
    # def Drop_table(self,tabname):
    #     try:
    #         res, status = self.query(f"SHOW TABLES;")
    #         if status != 200:
    #             return res, status
            
    #         if tabname not in [r[0] for r in res]:
    #             return f"Table with name {tabname} does not exist in the database, hence cannot be dropped",404
            
    #         res, status = self.query(f"DROP TABLE {tabname}")
    #         if status != 200:
    #             return res, status
            
    #         return "Table dropped successfully",200
    #     except Exception as e:
    #         return e, 500


    def Clear_table(self,tabname):
        
        try:
            res,status = self.query(f"SHOW TABLES;")
            if status != 200:
                return res, status
            
            if tabname not in [r[0] for r in res]:
                return f"Table with name {tabname} does not exist in the database, hence cannot be cleared",404
            
            # Execute SQL query to clear the table
            res,status = self.query(f"DELETE FROM {tabname}")
            if status != 200:
                return res, status
            
            # # check if the table is empty
            # res,status = self.query(f"SELECT * FROM {tabname}")
            # if status != 200:
            #     return res, status
            
            # if len(res) != 0: # Table not cleared
            #     return f"Could not fully clear table {tabname}", 500
    
            # Table cleared successfully
            return f"Table {tabname} cleared successfully!", 200

        except Exception as e:
            return e, 500

    def Get_range(self,table_name,low, high, col):
        
        try:
            # Execute SQL query to retrieve entries within the specified range
            res,status = self.query(f"SELECT * FROM {table_name} WHERE `{col}`>={low} AND `{col}`<{high}")
            if status != 200:
                    return res, status
            # Check if any rows were returned
            if len(res) == 0:
                return "No matching entries found",404
            else:
                # Return the list of values from the query result
                return res,200
        except Exception as e:
            return e, 500


    def Delete_entry(self,table_name,idx,col):
        try:
            res,status = self.query(f"SELECT * FROM {table_name} WHERE `{col}`='{idx}'")
            if status != 200:
                return res, status
            if res == []:
                return "No matching entries found",404
            res,status = self.query(f"DELETE FROM {table_name} WHERE `{col}`='{idx}';")
            if status != 200:
                return res, status
            else:
                return res, status
        except Exception as e:
            return e,500


    def Update_database(self,table_name,Stud_id,updated_val,col):
        try:
            res,status = self.query(f"SELECT * FROM {table_name} WHERE `{col}`={Stud_id}")
            if status != 200:
                return res, status
            
            if res == []:
                return "No matching entries found",404
            
            # Iterate through the key-value pairs of the dictionary
            # update_queries = []
            # for col, val in updated_val.items():
            #     # Check the type of the value
            #     if isinstance(val, str):
            #         # For string values, enclose in single quotes
            #         update_queries.append(f"{col}='{val}'")
            #     else:
            #         update_queries.append(f"{col}={val}")

            # # Join the update queries into a single string
            # update_query_str = ', '.join(update_queries)

            update_query_str = ', '.join([f"{col}='{val}'" for col, val in updated_val.items()])

            # Execute the update query
            res,status = self.query(f"UPDATE {table_name} SET {update_query_str} WHERE Stud_id={Stud_id}")
            if status != 200:
                    return res, status
            print("Entry updated successfully", flush=True)
            return "Entry updated successfully",200
        except Exception as e:
            return e,500

    def Insert(self,table_name,row_str,schema):  
        try:
            res,status = self.query(f"INSERT INTO {table_name} ({schema}) VALUES {row_str};")
            if status != 200:
                return res, status
            
            return f"Successfully inserted", 200
        except Exception as e:
            return e, 500

