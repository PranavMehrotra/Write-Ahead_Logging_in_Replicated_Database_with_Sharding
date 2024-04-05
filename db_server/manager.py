from helper import SQLHandler
import json

class Manager:
    def __init__(self,host='localhost',user='root',password='',db='db_server_database'):
        self.sql_handler=SQLHandler(host=host,user=user,password=password,db=db)
        self.schemas = {}
        
        
    def Config_database(self,config):
        try:
            
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                
                if status != 200:
                    return message, status

            # if isinstance(config, str):
            #     config = json.loads(config)
        
            schemas = config.get('schemas', {})
            for table in schemas.keys():
                schema = schemas[table]
                columns = list(schema.get('columns', []))
                dtypes = schema.get('dtypes', [])
                pk = schema.get('pk', [])
                unique_cols = set(columns)
                if len(unique_cols) != len(dtypes):
                    return f"Number of columns and dtypes don't match for table {table}", 400

                if schema and columns and dtypes:    
                    message, status = self.sql_handler.Create_table(table, columns, dtypes, pk)
                    
                    if status == 200:
                        self.schemas[table] = {"columns": columns, "dtypes": dtypes, "pk": pk}
                    else:
                        return message, status
                else:
                    return "Invalid JSON format", 400
                
            return "Database configured", 200
        
        except Exception as e:
            return e, 500
    
    def Read_database(self):
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status           
            
            database_copy = {}
            for table_name in self.schemas.keys():   
                
                table_rows,status = self.sql_handler.Get_table_rows(table_name)        
                if status != 200:
                    message = table_rows
                    return message, status
            
                database_copy[table_name] = table_rows
                
            return database_copy, 200

        except Exception as e:

            print(f"Error copying database: {str(e)}")
            return e, 500
        
    
    def Write_database(self,request_json):
        
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

            if 'table' not in request_json:
                return "'table' field missing in request", 400
        
            tablename = request_json.get("table")
            data = request_json.get("data", [])
            
            if tablename not in self.schemas:
                return f"Table {tablename} not found", 400
            
            row_str = ''
            # print(f"manager: data: {data}", flush=True)
            schema = self.schemas[tablename]["columns"]
            # print(f"manager: schema: {schema}", flush=True)
            schema_set = set(schema)
            for v in data:
                missing_columns = schema_set - set(v.keys())

                if missing_columns:
                    missing_column = missing_columns.pop()  # Get the first missing column
                    return f"{missing_column} not found", 400
                
                # row = ''
                ## Slow
                # for k in schema:
                #     if type(v[k]) == str:
                #         row += f"'{v[k]}',"
                #     else:
                #         row += f"{v[k]},"
                # row = row[:-1]

                ## More efficient way
                row = ','.join([f"'{v[k]}'" for k in schema])
                row_str += f"({row}),"
            row_str = row_str[:-1]
             
            
            schema_str = (',').join(schema)
            message, status = self.sql_handler.Insert(tablename, row_str,schema_str)
            
            if status != 200:    
                return message, status
            
            return "Data entries added", 200
        
        except Exception as e:
            return e, 500


    def Update_database(self,request_json):
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

            if 'table' not in request_json:
                return "'table' field missing in request", 400

            if 'column' not in request_json or 'update_column' not in request_json:
                return "'column' or 'update_column' field missing in request", 400

            tablename = request_json.get("table")
            column = request_json.get("column")
            keys = request_json.get("keys", [])
            update_column = request_json.get("update_column")
            update_vals = request_json.get("update_vals", []) 
            
            for key,val in zip(keys,update_vals):
                message, status = self.sql_handler.Update(tablename, column, key, update_column, val)
                if status != 200:
                    return message, status
                
            return "Data entries updated", 200
        
        except Exception as e:
            return e, 500
    
    def Delete_entry(self,request_json):
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

            if 'table' not in request_json:
                return "'table' field missing in request", 400
            
            if 'column' not in request_json:
                return "'column' field missing in request", 400
        
            tablename = request_json.get("table")
            column = request_json.get("column")
            keys = request_json.get("keys", [])
            
            for key in keys:
                message, status = self.sql_handler.Delete(tablename, column, key)
                if status != 200:
                    return message, status
                
            return "Data entries deleted", 200
        
        except Exception as e:
            return e, 500
        
    def Delete_table(self,request_json):
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

            if 'table' not in request_json:
                return "'table' field missing in request", 400
        
            tablename = request_json.get("table")
            message, status = self.sql_handler.Drop_table(tablename)
            if status != 200:
                return message, status
            # Remove table from schema, do not raise error if table not found
            self.schemas.pop(tablename, None)            
            return "Table deleted", 200
        
        except Exception as e:
            return e, 500
        
    def Clear_table(self,request_json):
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

            if 'table' not in request_json:
                return "'table' field missing in request", 400
        
            tablename = request_json.get("table")
            message, status = self.sql_handler.Clear_table(tablename)
            if status != 200:
                return message, status
            
            return "Table cleared", 200
        
        except Exception as e:
            return e, 500