from helper import SQLHandler
import json

class Manager:
    def __init__(self,host='localhost',user='root',password='',db='server_database'):
        self.sql_handler=SQLHandler(host=host,user=user,password=password,db=db)
        self.schema: list = []
        self.schema_set: set = set()
        self.schema_str: str = ''
        
    def Config_database(self,config):
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
            
                if status != 200:
                    return message, status

            if isinstance(config, str):
                config = json.loads(config)
            
            elif isinstance(config, dict):
                config = config
            if 'schema' not in config:
                return "'schema' field missing in request", 400
               
            if 'shards' not in config:
                return "'shards' field missing in request", 400
            

        
            schema = config.get('schema', {})
            shards = config.get('shards', [])
            shards = set(shards)
            # print(f"Shards: {shards}", flush=True)
            columns = schema.get('columns', [])
            dtypes = schema.get('dtypes', [])
            
            
            self.schema = list(columns)
            self.schema_set = set(columns)
            self.schema_str = (',').join(self.schema)
            if len(self.schema_set) != len(dtypes):
                return "Number of columns and dtypes don't match", 400
            

            if schema and columns and dtypes and shards:    
                for shard in shards:
                    message, status = self.sql_handler.Create_table(shard, columns, dtypes)
                    # print(message, status, flush=True)
                    # print(f"status: {status}", flush=True)
                    if status != 200:
                        return message, status
                    
                return "Tables created", 200
            else:
                return "Invalid JSON format", 400
        
        except Exception as e:
            return e, 500
    
    def Copy_database(self,json_data):
        
        try:
            if 'shards' not in json_data:
                return "'shards' field missing in request",400
        
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status            

            if isinstance(json_data, str):
                schema = json.loads(json_data)
            
            elif isinstance(json_data, dict):
                schema = json_data
            
            database_copy = {}
            for table_name in schema['shards']:   
                
                table_rows,status = self.sql_handler.Get_table_rows(table_name) 
                # Remove first column (auto increment ID column) from each row and convert to list of dictionaries
                len_row = len(table_rows[0]) if table_rows else 0
                dict_table_rows = [{self.schema[i-1]: row[i] for i in range(1, len_row)} for row in table_rows]
                database_copy[table_name] = dict_table_rows
            
                if status != 200:
                    message = table_rows
                    return message, status
                
            return database_copy,200

        except Exception as e:

            print(f"Error copying database: {str(e)}")
            return e,500
    
    def Read_database(self,request_json):
        try:
            
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status
                
            if 'shard' not in request_json:
                return "'shard' field missing in request", 400
    
            stud_id_obj = request_json.get("Stud_id",{})

            if "low" not in stud_id_obj or "high" not in stud_id_obj:
                return "Both low and high values are required for reading a range of entries.", 400
            
            
            table_name = request_json['shard']
            low, high = stud_id_obj["low"], stud_id_obj["high"]

            table_rows,status = self.sql_handler.Get_range(table_name,low,high, "Stud_id")
            if status==200: 
                # Remove first column (auto increment ID column) from each row and convert to list of dictionaries
                len_row = len(table_rows[0]) if table_rows else 0
                dict_table_rows = [{self.schema[i-1]: row[i] for i in range(1,len_row)} for row in table_rows]            
                return dict_table_rows,status
            else:
                message = table_rows
                
                return message,status
        except Exception as e:
            
            print(f"Error reading database: {str(e)}")
            return e,500
        
    
    def Write_database(self,request_json):
        
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status, -1

            if 'shard' not in request_json:
                return "'shard' field missing in request", 400, -1
            
            # if 'curr_idx' not in request_json:
            #     return "'curr_idx' field missing in request", 400, -1
            
            if 'data' not in request_json:
                return "'data' field missing in request", 400, -1
        
            tablename = request_json.get("shard")
            data = request_json.get("data")
            
            # curr_idx is not to be used for this assignment
            # curr_idx = request_json.get("curr_idx")
            num_entry = len(data)

            # res,status = self.sql_handler.query(f"SELECT COUNT(*) FROM {tablename}")
            # if status != 200:
            #     return res, status, -1
            
            # valid_idx = res[0][0]

            # if(curr_idx!=valid_idx+1):                
            #     return "Invalid current index provided",400,valid_idx
            valid_idx = 0
            row_str = ''
            
            
            for v in data:
                missing_columns = self.schema_set - set(v.keys())

                if missing_columns:
                    missing_column = missing_columns.pop()  # Get the first missing column
                    return f"{missing_column} not found", 400, valid_idx
                
                # row = ''
                ### 1. Why is explicit check for string type required? Can't we just use f"{v[k]},"?
                ### 2. Make more efficient by using join
                ## Slow
                # for k in self.schema:
                #     if type(v[k]) == str:
                #         row += f"'{v[k]}',"
                #     else:
                #         row += f"{v[k]},"
                # row = row[:-1]

                ## More efficient way
                row = ','.join([f"'{v[k]}'" for k in self.schema])
                row_str += f"({row}),"
            row_str = row_str[:-1]
             
            
            # schema_str = (',').join(self.schema)
            message, status = self.sql_handler.Insert(tablename, row_str,self.schema_str)
            
            if status != 200:    
                return message, status,valid_idx
            
            # check if the no of entries added is equal to the no of entries in the data object
            rows_added = self.sql_handler.mydb.cursor().rowcount
            if rows_added != num_entry:
                return "Could not add all data entries", 500, valid_idx + rows_added
            
            return "Data entries added", 200, valid_idx+num_entry
        
        except Exception as e:
            return e, 500, -1
        
    def Update_database(self,request_json):
       
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

                
            if 'shard' not in request_json:
                return "'shard' field missing in request", 400
            
            if 'Stud_id' not in request_json:
                return "'Stud_id' field missing in request", 400
            
            if 'data' not in request_json:
                return "'data' field missing in request", 400
            
            stud_id = request_json.get("Stud_id")
            data = request_json.get("data")
            tablename = request_json.get("shard")

            # Check if the 'Stud_id' in the payload matches the 'Stud_id' in the 'data' object
            if stud_id == data["Stud_id"]:
                message, status = self.sql_handler.Update_database(tablename, stud_id,data,'Stud_id')
            
                if status==200:
                    if message == []:
                        return "No matching entries found",404
                    return message,200

                else:                    
                    return  message,status
            else:
                return "Stud_id in 'data' does not match Stud_id in payload", 400

        except Exception as e:            
            return e,500
        
    
    def Delete_database(self,request_json):
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

            if 'shard' not in request_json:
                return "'shard' field missing in request", 400

            if 'Stud_id' not in request_json:
                return "'Stud_id' field missing in request", 400
            
            stud_id = request_json.get("Stud_id")
            tablename = request_json.get("shard")

            message, status = self.sql_handler.Delete_entry(tablename, stud_id,'Stud_id')
            
            return message,status
        
        except Exception as e:    
            return e,500 
    

    def Commit(self):
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

            self.sql_handler.mydb.commit()
            return "Changes committed", 200

        except Exception as e:
            return e,500
        
    def Rollback(self):
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

            self.sql_handler.mydb.rollback()
            return "Changes rolled back", 200

        except Exception as e:
            return e,500
        
    def Reconfig_table(self, table_name, data):
        
        try:
            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status
                
                
                # clear the table
                message, status = self.sql_handler.Clear_table(table_name)
                if status != 200:
                    return message, status
                
                print(f"Table {table_name} cleared successfully", flush=True)
                
                # copy the consistent data to the table
                shard = table_name         
                request_json = {"shard": shard, "data": data}
                message, status, _ = self.Write_database(request_json)
                
                if status != 200:
                    return message, status
                
                print(f"Data copied successfully to {shard}", flush=True)
                
                return f"Table {shard} reconfigured successfully", 200
            
            else:
                return "Invalid JSON format for reconfiguring table", 400
            
        except Exception as e:
            return e, 500
            