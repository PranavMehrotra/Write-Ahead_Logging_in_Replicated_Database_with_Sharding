import asyncio
from helper import SQLHandler
import json
import aiohttp
from RWLock import RWLock
from typing import Dict, List, TextIO

SERVER_PORT = 5000
MAJORITY_CRITERIA = 0.5
WRITE_STR_LOG = "write"
UPDATE_STR_LOG = "update"
DELETE_STR_LOG = "delete"

class Manager:
    def __init__(self,host='localhost',user='root',password='',db='server_database'):
        self.sql_handler=SQLHandler(host=host,user=user,password=password,db=db)
        self.schema: list = []
        self.schema_set: set = set()
        self.schema_str: str = ''
        # Log file for write ahead logging
        self.log_file_path = 'wal.log'
        # self.log_file = open(self.log_file_path, 'a', buffering=1)  # Open in append mode with line buffering
        # self.last_tx_id = 0
        self.log_files: Dict[str, TextIO] = {}
        self.last_tx_ids: Dict[str, int] = {}
        self.log_file_locks: Dict[str, RWLock] = {}
        
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
                    # Create log file for write ahead logging
                    log_file = open(f'~/logs/{shard}_wal.log', 'a', buffering=1)
                    self.log_files[shard] = log_file
                    self.last_tx_ids[shard] = 0
                    self.log_file_locks[shard] = RWLock()

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

            # if isinstance(json_data, str):
            #     tables = json.loads(json_data)
            
            # elif isinstance(json_data, dict):
            #     tables = json_data
            
            database_copy = {}
            for table_name in json_data['shards']:   
                
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
        
    
    def write_ahead_logging(self, log_file: TextIO, tx_id, type:str, commit = True, data = None):
        if commit:
            log_file.write(f"0_{tx_id}_{type}\n")
            print(f"log_file: 0_{tx_id}_{type}\n", flush=True)
        else:
            log_file.write(f"1_{tx_id}_{type}_{data}\n")
            print(f"log_file: 1_{tx_id}_{type}_{data}\n", flush=True)


    async def Write_database(self,request_json):
        
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
            is_primary = False
            if 'is_primary' in request_json:
                is_primary = bool(request_json.get("is_primary", False))
                
            if tablename not in self.log_files:
                return f"Shard: {tablename} not found", 404, -1
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
             
            
            if is_primary:
                if 'servers' not in request_json:
                    return "'servers' field missing in request", 400, -1
                servers = set(request_json.get("servers"))
                if not servers:
                    return "No servers specified", 400, -1
                # Send the data to all secondary servers
                majority = int(len(servers) * MAJORITY_CRITERIA)
                majority+=1  # Add 1 to majority after floor
                
                # WAL
                lock = self.log_file_locks[tablename]
                lock.acquire_writer()
                self.last_tx_ids[tablename] += 1
                tx_id = self.last_tx_ids[tablename]
                log_file = self.log_files[tablename]
                self.write_ahead_logging(log_file, tx_id, WRITE_STR_LOG, commit=False, data=f"[{row_str}]")
                lock.release_writer()

                responses = []
                async with aiohttp.ClientSession() as session:
                    payload = {
                        "shard": tablename,
                        "tx_id": tx_id,
                        "data": data
                    }
                    tasks = [self.communicate_with_server(session, server, "write", payload) for server in servers]
                    responses = await asyncio.gather(*tasks)

                success_count = 0
                failed_servers = []
                maiority = 1
                for i in range(len(servers)):
                    status, response = responses[i]
                    if status == 200:
                        success_count += 1
                    else:
                        failed_servers.append(servers[i])
                
                if success_count >= maiority:
                    # Insert the data into the primary server
                    message, status = self.sql_handler.Insert(tablename, row_str,self.schema_str)
            
                    if status != 200:    
                        return message, status,valid_idx
                    # WAL
                    lock.acquire_writer()
                    self.write_ahead_logging(log_file, tx_id, WRITE_STR_LOG, commit=True)
                    lock.release_writer()

                    # Refresh the failed servers
                    # Use Copy_database to get the data from the primary server
                    resp, status = self.Copy_database({"shards": [tablename]})
                    if status != 200:
                        return f"Failed to copy data from primary server: {status}", 500, -1
                    full_data = resp[tablename]
                    payload = {
                        "shard": tablename,
                        "data": full_data,
                        "latest_tx_id": tx_id
                    }
                    if len(failed_servers) > 0:
                        async with aiohttp.ClientSession() as session:
                            tasks = [self.communicate_with_server(session, server, "refresh", payload) for server in failed_servers]
                            responses = await asyncio.gather(*tasks)
                        for i in range(len(failed_servers)):
                            status, response = responses[i]
                            if status != 200:
                                print(f"Failed to refresh server {failed_servers[i]}, ***DATA INCONSISTENCY***", flush=True)

                    return "Data entries added", 200, valid_idx+num_entry
                else:
                    # return that cannot write to majority of servers
                    return f"Failed to write to majority of servers: {failed_servers}", 500, -1

            else:
                if 'tx_id' not in request_json:
                    return "'tx_id' field missing in request", 400, -1
                tx_id = request_json.get("tx_id")
                # WAL
                lock = self.log_file_locks[tablename]
                lock.acquire_writer()
                log_file = self.log_files[tablename]
                self.write_ahead_logging(log_file, tx_id, WRITE_STR_LOG, commit=False, data=f"[{row_str}]")
                lock.release_writer()

                message, status = self.sql_handler.Insert(tablename, row_str,self.schema_str)
                if status != 200:
                    return message, status, valid_idx
                
                # WAL
                lock.acquire_writer()
                self.write_ahead_logging(log_file, tx_id, WRITE_STR_LOG, commit=True)
                lock.release_writer()

                return "Data entries added", 200, valid_idx+num_entry
        
        except Exception as e:
            return e, 500, -1
        
    async def Update_database(self,request_json):
       
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

            is_primary = False
            if 'is_primary' in request_json:
                is_primary = bool(request_json.get("is_primary", False))

            if is_primary:
                if 'servers' not in request_json:
                    return "'servers' field missing in request", 400
                servers = set(request_json.get("servers"))
                if not servers:
                    return "No servers specified", 400
                # Send the data to all secondary servers
                majority = int(len(servers) * MAJORITY_CRITERIA)
                majority+=1

                # WAL
                lock = self.log_file_locks[tablename]
                lock.acquire_writer()
                self.last_tx_ids[tablename] += 1
                tx_id = self.last_tx_ids[tablename]
                log_file = self.log_files[tablename]
                self.write_ahead_logging(log_file, tx_id, UPDATE_STR_LOG, commit=False, data=f"{stud_id}:{data}")
                lock.release_writer()

                responses = []
                async with aiohttp.ClientSession() as session:
                    payload = {
                        "shard": tablename,
                        "tx_id": tx_id,
                        "Stud_id": stud_id,
                        "data": data
                    }
                    tasks = [self.communicate_with_server(session, server, "update", payload) for server in servers]
                    responses = await asyncio.gather(*tasks)

                success_count = 0
                failed_servers = []
                maiority = 1
                for i in range(len(servers)):
                    status, response = responses[i]
                    if status == 200:
                        success_count += 1
                    else:
                        failed_servers.append(servers[i])

                if success_count >= maiority:
                    # Update the data in the primary server
                    message, status = self.sql_handler.Update_database(tablename, stud_id,data,'Stud_id')
                    if status != 200:
                        return message, status
                
                    # WAL
                    lock.acquire_writer()
                    self.write_ahead_logging(log_file, tx_id, UPDATE_STR_LOG, commit=True)
                    lock.release_writer()

                    # Refresh the failed servers
                    # Use Copy_database to get the data from the primary server
                    resp, status = self.Copy_database({"shards": [tablename]})
                    if status != 200:
                        return f"Failed to copy data from primary server: {status}", 500
                    full_data = resp[tablename]
                    payload = {
                        "shard": tablename,
                        "data": full_data,
                        "latest_tx_id": tx_id
                    }
                    if len(failed_servers) > 0:
                        async with aiohttp.ClientSession() as session:
                            tasks = [self.communicate_with_server(session, server, "refresh", payload) for server in failed_servers]
                            responses = await asyncio.gather(*tasks)
                        for i in range(len(failed_servers)):
                            status, response = responses[i]
                            if status != 200:
                                print(f"Failed to refresh server {failed_servers[i]}, ***DATA INCONSISTENCY***", flush=True)

                    return "Data entry updated", 200
                
                else:
                    # return that cannot write to majority of servers
                    return f"Failed to write to majority of servers: {failed_servers}", 500
                
            else:
                if 'tx_id' not in request_json:
                    return "'tx_id' field missing in request", 400
                tx_id = request_json.get("tx_id")
                # WAL
                lock = self.log_file_locks[tablename]
                lock.acquire_writer()
                log_file = self.log_files[tablename]
                self.write_ahead_logging(log_file, tx_id, UPDATE_STR_LOG, commit=False, data=f"{stud_id}:{data}")
                lock.release_writer()

                message, status = self.sql_handler.Update_database(tablename, stud_id,data,'Stud_id')
                if status != 200:
                    return message, status
                
                # WAL
                lock.acquire_writer()
                self.write_ahead_logging(log_file, tx_id, UPDATE_STR_LOG, commit=True)
                lock.release_writer()

                return "Data entry updated", 200
            

        except Exception as e:
            return e, 500
        
    
    async def Delete_database(self,request_json):
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
            is_primary = False
            if 'is_primary' in request_json:
                is_primary = bool(request_json.get("is_primary", False))

            if is_primary:
                if 'servers' not in request_json:
                    return "'servers' field missing in request", 400
                servers = set(request_json.get("servers"))
                if not servers:
                    return "No servers specified", 400
                # Send the data to all secondary servers
                majority = int(len(servers) * MAJORITY_CRITERIA)
                majority+=1

                # WAL
                lock = self.log_file_locks[tablename]
                lock.acquire_writer()
                self.last_tx_ids[tablename] += 1
                tx_id = self.last_tx_ids[tablename]
                log_file = self.log_files[tablename]
                self.write_ahead_logging(log_file, tx_id, DELETE_STR_LOG, commit=False, data=f"{stud_id}")
                lock.release_writer()

                responses = []
                async with aiohttp.ClientSession() as session:
                    payload = {
                        "shard": tablename,
                        "tx_id": tx_id,
                        "Stud_id": stud_id
                    }
                    tasks = [self.communicate_with_server(session, server, "del", payload) for server in servers]
                    responses = await asyncio.gather(*tasks)

                success_count = 0
                failed_servers = []
                maiority = 1
                for i in range(len(servers)):
                    status, response = responses[i]
                    if status == 200:
                        success_count += 1
                    else:
                        failed_servers.append(servers[i])

                if success_count >= maiority:
                    # Delete the data in the primary server
                    message, status = self.sql_handler.Delete_database(tablename, stud_id,'Stud_id')
                    if status != 200:
                        return message, status
                
                    # WAL
                    lock.acquire_writer()
                    self.write_ahead_logging(log_file, tx_id, DELETE_STR_LOG, commit=True)
                    lock.release_writer()

                    # Refresh the failed servers
                    # Use Copy_database to get the data from the primary server
                    resp, status = self.Copy_database({"shards": [tablename]})
                    if status != 200:
                        return f"Failed to copy data from primary server: {status}", 500
                    full_data = resp[tablename]
                    payload = {
                        "shard": tablename,
                        "data": full_data,
                        "latest_tx_id": tx_id
                    }
                    if len(failed_servers) > 0:
                        async with aiohttp.ClientSession() as session:
                            tasks = [self.communicate_with_server(session, server, "refresh", payload) for server in failed_servers]
                            responses = await asyncio.gather(*tasks)
                        for i in range(len(failed_servers)):
                            status, response = responses[i]
                            if status != 200:
                                print(f"Failed to refresh server {failed_servers[i]}, ***DATA INCONSISTENCY***", flush=True)

                    return "Data entry deleted", 200
                
                else:
                    # return that cannot write to majority of servers
                    return f"Failed to write to majority of servers: {failed_servers}", 500
                
            else:
                if 'tx_id' not in request_json:
                    return "'tx_id' field missing in request", 400
                tx_id = request_json.get("tx_id")
                # WAL
                lock = self.log_file_locks[tablename]
                lock.acquire_writer()
                log_file = self.log_files[tablename]
                self.write_ahead_logging(log_file, tx_id, DELETE_STR_LOG, commit=False, data=f"{stud_id}")
                lock.release_writer()

                message, status = self.sql_handler.Delete_entry(tablename, stud_id,'Stud_id')
                if status != 200:
                    return message, status
                
                # WAL
                lock.acquire_writer()
                self.write_ahead_logging(log_file, tx_id, DELETE_STR_LOG, commit=True)
                lock.release_writer()

                return "Data entry deleted", 200

        except Exception as e:
            return e, 500
    

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
        
    # Clear the table and add all the data from the payload and write the latest committed tx_id to log file
    def Refresh_table(self, request_json):
        try:
            if 'shard' not in request_json:
                return "'shard' field missing in request", 400

            if 'data' not in request_json:
                return "'data' field missing in request", 400
            
            if 'latest_tx_id' not in request_json:
                return "'latest_tx_id' field missing in request", 400

            table_name = request_json.get("shard")
            data = request_json.get("data")
            latest_tx_id = int(request_json.get("latest_tx_id"))

            if not self.sql_handler.connected:
                message, status = self.sql_handler.connect()
                if status != 200:
                    return message, status

            message, status = self.sql_handler.Clear_table(table_name)
            if status != 200:
                return message, status
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

            message, status = self.sql_handler.Insert(table_name, row_str, self.schema_str)
            if status != 200:
                return message, status, valid_idx
            
            # WAL
            lock = self.log_file_locks[table_name]
            lock.acquire_writer()
            self.last_tx_ids[table_name] = latest_tx_id
            log_file = self.log_files[table_name]
            self.write_ahead_logging(log_file, latest_tx_id, WRITE_STR_LOG, commit=True)
            lock.release_writer()

            return "Table refreshed", 200

        except Exception as e:
            return e, 500
    
    def latest_tx_id(self, request_json):
        try:
            if 'shard' not in request_json:
                return "'shard' field missing in request", 400

            table_name = request_json.get("shard")
            if table_name not in self.log_files:
                return f"Shard: {table_name} not found", 404

            lock = self.log_file_locks[table_name]
            lock.acquire_reader()
            latest_tx_id = self.last_tx_ids[table_name]
            lock.release_reader()
            return latest_tx_id, 200

        except Exception as e:
            return e, 500

    async def communicate_with_server(session: aiohttp.ClientSession, server, endpoint, payload={}):
        try:
            # async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1)) as session:
            request_url = f'http://{server}:{SERVER_PORT}/{endpoint}'
            
            if endpoint == "copy" or endpoint == "commit" or endpoint == "rollback":
                async with session.get(request_url, json=payload) as response:
                    return response.status, await response.json()
                
                    # response_status = response.status
                    # if response_status == 200:
                    #     return True, await response.json()
                    # else:
                    #     return False, await response.json()
            
            elif endpoint == "read" or endpoint == "write" or endpoint == "config":
                async with session.post(request_url, json=payload) as response:
                    return response.status, await response.json()
                    
            elif endpoint == "update":
                async with session.put(request_url, json=payload) as response:
                    return response.status, await response.json()
                    
            elif endpoint == "del":
                async with session.delete(request_url, json=payload) as response:
                    return response.status, await response.json()
            else:
                return 500, {"message": "Invalid endpoint"}
                
        except Exception as e:
            return 500, {"message": f"{e}"}