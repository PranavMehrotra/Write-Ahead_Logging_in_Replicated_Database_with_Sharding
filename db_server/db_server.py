# Import necessary modules
from aiohttp import web
from manager import Manager
import json
import os
from typing import Dict, List, Tuple
from heartbeat_new import HeartBeat
from load_balancer.docker_utils import kill_server_cntnr
from load_balancer.RWLock import RWLock
import aiohttp
import asyncio

SERVER_PORT = 5000
server_id = os.environ.get("SERVER_ID", "server")
mgr = Manager(host='localhost',user='root',password=f"{server_id}@123")
StudT_schema = {}

hb_threads: Dict[str, HeartBeat] = {}


# MapT is a dictionary with key as shard_id and value as a list whose 
# first item is a string (the primary server for that shard) and second item is a list of secondary servers
MapT_dict: Dict[str, List[str, List[str]]] = {}
MapT_dict_lock = RWLock()


async def communicate_with_server(server, endpoint, payload={}):
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1)) as session:
            request_url = f'http://{server}:{SERVER_PORT}/{endpoint}'
            
            if endpoint == "latest_tx_id":
                async with session.get(request_url) as response:
                    return response.status, await response.json()
                
            else:
                return 500, {"message": "Invalid endpoint"}
            
    except Exception as e:
        print(f"DB_Server: Error in communicating with server {server}: {str(e)}")
        return 500, {"error": "Internal Server Error"}
            
        
async def elect_primary_server(shard: str, active_servers: List[str]) -> str:
    
    # Elect the server with the latest transaction id as the primary server
    
    serv_transaction_ids = {}
    tasks = []
    for server in active_servers:
        server_json = {
            "shard": shard
        }
        tasks.append(communicate_with_server(server, "latest_tx_id", server_json))
                
    
    results = await asyncio.gather(*tasks)
    for (status, response), server in zip(results, active_servers):
        if status == 200:
            serv_transaction_ids[server] = response.get("latest_tx_id", -1)
        else:
            print(f"DB_Server:Coudn't get latest tx_id from server {server}")
            serv_transaction_ids[server] = -1
            
    
    max_tx_id = -1
    max_tx_id_server = ""
    for server, tx_id in serv_transaction_ids.items():
        if tx_id > max_tx_id:
            max_tx_id = tx_id
            max_tx_id_server = server
            
    if max_tx_id_server == "":
        print(f"DB_Server: Error in electing primary server for shard {shard} as no server has a valid transaction id")
        return ""
    
    return max_tx_id_server
    

async def config(request):
    try:
        global StudT_schema
        request_json = await request.json()
        if isinstance(request_json, str):
            request_json = json.loads(request_json)
        
        message, status = mgr.Config_database(request_json)

        if status == 200:
            schemas = request_json.get('schemas', [])
            message = ", ".join([table for table in schemas.keys()]) + " tables created"
            
            StudT_schema = dict(request_json.get("StudT_schema", {}))
            response_json = {
                "message": message,
                "status": "success"
            }
            
        else:
           
            response_json = {
                "error": str(message),
                "status": "failure"
            }
        
        return web.json_response(response_json, status=status)

    except Exception as e:
        print(f"DB_Server: Error in Config endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)
    
async def heartbeat(request):
    try:
        return web.Response(status=200)
    
    except Exception as e:
        print(f"DB_Server: Error in heartbeat endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)
    

async def list_active_hb_threads(request):
    global hb_threads
    
    try:
        # Get the list of active heartbeat threads
        active_threads = list(hb_threads.keys())
        response_json = {
            "active_hb_threads": active_threads,
            "status": "success"
        }
        return web.json_response(response_json, status=200)
    except Exception as e:
        print(f"DB_Server: Error in list_active_hb_threads endpoint: {str(e)}")
        response_json = {
            "message": f"<Error>: {str(e)}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
        

async def read_database(request):
    try:
        global StudT_schema
        # request_json = await request.json()
        database_entry, status = mgr.Read_database()
        
        if status==200:
            response_data = {
                    "StudT_schema": StudT_schema,
                    "data": database_entry,
                    "status": "success"
                }
        else:
            response_data = {
                    "error": database_entry,
                    "status": "failure"
                }
    
        return web.json_response(response_data, status=status)
   
    except Exception as e:
    
        print(f"DB_Server: Error in read endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

async def write_database(request):
    
    try:
        print("Write endpoint called")
        request_json = await request.json()  
        message, status = mgr.Write_database(request_json)
        
        if status == 200:
            response_data = {
                    "message": message,
                    "status": "success"
                }
            
        else:
            response_data = {
                    "error": message,
                    "status": "failure"
                }
            print(f"DB_Server: Error in write endpoint: {str(message)}", flush=True)
            
        return web.json_response(response_data, status=status)  
    
    except Exception as e:
        
        print(f"DB_Server: Error in write endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)  

# Update endpoint to update an existing entry in the database
async def update_database(request):
    
    try:
        print("Update endpoint called")
        # Get the JSON data from the request
        request_json = await request.json()  
        message, status = mgr.Update_database(request_json)
    
        response_data = {}
        # Create a response JSON
        if status == 200:
            response_data = {
                "message": message,
                "status": "success"
            }
        
        else:
            response_data = {
                "message": f"{str(message)}",
                "status": "failure"
            }
        return web.json_response(response_data, status=status)        
        
    except Exception as e:
        
        print(f"DB_Server: Error in update endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

async def delete_entries(request):
    try:
        print("Delete endpoint called")
        # Get the JSON data from the request
        request_json = await request.json()  
        message, status = mgr.Delete_entry(request_json)
    
        response_data = {}
        # Create a response JSON
        if status == 200:
            response_data = {
                "message": message,
                "status": "success"
            }
        
        else:
            response_data = {
                "message": f"{str(message)}",
                "status": "failure"
            }
        return web.json_response(response_data, status=status)        
        
    except Exception as e:
        
        print(f"DB_Server: Error in delete endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)
    
    

async def delete_table(request):
    try:
        print("Delete endpoint called")
        # Get the JSON data from the request
        request_json = await request.json()  
        message, status = mgr.Delete_table(request_json)
    
        response_data = {}
        # Create a response JSON
        if status == 200:
            response_data = {
                "message": message,
                "status": "success"
            }
        
        else:
            response_data = {
                "message": f"{str(message)}",
                "status": "failure"
            }
        return web.json_response(response_data, status=status)        
        
    except Exception as e:
        
        print(f"DB_Server: Error in delete endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

async def clear_table(request):
    try:
        print("Delete endpoint called")
        # Get the JSON data from the request
        request_json = await request.json()  
        message, status = mgr.Clear_table(request_json)
    
        response_data = {}
        # Create a response JSON
        if status == 200:
            response_data = {
                "message": message,
                "status": "success"
            }
        
        else:
            response_data = {
                "message": f"{str(message)}",
                "status": "failure"
            }
        return web.json_response(response_data, status=status)        
        
    except Exception as e:
        
        print(f"DB_Server: Error in delete endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

async def not_found(request):
    return web.Response(text="Not Found", status=400)


async def config_change_handler(request):
    global hb_threads
    global MapT_dict
    global MapT_dict_lock
    
    try:
        request_json = await request.json()
        if isinstance(request_json, str):
            request_json = json.loads(request_json)

        if 'num_servers' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'num_servers' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)     
            
        if 'action' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'action' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        action = str(request_json['action'])
        
        if action.startswith("add"):
            
            if 'servers_to_shard' not in request_json:
                response_json = {
                    "message": f"<Error> Invalid payload format: 'servers_to_shard' field missing in request",
                    "status": "failure"
                }
                return web.json_response(response_json, status=400)
            
            servers_to_shard = request_json['servers_to_shard']
            num_servers = request_json['num_servers']

            if not isinstance(servers_to_shard, dict):
                response_json = {
                    "message": f"<Error> Invalid payload format: 'servers_to_shard' field should be a dictionary",
                    "status": "failure"
                }
                return web.json_response(response_json, status=400)            
            
            if num_servers != len(list(servers_to_shard.keys())):
                response_json = {
                    "message": f"<Error> Invalid payload format: 'num_servers' field and no of servers in 'servers_to_shard' do not match",
                    "status": "failure"
                }
                return web.json_response(response_json, status=400)              
            
            
            # update the MapT_dict with the new servers
            
            shards_to_servers = {}
            for server, shards in servers_to_shard.items():
                for shard in shards:
                    if shard not in shards_to_servers:
                        shards_to_servers[shard] = []
                    shards_to_servers[shard].append(server)
            
            for shard in list(shards_to_servers.keys()):
                shards_to_servers[shard] = list(set(shards_to_servers[shard]))        
                    
            MapT_dict_lock.acquire_writer()
            for shard, servers in shards_to_servers.items():
                if shard not in MapT_dict:
                    
                    # elect the first server as primary and rest as secondary
                    primary_server = servers[0]
                    secondary_servers = servers[1:]
                    MapT_dict[shard] = [primary_server, secondary_servers]
                    assert type(primary_server) == str and type(secondary_servers) == list
                    
                else:
                    MapT_dict[shard][1].extend(servers)
                    MapT_dict[shard][1] = list(set(MapT_dict[shard][1]))
                    
            MapT_dict_lock.release_writer()
            
            # start the heartbeat threads for the new servers
            servers = list(servers_to_shard.keys())
            
            for server in servers:
                t1 = HeartBeat(server, StudT_schema, MapT_dict, MapT_dict_lock)
                hb_threads[server] = t1
                t1.start()
            
            response_json = {
                "message": f"Started Heartbeat threads for servers: {', '.join(servers)} and updated MapT_dict",
                "status": "success"
            }
            return web.json_response(response_json, status=200)
        
        elif action.startswith("remove"):         
            
            if 'servers' not in request_json:
                response_json = {
                    "message": f"<Error> Invalid payload format: 'servers' field missing in request",
                    "status": "failure"
                }
                return web.json_response(response_json, status=400)
            
            servers_rm = request_json.get('servers', [])
            num_servers = request_json['num_servers']

            if not isinstance(servers_rm, list):
                response_json = {
                    "message": f"<Error> Invalid payload format: 'servers' field should be a list",
                    "status": "failure"
                }
                return web.json_response(response_json, status=400)
            
            if num_servers != len(servers):
                response_json = {
                    "message": f"<Error> Invalid payload format: 'num_servers' field and no of servers in 'servers' do not match",
                    "status": "failure"
                }
                return web.json_response(response_json, status=400)  
            
            MapT_dict_lock.acquire_writer()
            for shard, servers in MapT_dict.items():
                primary_server = servers[0]
                secondary_servers = servers[1]
                
                # remove first the servers from secondary servers
                secondary_servers = list(set(secondary_servers) - set(servers_rm))
                
                
                # now check if primary server is in the list of servers to remove
                if primary_server in servers_rm:
                    if len(secondary_servers) > 0:
                        
                        ### FUNCTION CALL TO ALGORITHM TO ELECT NEW PRIMARY SERVER
                        primary_server = elect_primary_server(shard, secondary_servers)
                        if primary_server != "":
                            secondary_servers.remove(primary_server)
                        
                        else:
                            print(f"DB_Server: Error in config_change_handler endpoint: Couldn't elect new primary server for shard {shard}")
                            response_json = {
                                "message": f"<Error>: Couldn't remove the servers from the system",
                                "status": "failure"
                            }
                            return web.json_response(response_json, status=400)
                        
                        # update the MapT_dict
                        MapT_dict[shard] = [primary_server, secondary_servers]
                        
                    else:
                        primary_server = ""
                        secondary_servers = []
                        del MapT_dict[shard]
                        
                else:
                    MapT_dict[shard] = [primary_server, secondary_servers]
                        
            MapT_dict_lock.release_writer()
            
            servers = list(set(servers_rm))
            num_servers = len(servers)
            
            for server in servers:
                if server in hb_threads:
                    hb_threads[server].stop()
                    del hb_threads[server]
                    
                    # kill_server_cntnr(server)
                    
                else:
                    print(f"DB_Server: Error in config_change_handler endpoint: {server} not found in the list of servers")
                    response_json = {
                        "message": f"<Error>: {server} not found in the list of active servers",
                        "status": "failure"
                    }
                    return web.json_response(response_json, status=400)
            
            
            response_json = {
                "message": f"Stopped Hb threads for servers: {', '.join(servers)} and updated MapT_dict",
                "status": "success"
            }
            
            return web.json_response(response_json, status=200)
        
    except Exception as e:
        print(f"DB_Server: Error in config_change_handler endpoint: {str(e)}")
        response_json = {
            "message": f"<Error>: {str(e)}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
                 
async def get_primary_server(request):
    global MapT_dict
    global MapT_dict_lock
    
    try:
        request_json = await request.json()
        
        if 'shard' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'shard' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        shard = request_json['shard']
        
        MapT_dict_lock.acquire_reader()
        if shard not in MapT_dict:
            MapT_dict_lock.release_reader()
            response_json = {
                "message": f"<Error> Shard {shard} not found in the database",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        primary_server = MapT_dict[shard][0]
        secondary_servers = MapT_dict[shard][1]
        MapT_dict_lock.release_reader()
        
        response_json = {
            "shard": shard,
            "primary_server": primary_server,
            "secondary_servers": secondary_servers,
            # "status": "success"
        }
        return web.json_response(response_json, status=200)
    
    except Exception as e:
        print(f"DB_Server: Error in get_primary_server endpoint: {str(e)}")
        response_json = {
            "message": f"<Error>: {str(e)}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)          
                    

# Define the main function to run the web server
def run_server():
    
    global hb_threads
    
    # Create an instance of the web Application
    app = web.Application()

    # Add routes for the home and heartbeat endpoints
    app.router.add_post('/config', config)
    app.router.add_get('/heartbeat', heartbeat)
    app.router.add_get('/read', read_database)
    app.router.add_post('/write', write_database)
    app.router.add_put('/update', update_database)
    app.router.add_delete('/del', delete_entries)
    app.router.add_delete('/del_table', delete_table)
    app.router.add_post('/clear_table', clear_table)
    
    
    # app.router.add_post('/init_servers_hb', init_servers_hb)
    # app.router.add_post('/stop_servers_hb', stop_servers_hb)
    app.router.add_get('/list_active_hb_threads', list_active_hb_threads)
    app.router.add_post('/config_change', config_change_handler)
    app.router.add_post('/get_primary_server', get_primary_server)

    # Add a catch-all route for any other endpoint, which returns a 400 Bad Request
    app.router.add_route('*', '/{tail:.*}', not_found)

    # Run the web application on port 5000
    web.run_app(app, port=5000)
    
    print("Shard Manager ready!", flush=True)
    
    for thread in hb_threads.values():
        thread.join()

# Entry point of the script
if __name__ == '__main__':
    # Run the web server
    run_server()








    
# async def init_servers_hb(request):
#     global hb_threads
    
#     try:
#         request_json = await request.json()
#         if isinstance(request_json, str):
#             request_json = json.loads(request_json)
            
#         if 'num_servers' not in request_json:
#             response_json = {
#                 "message": f"<Error> Invalid payload format: 'num_servers' field missing in request",
#                 "status": "failure"
#             }
#             return web.json_response(response_json, status=400)    
        
#         if 'servers' not in request_json:
#             response_json = {
#                 "message": f"<Error> Invalid payload format: 'servers' field missing in request",
#                 "status": "failure"
#             }
#             return web.json_response(response_json, status=400)
        
#         servers = request_json.get('servers', [])
#         num_servers = request_json['num_servers']
        
#         if num_servers != len(servers):
#             response_json = {
#                 "message": f"<Error> Invalid payload format: 'num_servers' field and no of servers in 'servers' do not match",
#                 "status": "failure"
#             }
#             return web.json_response(response_json, status=400)        
        
#         servers = set(servers)
#         servers = list(servers)
#         num_servers = len(servers)
        
#         for server in servers:
#             t1 = HeartBeat(server, StudT_schema, MapT_dict, MapT_dict_lock)
#             hb_threads[server] = t1
#             t1.start()
            
            
#         response_json = {
#             "message": f"Started Heartbeat threads for servers: {', '.join(servers)}",
#             "status": "success"
#         }
#         return web.json_response(response_json, status=200)
            
#     except Exception as e:
#         print(f"DB_Server: Error in init_servers_hb endpoint: {str(e)}")
#         response_json = {
#             "message": f"<Error>: {str(e)}",
#             "status": "failure"
#         }
#         return web.json_response(response_json, status=400)

# async def stop_servers_hb(request):
#     global hb_threads
    
#     try:
#         request_json = await request.json()
#         if isinstance(request_json, str):
#             request_json = json.loads(request_json)
            
#         if 'num_servers' not in request_json:
#             response_json = {
#                 "message": f"<Error> Invalid payload format: 'num_servers' field missing in request",
#                 "status": "failure"
#             }
#             return web.json_response(response_json, status=400)    
        
#         if 'servers' not in request_json:
#             response_json = {
#                 "message": f"<Error> Invalid payload format: 'servers' field missing in request",
#                 "status": "failure"
#             }
#             return web.json_response(response_json, status=400)
        
#         servers = request_json.get('servers', [])
#         num_servers = request_json['num_servers']
        
#         if num_servers != len(servers):
#             response_json = {
#                 "message": f"<Error> Invalid payload format: 'num_servers' field and no of servers in 'servers' do not match",
#                 "status": "failure"
#             }
#             return web.json_response(response_json, status=400)        
             
#         servers = set(servers)
#         servers = list(servers)
#         num_servers = len(servers)
                            
#         for server in servers:
#             if server in hb_threads:
#                 hb_threads[server].stop()
#                 del hb_threads[server]
                
#                 # kill_server_cntnr(server)
                
#             else:
#                 print(f"DB_Server: Error in stop_servers_hb endpoint: {server} not found in the list of servers")
#                 response_json = {
#                     "message": f"<Error>: {server} not found in the list of active servers",
#                     "status": "failure"
#                 }
#                 return web.json_response(response_json, status=400)
        servers_to_shard
#         response_json = {
#             "message": f"Stopped Heartbeat threads for servers: {', '.join(servers)}",
#             "status": "success"
#         }    
#         return web.json_response(response_json, status=200)
    
#     except Exception as e:
#         print(f"DB_Server: Error in stop_servers_hb endpoint: {str(e)}")
#         response_json = {
#             "message": f"<Error>: {str(e)}",
#             "status": "failure"
#         }
#         return web.json_response(response_json, status=400)
