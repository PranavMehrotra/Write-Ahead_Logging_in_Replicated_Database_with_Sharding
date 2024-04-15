import threading
import sys
import os
import requests
import time
import requests
import aiohttp
import asyncio
from typing import List
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from RWLock import RWLock
# from docker_utils import kill_server_cntnr

HEARTBEAT_INTERVAL = 0.2
SEND_FIRST_HEARTBEAT_AFTER = 2
SERVER_PORT = 5000
LB_IP_ADDR = 'load_balancer_con'
LB_PORT = 5000

def synchronous_communicate_with_server(server, endpoint, payload={}):
    try:
        request_url = f'http://{server}:{SERVER_PORT}/{endpoint}'
        if endpoint == "copy" or endpoint == "commit" or endpoint == "rollback":
            response = requests.get(request_url, json=payload)
            return response.status_code, response.json()
            
        elif endpoint == "read" or endpoint == "write" or endpoint == "config" or endpoint == "latest_tx_id":
            response = requests.post(request_url, json=payload)
            return response.status_code, response.json()
        
        elif endpoint == "update":
            response = requests.put(request_url, json=payload)
            return response.status_code, response.json()
        
        elif endpoint == "del":
            response = requests.delete(request_url, json=payload)
            return response.status_code, response.json()
        else:
            return 500, {"message": "Invalid endpoint"}
    except Exception as e:
        return 500, {"message": f"{e}"}


def sync_communicate_with_lb(lb_ip, endpoint, payload={}):
    try:
        request_url = f'http://{lb_ip}:{LB_PORT}/{endpoint}'
        response = requests.post(request_url, json=payload)
        return response.status_code, response.json()
    except Exception as e:
        return 500, {"message": f"{e}"}


def elect_primary_server(shard: str, active_servers: List) -> str:
    
    # Elect the server with the latest transaction id as the primary server
    
    serv_transaction_ids = {}
    # tasks = []
    for server in active_servers:
        server_json = {
            "shard": shard
        }
        status, response = synchronous_communicate_with_server(server, "latest_tx_id", server_json)
        if status != 200:
            print(f"heartbeat: Error in getting latest tx_id from server {server}")
            serv_transaction_ids[server] = -1
        else:
            serv_transaction_ids[server] = response.get("latest_tx_id", -1)
                
    
    # results = await asyncio.gather(*tasks)
    # for (status, response), server in zip(results, active_servers):
    #     if status == 200:
    #         serv_transaction_ids[server] = response.get("latest_tx_id", -1)
    #     else:
    #         print(f"DB_Server:Coudn't get latest tx_id from server {server}")
    #         serv_transaction_ids[server] = -1
            
    
    max_tx_id = -1
    max_tx_id_server = ""
    for server, tx_id in serv_transaction_ids.items():
        if tx_id > max_tx_id:
            max_tx_id = tx_id
            max_tx_id_server = server
            
    if max_tx_id_server == "":
        print(f"heartbeat: Error in electing primary server for shard {shard} as no server has a valid transaction id")
        return ""
    
    return max_tx_id_server

# async def communicate_with_server(server, endpoint, payload={}):
#     try:
#         async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1)) as session:
#             request_url = f'http://{server}:{SERVER_PORT}/{endpoint}'
            
#             if endpoint == "copy" or endpoint == "commit" or endpoint == "rollback":
#                 async with session.get(request_url, json=payload) as response:
#                     return response.status, await response.json()
                
#                     # response_status = response.status
#                     # if response_status == 200:
#                     #     return True, await response.json()
#                     # else:
#                     #     return False, await response.json()
            
#             elif endpoint == "read" or endpoint == "write" or endpoint == "config":
#                 async with session.post(request_url, json=payload) as response:
#                     return response.status, await response.json()
                    
#             elif endpoint == "update":
#                 async with session.put(request_url, json=payload) as response:
#                     return response.status, await response.json()
                    
#             elif endpoint == "del":
#                 async with session.delete(request_url, json=payload) as response:
#                     return response.status, await response.json()
#             else:
#                 return 500, {"message": "Invalid endpoint"}
            
#     except Exception as e:
#         return 500, {"message": f"{e}"}

class HeartBeat(threading.Thread):
    def __init__(self, server_name, studt_schema, MapT_dict: dict, MapT_dict_lock: RWLock, server_port=5000):
        super(HeartBeat, self).__init__()
        self._server_name = server_name
        self._server_port = server_port
        self._stop_event = threading.Event()
        self.StudT_schema = studt_schema
        self.MapT_dict = MapT_dict
        self.MapT_dict_lock = MapT_dict_lock
        # self.elect_primary_server = elect_primary_server
        

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()
    
    def elect_primary_for_all_shards(self, server_name, server_shards):
        
        self.MapT_dict_lock.acquire_writer()
        for shard_id in server_shards:
            primary_server = self.MapT_dict[shard_id][0]
            secondary_servers = self.MapT_dict[shard_id][1]
            if primary_server == server_name:
                new_primary_server = elect_primary_server(shard=shard_id, active_servers=secondary_servers)
                if new_primary_server != "":
                    self.MapT_dict[shard_id][0] = new_primary_server
                    self.MapT_dict[shard_id][1] = list(set(secondary_servers) - set([new_primary_server]))
                    
                else:
                    print(f"heartbeat: Error in electing new primary server for shard {shard_id} after server {server_name} was removed")
                    # print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                    # return
                
            elif server_name in secondary_servers:
                self.MapT_dict[shard_id][1] = list(set(secondary_servers) - set([server_name]))
                
        self.MapT_dict_lock.release_writer()     
        
        return        

    def run(self):
        server_name = self._server_name
        server_port = self._server_port
        print("heartbeat: Heartbeat thread started for server: ", server_name, flush=True)
        time.sleep(SEND_FIRST_HEARTBEAT_AFTER)
        cntr = 0
        while True:
            # Check if the thread is stopped
            if self.stopped():
                print("heartbeat: Stopping heartbeat thread for server: ", server_name, flush=True)
                return
            # print("heartbeat: Starting a session!")
            # with aiohttp.ClientSession() as session:
            # print("heartbeat: Session started!")
            
            try:
                # with session.get(f'http://{server_name}:{server_port}/heartbeat') as response:
                    # print("heartbeat: Connected to server, Response received!")
                    # if response.status != 200 and {await response.text()}['message'] != "ok":
                    
                    ## To-Do: Check for timeout also
                response = requests.get(f'http://{server_name}:{server_port}/heartbeat', timeout=1)
                if response.status_code != 200 and response.status_code != 400:
                    cntr += 1
                    if cntr >= 2:
                        # Check if the thread is stopped
                        if self.stopped(): # this condition would be true when the container is already killed and the thread is still running (for a remove server operation)
                            # will prevent the thread from respawning the server container (as we explicitly need to remove the server from the load balancer)
                            print("heartbeat: Stopping heartbeat thread for server: ", server_name, flush=True)                    
                            # session.close()
                            
                            # NEED TO ELECT PRIMARY SERVER FOR EACH SHARD OF THE SERVER IF THE SERVER IS REMOVED
                            # ALREADY TAKEN CARE OF IN THE config_change_handler FUNCTION IN db_server.py before this thread was stopped
                            return
                        
                        
                        print(f"heartbeat: Server {server_name} is down!")
                        print(f"heartbeat: Spawning a new server: {server_name}!", flush=True)
                        cntr = 0
                        
                        ### NEED to change the remove and add servers function calls
                        #r emove server from the loadbalancer (conistent hashing) and then kill the server container
                        
                        # first get the serv_to_shard mapping using lb before removing the server 
                        # as we need to reconfigure the server with the same shard data
                    
                        # servers, serv_to_shard = lb.list_servers(send_shard_info=True)
                        
                        ### send request to the load balancer to get the server to shard mapping
                        request_payload = {
                            "send_shard_info": True
                        }
                        status, response = sync_communicate_with_lb(LB_IP_ADDR, "list_servers_lb", request_payload)
                        if status != 200:
                            print(f"heartbeat: Error in getting server to shard mapping from consistent hashing of load balancer\nError: {response.get('message', 'Unknown error')}")
                            print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                            return
                        
                        servers = response.get('servers', [])
                        serv_to_shard = response.get('serv_to_shard', {})
                        
                        
                        # then remove the server from the load balancer
                        # lb.remove_servers(1, [server_name])
                        
                        # send request to the load balancer to remove the server
                        request_payload = {
                            "num_servers": 1,
                            "servers": [server_name]
                        }
                        status, response = sync_communicate_with_lb(LB_IP_ADDR, "remove_servers_lb", request_payload)
                        if status != 200:
                            print(f"heartbeat: Error in removing server {server_name} from consistent hashing of load balancer\nError: {response.get('message', 'Unknown error')}")
                            print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                            return
                        
                        else:
                            print(f"heartbeat: Server {server_name} removed successfully from the consistent hashing of load balancer!", flush=True)
                        
                        try: 
                            # kill_server_cntnr(server_name)
                            status, response = sync_communicate_with_lb(LB_IP_ADDR, "kill_server_cntnr", {"server": server_name})
                            if status != 200:
                                print(f"heartbeat: Error in killing server {server_name}\nError: {response.get('message', 'Unknown error')}", flush=True)
                                print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                                return
                            
                        except Exception as e:
                            print(f"heartbeat: could not kill server {server_name}\nError: {e}", flush=True)
                            print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                            return  # IS IT OKAY TO RETURN HERE?
                        
                        # reinstantiate an image of the server
                        server_shard_map = {server_name: serv_to_shard[server_name]}
                        # lb.add_servers(1, server_shard_map)
                        
                        ### DO PRIMARY SERVER ELECTION FOR EACH SHARD OF THIS SERVER (FOR WHICH THIS SERVER WAS PRIMARY) BEFORE ADDING THE SERVER BACK TO THE SYSTEM
                        server_shards = serv_to_shard[server_name]
                        
                        self.elect_primary_for_all_shards(server_name, server_shards)
                        
                        ### ADD THE SERVER BACK TO THE SYSTEM
                        
                        # send request to the load balancer to add the servers and their server to shard mapping
                        request_payload = {
                            "num_servers": 1,
                            "serv_to_shard": server_shard_map
                        }
                        status, response = sync_communicate_with_lb(LB_IP_ADDR, "add_servers_lb", request_payload)
                        if status != 200:
                            print(f"heartbeat: Error in adding server {server_name} to consistent hashing of load balancer\nError: {response.get('message', 'Unknown error')}")
                            print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                            return
                        
                        else:
                            print(f"heartbeat: Server {server_name} added successfully to the consistent hashing of load balancer!", flush=True)
                            
                        
                        # function to configure the server based on the primary server for each shard
                        status, response = self.config_server(server_name, serv_to_shard)
                        if (status == 200):
                            print(f"heartbeat: Server {server_name} reconfigured successfully with all the data!", flush=True)
                            
                            # add the server back as a secondary server for the shards in MapT_dict
                            self.MapT_dict_lock.acquire_writer()
                            for shard_id in serv_to_shard[server_name]:
                                self.MapT_dict[shard_id][1].append(server_name)
                            self.MapT_dict_lock.release_writer()
                            
                            
                        else:
                            print(f"heartbeat: Error in reconfiguring server {server_name}\nError: {response}", flush=True)
                            print(f"heartbeat: Killing the server {server_name} permanently!", flush=True)
                            try:
                                # kill_server_cntnr(server_name)
                                status, response = sync_communicate_with_lb(LB_IP_ADDR, "kill_server_cntnr", {"server": server_name})
                                if status != 200:
                                    print(f"heartbeat: Error in killing server {server_name}\nError: {response.get('message', 'Unknown error')}", flush=True)
                                    print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                                    return    
                                    
                                print(f"heartbeat: Server {server_name} killed successfully!", flush=True)
                                
                            except Exception as e:
                                print(f"heartbeat: could not kill server {server_name}\nError: {e}", flush=True)
                                
                            print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                            return # IS IT OKAY TO RETURN HERE?
                            

                else :
                    cntr = 0

            # except aiohttp.client_exceptions.ClientConnectorError as e:
            except Exception as e: # this is better as it is more generic and will catch all exceptions

                cntr += 1
                if cntr >= 2:
                    print(f"heartbeat: Could not connect to server {server_name} due to {str(e.__class__.__name__)}")
                    print(f"heartbeat: Error: {e}")
                # Check if the thread is stopped
                    if self.stopped(): # this condition would be true when the container is already killed and the thread is still running (for a remove server operation)
                        # will prevent the thread from respawning the server container (as we explicitly need to remove the server from the load balancer)
                        print("heartbeat: Stopping heartbeat thread for server: ", server_name, flush=True)
                        # session.close()
                        
                        # NEED TO ELECT PRIMARY SERVER FOR EACH SHARD OF THE SERVER IF THE SERVER IS REMOVED
                        # ALREADY TAKEN CARE OF IN THE config_change_handler FUNCTION IN db_server.py before this thread was stopped                        
                        return
                    
                    
                    print(f"heartbeat: Server {server_name} is down!")
                    print(f"heartbeat: Spawning a new server: {server_name}!", flush=True)
                    cntr = 0
                    
                    ### NEED to change the remove and add servers function calls
                    #remove server from the loadbalancer
                    
                    # first get the serv_to_shard mapping using lb before removing the server 
                    # as we need to reconfigure the server with the same shard data
                    
                    # servers, serv_to_shard = lb.list_servers(send_shard_info=True)

                    request_payload = {
                        "send_shard_info": True
                    }
                    status, response = sync_communicate_with_lb(LB_IP_ADDR, "list_servers_lb", request_payload)
                    if status != 200:
                        print(f"heartbeat: Error in getting server to shard mapping from consistent hashing of load balancer\nError: {response.get('message', 'Unknown error')}")
                        print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                        return
                    
                    servers = response.get('servers', [])
                    serv_to_shard = response.get('serv_to_shard', {})                    
                    
                    
                    # then remove the server from the load balancer
                    # lb.remove_servers(1, [server_name])
                    
                    # send request to the load balancer to remove the server
                    request_payload = {
                        "num_servers": 1,
                        "servers": [server_name]
                    }
                    status, response = sync_communicate_with_lb(LB_IP_ADDR, "remove_servers_lb", request_payload)
                    if status != 200:
                        print(f"heartbeat: Error in removing server {server_name} from consistent hashing of load balancer\nError: {response.get('message', 'Unknown error')}")
                        print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                        return
                    
                    else:
                        print(f"heartbeat: Server {server_name} removed successfully from the consistent hashing of load balancer!", flush=True)                   
                    
                    try:
                        # kill_server_cntnr(server_name)
                        status, response = sync_communicate_with_lb(LB_IP_ADDR, "kill_server_cntnr", {"server": server_name})
                        if status != 200:
                            print(f"heartbeat: Error in killing server {server_name}\nError: {response.get('message', 'Unknown error')}", flush=True)
                            print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                            return   
                                              
                    except Exception as e:
                        print(f"heartbeat: could not kill server {server_name}\nError: {e}", flush=True)
                        print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                        return  # IS IT OKAY TO RETURN HERE?    
                
                    # reinstantiate an image of the server
                    server_shard_map = {server_name: serv_to_shard[server_name]}
                    # lb.add_servers(1, server_shard_map)
                    
                    ### DO PRIMARY SERVER ELECTION FOR EACH SHARD OF THIS SERVER (FOR WHICH THIS SERVER WAS PRIMARY) BEFORE ADDING THE SERVER BACK TO THE SYSTEM
                    server_shards = serv_to_shard[server_name]
                    
                    self.elect_primary_for_all_shards(server_name, server_shards)
                    
                    ### ADD THE SERVER BACK TO THE SYSTEM                    
                    
                    # send request to the load balancer to add the servers and their server to shard mapping
                    request_payload = {
                        "num_servers": 1,
                        "serv_to_shard": server_shard_map
                    }
                    status, response = sync_communicate_with_lb(LB_IP_ADDR, "add_servers_lb", request_payload)
                    if status != 200:
                        print(f"heartbeat: Error in adding server {server_name} to consistent hashing of load balancer\nError: {response.get('message', 'Unknown error')}")
                        print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                        return
                    
                    else:
                        print(f"heartbeat: Server {server_name} added successfully to the consistent hashing of load balancer!", flush=True)                    

                    # function to configure the server based on an existing server which is already up and running
                    status, response = self.config_server(server_name, serv_to_shard)
                    if (status == 200):
                        print(f"heartbeat: Server {server_name} reconfigured successfully with all the data!", flush=True)
                        # add the server back as a secondary server for the shards in MapT_dict
                        self.MapT_dict_lock.acquire_writer()
                        for shard_id in serv_to_shard[server_name]:
                            self.MapT_dict[shard_id][1].append(server_name)
                        self.MapT_dict_lock.release_writer()    
                                            
                    else:
                        print(f"heartbeat: Error in reconfiguring server {server_name}\nError: {response}")
                        print(f"heartbeat: Killing the server {server_name} permanently!", flush=True)
                        try:
                            # kill_server_cntnr(server_name)
                            status, response = sync_communicate_with_lb(LB_IP_ADDR, "kill_server_cntnr", {"server": server_name})
                            if status != 200:
                                print(f"heartbeat: Error in killing server {server_name}\nError: {response.get('message', 'Unknown error')}", flush=True)
                                print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                                return                             
                            
                            print(f"heartbeat: Server {server_name} killed successfully!", flush=True)
                        
                        except Exception as e:
                            print(f"heartbeat: could not kill server {server_name}\nError: {e}", flush=True)
                            
                        print(f"heartbeat: Stopping heartbeat thread for server: {server_name}", flush=True)
                        return # IS IT OKAY TO RETURN HERE?                
                


            # print("heartbeat: Closing session and sleeping!")
            # session.close()
            time.sleep(HEARTBEAT_INTERVAL)

    def config_server(self, server_name, serv_to_shard):
        
        shards_for_server = serv_to_shard[server_name]
        
        # send the config request to the server
        payload = {
            "schema": self.StudT_schema,
            "shards": shards_for_server
        }
          
        # print("Sleeping for 2 seconds", flush=True)          
        time.sleep(2)
        print("heartbeat: Sending config request to server", flush=True)
        
        
        status, response = synchronous_communicate_with_server(server_name, "config", payload)
        if status != 200:
            return status, response.get('message', f'Unknown error in reconfiguring server {server_name}')
        
        # print(f"heartbeat: Server {server_name} reconfigured successfully!")
        print(f"heartbeat: Server {server_name} initialized with the schema and shards successfully!", flush=True)
        shard_data_copy = {}
        shard_latest_tx_id = {}
        
        # copy the shard data from the existing server to the new server      
        for shard_id in shards_for_server:

            data_copied = False

            # find the primary server for the shard
            self.MapT_dict_lock.acquire_reader()
            primary_server = self.MapT_dict[shard_id][0]
            self.MapT_dict_lock.release_reader()
            
            if primary_server == server_name:
                print(f"heartbeat: <Error> Cannot copy data for shard {shard_id} as this server is itself the primary server for the shard!", flush=True)
                return 500, f"Internal Server Error: Cannot copy data for shard {shard_id} as this server is itself the primary server for the shard!"
            
            elif primary_server == "":
                print(f"heartbeat: <Error> Cannot copy data for shard {shard_id} as the primary server for the shard is not available!", flush=True)
                return 500, f"Internal Server Error: Cannot copy data for shard {shard_id} as the primary server for the shard is not available!"
            
            else:
                payload = {
                    "shards": [shard_id]
                }
                status, response = synchronous_communicate_with_server(primary_server, "copy", payload)
                if status != 200:
                    print(f"heartbeat: Error in copying {shard_id} data from server {primary_server} to server {server_name}\nError: {response.get('message', 'Unknown error')}", flush=True)
                    return status, response.get('message', f"Error in copying {shard_id} data from server {primary_server} to server {server_name}")
                else:
                    shard_data_copy[shard_id] = response[shard_id]
                    shard_latest_tx_id[shard_id] = response['latest_tx_ids'][shard_id]
                    data_copied = True
            
        # wrote the copied data to the new server
        for shard_id in shard_data_copy.keys():
            
            # if shard_data_copy[shard_id] is an empty list, then skip writing the data to the server 
            # as it means that the shard is empty in all the active servers and hence the new server should also have an empty shard
            if len(shard_data_copy[shard_id]) == 0:
                print(f"heartbeat: Skipping writing {shard_id} data to server {server_name} as it is empty in all active servers", flush=True)
                continue
            
            write_payload = {
                "shard": shard_id,
                # "curr_idx": 0,
                "data": shard_data_copy[shard_id],
                "tx_id": shard_latest_tx_id[shard_id]
            }
            # tasks.append(communicate_with_server(server_name, "write", write_payload))
            status, response = synchronous_communicate_with_server(server_name, "write", write_payload)
            if status != 200:
                print(f"heartbeat: Error in writing {shard_id} data to server {server_name}\nError: {response.get('message', 'Unknown error')}", flush=True)

                return 400, f"Error in writing {shard_id} data to server {server_name}"
                
            else:
                print(f"heartbeat: Successfully written {shard_id} data to server {server_name}", flush=True)
           
        return 200, "Server reconfigured successfully with all the data!"    
            

            

                
            

        
    
    