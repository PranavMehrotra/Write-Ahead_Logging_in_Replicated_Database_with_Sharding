from aiohttp import web
import random
from load_balancer import LoadBalancer
from db_checkpointer import Checkpointer
from docker_utils import *
import aiohttp
import requests
from typing import Dict, List, Tuple
import bisect
from RWLock import RWLock

SERVER_PORT = 5000
NUM_INITIAL_SERVERS = 3
RANDOM_SEED = 4326
SLEEP_BEFORE_FIRST_REQUEST = 2
INT_MAX = 2**31 - 1  # 2147483647
COMMIT_ROLLBACK_RETRY_CNT = 3

lb : LoadBalancer = ""
# hb_threads: Dict[str, HeartBeat] = {}

checkpointer_thread: Checkpointer = ""

shardT_lock = RWLock()

shardT = {}   # shardT is a dictionary that maps "Stud_id_low" to a list ["Shartd_id", "Shard_size"]
                # Example: shardT[100] = ["sh1", 100]
stud_id_low: List[Tuple[int, int]] = [] # stud_id_low is a global list of tuples that stores all the (Stud_id_low, Stud_id_low + Shard_size) values (of all shards) in sorted order

StudT_schema = {}   # schema is a dictionary, which has list of all columns of the StudT table and their data types
db_server_hostname = "db_server"
ShardT_schema = {
    "columns": ["Stud_id_low", "Shard_id", "Shard_size"],
    "dtypes": ["Number", "String", "Number"],
    "pk": ["Stud_id_low"],
}

MapT_schema = {
    "columns": ["Shard_id", "Server_id"],
    "dtypes": ["String", "String"],
    "pk": [],
}

init_done = False


def generate_random_req_id():
    return random.randint(10000, 99999)

def find_shard_id(stud_id):
    
    '''
    input: stud_id
    output: shard_id, stud_id_low of shard_id, error_message
    '''
    
    global shardT
    global stud_id_low
    global shardT_lock
    
    err=""
    shardT_lock.acquire_reader()
    idx = bisect.bisect_right(stud_id_low, (stud_id, INT_MAX))-1
    # if stud_id is less than the lowest stud_id, then it is invalid
    if (idx<0):
        shardT_lock.release_reader()
        err= "Invalid Stud_id: No matching entries found in the database"
        return "", -1, err
    
    # if stud_id is greater than or equal to the shard size, then it is invalid
    elif (stud_id >= stud_id_low[idx][1]):
        shardT_lock.release_reader()
        err= "Invalid Stud_id: No matching entries found in the database"
        return "", -1, err
    
    else:
        shard_id = shardT[stud_id_low[idx][0]][0]
        shardT_lock.release_reader()
        return shard_id, stud_id_low[idx][0], err
    
# function to get the shards and the corresponding range of stud_ids for a given range (low, high) of stud_ids
def find_shard_id_range(low, high):
    
    '''
    input: low, high (range of stud_ids)
    output: list of tuples (shard_id, lower_limit, upper_limit, lowest_stud_id_in_shard) and error_message
    '''
    
    global shardT   
    global stud_id_low
    global shardT_lock
    
    if (low > high):
        return [], "Invalid range: stud_id_low > stud_id_high"
    
    err=""
    limit_left = low
    limit_right = high + 1  # to include the high value in the range
    shardT_lock.acquire_reader() 
    idx_left = bisect.bisect_right(stud_id_low, (low, INT_MAX))-1
    
    # if (idx_left > len(stud_id_low)-1):
    #     shardT_lock.release_reader()
    #     return [], "Invalid range: Both stud_id_low and stud_id_high are invalid"
    # print(f"client_handler: idx_left: {idx_left}", flush=True)
    
    if (idx_left<0):
        idx_left = 0
        
    if (low >= stud_id_low[idx_left][1]):
        idx_left += 1
 
    if (idx_left > len(stud_id_low)-1):
        shardT_lock.release_reader()
        return [], "No matching entries found for the given range of Stud_ids"

    # limit_left = stud_id_low[idx_left][0]
    limit_left = max(low, stud_id_low[idx_left][0])


    idx_right = bisect.bisect_right(stud_id_low, (high, INT_MAX))-1
    if (idx_right<0):
        shardT_lock.release_reader()
        return [], "No matching entries found for the given range of Stud_ids"
    
    if (high >= stud_id_low[idx_right][1]):
        limit_right = stud_id_low[idx_right][1]
        
    shards = []
    
    if (idx_left == idx_right): # if the range lies within a single shard
        # if (shardT[stud_id_low[idx_left]][1] > 0):
        shards.append((shardT[stud_id_low[idx_left][0]][0], limit_left, limit_right, stud_id_low[idx_left][0]))
        shardT_lock.release_reader()
        if len(shards) == 0:
            err= "No matching entries found for the given range of Stud_ids"
        return shards, err
    
    elif (idx_left < idx_right):
        shards.append((shardT[stud_id_low[idx_left][0]][0], limit_left, stud_id_low[idx_left][1], stud_id_low[idx_left][0]))
        
        for i in range(idx_left+1, idx_right):
            shards.append((shardT[stud_id_low[i][0]][0], stud_id_low[i][0], stud_id_low[i][1], stud_id_low[i][0]))

        shards.append((shardT[stud_id_low[idx_right][0]][0], stud_id_low[idx_right][0], limit_right, stud_id_low[idx_right][0]))
        shardT_lock.release_reader()
        if len(shards) == 0:
            err= "No matching entries found for the given range of Stud_ids"
        return shards, err
    
    else:
        shardT_lock.release_reader()
        return [], "No matching entries found for the given range of Stud_ids"


def check_shard_ranges(shards: list) -> Tuple[bool, str]:
    global stud_id_low
    global shardT_lock
    # Check if the shard ranges are valid
    for i in range(len(shards)):
        if shards[i][2]<=0:
            return False, "Invalid shard size"
    # Sort the shards based on the (start, end) of the shard range
    shards.sort(key=lambda x: (x[0], x[0]+x[2]))
    for i in range(len(shards)-1):
        if shards[i][0]+shards[i][2] > shards[i+1][0]:
            return False, "Shard ranges overlap"
    
    # Check if the shard ranges overlap with the existing shard ranges
    shardT_lock.acquire_reader()
    for shard in shards:
        # Use the binary search to find the shard range that overlaps with the given shard
        tem_tuple = (shard[0], shard[0]+shard[2])
        idx = bisect.bisect_left(stud_id_low, tem_tuple)
        if idx>0 and stud_id_low[idx-1][1]>tem_tuple[0]:
            shardT_lock.release_reader()
            return False, "Shard ranges overlap with existing shard ranges"
        if idx<len(stud_id_low) and stud_id_low[idx][0]<tem_tuple[1]:
            shardT_lock.release_reader()
            return False, "Shard ranges overlap with existing shard ranges"
    shardT_lock.release_reader()
    return True, ""


async def communicate_with_server(server, endpoint, payload={}):
    try:
        async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1)) as session:
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


def synchronous_communicate_with_server(server, endpoint, payload={}):
    try:
        request_url = f'http://{server}:{SERVER_PORT}/{endpoint}'
        if endpoint == "copy" or endpoint == "commit" or endpoint == "rollback":
            response = requests.get(request_url, json=payload)
            return response.status_code, response.json()
            
        elif endpoint == "read" or endpoint == "write" or endpoint == "config":
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


def synchronous_communicate_with_db_server(endpoint, payload={}):
    try:
        request_url = f'http://{db_server_hostname}:{SERVER_PORT}/{endpoint}'
        
        if endpoint == "config_change" or endpoint == "get_primary_server":
            response = requests.post(request_url, json=payload)
            return response.status_code, response.json()
        
        elif endpoint == "list_active_hb_threads":
            response = requests.get(request_url)
            return response.status_code, response.json()
        
        else:
            return 500, {"message": "Invalid endpoint"}
        
    except Exception as e:
        return 500, {"message": f"{e}"}

# async def home(request):
#     global lb
    
#     # Generate a random request id
#     req_id = generate_random_req_id()

#     # Assign a server to the request using the load balancer
    # server = lb.assign_server(req_id)
#     if (server == ""):
#         # No servers available, return a failure response
#         response_json = {
#             "message": f"<Error> Cannot process request! No active servers!",
#             "status": "failure"
#         }
#         return web.json_response(response_json, status=400)
    
#     print(f"client_handler: Request {req_id} assigned to server: {server}", flush=True)

#     try:
#         # Send the request to the server and get the response, use aiohttp
#         async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=1)) as session:
#             async with session.get(f'http://{server}:{SERVER_PORT}/home') as response:
#             # async with request.app['client_session'].get(f'http://{server}:{SERVER_PORT}/home') as response:
#                 response_json = await response.json()
#                 response_status = response.status
                
#                 # increment count of requests served by the server
#                 lb.increment_server_req_count(server)
                
#                 # Return the response from the server
#                 return web.json_response(response_json, status=response_status, headers={"Cache-Control": "no-store"})
#         # async with request.app['client_session'].get(f'http://{server}:{SERVER_PORT}/home') as response:
#             # response_json = await response.json()
#             # response_status = response.status
#             # # Return the response from the server
#             # return web.json_response(response_json, status=response_status, headers={"Cache-Control": "no-store"})
#     except:
#         # Request failed, return a failure response
#         response_json = {
#             "message": f"<Error> Request Failed",
#             "status": "failure"
#         }
#         return web.json_response(response_json, status=400)

 
# async def lb_analysis(request):
#     global lb
#     print(f"client_handler: Received Request to provide server load statistics", flush=True)
#     load_count = lb.get_server_load_stats()
#     response_json = {
#         "message": f"Server Load Statistics:",
#         "dict": load_count,
#         "status": "successful"
#     }
#     return web.json_response(response_json, status=200)    


async def add_server_handler(request):
    global lb
    # global hb_threads
    global shardT
    global shardT_lock
    global stud_id_low
    global StudT_schema
    
    try:
        # Get a payload json from the request
        payload = await request.json()
        # print(payload, flush=True)
        # Get the number of servers to be added
        num_servers = int(payload['n'])
        # Get the list of new_shards
        shards = list(payload.get('new_shards', []))
        # Get the dictionary of server to shard mapping
        serv_to_shard = dict(payload.get('servers', {}))
        
        print(f"client_handler: Received Request to add N: {num_servers} servers, shards: {shards}, server to shard mapping: {serv_to_shard}", flush=True)
    except:
        response_json = {
            "message": f"<Error> Invalid payload format",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    if num_servers<=0:
        response_json = {
            "message": f"<Error> Invalid number of servers to be added: {num_servers}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
    # Add the shards to the system
    if len(shards) > 0:
        def get_values(shard):
            return [shard[col] for col in ShardT_schema["columns"] if col in shard]

        list_of_shards = list(map(get_values, shards))
        # Sanity checks on shard sizes and no overlap in shard ranges
        success, err = check_shard_ranges(list_of_shards)
        if not success:
            response_json = {
                "message": f"<Error> {err}",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)

        print(f"client_handler: Adding Shards: {list_of_shards}", flush=True)
        new_shards = lb.add_shards(list_of_shards)
        if len(new_shards) <= 0:
            response_json = {
                "message": f"<Error> Failed to add shards to the system",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        # Populate the shardT and update stud_id_low
        # Acquire the writer lock for the shardT
        shardT_lock.acquire_writer()
        for shard in new_shards:
            shardT[shard[0]] = shard[1:]
            # Insert the (Stud_id_low, Stud_id_low + Shard_size) tuple in the stud_id_low list, maintaining the sorted order
            bisect.insort(stud_id_low, (shard[0], shard[0]+shard[2]))
        shardT_lock.release_writer()
        # Set checkpoint event
        checkpointer_thread.write_ShardT()
        print(f"client_handler: Added {len(new_shards)} shards to the system")
        print(f"client_handler: ShardT: {shardT}")
    # Add the servers to the system
    num_added, added_servers, err = lb.add_servers(num_servers, serv_to_shard)

    if num_added<=0:
        response_json = {
            "message": f"<Error> Failed to add servers to the system",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
    # Set checkpoint event
    checkpointer_thread.write_MapT()

    print(f"client_handler: Added {num_added} servers to the system")
    print(f"client_handler: Added Servers: {added_servers}", flush=True)
    if err!="":
        print(f"client_handler: Error: {err}")

    # # Spawn the heartbeat threads for the added servers
    # for server in added_servers:
    #     t1 = HeartBeat(server, StudT_schema)
    #     hb_threads[server] = t1
    #     t1.start()
    
    # Trim serv_to_shard to only include the added servers
    serv_to_shard = {server: serv_to_shard[server] for server in added_servers}

    # send message to db_server to start the heartbeat thread for the added servers
    payload = {
        "num_servers": num_added,
        "servers_to_shard": serv_to_shard,
        "action": "add_server",
    }
    status, response = synchronous_communicate_with_db_server("config_change", payload)
    if status!=200:
        print(f"client_handler: Failed to start heartbeat threads for the added servers")
        print(f"client_handler: {response.get('message', 'Unknown Error')}", flush=True)
        
        ### WHAT TO DO IN THIS CASE?? -> KILL THE SERVERS AND REMOVE THEM FROM THE SYSTEM
        
        response_json = {
            "message": f"<Error>: Failed to add servers to the system",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)

    # if status is 200, then hb threads for the added servers have started successfully, s
    # so continue with config stage
    

    # Sleep before sending the config request to the added servers
    await asyncio.sleep(SLEEP_BEFORE_FIRST_REQUEST)

    # Configure the added servers
    payload = {
        "schema": StudT_schema,
        "shards": []
    }
    for server in added_servers:
        payload["shards"] = serv_to_shard[server]
        # status, response = await communicate_with_server(server, "config", payload)
        status, response = synchronous_communicate_with_server(server, "config", payload)
        if status!=200:
            print(f"client_handler: Failed to configure server: {server}")
            print(f"client_handler: Error: {response.get('message', 'Unknown Error')}", flush=True)
            # Kill the server and let HeartBeat thread spawn a new server and configure it
            kill_server_cntnr(server)

    # Return the full list of servers in the system
    server_list = lb.list_servers()
    response_json = {
        "N" : len(server_list),
        "message": f"Added servers: {added_servers}",
        "status": "successful"
    }

    return web.json_response(response_json, status=200)


async def remove_server_handler(request):
    global lb
    # global hb_threads

    try:
        # Get a payload json from the request
        payload = await request.json()
        # payload = await request.text()
        # print(payload, flush=True)
        # Get the number of servers to be removed
        num_servers = int(payload['n'])
        # num_servers = 3
        # Get the list of preferred hostnames
        pref_hosts = list(payload.get('servers', []))
        # pref_hosts = ['pranav']
        print(f"client_handler: Received Request to remove N: {num_servers} servers, Hostnames: {pref_hosts}", flush=True)
    except:
        response_json = {
            "message": f"<Error> Invalid payload format",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)

    if num_servers<=0:
        response_json = {
            "message": f"<Error> Invalid number of servers to be removed: {num_servers}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
    # Remove the servers from the system
    num_removed, removed_servers, err = lb.remove_servers(num_servers, pref_hosts)

    if num_removed<=0:
        response_json = {
            "message": f"<Error> Failed to remove servers from the system",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
    # Set checkpoint event
    checkpointer_thread.write_MapT()
    
    print(f"client_handler: Removed {num_removed} servers from the system")
    print(f"client_handler: Removed Servers: {removed_servers}", flush=True)
    if err!="":
        print(f"client_handler: Error: {err}")

    # # Kill the heartbeat threads for the removed servers
    # for server in removed_servers:
    #     hb_threads[server].stop()
    #     del hb_threads[server]

    # send message to db_server to stop the heartbeat thread for the removed servers
    payload = {
        "num_servers": num_removed,
        "servers": removed_servers,
        "action": "remove_server",
    }
    status, response = synchronous_communicate_with_db_server("config_change", payload)
    if status!=200:
        print(f"client_handler: Failed to stop heartbeat threads for the removed servers")
        print(f"client_handler: {response.get('message', 'Unknown Error')}", flush=True)
        
        ### WHAT TO DO IN THIS CASE?? -> IF HB THREADS ARE NOT STOPPED, THEN THE SERVERS WILL BE RESPAWNED BY THE HEARTBEAT THREADS
        
        response_json = {
            "message": f"<Error>: Failed to remove servers from the system",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)

    # if status is 200, then hb threads for the removed servers have stopped successfully, 
    # so continue with killing the server containers
        
    for server in removed_servers:     
        # close the docker containers and corresponding threads for the servers that were finally removed   
        kill_server_cntnr(server)

    # Return the full list of servers in the system
    server_list = lb.list_servers()
    response_json = {
        "message": {
            "N" : len(server_list),
            "servers": removed_servers
        },
        "status": "successful"
    }

    return web.json_response(response_json, status=200)


# function to read a range of data entries from the database
async def read_data_handler(request):
    global lb
    
    print(f"client_handler: Received Request to read a range of data entries", flush=True)
    default_response_json = {
        "message": f"<Error> Internal Server Error: The requested data could not be read",
        "status": "failure"
    }
    
    try:
        request_json = await request.json()
        
        # Check if the payload has the required fields
        if 'Stud_id' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'Stud_id' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        
        stud_id_obj = request_json.get("Stud_id", {})
        
        if 'low' not in stud_id_obj or 'high' not in stud_id_obj:
            response_json = {
                "message": f"<Error> Invalid payload format: Both low and high Stud_ids are required for reading a range of entries",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        # Find the shards to be queried for the given range of Stud_ids
        shard_range_list, err = find_shard_id_range(stud_id_obj["low"], stud_id_obj["high"])
        if len(shard_range_list)==0:
            response_json = {
                "message": f"<Error> {err}",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        data_read = []
        shards_read = []
        
        # SEQUENTIAL READ REQUESTS TO ALL SHARDS 
        
        # # Read the data entries from the shards one by one
        # for entry in shard_range_list:
        #     shard_id=entry[0]
        #     low=entry[1]
        #     high=entry[2]
            
        #     req_id = generate_random_req_id()
        #     # NO NEED TO ACQUIRE READ LOCK  OF SHARD CONSISTENT HASH HERE, AS THE LOCK IS ALREADY ACQUIRED BY THE 'ASSIGN_SERVER' FUNCTION 
        #     server =lb.assign_server(shard_id, req_id) # select a server among those which have a replica of the shard (based on the consistent hashing)
            
        #     server_json = {
        #         "shard": shard_id,
        #         "Stud_id": {"low": low, "high": high}
        #     }
            
        #     status, response = synchronous_communicate_with_server(server, "read", server_json)
        #     if status==200:
        #         data_read.extend(response.get("data", []))
        #         shards_read.append(shard_id)
        #     else:
        #         data_read = []
        #         shards_read = []
        #         return web.json_response(default_response_json, status=500)

        # print(f"client_handler: Data read from shards: {shards_read}", flush=True)   
        # response_json = {
        #     "shards_queried": shards_read,
        #     "data": data_read,
        #     "status": "success"
        # }
        # return web.json_response(response_json, status=200)
    
        # PARALLEL READ REQUESTS TO ALL SHARDS USING ASYNCIO.GATHER
        tasks = []
        for shard in shard_range_list:
            shard_id = shard[0]
            low = shard[1]
            high = shard[2]
        
            req_id = generate_random_req_id()
            # NO NEED TO ACQUIRE READ LOCK  OF SHARD CONSISTENT HASH HERE, AS THE LOCK IS ALREADY ACQUIRED BY THE 'ASSIGN_SERVER' FUNCTION 
            server =lb.assign_server(shard_id, req_id) # select a server among those which have a replica of the shard (based on the consistent hashing)
 
            server_json = {
                "shard": shard_id,
                "Stud_id": {"low": low, "high": high}
            }           
            # append the read request to the list of tasks
            tasks.append(communicate_with_server(server, "read", server_json))
            
        # Wait for all the reads to complete and get the results
        results = await asyncio.gather(*tasks)
        
        success_flag = True
        for (status, response), shard_id in zip(results, shard_range_list):
        
        # for status, response in results:
            if status!=200:
                print(f"client_handler: <Error> Failed to read data from shard: {shard_id[0]}, message: {response.get('message', 'Internal Server Error')}", flush=True)
                # print(f"client_handler: <Error> Failed to read data, message: {response.get('message', 'Internal Server Error')}", flush=True)
                print(f"message : {response.get('message')}", flush=True)
                if (response.get('message', 'Internal Server Error')=="No matching entries found"):
                    continue
                    
                success_flag = False
            else:
                data_read.extend(response.get("data", []))
                shards_read.append(shard_id[0])
                
        if not success_flag:
            return web.json_response(default_response_json, status=500)
        
        if len(data_read)==0: # to handle the case when no matching entries are found
            response_json = {
                "shards_queried": shards_read,
                "message": f"No matching entries found for the given range of Stud_ids",
                "status": "success"
            }
            return web.json_response(response_json, status=200)
        
        print(f"client_handler: Data read from shards: {shards_read}", flush=True)
        response_json = {
            "shards_queried": shards_read,
            "data": data_read,
            "status": "success"
        }
        return web.json_response(response_json, status=200)
    
    except Exception as e:
        response_json = {
            "message": f"<Error> Invalid payload format: {e}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
                

# function to write data entries of one shard replica in multiple servers, to be called by the write_data_handler
async def write_one_shard(shard_id, shard_stud_id_low, data):
    global lb
    global shardT
    global shardT_lock
    # print(f"client_handler: Write request for shard: {shard_id}, data: {data}")
    temp_lock=lb.consistent_hashing[shard_id].lock
    # temp_lock.acquire_reader()
    # servers = lb.list_shard_servers(shard_id)
    # temp_lock.release_reader()
    
    # Get primary server from db_server
    payload = {
        "shard": shard_id
    }
    status, response = synchronous_communicate_with_db_server("get_primary_server", payload)
    if status!=200:
        print(f"client_handler: Failed to get primary server for shard: {shard_id}")
        print(f"client_handler: Error: {response.get('message', 'Unknown Error')}", flush=True)
        return 500, {"message": f"Internal Server Error: The requested data could not be written"}
    
    primary_server = response.get("primary_server", "")
    if primary_server=="":
        print(f"client_handler: No primary server found for shard: {shard_id}")
        return 500, {"message": f"Internal Server Error: The requested data could not be written"}
    
    secondary_servers = response.get("secondary_servers", [])
    print(f"client_handler: Shard_ID: {shard_id}, Primary Server: {primary_server}, Secondary Servers: {secondary_servers}")

    # servers_updated = []
    
    # error_msg = ""
    # error_flag = False
    # rollback = False
    temp_lock.acquire_writer()
    
    ### VALID-IDX IS NOT NEEDED ANYMORE AS WE ARE FOLLOWING THE WRITE-AHEAD LOGGING PROTOCOL
    
    # shardT_lock.acquire_reader()
    # try:
    #     valid_idx = shardT[shard_stud_id_low][2]
    # except KeyError:
    #     shardT_lock.release_reader()
    #     temp_lock.release_writer()
    #     return 500, {"message": f"Internal Server Error: The requested data could not be written"}
    # shardT_lock.release_reader()
    
    
    payload = {
        "shard": shard_id,
        # "curr_idx": valid_idx,
        "data": data,
        "is_primary": True,
        "servers": secondary_servers
    }
    # print(f"client_handler: Writing data to shard: {shard_id}, data: {data}")
    status, response = synchronous_communicate_with_server(primary_server, "write", payload)
    if status!=200:
        print(f"client_handler: Failed to write data to primary server: {primary_server}")
        print(f"client_handler: Error: {response.get('message', 'Unknown Error')}", flush=True)
        temp_lock.release_writer()
        return 500, {"message": f"Internal Server Error: The requested data could not be written"}
        
    # if rollback:
    #     print(f"client_handler: Rollback required for shard: {shard_id}", flush=True)
    #     print(f"client_handler: Error: {error_msg}", flush=True)
    #     # rollback the write operation on the servers
    #     for server in servers_updated:
    #         retry_cntr = COMMIT_ROLLBACK_RETRY_CNT
    #         while retry_cntr > 0:
    #             status, response = synchronous_communicate_with_server(server, "rollback")
    #             if status==200:
    #                 break
    #             retry_cntr -= 1
            
    #         if retry_cntr == 0:
    #             temp_lock.release_writer()
    #             print(f"client_handler: Rollback failed on server: {server}", flush=True)
    #             print(f"client_handler: Data inconsistency: The requested write created an inconsistency in the database", flush=True)
                
    #             ## TO-DO: Need to handle this case of how to make all shard copies consistent in case of a rollback failure
    #             ## FOR NOW: Just return a failure response explicitly stating data inconsistency
    #             response_json = {
    #                 "message": f"<Error> Data inconsistency: The requested write created an inconsistency in the database",
    #                 "status": "failure"
    #             }
    #             return 500, response_json
        
    #     temp_lock.release_writer()
    #     if error_flag: # it means some other error than an exception occurred while writing the entry to the servers
    #         response_json = {
    #             "message": f"<Error> {error_msg}",
    #             "status": "failure"
    #         }
    #         return 400, response_json
                
    #     else:
    #         return 500, {"message": f"Internal Server Error: The requested data could not be written"}
        
    # # commit the write operation on all the servers
    # else:
    #     # assert (servers_updated == servers)
    #     for server in servers_updated:
    #         retry_cntr = COMMIT_ROLLBACK_RETRY_CNT
    #         while retry_cntr > 0:
    #             status, response = synchronous_communicate_with_server(server, "commit")
    #             if status==200:
    #                 break
    #             retry_cntr -= 1
            
    #         if retry_cntr == 0:
    #             temp_lock.release_writer()
    #             print(f"client_handler: Commit failed on server: {server}", flush=True)
    #             print(f"client_handler: Data inconsistency: The requested write created an inconsistency in the database", flush=True)
                
    #             ## TO-DO: Need to handle this case of how to make all shard copies consistent in case of a commit failure
    #             ## FOR NOW: Just return a failure response explicitly stating data inconsistency
    #             response_json = {
    #                 "message": f"<Error> Data inconsistency: The requested write created an inconsistency in the database",
    #                 "status": "failure"
    #             }
    #             return 500, response_json
        
        
        ### VALID-IDX IS NOT NEEDED ANYMORE AS WE ARE FOLLOWING THE WRITE-AHEAD LOGGING PROTOCOL
        
        # # Update the valid_idx in the shardT
        # shardT_lock.acquire_reader()
        # try:
        #     shardT[shard_stud_id_low][2] = valid_idx + len(data)
        # except KeyError:
        #     shardT_lock.release_reader()
        #     temp_lock.release_writer()
        #     return 500, {"message": f"Internal Server Error: The requested data could not be written"}
        # shardT_lock.release_reader()
        
    temp_lock.release_writer()
    return 200, {"message": "success"}


# function to write a bunch of data entries across all shard replicas
async def write_data_handler(request):
    global lb
    
    print(f"client_handler: Received Request to write data entries", flush=True)
    default_response_json = {
        "message": f"<Error> Internal Server Error: The requested data could not be written",
        "status": "failure"
    }
    
    try:
        request_json = await request.json()
        
        if 'data' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'data' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        data_list=list(request_json["data"])
        if len(data_list)==0:
            response_json = {
                "message": f"<Error> Invalid payload format: 'data' field is empty",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        # Sort the data entries based on the Stud_id
        data_list.sort(key=lambda x: x["Stud_id"])
        # Find maximum and minimum Stud_id in the data entries
        min_stud_id = data_list[0]["Stud_id"]
        max_stud_id = data_list[-1]["Stud_id"]
        # Find the shards corresponding to the range of Stud_ids
        shard_range_list, err = find_shard_id_range(min_stud_id, max_stud_id)
        if len(shard_range_list)==0:
            response_json = {
                "message": f"<Error> {err}",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        # shard_data = {}
        data_idx = 0
        data_len = len(data_list)
        last_idx = 0
        # To write the data entries to the shards all at once, using async functions
        tasks = []
        for shard in shard_range_list:
            low = shard[1]
            high = shard[2]
            shard_id = shard[0]
            shard_stud_id_low = shard[3]
            while(True):
                temp_id = data_list[data_idx]["Stud_id"]
                if temp_id >= low:
                    if temp_id < high:
                        data_idx += 1
                    else:
                        break
                else:
                    data_idx += 1
                    last_idx = data_idx
                if data_idx >= data_len:
                    break
            
            # shard_data[shard_id] = data_list[last_idx:data_idx]
            tasks.append(write_one_shard(shard_id, shard_stud_id_low, data_list[last_idx:data_idx]))
            last_idx = data_idx
        
        assert (data_idx == data_len)

        # Wait for all the writes to complete
        results = await asyncio.gather(*tasks)
        # Check if all the writes were successful
        success_flag = True
        for status, response in results:
            if status!=200:
                print(f"client_handler: <Error> Failed to write, messsage: {response.get('message', 'Internal Server Error')}", flush=True)
                success_flag = False
    
        if not success_flag:
            return web.json_response(default_response_json, status=500)
                
        response_json = {
            "message": f"Data entries written to the database",
            "status": "success"
        }
        return web.json_response(response_json, status=200)
    
    except Exception as e:
        response_json = {
            "message": f"<Error> Invalid payload format: {e.__str__()}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)


# function to update an existing data entry across all shard replicas
async def update_data_handler(request):
    global lb
    
    print(f"client_handler: Received Request to update a data entry", flush=True)
    default_response_json = {
        "message": f"<Error> Internal Server Error: The requested Stud_id could not be updated",
        "status": "failure"
    }
    
    try:
        request_json = await request.json()
        
        # Check if the payload has the required fields
        if 'Stud_id' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'Stud_id' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
    
        if 'data' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'data' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        if 'Stud_id' not in request_json["data"]:
            response_json = {
                "message": f"<Error> Invalid payload format: 'Stud_id' field missing in 'data' field",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        stud_id=request_json.get("Stud_id")
        # Find the shard to be updated for the given Stud_id
        shard_id, shard_stud_id_low, err = find_shard_id(stud_id) 
        
        # Handle errors in finding the shard_id
        if shard_id=="":
            response_json = {
                "message": f"<Error> {err}",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)

        temp_lock=lb.consistent_hashing[shard_id].lock
    
        # Get primary server from db_server
        payload = {
            "shard": shard_id
        }
        status, response = synchronous_communicate_with_db_server("get_primary_server", payload)
        if status!=200:
            print(f"client_handler: Failed to get primary server for shard: {shard_id}")
            print(f"client_handler: Error: {response.get('message', 'Unknown Error')}", flush=True)
            return 500, {"message": f"Internal Server Error: The requested data could not be written"}
        
        primary_server = response.get("primary_server", "")
        if primary_server=="":
            print(f"client_handler: No primary server found for shard: {shard_id}")
            return 500, {"message": f"Internal Server Error: The requested data could not be written"}
        
        secondary_servers = response.get("secondary_servers", [])
        print(f"client_handler: Shard_ID: {shard_id}, Primary Server: {primary_server}, Secondary Servers: {secondary_servers}")

        # # servers to which the update request will be sent corresponding to the shard_id
        # # temp_lock.acquire_reader()
        # servers = lb.list_shard_servers(shard_id)
        # # temp_lock.release_reader()
        
        # # if no servers are available for the shard, return a failure response
        # if len(servers)==0:
        #     print(f"client_handler: No active servers for shard: {shard_id}", flush=True)
        #     return web.json_response(default_response_json, status=500)
        
        # servers_updated = []
        
        server_json = {
            "shard": shard_id,
            "Stud_id": stud_id,
            "data": dict(request_json.get("data", {})),
            "is_primary": True,
            "servers": secondary_servers
        }
        
        # error_msg = ""
        # error_flag = False
        # rollback = False


        
        temp_lock.acquire_writer()
        # update the entry on the primary server
        status, response = synchronous_communicate_with_server(primary_server, "update", server_json)
        if status!=200:
            temp_lock.release_writer()
            print(f"client_handler: Failed to update data on primary server: {primary_server}")
            print(f"client_handler: Error: {response.get('message', 'Unknown Error')}", flush=True)
            return 500, {"message": f"Internal Server Error: The requested Stud_id could not be updated"}

        # update the entry one by one on all the servers which have a replica of the shard
        # for server in servers:
        #     # update the entry on the servers one by one
        #     status, response = synchronous_communicate_with_server(server, "update", server_json)
        #     if status==200:
        #         servers_updated.append(server)
        #     else: # if the update request failed on a server
        #         if status!=500:
        #             error_msg = response.get("message", "Unknown Error")
        #             error_flag = True
        #         rollback = True # set the rollback flag to True to rollback the update operation on all the servers
        #         break

        
        # if rollback:
        #     # rollback the update operation on the servers
        #     for server in servers_updated:
        #         retry_cntr = COMMIT_ROLLBACK_RETRY_CNT # retry counter for commit/rollback operations on the servers
        #         while retry_cntr > 0:
        #             status, response = synchronous_communicate_with_server(server, "rollback")
        #             if status==200:
        #                 break
        #             retry_cntr -= 1
                
        #         if retry_cntr == 0: # if the rollback operation failed on a server
        #             temp_lock.release_writer()
        #             print(f"client_handler: Rollback failed on server: {server}", flush=True)
        #             print(f"client_handler: Data inconsistency: The requested update created an inconsistency in the database", flush=True)
                    
        #             ## TO-DO: Need to handle this case of how to make all shard copies consistent in case of a rollback failure
        #             ## FOR NOW: Just return a failure response explicitly stating data inconsistency
        #             response_json = {
        #                 "message": f"<Error> Data inconsistency: The requested update created an inconsistency in the database",
        #                 "status": "failure"
        #             }

        #             return web.json_response(response_json, status=500)
             
        #     temp_lock.release_writer()
        #     if error_flag: # it means some other error than an exception occurred while updating the servers
        #         response_json = {
        #             "message": f"<Error> {error_msg}",
        #             "status": "failure"
        #         }
                
        #         return web.json_response(response_json, status=400)
                    
        #     else:   
        #         return web.json_response(default_response_json, status=500)
            
        # # commit the update operation on all the servers if the update was successful on all the servers
        # else:
            
        #     # assert (servers_updated == servers) # as all servers should have been updated
        #     for server in servers_updated:
        #         retry_cntr = COMMIT_ROLLBACK_RETRY_CNT
        #         while retry_cntr > 0:
        #             status, response = synchronous_communicate_with_server(server, "commit")
        #             if status==200:
        #                 break
        #             retry_cntr -= 1
                
        #         if retry_cntr == 0: # if the commit operation failed on a server
        #             temp_lock.release_writer()
        #             print(f"client_handler: Commit failed on server: {server}", flush=True)
        #             print(f"client_handler: Data inconsistency: The requested update created an inconsistency in the database", flush=True)
                    
        #             ## TO-DO: Need to handle this case of how to make all shard copies consistent in case of a commit failure
        #             ## FOR NOW: Just return a failure response explicitly stating data inconsistency
        #             response_json = {
        #                 "message": f"<Error> Data inconsistency: The requested update created an inconsistency in the database",
        #                 "status": "failure"
        #             }
                    
        #             return web.json_response(response_json, status=500)
                
        temp_lock.release_writer()
        print(f"client_handler: Data entry with Stud_id:{stud_id} updated in all replicas", flush=True)
        response_json = {
            "message": f"Data entry with Stud_id:{stud_id} updated in the database",
            "status": "success"
        }
        return web.json_response(response_json, status=200)
        
    except Exception as e:
        ## Not Safe to release the lock here, as the lock might not have been acquired by this function
        # # Check if the lock was acquired before releasing it
        # if temp_lock.acquired_by_reader():
        #     temp_lock.release_reader()
        # elif temp_lock.acquired_by_writer():
        #     temp_lock.release_writer()
        response_json = {
            "message": f"<Error> Invalid payload format: {e}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
                    
                
# function to delete a data entry from all shard replicas
async def del_data_handler(request):
    global lb
    
    print(f"client_handler: Received Request to delete a data entry", flush=True)
    default_response_json = {
        "message": f"<Error> Internal Server Error: The requested Stud_id could not be deleted",
        "status": "failure"
    }
    
    try:
        request_json = await request.json()
        
        # Check if the payload has the required fields
        if 'Stud_id' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'Stud_id' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        stud_id=request_json.get("Stud_id")
        # Find the shard from which the entry is to be deleted for the given Stud_id
        shard_id, shard_stud_id_low, err = find_shard_id(stud_id)
        if shard_id=="":
            response_json = {
                "message": f"<Error> {err}",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        temp_lock=lb.consistent_hashing[shard_id].lock

        # Get primary server from db_server
        payload = {
            "shard": shard_id
        }
        status, response = synchronous_communicate_with_db_server("get_primary_server", payload)
        if status!=200:
            print(f"client_handler: Failed to get primary server for shard: {shard_id}")
            print(f"client_handler: Error: {response.get('message', 'Unknown Error')}", flush=True)
            return 500, {"message": f"Internal Server Error: The requested data could not be written"}
        
        primary_server = response.get("primary_server", "")
        if primary_server=="":
            print(f"client_handler: No primary server found for shard: {shard_id}")
            return 500, {"message": f"Internal Server Error: The requested data could not be written"}
        
        secondary_servers = response.get("secondary_servers", [])
        print(f"client_handler: Shard_ID: {shard_id}, Primary Server: {primary_server}, Secondary Servers: {secondary_servers}")

        # # temp_lock.acquire_reader()
        # servers = lb.list_shard_servers(shard_id)
        # # temp_lock.release_reader()
        
        # # if no servers are available for the shard, return a failure response
        # if len(servers)==0:
        #     print(f"client_handler: No active servers for shard: {shard_id}", flush=True)
        #     return web.json_response(default_response_json, status=500)
        
        # servers_updated = []
        
        server_json = {
            "shard": shard_id,
            "Stud_id": stud_id,
            "is_primary": True,
            "servers": secondary_servers
        }
        
        # error_msg = ""
        # error_flag = False
        # rollback = False

        temp_lock.acquire_writer()
        # delete the entry on the primary server
        status, response = synchronous_communicate_with_server(primary_server, "del", server_json)
        if status!=200:
            temp_lock.release_writer()
            print(f"client_handler: Failed to delete data on primary server: {primary_server}")
            print(f"client_handler: Error: {response.get('message', 'Unknown Error')}", flush=True)
            return 500, {"message": f"Internal Server Error: The requested Stud_id could not be deleted"}
        

        # for server in servers:            
        #     # delete the entry from the servers one by one
        #     status, response = synchronous_communicate_with_server(server, "del", server_json)
        #     if status==200:
        #         servers_updated.append(server)
        #     else:
        #         if status!=500: # if the delete request failed on a server, set the error flag and rollback flag
        #             error_msg = response.get("message", "Unknown Error")
        #             error_flag = True
        #         rollback = True # set the rollback flag to True to rollback the delete operation on all the servers
        #         break
          
        
        # if rollback:
        #     # rollback the delete operation on the servers
        #     for server in servers_updated:
        #         retry_cntr = COMMIT_ROLLBACK_RETRY_CNT
        #         while retry_cntr > 0:
        #             status, response = synchronous_communicate_with_server(server, "rollback")
        #             if status==200:
        #                 break
        #             retry_cntr -= 1
                
        #         if retry_cntr == 0: # if the rollback operation failed on a server
        #             temp_lock.release_writer()
        #             print(f"client_handler: Rollback failed on server: {server}", flush=True)
        #             print(f"client_handler: Data inconsistency: The requested update created an inconsistency in the database", flush=True)
                    
        #             ## TO-DO: Need to handle this case of how to make all shard copies consistent in case of a rollback failure
        #             ## FOR NOW: Just return a failure response explicitly stating data inconsistency
        #             response_json = {
        #                 "message": f"<Error> Data inconsistency: The requested deletion created an inconsistency in the database",
        #                 "status": "failure"
        #             }
        #             return web.json_response(response_json, status=500)
            
        #     temp_lock.release_writer()
        #     if error_flag: # it means some other error than an exception occurred while deleting the entry from the servers
        #         response_json = {
        #             "message": f"<Error> {error_msg}",
        #             "status": "failure"
        #         }
        #         return web.json_response(response_json, status=400)
                    
        #     else:
        #         return web.json_response(default_response_json, status=500)  
      
      
        # # commit the delete operation on all the servers if the delete was successful on all the servers
        # else:
 
        #     # assert (servers_updated == servers) # as all servers should be updated
        #     for server in servers_updated:
        #         retry_cntr = COMMIT_ROLLBACK_RETRY_CNT
        #         while retry_cntr > 0:
        #             status, response = synchronous_communicate_with_server(server, "commit")
        #             if status==200:
        #                 break
        #             retry_cntr -= 1 

        #         if retry_cntr == 0: # if the commit operation failed on a server
        #             temp_lock.release_writer()
        #             print(f"client_handler: Commit failed on server: {server}", flush=True)
        #             print(f"client_handler: Data inconsistency: The requested update created an inconsistency in the database", flush=True)                   
                    
        #             ## TO-DO: Need to handle this case of how to make all shard copies consistent in case of a commit failure
        #             ## FOR NOW: Just return a failure response explicitly stating data inconsistency
        #             response_json = {
        #                 "message": f"<Error> Data inconsistency: The requested deletion created an inconsistency in the database",
        #                 "status": "failure"
        #             }
        #             return web.json_response(response_json, status=500)

            ### VALID-IDX IS NOT NEEDED ANYMORE AS WE ARE FOLLOWING THE WRITE-AHEAD LOGGING PROTOCOL

            # # Update the valid_idx in the shardT, without acquiring writer lock
            # shardT_lock.acquire_reader()
            # shardT[shard_stud_id_low][2] -= 1
            # shardT_lock.release_reader()

        temp_lock.release_writer()
        print(f"client_handler: Data entry with Stud_id:{stud_id} deleted from all replicas", flush=True)
        response_json = {
            "message": f"Data entry with Stud_id:{stud_id} deleted from all replicas",
            "status": "success"
        }
        return web.json_response(response_json, status=200)
        
    except Exception as e:
        response_json = {
            "message": f"<Error> Invalid payload format: {e}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    

# Function to send a heartbeat to the db server and return True if the server is alive, else False
def heartbeat_db_server():
    try:
        response = requests.get(f'http://{db_server_hostname}:{SERVER_PORT}/heartbeat')
        response_status = response.status_code
        if response_status == 200:
            return True
        else:
            return False
    except:
        return False


async def spawn_and_config_db_server(serv_to_shard: Dict[str, list]):
    global StudT_schema
    
    # Spawn the db_server container
    if not spawn_db_server_cntnr(db_server_hostname):
        response_json = {
            "message": f"<Error> Failed to start the db_server container",
            "status": "failure"
        }
        return False, response_json
    
    await asyncio.sleep(SLEEP_BEFORE_FIRST_REQUEST)
    # Configure the db_server with the two tables: ShardT and MapT
    payload = {
        "schemas": {
            "ShardT": ShardT_schema,
            "MapT": MapT_schema
        },
        "StudT_schema": StudT_schema,
    }
    # if not await config_server(db_server_hostname, payload):
    status, response = synchronous_communicate_with_server(db_server_hostname, "config", payload)
    if status!=200:
        response_json = {
            "message": f"<Error> Failed to configure the db_server, error: {response}",
            "status": "failure"
        }
        return False, response_json
    
    # Populate the ShardT_schema and MapT_schema
    payload = {
        "table": "ShardT",
        "data": [],
    }
    for shard, val in shardT.items():
        # Map ShardT_schema["columns"] to shard, val
        payload["data"].append(dict(zip(ShardT_schema["columns"], [shard] + val)))
    # print(f"client_handler: Writing ShardT to db_server: {payload}", flush=True)
    status, response = synchronous_communicate_with_server(db_server_hostname, "write", payload)
    # if not await write_server(db_server_hostname, payload):
    if status!=200:
        response_json = {
            "message": f"<Error> Failed to write ShardT table to the db_server",
            "status": "failure"
        }
        return False, response_json
    
    payload = {
        "table": "MapT",
        "data": [],
    }
    for server, shards in serv_to_shard.items():
        for shard in shards:
            payload["data"].append(dict(zip(MapT_schema["columns"], [shard, server])))
    # if not await write_server(db_server_hostname, payload):
    status, response = synchronous_communicate_with_server(db_server_hostname, "write", payload)
    if status!=200:
        response_json = {
            "message": f"<Error> Failed to write MapT table to the db_server",
            "status": "failure"
        }
        return False, response_json
    print(f"client_handler: db_server container started and configured successfully")
    response_json = {
        "message": f"db_server container started and configured successfully",
        "status": "successful"
    }
    return True, response_json

async def init_handler(request):
    global lb
    # global hb_threads
    global shardT
    global shardT_lock
    global StudT_schema
    global stud_id_low
    global checkpointer_thread
    global init_done
    # Check init_done flag
    if init_done:
        response_json = {
            "message": f"<Error> Database already initialized from the /init endpoint, init can be done only once",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    try:
        # Get a payload json from the request
        payload = await request.json()
        # print(payload, flush=True)
        # Get the number of servers to be added
        num_servers = int(payload['N'])
        # Get the StudT_schema
        studt_schema = dict(payload['schema'])
        # Get the list of new shards and their details
        shards = list(payload['shards'])
        # Get the dictionary of server to shard mapping
        serv_to_shard = dict(payload['servers'])
        
        print(f"client_handler: Received Request to add N: {num_servers} servers, StudT schema: {studt_schema}, shards: {shards}, server to shard mapping: {serv_to_shard}", flush=True)
    except:
        response_json = {
            "message": f"<Error> Invalid payload format",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    if num_servers<=0:
        response_json = {
            "message": f"<Error> Invalid number of servers to be added: {num_servers}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
    # If the db_server container is already running, then remove the existing servers, stop the heartbeat threads
    if heartbeat_db_server():
        # Kill the heartbeat threads for tem_servers
        # tem_servers = list(hb_threads.keys())
        
        status, response = synchronous_communicate_with_db_server("list_active_hb_threads", payload={})
        if status!=200:
            print(f"client_handler: <Error> Failed to list active heartbeat threads from db_server: {response.get('message', 'Unknown Error')}", flush=True)
            response_json = {
                "message": f"<Error>: Failed to initialize the database",
                "status": "failure"
            }
            return web.json_response(response_json, status=500)

        tem_servers = response.get("active_hb_threads", [])
        
        # # stop the heartbeat threads for the servers that were finally removed
        # for server in tem_servers:
        #     hb_threads[server].stop()
        #     del hb_threads[server]
            
        # send message to db_server to stop the heartbeat thread for the removed servers
        payload = {
            "num_servers": len(tem_servers),
            "servers": tem_servers,
            "action": "remove_server"
        }
        status, response = synchronous_communicate_with_db_server("config_change", payload)
        if status!=200:
            print(f"client_handler: Failed to stop heartbeat threads for the removed servers")
            print(f"client_handler: {response.get('message', 'Unknown Error')}", flush=True)
            
            ### WHAT TO DO IN THIS CASE?? -> IF HB THREADS ARE NOT STOPPED, THEN THE SERVERS WILL BE RESPAWNED BY THE HEARTBEAT THREADS
            
            response_json = {
                "message": f"<Error>: Failed to remove servers from the system",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)

        # if status is 200, then hb threads for the removed servers have stopped successfully, 
        # so continue with killing the server containers            
            
        for server in tem_servers:    
            kill_server_cntnr(server)
        
        
        # Stop checkpointer thread
        checkpointer_thread.stop()
        del checkpointer_thread
        # Kill the db_server container
        kill_db_server_cntnr(db_server_hostname)

        # Acquire the writer lock for the old shardT
        shardT_lock.acquire_writer()
        # Delete the LoadBalancer object
        del lb
        # New LoadBalancer object
        lb = LoadBalancer()
        # Clear the shardT and stud_id_low
        shardT = {}
        stud_id_low = []
        
        # # Clear the hb_threads
        # hb_threads = {}
        
        shardT_lock.release_writer()

        # Start a new checkpointer thread
        checkpointer_thread = Checkpointer(lb, shardT, shardT_lock, db_server_hostname)
        checkpointer_thread.start()

    new_shards = []
    # Add the shards to the system
    if len(shards) > 0:
        def get_values(shard):
            return [shard[col] for col in ShardT_schema["columns"] if col in shard]
        
        list_of_shards = list(map(get_values, shards))
        # Sanity checks on shard sizes and no overlap in shard ranges
        success, err = check_shard_ranges(list_of_shards)
        if not success:
            response_json = {
                "message": f"<Error> {err}",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)

        print(f"client_handler: Adding Shards: {list_of_shards}", flush=True)
        new_shards = lb.add_shards(list_of_shards)
        if len(new_shards) <= 0:
            response_json = {
                "message": f"<Error> Failed to add shards to the system",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        # Populate the shardT and update stud_id_low
        # Acquire the writer lock for the shardT
        shardT_lock.acquire_writer()
        for shard in new_shards:
            shardT[shard[0]] = shard[1:]
        stud_id_low = [(shard[0], shard[0]+shard[2]) for shard in new_shards]
        stud_id_low.sort()
        shardT_lock.release_writer()
        print(f"client_handler: Added {len(new_shards)} shards to the system")
        print(f"client_handler: ShardT: {shardT}", flush=True)
    # Add the servers to the system
    num_added, added_servers, err = lb.add_servers(num_servers, serv_to_shard)
    if err!="":
        print(f"client_handler: Error: {err}")
    if num_added<=0:
        response_json = {
            "message": f"<Error> Failed to add servers to the system",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    print(f"client_handler: Added {num_added} servers to the system", flush=True)

    # Populate the StudT_schema
    StudT_schema = studt_schema
    
    # Trim serv_to_shard to only include the added servers
    serv_to_shard = {server: serv_to_shard[server] for server in added_servers}


    # db_server is not running, spawn a new container and configure it
    success, response_json = await spawn_and_config_db_server(serv_to_shard)
    if not success:
        return web.json_response(response_json, status=400)
    
    # # Spawn the heartbeat threads for the added servers
    # for server in added_servers:
    #     t1 = HeartBeat(server, StudT_schema)
    #     hb_threads[server] = t1
    #     t1.start()
    
    # send message to db_server to start the heartbeat thread for the added servers
    payload = {
        "num_servers": num_added,
        "servers_to_shard": serv_to_shard,
        "action": "add_server"
    }
    status, response = synchronous_communicate_with_db_server("config_change", payload)
    if status!=200:
        print(f"client_handler: Failed to start heartbeat threads for the added servers")
        print(f"client_handler: {response.get('message', 'Unknown Error')}", flush=True)
        
        ### WHAT TO DO IN THIS CASE?? -> KILL THE SERVERS AND REMOVE THEM FROM THE SYSTEM
        
        response_json = {
            "message": f"<Error>: Failed to initisalize the database",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)

    # if status is 200, then hb threads for the added servers have started successfully, s
    # so continue with config stage

    
    # await asyncio.sleep(SLEEP_BEFORE_FIRST_REQUEST)


    payload = {
        "schema": studt_schema,
        "shards": []
    }
    # Configure the servers with the StudT schema and the server to shard mapping
    for server in added_servers:
        payload["shards"] = serv_to_shard[server]
        # Send a POST request to the server /config endpoint to initialize the database
        # if not await config_server(server, payload):
        status, response = synchronous_communicate_with_server(server, "config", payload)
        if status!=200:
            error=str(response.get("message", "Unknown Error"))
            response_json = {
                "message": f"<Error> Failed to configure server {server}: {error}", 
                "status": "failure"
            }
            return web.json_response(response_json, status=400)



    print(f"client_handler: Initialized all the servers and configured them successfully", flush=True)
    # Set the init_done flag to True
    init_done = True

    response_json = {
        "message": "Configured Database",
        "status": "successful"
    }
    return web.json_response(response_json, status=200)

async def status_handler(request):
    global lb
    global shardT
    global StudT_schema
    global init_done
    servers, serv_to_shard = lb.list_servers(send_shard_info=True)
    shards = [dict(zip(ShardT_schema["columns"], [shard] + shardT[shard])) for shard in shardT.keys()]
    for shard in shards:
        # Get primary server from db_server
        payload = {
            "shard": shard["Shard_id"]
        }
        status, response = synchronous_communicate_with_db_server("get_primary_server", payload)
        if status!=200:
            print(f"client_handler: Failed to get primary server for shard: {shard['Shard_id']}")
            print(f"client_handler: Error: {response.get('message', 'Unknown Error')}", flush=True)
            return 500, {"message": f"Internal Server Error: The requested data could not be written"}
        primary_server = response.get("primary_server", "")
        if primary_server=="":
            print(f"client_handler: No primary server found for shard: {shard['Shard_id']}")
            return 500, {"message": f"Internal Server Error: The requested data could not be written"}
        
        shard["primary_server"] = primary_server
    response_json = {
        "message": {
            "N": len(servers),
            "schema": StudT_schema,
            "shards": shards,
            "servers": serv_to_shard,
        },
        "status": "successful"
    }
    return web.json_response(response_json, status=200)

def recover_from_db_server():
    global lb
    # global hb_threads
    global shardT
    global StudT_schema
    try:
        if heartbeat_db_server():
            # Get the database from the db_server
            response = requests.get(f'http://{db_server_hostname}:{SERVER_PORT}/read')
            response_status = response.status_code
            response_json = response.json()
            if response_status == 200:
                print(f"client_handler: Database recovery from db_server started")
                # Get the StudT_schema
                StudT_schema = response_json["StudT_schema"]
                database = response_json["data"]
                shardT_data = database["ShardT"]
                # Add the shards to the system
                new_shards = lb.add_shards(shardT_data)
                if len(new_shards) <= 0:
                    print(f"client_handler: Failed to add shards to the system")
                    return False
                # Populate the shardT
                for shard in new_shards:
                    shardT[shard[0]] = shard[1:]
                print(f"client_handler: Added {len(new_shards)} shards to the system")
                print(f"client_handler: ShardT: {shardT}")
                
                # Get the MapT
                mapT_data = database["MapT"]
                serv_to_shard = {}
                for val in mapT_data:
                    if val[1] not in serv_to_shard:
                        serv_to_shard[val[1]] = []
                    serv_to_shard[val[1]].append(val[0])
                # Add the servers to the system
                num_added, added_servers, err = lb.add_servers(len(serv_to_shard), serv_to_shard, should_spawn=False)
                if err!="":
                    print(f"client_handler: Error: {err}")
                    return False
                if num_added<=0:
                    print(f"client_handler: Failed to add servers to the system")
                    return False
                print(f"client_handler: Added {num_added} servers to the system")
                
                # Trim serv_to_shard to only include the added servers
                serv_to_shard = {server: serv_to_shard[server] for server in added_servers}


                # # Spawn the heartbeat threads for the added servers
                # for server in added_servers:
                #     t1 = HeartBeat(server, StudT_schema)
                #     hb_threads[server] = t1
                #     t1.start()
                
                # send message to db_server to start the heartbeat thread for the added servers
                payload = {
                    "num_servers": num_added,
                    "servers_to_shard": serv_to_shard,
                    "action": "add_server"
                }
                status, response = synchronous_communicate_with_db_server("config_change", payload)
                if status!=200:
                    print(f"client_handler: Failed to start heartbeat threads for the added servers")
                    print(f"client_handler: {response.get('message', 'Unknown Error')}", flush=True)
                    
                    ### WHAT TO DO IN THIS CASE?? -> KILL THE SERVERS AND REMOVE THEM FROM THE SYSTEM
                    
                    response_json = {
                        "message": f"<Error>: Failed to recover from db_server",
                        "status": "failure"
                    }
                    # return web.json_response(response_json, status=400)   
                    return False             
                
                return True
            else:
                return False
        else:
            return False
    except Exception as e:
        print(f"client_handler: Error in recovering from db_server: {str(e)}")
        return False

async def not_found(request):
    global lb
    print(f"client_handler: Invalid Request Received: {request.rel_url}", flush=True)
    response_json = {
        "message": f"<Error> '{request.rel_url}' endpoint does not exist in server replicas",
        "status": "failure"
    }
    return web.json_response(response_json, status=400)

    
async def list_servers_from_lb(request):
    global lb

    try:
        request_json = await request.json()
        
        # check if the payload has the required fields
        if 'send_shard_info' not in request_json or request_json['send_shard_info'] not in [True, False]:
            response_json = {
                "message": f"<Error> Invalid payload format: 'send_shard_info' field missing or invalid in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        send_shard_info = request_json['send_shard_info']
        
        if send_shard_info:
            servers, serv_to_shard = lb.list_servers(send_shard_info=True)
            response_json = {
                "servers": servers,
                "serv_to_shard": serv_to_shard
            }
            return web.json_response(response_json, status=200)
        
        else:
            servers = lb.list_servers(send_shard_info=False)
            response_json = {
                "servers": servers
            }
            return web.json_response(response_json, status=200)
            
    except Exception as e:
        print(f"client_handler: <Error> : {str(e)}", flush=True)
        response_json = {
            "message": f"<Error>: {str(e)}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
async def add_servers_to_lb(request):
    global lb
    
    try:
        request_json = await request.json()
        
        if 'num_servers' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'num_servers' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        if 'serv_to_shard' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'serv_to_shard' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        num_servers = request_json['num_servers']
        serv_to_shard = request_json.get('serv_to_shard', {})
        
        if num_servers != len(list(serv_to_shard.keys())):
            response_json = {
                "message": f"<Error> Invalid payload format: 'num_servers' field and no of servers in 'serv_to_shard' do not match",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        num_servers_added, added_servers, err = lb.add_servers(num_servers, serv_to_shard)
        if num_servers_added == num_servers and err=="":
            response_json = {
                "message": f"Added {num_servers_added} servers to the consistent hashing",
                "status": "success"
            }
            return web.json_response(response_json, status=200)
        else:
            response_json = {
                "message": f"<Error> Failed to add servers to the consistent hashing: {err}",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
    except Exception as e:
        print(f"client_handler: <Error> : {str(e)}", flush=True)
        response_json = {
            "message": f"<Error>: {str(e)}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
            
    
async def remove_servers_from_lb(request):
    global lb
    
    try:
        request_json = await request.json()
        
        if 'num_servers' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'num_servers' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        if 'servers' not in request_json:
            response_json = {
                "message": f"<Error> Invalid payload format: 'servers' field missing in request",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        servers = request_json.get('servers', [])
        num_servers = request_json['num_servers']
        
        if num_servers != len(servers):
            response_json = {
                "message": f"<Error> Invalid payload format: 'num_servers' field and no of servers in 'servers' do not match",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
        num_servers_removed, removed_servers, err = lb.remove_servers(servers)
        
        if num_servers_removed == num_servers and err=="":
            response_json = {
                "message": f"Removed {num_servers_removed} servers from the consistent hashing",
                "status": "success"
            }
            return web.json_response(response_json, status=200)
        else:
            response_json = {
                "message": f"<Error> Failed to remove servers from the consistent hashing: {err}",
                "status": "failure"
            }
            return web.json_response(response_json, status=400)
        
    except Exception as e:
        print(f"client_handler: <Error> : {str(e)}", flush=True)
        response_json = {
            "message": f"<Error>: {str(e)}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)
    
async def list_shard_servers(request):
    
    
    pass
        

def run_load_balancer():
    global lb
    # global hb_threads
    global shardT
    global shardT_lock
    global db_server_hostname
    global checkpointer_thread
    random.seed(RANDOM_SEED)
    

    ### Add check if DB server already exists(by sending heartbeat to it)
    ### if yes, then copy the configurations and start heartbeat threads for them 
    ### DO NOT SPAWN NEW SERVERS, cause they might be already running, if not, heartbeat will take care of it
    # initial_servers = []
    # for i in range(NUM_INITIAL_SERVERS):
    #     initial_servers.append(f"server{i+1}")
    lb = LoadBalancer()
    ### Call Add shards and add servers here(without spawning new containers)
    # Try to recover from the db_server
    done = recover_from_db_server()
    if done:
        print(f"client_handler: Recovered from db_server successfully", flush=True)
    else:
        print(f"client_handler: DB server not running, starting fresh", flush=True)

    # Spawn Checkpointer thread
    checkpointer_thread = Checkpointer(lb, shardT, shardT_lock, db_server_hostname)
    checkpointer_thread.start()

    app = web.Application()
    # app.router.add_get('/home', home)
    # app.router.add_get('/lb_analysis', lb_analysis)
    
    app.router.add_post('/add', add_server_handler)
    app.router.add_delete('/rm', remove_server_handler)
    app.router.add_post('/init', init_handler)
    app.router.add_get('/status', status_handler)
    
    app.router.add_post('/read', read_data_handler)
    app.router.add_post('/write', write_data_handler)
    app.router.add_put('/update', update_data_handler)
    app.router.add_delete('/del', del_data_handler)
    
    
    # For communicating b/w Shard Manager and Load Balancer
    app.router.add_post('/list_servers_lb', list_servers_from_lb)
    app.router.add_post('/add_servers_lb', add_servers_to_lb)
    app.router.add_post('/remove_servers_lb', remove_servers_from_lb)
    app.router.add_post('/list_shard_servers', list_shard_servers)

    app.router.add_route('*', '/{tail:.*}', not_found)

    web.run_app(app, port=5000)

    print("Load Balancer Ready!", flush=True)

    # for thread in hb_threads.values():
    #     thread.join()


# if __name__ == "__main__":
#     # global shardT
#     # global stud_id_low
#     shardT = {}
#     stud_id_low = []

#     shardT[0] = ["sh1", 100, 0]
#     shardT[100] = ["sh2", 100, 0]
#     shardT[200] = ["sh3", 100, 0]
#     shardT[300] = ["sh4", 100, 0]
#     shardT[400] = ["sh5", 100, 0]
#     shardT[500] = ["sh6", 79, 0]
#     shardT[580] = ["sh7", 100, 0]
#     shardT[700] = ["sh8", 100, 0]
#     shardT[800] = ["sh9", 100, 0]
#     shardT[900] = ["sh10", 100, 0]
#     shardT[1001] = ["sh11", 1, 0]
#     shardT[1002] = ["sh12", 0, 0]
    
    
#     stud_id_low = [(0, 100), (100, 200), (200, 300), (300, 400), (400, 500), (500, 579), (580, 680), (700, 800), (800, 900), (900, 1000), (1001, 1002), (1002, 1002)]
    
#     # shard_id1, err = find_shard_id(51)
#     # print(f"Shard ID for 51: {shard_id1}, {err}")
#     # shard_id2, err = find_shard_id(0)
#     # print(f"Shard ID for 0: {shard_id2}, {err}")
#     # shard_id3, err = find_shard_id(-1)
#     # print(f"Shard ID for -1: {shard_id3}, {err}")
#     # shard_id4, err = find_shard_id(1000)
#     # print(f"Shard ID for 1000: {shard_id4}, {err}")
#     # shard_id5, err = find_shard_id(1001)
#     # print(f"Shard ID for 1001: {shard_id5}, {err}")
#     # shard_id6, err = find_shard_id(1002)
#     # print(f"Shard ID for 1002: {shard_id6}, {err}")

    
#     low = 593
#     high= 632
#     shards, err = find_shard_id_range(low, high)
#     print(f"Shards for the range: {low}-{high}: {shards}, {err}")

#     shard, _, err = find_shard_id(low)
#     print(f"Shard for the value: {low}: {shard}, {err}")
    
    