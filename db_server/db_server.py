# Import necessary modules
from aiohttp import web
from manager import Manager
import json
import os
from typing import Dict, List, Tuple
from heartbeat_new import HeartBeat
from load_balancer.docker_utils import kill_server_cntnr

server_id = os.environ.get("SERVER_ID", "server")
mgr = Manager(host='localhost',user='root',password=f"{server_id}@123")
StudT_schema = {}

hb_threads: Dict[str, HeartBeat] = {}

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
    
    
async def init_servers_hb(request):
    
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
        
        servers = set(servers)
        servers = list(servers)
        num_servers = len(servers)
        
        for server in servers:
            t1 = HeartBeat(server, StudT_schema)
            hb_threads[server] = t1
            t1.start()
            
            
        response_json = {
            "message": f"Started Heartbeat threads for servers: {', '.join(servers)}",
            "status": "success"
        }
        return web.json_response(response_json, status=200)
            
    except Exception as e:
        print(f"DB_Server: Error in init_servers_hb endpoint: {str(e)}")
        response_json = {
            "message": f"<Error>: {str(e)}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)

async def stop_servers_hb(request):
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
             
        servers = set(servers)
        servers = list(servers)
        num_servers = len(servers)
                            
        for server in servers:
            if server in hb_threads:
                hb_threads[server].stop()
                del hb_threads[server]
                
                # kill_server_cntnr(server)
                
            else:
                print(f"DB_Server: Error in stop_servers_hb endpoint: {server} not found in the list of servers")
                response_json = {
                    "message": f"<Error>: {server} not found in the list of active servers",
                    "status": "failure"
                }
                return web.json_response(response_json, status=400)
        
        response_json = {
            "message": f"Stopped Heartbeat threads for servers: {', '.join(servers)}",
            "status": "success"
        }    
        return web.json_response(response_json, status=200)
    
    except Exception as e:
        print(f"DB_Server: Error in stop_servers_hb endpoint: {str(e)}")
        response_json = {
            "message": f"<Error>: {str(e)}",
            "status": "failure"
        }
        return web.json_response(response_json, status=400)


async def list_active_hb_threads(request):
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



# Define the main function to run the web server
def run_server():
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
    
    
    app.router.add_post('/init_servers_hb', init_servers_hb)
    app.router.add_post('/stop_servers_hb', stop_servers_hb)
    app.router.add_get('/list_active_hb_threads', list_active_hb_threads)

    # Add a catch-all route for any other endpoint, which returns a 400 Bad Request
    app.router.add_route('*', '/{tail:.*}', not_found)

    # Run the web application on port 5000
    web.run_app(app, port=5000)

# Entry point of the script
if __name__ == '__main__':
    # Run the web server
    run_server()
