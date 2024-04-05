# Import necessary modules
from aiohttp import web
from manager import Manager
import json
import os

server_id = os.environ.get("SERVER_ID", "server")
mgr = Manager(host='localhost',user='root',password=f"{server_id}@123")
StudT_schema = {}

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


    # Add a catch-all route for any other endpoint, which returns a 400 Bad Request
    app.router.add_route('*', '/{tail:.*}', not_found)

    # Run the web application on port 5000
    web.run_app(app, port=5000)

# Entry point of the script
if __name__ == '__main__':
    # Run the web server
    run_server()
