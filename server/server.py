# Import necessary modules
from aiohttp import web
from manager import Manager
import os

server_id = os.environ.get("SERVER_ID", "server")
# print(f"Server ID: {server_id}")
mgr = Manager(host='localhost',user='root',password=f"{server_id}@123")

# Config endpoint to initialize the database
async def config(request):
    try:
        print("Config endpoint called")

        # Get the JSON data from the request
        request_json = await request.json() 
        
        # Call the Config_database method of the Manager class 
        message, status = mgr.Config_database(request_json)

        response_json = {}
        # Create a response JSON
        if status == 200:
            shards = request_json.get('shards', [])
            shards=set(shards)
            # print(f"Shards: {shards}", flush=True)
            message = ", ".join([f"{server_id}:{shard}" for shard in shards]) + " configured"
            
            response_json = {
                "message": message,
                "status": "success"
            }
            
        else:
           # fail json message
            response_json = {
                "message": str(message),
                # "message": f"<Error>: {message}",
                "status": "failure"
            }
        
        return web.json_response(response_json, status=status)

    except Exception as e:
        print(f"Server: Error in Config endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

# Heartbeat endpoint to check if the server is running    
async def heartbeat(request):
    try:
        return web.Response(status=200)
    
    except Exception as e:
        print(f"Server: Error in heartbeat endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

# Copy endpoint to return database entries
async def copy_database(request):
    try:
        print("Copy endpoint called", flush=True)
        # Get the JSON data from the request
        request_json = await request.json()         
        database_copy, status = mgr.Copy_database(request_json)
        
        response_json = {}
        # Create a response JSON
        if status == 200:
            response_json = database_copy
            response_json["status"] = "success"
            
        else:
            message = database_copy
            response_json = {
                # "error": str(message),
                "message": str(message),
                # "message": f"<Error>: {message}",
                "status": "failure"
            }
        return web.json_response(response_json, status=status)
    
    except Exception as e:
        
        print(f"Server: Error in copy endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

# Read endpoint to return database entries where Stud_id>= low and Stud_id < high
async def read_database(request):
    try:
        print("Read endpoint called")
        # Get the JSON data from the request
        request_json = await request.json()  
        database_entry, status = mgr.Read_database(request_json)
        
        response_data = {}
        # Create a response JSON
        if status==200:
            response_data = {
                "data": database_entry,
                "status": "success"
            }
        else:
            message = database_entry
            response_data = {
                "message": str(message),
                # "message": f"<Error>: {message}",
                "status": "failure"
            }
    
        return web.json_response(response_data, status=status)
   
    except Exception as e:
    
        print(f"Server: Error in read endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

# Write endpoint to add a new entry to the database
async def write_database(request):
    
    try:
        print("Write endpoint called")
        # Get the JSON data from the request
        request_json = await request.json()  
        message, status,id = mgr.Write_database(request_json)
        
        response_data = {}
        # Create a response JSON
        if status == 200:
            response_data = {
                "message": message,
                # "current_idx":id,
                "status": "success"
            }
            
        else:
            response_data = {
                # "error": message,
                # "message": f"<Error>: {message}",
                "message": str(message),
                # "current_idx":id,
                "status": "failure"
            }
        # print(f"Response: {response_data}", flush=True)
        return web.json_response(response_data, status=status)  
    
    except Exception as e:
        
        print(f"Server: Error in write endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)  

# Update endpoint to update an existing entry in the database
async def update(request):
    
    try:
        print("Update endpoint called")
        # Get the JSON data from the request
        request_json = await request.json()  
        message, status = mgr.Update_database(request_json)
    
        response_data = {}
        # Create a response JSON
        if status == 200:
            stud_id = request_json.get("Stud_id",[])
            response_data = {
                "message": f"Data entry for Stud_id:{stud_id} updated",
                "status": "success"
            }
        
        else:
            response_data = {
                "message": str(message),
                # "message": f"<Error>: {message}",
                "status": "failure"
            }
        return web.json_response(response_data, status=status)        
        
    except Exception as e:
        
        print(f"Server: Error in update endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

# Delete endpoint to remove an entry from the database
async def del_database(request):
    
    try:
        print("Delete endpoint called")
        # Get the JSON data from the request
        request_json = await request.json()  

        message, status = mgr.Delete_database(request_json)

        response_data = {}
        # Create a response JSON
        if status==200:
            stud_id = request_json.get("Stud_id", [])
            response_data = {
                "message": f"Data entry with Stud_id:{stud_id} removed",
                "status": "success"
            }
        
        else:
            response_data = {
                # "error": f'{str(message)}',
                "message": str(message),
                # "message": f"<Error>: {message}",
                "status": "failure"
            }    
        
        return web.json_response(response_data, status=status)
   
    except Exception as e:
        
        print(f"Server: Error in delete endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)

# Commit endpoint to commit the changes to the database
async def commit(request):
    try:
        print("Commit endpoint called")
        # Get the JSON data from the request  
        message, status = mgr.Commit()
        # message, status = mgr.sql_handler.db.commit()
        
        response_data = {}
        # Create a response JSON
        if status==200:
            response_data = {
                "message": message,
                "status": "success"
            }
        else:
            response_data = {
                # "error": message,
                "message": str(message),
                # "message": f"<Error>: {message}",
                "status": "failure"
            }
        
        return web.json_response(response_data, status=status)
    
    except Exception as e:
        
        print(f"Server: Error in commit endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)


# Rollback endpoint to rollback the changes to the database
async def rollback(request):
    try:
        print("Rollback endpoint called")
        message, status = mgr.Rollback()
        
        response_data = {}
        # Create a response JSON
        if status==200:
            response_data = {
                "message": message,
                "status": "success"
            }
        else:
            response_data = {
                # "error": message,
                # "message": f"<Error>: {message}",
                "message": str(message),
                "status": "failure"
            }
        
        return web.json_response(response_data, status=status)        
  
    except Exception as e:
        
        print(f"Server: Error in commit endpoint: {str(e)}")
        return web.json_response({"error": "Internal Server Error"}, status=500)
      
        
# Catch-all endpoint for any other request
async def not_found(request):
    return web.Response(text="Not Found", status=400)


# Define the main function to run the web server
def run_server():
    # Create an instance of the web Application
    app = web.Application()

    # Add routes for the home and heartbeat endpoints
    app.router.add_post('/config', config)
    app.router.add_get('/heartbeat', heartbeat)
    app.router.add_get('/copy', copy_database)
    app.router.add_post('/read', read_database)
    app.router.add_post('/write', write_database)
    app.router.add_put('/update', update)
    app.router.add_delete('/del', del_database)
    app.router.add_get('/commit', commit)
    app.router.add_get('/rollback', rollback)


    # Add a catch-all route for any other endpoint, which returns a 400 Bad Request
    app.router.add_route('*', '/{tail:.*}', not_found)

    # Run the web application on port 5000
    web.run_app(app, port=5000)

# Entry point of the script
if __name__ == '__main__':
    print("Starting server...", flush=True)

    # Get the server_id from the environment variable
    # server_id = 'Server0' #os.environ.get("SERVER_ID"
    # # Run the web server
    # print("\nHi.", flush=True)
    run_server()

