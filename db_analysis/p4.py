import sys
import random
import time
import asyncio
import argparse
from analysis import send_requests

random.seed(42)

SHARD_SIZE = 4096

'''
    TASK 1:

        Compute speeds for 10000 read and 10000 write requests.

'''

parser = argparse.ArgumentParser()

parser.add_argument("--type", type=str, required=True, default="read")
parser.add_argument("--nreqs", type=int, default=1)

args = parser.parse_args()

if __name__ == '__main__':
  
    # NUM_WRITE_REQUESTS = 10000
    # NUM_READ_REQUESTS = 10000
    # NUM_UPDATE_REQUESTS = 1
    # NUM_DELETE_REQUESTS = 1
    # request_type = "write"

    NUM_SERVERS = 6
    NUM_SHARDS = 4
    NUM_REPLICAS = 3

    request_type = args.type
    num_requests = args.nreqs
    
    try:   
        
        # if len(sys.argv) > 1:
        #     request_type = sys.argv[1]
            
        print(f"Request Type: {request_type}")

        if request_type == "init":
            start = time.time()
            config =  {
                "N": NUM_SERVERS,
                "schema": {
                            "columns":["Stud_id","Stud_name","Stud_marks"],
                            "dtypes":["Number","String","String"]
                        },
                "shards": [
                    {
                        "Stud_id_low": SHARD_SIZE * i,
                        "Shard_id": f"sh{i+1}",
                        "Shard_size": SHARD_SIZE
                    }
                    for i in range(NUM_SHARDS)
                ],
                "servers":
                { 
                            'Server0': ['sh1', 'sh2'],
                            'Server1': ['sh1', 'sh3'],
                            'Server2': ['sh1', 'sh4'],
                            'Server3': ['sh2', 'sh3'],
                            'Server4': ['sh2', 'sh4'],
                            'Server5': ['sh3', 'sh4']
                            
                        }
            }
            asyncio.run(send_requests(1,"init",config))
            end = time.time()
            print(f"Time taken to send init request: {end-start} seconds")
 
        elif request_type == "status":
            start = time.time()
            asyncio.run(send_requests(1,"status"))
            end = time.time()
            print(f"Time taken to send status request: {end-start} seconds")
        
        elif request_type == "add":
            start = time.time()
            config = {
                "n": 2,
                "new_shards": [{"Stud_id_low": 16384, "Shard_id": "sh5", "Shard_size": SHARD_SIZE}],
                "servers": {
                    "Server3": ["sh4", "sh5"],
                    "Server4": ["sh1", "sh5"]
                }
            }
            asyncio.run(send_requests(1,"add",config))
            end = time.time()
            print(f"Time taken to send add request: {end-start} seconds")
        
        elif request_type == "rm":
            start = time.time()
            config = {
                "n": 2,
                "servers": ["Server3", "Server1", "Server2"]
            }
            asyncio.run(send_requests(1,"rm",config))
            end = time.time()
            print(f"Time taken to send rm request: {end-start} seconds")

        elif request_type == "write": 
            start = time.time()
            asyncio.run(send_requests(num_requests, "write"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")

        elif request_type == "read":
            start = time.time()
            asyncio.run(send_requests(num_requests,"read"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")
        
        elif request_type == "update":
            # if len(sys.argv) > 2:
            #     NUM_UPDATE_REQUESTS = int(sys.argv[2])
            start = time.time()
            asyncio.run(send_requests(num_requests,"update"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")
            
        elif request_type == "delete":
            # if len(sys.argv) > 2:
            #     NUM_DELETE_REQUESTS = int(sys.argv[2])
            start = time.time()
            asyncio.run(send_requests(num_requests,"delete"))
            end = time.time()
            print(f"Time taken to send {num_requests} requests: {end-start} seconds")

        
    except Exception as e:
        print(f"Error: An exception occurred in overall run: {e}")