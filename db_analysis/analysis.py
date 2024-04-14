import aiohttp
import asyncio
from pprint import pprint
import string
import random
from typing import Dict

random.seed(42)

ip_address = '0.0.0.0'
port = 5000
STUD_ID_MAX = 16000
SHARD_SIZE = 4096
SEMAPHORE_LIMIT = 1000

def generate_random_string(length=4):
    letters = string.ascii_uppercase
    return "S_" + ''.join(random.choice(letters) for _ in range(length))

def generate_random_range():
    low = random.randint(0, STUD_ID_MAX)
    # high = random.randint(low, low + 1000)
    high = random.randint(low, low + 4000)
    return low, high

# ----- API Calls -----

async def init(session, json_data):
    try:
        async with session.post(f'http://{ip_address}:{port}/init', json=json_data) as response:
            if response.status == 200:
                print("JSON Request Successful")
                print(await response.json())
            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
            return response.status
    except Exception as e:
        print("Error:", e)
        return 500

async def status(session):
    try:
        async with session.get(f'http://{ip_address}:{port}/status') as response:
            if response.status == 200:
                response_json = await response.json()
                
                if response_json.get("data", None) is not None:
                    data_list = response_json.get("data", [])

                else:
                    print("Response message: ")
                    pprint(response_json.get('message', 'No message'))

            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
        
            return response.status
    except Exception as e:
        print("Error:", e)
        return 500
    
async def read_shard(session, json_data):
    try:
            # print(f"Sending Read Request: {json_data}")
        # async with aiohttp.ClientSession() as session:
        async with session.post(f'http://{ip_address}:{port}/read', json=json_data) as response:
            if response.status == 200:
                    # print("JSON Request Successful")
                response_json = await response.json()
                
                if response_json.get("data", None) is not None:
                    data_list = response_json.get("data", [])

                    print("Shards queried: ", response_json.get("shards_queried", 0))

                    # print(response_json)

                    print("No of data points read:", len(data_list))

                    # data_list.sort(key=lambda x: x["Stud_id"])
                    # print("Data points read:")

                    # for data in response_json.get("data", []):
                    #     print(data)

                    print("No of data points read for shard 1:", len([data for data in data_list if data["Stud_id"] < 4096]))
                    print("No of data points read for shard 2:", len([data for data in data_list if data["Stud_id"] >= 4096 and data["Stud_id"] < 8192]))
                    print("No of data points read for shard 3:", len([data for data in data_list if data["Stud_id"] >= 8192 and data["Stud_id"] < 12288]))
                    print("No of data points read for shard 4:", len([data for data in data_list if data["Stud_id"] >= 12288 and data["Stud_id"] < 16384]))
                    # print("No of data points read for shard 5:", len([data for data in data_list if data["Stud_id"] >= 16384 and data["Stud_id"] < 20480]))
                else:
                    print(f"Response message: {response_json.get('message', 'No message')}")
                # print("No of data points 
            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
        
            return response.status
    except Exception as e:
        print("Error:", e)
        return 500

async def write_shard(sem, session, json_data):
    try:
        # print(f"Sending Write Request: {json_data}")
        # connector = aiohttp.TCPConnector(force_close=True)
        # async with aiohttp.ClientSession(connector=connector) as session:
        async with sem:
            async with session.post(f'http://{ip_address}:{port}/write', json=json_data) as response:
                if response.status == 200:
                    # print("JSON Request Successful")
                    # print(await response.json())
                    pass
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text(), flush=True)
                return response.status
    except Exception as e:
        print("Error: ", str(e))
        return 500

async def update_shard_entry(session, json_data):
    try:
        print(f"Sending Update Request:")
        pprint(json_data)
        async with session.put(f'http://{ip_address}:{port}/update', json=json_data) as response:
            if response.status == 200:
                # print("JSON Request Successful")
                pprint(await response.json())
            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
            return response.status
    except Exception as e:
        print("Error:", repr(e))
        return 500
    
async def delete_shard_entry(session, json_data):
    try:
        print(f"Sending Delete Request:")
        pprint(json_data)
        async with session.delete(f'http://{ip_address}:{port}/del', json=json_data) as response:
            if response.status == 200:
                # print("JSON Request Successful")
                pprint(await response.json())
            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
            return response.status
    except Exception as e:
        print("Error:", e)
        return 500
        
async def add_server(session, json_data):
    try:
        print(f"Sending Add Request:")
        pprint(json_data)
        # connector = aiohttp.TCPConnector(force_close=True)
        # async with aiohttp.ClientSession(connector=connector) as session:
        async with session.post(f'http://{ip_address}:{port}/add', json=json_data) as response:
            
            if response.status == 200:
                pprint(await response.json())
            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
            return response.status
    except Exception as e:
        print("Error: ", str(e))
        return 500
    
async def rm_server(session, json_data):
    try:
        print(f"Sending Remove Request:")
        pprint(json_data)
        async with session.delete(f'http://{ip_address}:{port}/rm', json=json_data) as response:
            if response.status == 200:
                pprint(await response.json())
            else:
                print(f"Error in JSON Request {response.status}")
                print(await response.text(), flush=True)
            return response.status
    except Exception as e:
        print("Error:", e)
        return 500
    

async def send_requests(
        num_requests: int, 
        type: str = "write",
        data: Dict = None):
    try:
        sem = asyncio.Semaphore(SEMAPHORE_LIMIT)
        async with aiohttp.ClientSession() as session:
            tasks = []

            if type == "init":
                tasks.append(init(session, data))

            elif type == "status":
                tasks.append(status(session))

            elif type == "write":
                ids = list(random.sample(range(1, STUD_ID_MAX), num_requests))
                # num_per_req = num_requests//100
                num_bundled_requests = num_requests//100
                
                # 100 reqs, each of num_per_req data points
                # tasks = [write_shard(sem, session, {"data": [{"Stud_id": ids[i*num_per_req+j], "Stud_name": generate_random_string(), "Stud_marks": random.randint(0, 100)} for j in range(num_per_req)]}) for i in range(100)]
                # num_bundled_requests reqs, each of 100 data points
                # tasks = [write_shard(sem, session, {"data": [{"Stud_id": ids[i*100 + j], "Stud_name": generate_random_string(), "Stud_marks": random.randint(0, 100)} for j in range(100)]}) for i in range(num_bundled_requests)]
                # num_requests reqs, with 1 data point each
                tasks = [write_shard(sem, session, {"data": [{"Stud_id": id, "Stud_name": generate_random_string(), "Stud_marks": random.randint(0, 100)}]}) for id in ids]
                
                # FOR Loop
                # tasks = [{"data": [{"Stud_id": id, "Stud_name": generate_random_string(), "Stud_marks": random.randint(0, 100)}]} for id in ids]
                # for task in tasks:
                #     await write_shard(task)
            elif type == "read":
                for i in range(num_requests):
                    low,high = generate_random_range()
                    low = 0
                    high = STUD_ID_MAX
                    read_json = {
                        "Stud_id":{ "low": low, "high": high}
                    }
                    
                    tasks.append(read_shard(session, read_json))
                  
            elif type == "add":
                tasks.append(add_server(session, data))

            elif type == "rm":
                tasks.append(rm_server(session, data))
                
            elif type == "update":
                for i in range(num_requests):
                    stu_id = random.randint(1, STUD_ID_MAX)
                    # stu_id = 11732
                    stu_name = generate_random_string()
                    # stu_name = "S_ABCD"
                    stu_marks = random.randint(0, 100)
                    # stu_marks = 68
                    update_json = {
                        "Stud_id": stu_id,
                        "data": {"Stud_id": stu_id, "Stud_name": stu_name, "Stud_marks": stu_marks}
                        # "data": {"Stud_id": stu_id, "Stud_marks": stu_marks}
                        # "data": {"Stud_id": stu_id, "Stud_name": stu_name}
                        # "data": {"Stud_id": stu_id}
                    }
                    print("Update data: ")
                    pprint(update_json)
                    tasks.append(update_shard_entry(session, update_json))
                    
            elif type == "delete":
                for i in range(num_requests):
                    stu_id = random.randint(1, STUD_ID_MAX)
                    # stu_id = 435
                    delete_json = {
                        "Stud_id": stu_id
                    }
                    tasks.append(delete_shard_entry(session, delete_json))
            # await asyncio.gather(*tasks)
                

            statuses = await asyncio.gather(*tasks)
        
        # print(f"Success: {num_requests} {type} requests to the load balancer sent successfully.")
        # print(f"\n")
        # check the no of successful requests and failed requests
        num_success = statuses.count(200)
        num_failed =  len(statuses) - num_success
        print(f"No of successful requests: {num_success}/{num_requests}")
        print(f"No of failed requests: {num_failed}/{num_requests}")
 
    except Exception as e:
        print(f"Error: An exception occurred while sending multiple {type} requests: {e}")
