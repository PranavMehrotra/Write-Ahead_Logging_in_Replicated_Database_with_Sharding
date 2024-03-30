import asyncio
from time import sleep
import aiohttp

async def send_json_request(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post('http://0.0.0.0:5000/config', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text(), flush=True)
    except Exception as e:
        print("Error:", e)


async def send_copy(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get('http://0.0.0.0:5000/copy', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text(), flush=True)
    except Exception as e:
        print("Error:", e)

async def read_shard(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post('http://0.0.0.0:5000/read', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text(), flush=True)
    except Exception as e:
        print("Error:", e)

async def update_shard(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put('http://0.0.0.0:5000/update', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text(), flush=True)
    except Exception as e:
        print("Error:", e)

async def delete_shard(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.delete('http://0.0.0.0:5000/del', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text(), flush=True)
    except Exception as e:
        print("Error:", e)


async def write_shard(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post('http://0.0.0.0:5000/write', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text(), flush=True)
    except Exception as e:
        print("Error:", e)

async def main():
    # Example JSON data

    # config_json = {
    #     "schema": {
    #         "columns": ["Stud_name", "Stud_marks"],
    #         "dtypes": ["Number", "String", "Number"]
    #     },
    #     "shards": ["sh1","sh2","sh3"]
    # }
    # await send_json_request(config_json)

    config_json = {
        "schema": {
            "columns": ["Stud_id", "Stud_name", "Stud_marks"],
            "dtypes": ["Number", "String", "Number"]
        },
        "shards": ["sh1","sh2","sh3"]
    }
    await send_json_request(config_json)
    
    # print("Config done", flush=True)
    # exit(0)

    # write_json = {
    # "shard":"sh3",
    # "curr_idx": 16,
    # "data": [{"Stud_id":12,"Stud_name":'pran',"Stud_marks":127}] 
    # }
    # await write_shard(write_json)

    write_json = {
    "shard":"sh3",
    "curr_idx": 1,
    "data": [{"Stud_id":65,"Stud_name":'saransh',"Stud_marks":28},{"Stud_id":88,"Stud_name":'mehrotra',"Stud_marks":30},{"Stud_id":37,"Stud_name":'sara',"Stud_marks":12},{"Stud_id":56,"Stud_name":'shar',"Stud_marks":23}] 
    }
    await write_shard(write_json)

    write_json2 = {
    "shard":"sh1",
    "curr_idx": 1,
    "data": [{"Stud_id":65,"Stud_name":'saransh',"Stud_marks":28},{"Stud_id":88,"Stud_name":'mehrotra',"Stud_marks":30},{"Stud_id":37,"Stud_name":'sara',"Stud_marks":12},{"Stud_id":56,"Stud_name":'shar',"Stud_marks":23}] 
    }
    await write_shard(write_json2)

    # await write_shard(write_json)

    # copy_json = {
    #     "shards": ["sh3", "sh10"]
    # }
    # await send_copy(copy_json)

    # exit(0)
    copy_json = {
        "shards": ["sh3", "sh1"]
    }
    await send_copy(copy_json)

    # exit(0)
    # read_json = {
    #     "shard": "sh3",
    #     "Stud_id":{ "low": 10, "high": 11}
    # }
    # await read_shard(read_json)


    # read_json = {
    #     "shard": "sh10",
    #     "Stud_id":{ "low": 12, "high": 13}
    # }
    # await read_shard(read_json)



    # read_json = {
    #     "shard": "sh3",
    #     "Stud_id":{"high": 13}
    # }
    # await read_shard(read_json)


    # read_json = {
    #     "shard": "sh1",
    #     "Stud_id":{ "low": 40, "high": 100}
    # }
    # await read_shard(read_json)

    # exit(0)
    # read_json = {
    #     "shard": "sh3",
    #     "Stud_id":{ "low": 0, "high": 1}
    # }
    # await read_shard(read_json)


    
    update_json = {
        "shard":"sh1",
        "Stud_id":65,
        "data": {"Stud_id":65,"Stud_name":'saransh_sharma',"Stud_marks":300} 
        }
    await update_shard(update_json)
    # exit(0)

    update_json = {
        "shard":"sh10",
        "Stud_id":65,
        "data": {"Stud_id":65,"Stud_name":'saransh_sharma',"Stud_marks":300} 
        }
    await update_shard(update_json)

    # exit(0)

    # update_json = {
    #     "shard":"sh3",
    #     "Stud_id":88,
    #     "data": {"Stud_id":65,"Stud_name":'saransh_sharma',"Stud_marks":300} 
    #     }
    # await update_shard(update_json)



    # del_json = {
    #     "shard":"sh3",
    #     "Stud_id":3,
    #     }
    # await delete_shard(del_json)

    del_json = {
        "shard":"sh10",
        "Stud_id":3,
        }
    await delete_shard(del_json)
    
    del_json = {
        "shard":"sh1",
        "Stud_id":37,
        }
    await delete_shard(del_json)

if __name__ == '__main__':
    print("Starting test")
    sleep(1)
    asyncio.run(main())

