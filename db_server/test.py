import asyncio
import aiohttp
import datetime

DB_SERVER_PORT = 32845


async def send_json_request(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f"http://0.0.0.0:{DB_SERVER_PORT}/config", json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
    except Exception as e:
        print("Error:", e)


async def send_copy(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f'http://0.0.0.0:{DB_SERVER_PORT}/copy', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
    except Exception as e:
        print("Error:", e)

async def read_shard():
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f'http://0.0.0.0:{DB_SERVER_PORT}/read') as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
    except Exception as e:
        print("Error:", e)

async def update_shard(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.put(f'http://0.0.0.0:{DB_SERVER_PORT}/update', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
    except Exception as e:
        print("Error:", e)

async def delete_shard(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.delete(f'http://0.0.0.0:{DB_SERVER_PORT}/del', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
    except Exception as e:
        print("Error:", e)


async def write_shard(json_data):
    try:
        async with aiohttp.ClientSession() as session:
            async with session.post(f'http://0.0.0.0:{DB_SERVER_PORT}/write', json=json_data) as response:
                if response.status == 200:
                    print("JSON Request Successful")
                    print(await response.json())
                else:
                    print(f"Error in JSON Request {response.status}")
                    print(await response.text())
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
        "schemas": {
            "ShardT": {
                "columns": ["Stud_id_low", "Shard_id", "Shard_size", "valid_idx"],
                "dtypes": ["Number", "String", "Number", "Number"],
                "pk": ["Stud_id_low"],   
            },
            "MapT": {
                "columns": ["Shard_id", "Server_id"],
                "dtypes": ["String", "String"],
                "pk": [],
            },
        },
    }
    # await send_json_request(config_json)

    # write_json = {
    #     "table": "ShardT",
    #     "data": [{"Stud_id_low": 0, "Shard_id": "sh1", "Shard_size": 100, "valid_idx": 0},
    #              {"Stud_id_low": 100, "Shard_id": "sh2", "Shard_size": 100, "valid_idx": 0},
    #              {"Stud_id_low": 200, "Shard_id": "sh3", "Shard_size": 100, "valid_idx": 0}] 
    # }
    # await write_shard(write_json)

    # write_json_2 = {
    #     "table": "MapT",
    #     "data": [{"Shard_id": "sh1", "Server_id": "server1"},
    #              {"Shard_id": "sh2", "Server_id": "server2"},
    #              {"Shard_id": "sh3", "Server_id": "server3"},
    #              {"Shard_id": "sh1", "Server_id": "server2"},
    #              {"Shard_id": "sh2", "Server_id": "server3"},
    #              {"Shard_id": "sh3", "Server_id": "server1"}]
    # }
    # await write_shard(write_json_2)

    print("Update ShardT")
    update_json = {
        "table": "ShardT",
        "column": "Stud_id_low",
        "keys": [0,8192],
        "update_column": "valid_idx",
        "update_vals": [112,218]
    }
    # await update_shard(update_json)

    ## Uncomment to delete entries
    # delete_entries = {
    #     "table": "MapT",
    #     "column": "Server_id",
    #     "keys": ["server2", "server3"]
    # }
    # await delete_shard(delete_entries)

    await read_shard()

if __name__ == '__main__':
    asyncio.run(main())

