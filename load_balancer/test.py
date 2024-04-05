#To test spawning and killing ability of loadbalancer

import aiohttp
import asyncio
import time
import os
import random
import string
from docker_utils import spawn_server_cntnr, kill_server_cntnr

async def generate_random_string(length):
    letters = string.ascii_letters
    return ''.join(random.choice(letters) for _ in range(length))

async def container_exists(container_id):
    # Check if the container with given ID exists
    try:
        check_command = f'sudo docker ps -q -f name={container_id}'
        return os.popen(check_command).read().strip() != ""
    except Exception as e:
        print(f"Error: An exception occurred during container existence check: {e}")
        return False

async def send_heartbeat(session, server_id, num_requests=5):
    try:
        print(f"Info: Waiting for the server {server_id} to initialize...")

        # Simulate some time passing
        await asyncio.sleep(10)
        
        for i in range(num_requests):
            try:
                # Send a GET request to the server
                async with session.get(f"http://{server_id}:5000/home") as response:
                    print(f"Client - Request {i+1} - Status Code: {response.status} - Response: {await response.text()}")
                await asyncio.sleep(3)
            except Exception as e:
                print(f"Error: An exception occurred during client request {i+1}: {e}")
    except Exception as e:
        print(f"Error: An exception occurred during heartbeat: {e}")


async def spawn_and_remove_container():
    try:
        # Generate a unique server_id
        server_id = await generate_random_string(6)

        # Check if the container with the generated ID already exists, generate a new one if it does
        while await container_exists(server_id):
            server_id = await generate_random_string(6)

        # Set the server_id as an environment variable
        try:
            await spawn_server_cntnr(server_id)
        except Exception as e:
            print(f"Error: An exception occurred during container spawn: {e}")

        # Send heartbeat
        async with aiohttp.ClientSession() as session:
            await send_heartbeat(session, server_id)

        async with aiohttp.ClientSession() as session:
            await send_heartbeat(session, "server1")

        # Remove the container
        try:
            kill_server_cntnr(server_id)
            kill_server_cntnr("server1")
        except Exception as e:
            print(f"Error: An exception occurred during container removal: {e}")

    except Exception as e:
        print(f"Error: An exception occurred during operations on container with ID {server_id}: {e}")

if __name__ == '__main__':
    asyncio.run(spawn_and_remove_container())
    exit()
