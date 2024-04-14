import os
import asyncio

def spawn_server_cntnr(hostname):
    try:
        # Spawn a new container with the environment variable
        res = os.popen(f'docker run --name {hostname} --network mynet --network-alias {hostname} -e SERVER_ID={hostname} -d -p 5000 server_img:latest').read()
        print(res)
        if res is None or len(res) == 0:
            print(f"docker_utils: Error: Unable to start container with ID {hostname}.", flush=True)
            return False
        else:
            print(f"docker_utils: Success: Container with ID {hostname} started successfully.", flush=True)
            return True
    
    except Exception as e:
        print(f"docker_utils: Error: An exception occurred during container spawn: {e}", flush=True)
        return False

def spawn_db_server_cntnr(hostname):
    try:
        # Spawn a new container with the environment variable
        res = os.popen(f'docker run --name {hostname} --network mynet --network-alias {hostname} -e SERVER_ID={hostname} -d -p 5000 db_server_img:latest').read()
        print(res)
        if res is None or len(res) == 0:
            print(f"docker_utils: Error: Unable to start container with ID {hostname}.", flush=True)
            return False
        else:
            print(f"docker_utils: Success: Container with ID {hostname} started successfully.", flush=True)
            return True
    
    except Exception as e:
        print(f"docker_utils: Error: An exception occurred during container spawn: {e}", flush=True)
        return False

def kill_db_server_cntnr(hostname):
    try:
        # Remove the container
        os.system(f"docker stop {hostname} && docker rm {hostname}")
        print(f"docker_utils: Success: Container with ID {hostname} stopped and removed.", flush=True)
        return True
    
    except Exception as e:
        print(f"docker_utils: Error: An exception occurred during container removal: {e}", flush=True)
        return False

# async def spawn_server_cntnr(hostname):
#     try:
#         # Spawn a new container with the environment variable
#         cmd = f'docker run --name {hostname} --network mynet --network-alias {hostname} -e SERVER_ID={hostname} -d -p 5000 server_img:latest'

#         process = await asyncio.create_subprocess_shell(
#             cmd,
#             stdout=asyncio.subprocess.PIPE,
#             stderr=asyncio.subprocess.PIPE
#         )

#         stdout, stderr = await process.communicate()

#         if process.returncode != 0:
#             print(f"docker_utils: Error: Unable to start container with ID {hostname}.")
#             print(f"docker_utils: stdout: {stdout.decode()}")
#             print(f"docker_utils: stderr: {stderr.decode()}")
#             return False
#         else:
#             print(f"docker_utils: Success: Container with ID {hostname} started successfully.")
#             return True
#     except Exception as e:
#         print(f"docker_utils: Exception: {e}")
#         return False


def kill_server_cntnr(hostname):
    try:
        # Remove the container
        os.system(f"docker stop {hostname} && docker rm {hostname}")
        print(f"docker_utils: Success: Container with ID {hostname} stopped and removed.")
        return True
    
    except Exception as e:
        print(f"docker_utils: Error: An exception occurred during container removal: {e}")
        return False

