import threading
import sys
import os
import requests
from RWLock import RWLock

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from load_balancer import LoadBalancer
from docker_utils import spawn_db_server_cntnr, kill_db_server_cntnr

# HEARTBEAT_INTERVAL = 0.2
FIRST_CHECKPOINT_AFTER = 10
CHECKPOINT_INTERVAL = 30

class Checkpointer(threading.Thread):
    def __init__(self, lb: LoadBalancer, shardT: dict, shardT_lock: RWLock, db_server_name, key_column="Stud_id_low", update_column="valid_idx", db_server_port=5000):
        super(Checkpointer, self).__init__()
        self._lb = lb
        self._shardT = shardT
        self._shardT_lock = shardT_lock
        self._key_column = key_column
        self._update_column = update_column
        self._db_server_name = db_server_name
        self._db_server_port = db_server_port
        self._stop_event = threading.Event()
        self._write_MapT = threading.Event()
        self._write_ShardT = threading.Event()
        self._wake_up = threading.Event()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()
    
    def write_MapT(self):
        self._write_MapT.set()
        self._wake_up.set()

    def should_write_MapT(self):
        return self._write_MapT.is_set()
    
    def write_ShardT(self):
        self._write_ShardT.set()
        self._wake_up.set()

    def should_write_ShardT(self):
        return self._write_ShardT.is_set()
    
    def wait_for_wake_up(self):
        self._wake_up.wait()
        self._wake_up.clear()
        return True
    
    def checkpoint(self):
        print("checkpointer: Checkpointing shardT", flush=True)
        success = True
        # if not self.should_write_ShardT():
        #     # Construct a payload for the shardT checkpoint, then send PUT to /update endpoint
        #     payload = {
        #         "table": "ShardT",
        #         "column": self._key_column,
        #         "keys": [],
        #         "update_column": self._update_column,
        #         "update_vals": []
        #     }
        #     # Acquire a read lock on the shardT
        #     self._shardT_lock.acquire_reader()
        #     if len(self._shardT) == 0:
        #         # Release the read lock on the shardT
        #         self._shardT_lock.release_reader()
        #         return
        #     tem_keys, tem_vals = zip(*[(key, val[2]) for key, val in self._shardT.items()])
        #     # Release the read lock on the shardT
        #     self._shardT_lock.release_reader()
        #     payload["keys"] = list(tem_keys)
        #     payload["update_vals"] = list(tem_vals)
        #     # Send the PUT request to the db server
        #     response = requests.put(f'http://{self._db_server_name}:{self._db_server_port}/update', json=payload)
        #     if response.status_code != 200:
        #         print(f"checkpointer: Error in updating shardT, message: {response.text}", flush=True)
        #         success = False
        
        if self.should_write_ShardT():
            # reset the write_ShardT event
            self._write_ShardT.clear()
            print("checkpointer: Checkpointing ShardT by overwriting new ShardT", flush=True)
            # First Clear the ShardT table, by sending a POST request to /clear_table endpoint  
            response = requests.post(f'http://{self._db_server_name}:{self._db_server_port}/clear_table', json={"table": "ShardT"})
            if response.status_code != 200:
                print(f"checkpointer: Error in clearing ShardT", flush=True)
            # Construct a payload for the ShardT checkpoint, then send POST to /write endpoint
            payload = {
                "table": "ShardT",
                "data": []
            }
            # Acquire a read lock on the shardT
            self._shardT_lock.acquire_reader()
            # Construct the data for the ShardT table
            payload["data"] = [{"Stud_id_low": key, "Shard_id": val[0], "Shard_size": val[1]} for key, val in self._shardT.items()]
            # Release the read lock on the shardT
            self._shardT_lock.release_reader()
            # Send the POST request to the db server
            response = requests.post(f'http://{self._db_server_name}:{self._db_server_port}/write', json=payload)
            if response.status_code != 200:
                print(f"checkpointer: Error in updating ShardT", flush=True)
                success = False

        if self.should_write_MapT():
            # reset the write_MapT event
            self._write_MapT.clear()
            print("checkpointer: Checkpointing MapT", flush=True)
            # First Clear the MapT table, by sending a POST request to /clear_table endpoint  
            response = requests.post(f'http://{self._db_server_name}:{self._db_server_port}/clear_table', json={"table": "MapT"})
            if response.status_code != 200:
                print(f"checkpointer: Error in clearing MapT", flush=True)
                return
            # Construct a payload for the MapT checkpoint, then send POST to /write endpoint
            payload = {
                "table": "MapT",
                "data": []
            }
            # Get the shard to server mapping from the load balancer
            shard_to_server = self._lb.list_shards(list_servers=True)
            # Construct the data for the MapT table
            payload["data"] = [{"Shard_id": shard, "Server_id": server} for shard, servers in shard_to_server.items() for server in servers]
            # Send the POST request to the db server
            response = requests.post(f'http://{self._db_server_name}:{self._db_server_port}/write', json=payload)
            if response.status_code != 200:
                print(f"checkpointer: Error in updating MapT", flush=True)
                success = False
        
        if success:
            print("checkpointer: Checkpointing done!", flush=True)
        else:
            print("checkpointer: Error in checkpointing!", flush=True)

    def run(self):
        print(f"checkpointer: Checkpointer thread started for server: {self._db_server_name}", flush=True)
        # time.sleep(FIRST_CHECKPOINT_AFTER)
        while self.wait_for_wake_up():
            try:
                # Check if the thread is stopped
                if self.stopped():
                    print(f"checkpointer: Stopping checkpointer thread for server: {self._db_server_name}", flush=True)
                    return
                # Checkpoint
                self.checkpoint()
            except Exception as e:
                print(f"checkpointer: Error in checkpointing: {str(e)}", flush=True)

            # time.sleep(CHECKPOINT_INTERVAL)

