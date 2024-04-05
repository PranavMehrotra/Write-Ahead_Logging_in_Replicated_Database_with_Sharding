import bisect
import numpy as np
import sys
import os
import hashlib

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from RWLock import RWLock

INITIAL_SERVER_ID = 3742
SERVER_ID_MULTIPLIER = 1
REPLICA_ID_MULTIPLIER = 42

class ConsistentHashing:
    def __init__(self, server_hostnames: list, num_servers=4, num_replicas=9, num_slots=512):
        if len(server_hostnames) < num_servers:
            print("consistent_hashing: Number of servers is greater than number of server hostnames provided")
            return
        self.num_servers = num_servers
        self.num_replicas = num_replicas
        self.num_slots = num_slots
        self.lock = RWLock()
        self.hash_map = []
        self.hash_array = np.zeros(self.num_slots)
        self.id_to_hostname = {}
        self.hostname_to_id = {}
        self.num_virtual_servers = 0
        self.next_server_id = INITIAL_SERVER_ID
        self.init_hash_map(server_hostnames)

    
    def init_hash_map(self, server_hostnames: list):
        self.lock.acquire_writer()
        for i in range(self.num_servers):
            # Check if server already exists
            if server_hostnames[i] in self.hostname_to_id:
                print(f"consistent_hashing: Server {server_hostnames[i]} already exists")
                continue
            self.hostname_to_id[server_hostnames[i]] = self.next_server_id
            self.id_to_hostname[self.next_server_id] = server_hostnames[i]
            for j in range(self.num_replicas):
                replica_hash = self.server_hash_func(SERVER_ID_MULTIPLIER*self.next_server_id, REPLICA_ID_MULTIPLIER*j)
                replica_hash = self.linear_probing(replica_hash)
                self.hash_map.append(replica_hash)
                self.hash_array[replica_hash] = self.next_server_id
            self.next_server_id += 1
        self.hash_map.sort()
        self.num_virtual_servers = len(self.hash_map)
        self.lock.release_writer()
        # self.__unique_checker()

    def server_hash_func(self, server_id, replica_id):
        # HASH FUNCTION 1 (Given in assignment)
        hash = (server_id*server_id) % self.num_slots
        hash += (replica_id*replica_id) % self.num_slots
        hash += (2*replica_id + 25) % self.num_slots
         
        # HASH FUNCTION 2 (SHA1 hash function) => Uncomment to do Analysis 4
        # Using SHA1 hash function, for uniform distribution of hash values, so that the replicas are distributed uniformly
        # hash_key = str(server_id) + str(replica_id)
        # hash_digest = hashlib.sha1(hash_key.encode('utf-8')).digest()
        # hash = sum(hash_digest) % self.num_slots
        
        # print(f"consistent_hashing: Server: {server_id}, Replica: {replica_id}, Hash: {hash}")
        return hash % self.num_slots
    
    def request_hash_func(self, request_id):
        # HASH FUNCTION 1 (Given in assignment)
        hash = (request_id*request_id) % self.num_slots
        hash += (2*request_id + 17) % self.num_slots

        # HASH FUNCTION 2 (MD5 hash function) => Uncomment to do Analysis 4
        # Using MD5 hash function, so that the requests are distributed uniformly across all the server replicas
        # hash_key = str(request_id)
        # hash_digest = hashlib.md5(hash_key.encode('utf-8')).digest()
        # hash = sum(hash_digest) % self.num_slots

        print(f"consistent_hashing: Request: {request_id}, Hash: {hash%self.num_slots}")
        return hash % self.num_slots
    
    def get_server(self, request_id):
        self.lock.acquire_reader()
        server_id = self.hash_array[self.hash_map[bisect.bisect_left(self.hash_map, self.request_hash_func(request_id)) % self.num_virtual_servers]]
        server = self.id_to_hostname[server_id]
        self.lock.release_reader()
        return server
    
    def linear_probing(self, replica_hash):
        i = replica_hash
        # if self.hash_array[i] != 0:
        #     print(f"consistent_hashing: Collision while adding at hash {replica_hash}, performing linear probing")
        while self.hash_array[i] != 0:
            i = (i+1) % self.num_slots
            if i == replica_hash:
                # Should never happen
                BufferError("consistent_hashing: No more slots available")
                return
        # if i != replica_hash:
        #     print(f"consistent_hashing: Linear probing completed at hash {i}")
        return i

    def linear_probing_delete(self, replica_hash, server_id):
        i = replica_hash
        # if self.hash_array[i] != server_id:
        #     print(f"consistent_hashing: Collision while removing at hash {replica_hash}, performing linear probing")
        while self.hash_array[i] != server_id:
            i = (i+1) % self.num_slots
            if i == replica_hash:
                # Should never happen
                BufferError(f"consistent_hashing: Replica does not exist of server {server_id}")
                return
        # if i != replica_hash:
        #     print(f"consistent_hashing: Linear probing completed at hash {i}")
        return i

    def add_server(self, server_hostname):
        if self.num_virtual_servers + self.num_replicas > self.num_slots:
            print("consistent_hashing: No more servers can be added, all slots are full")
            return
        self.lock.acquire_writer()
        # Check if server already exists
        if server_hostname in self.hostname_to_id:
            print(f"consistent_hashing: Server {server_hostname} already exists")
            self.lock.release_writer()
            return
        server_id = self.next_server_id
        self.next_server_id += 1
        self.id_to_hostname[server_id] = server_hostname
        self.hostname_to_id[server_hostname] = server_id
        for i in range(self.num_replicas):
            replica_hash = self.server_hash_func(SERVER_ID_MULTIPLIER*server_id, REPLICA_ID_MULTIPLIER*i)
            replica_hash = self.linear_probing(replica_hash)
            bisect.insort(self.hash_map, replica_hash)
            self.hash_array[replica_hash] = server_id
            self.num_virtual_servers += 1
        self.num_servers += 1
        self.lock.release_writer()
        # self.__unique_checker()

    def add_servers(self, server_hostnames: list):
        
        servers_added = []
        # new_server_ctr = 0
        
        if len(server_hostnames) == 0:
            print("consistent_hashing: No servers to add")
            return []
        self.lock.acquire_writer()
        for server_hostname in server_hostnames:
            # Check if slots are available
            if self.num_virtual_servers + self.num_replicas > self.num_slots:
                print("consistent_hashing: No more servers can be added, all slots are full")
                self.lock.release_writer()
                return []
            # Check if server already exists
            if server_hostname in self.hostname_to_id:
                print(f"consistent_hashing: Server {server_hostname} already exists")
                continue
            server_id = self.next_server_id
            self.next_server_id += 1
            self.id_to_hostname[server_id] = server_hostname
            self.hostname_to_id[server_hostname] = server_id
            for i in range(self.num_replicas):
                replica_hash = self.server_hash_func(SERVER_ID_MULTIPLIER*server_id, REPLICA_ID_MULTIPLIER*i)
                replica_hash = self.linear_probing(replica_hash)
                bisect.insort(self.hash_map, replica_hash)
                self.hash_array[replica_hash] = server_id
                self.num_virtual_servers += 1
            self.num_servers += 1
            servers_added.append(server_hostname)
            # new_server_ctr += 1
        self.lock.release_writer()

        return servers_added

    def remove_server(self, server_hostname):
 
        
        self.lock.acquire_writer()
        # Check if server exists
        if server_hostname not in self.hostname_to_id:
            print(f"consistent_hashing: Server {server_hostname} does not exist")
            self.lock.release_writer()
            return []
        server_id = self.hostname_to_id[server_hostname]
        del self.hostname_to_id[server_hostname]
        del self.id_to_hostname[server_id]
        for i in range(self.num_replicas):
            replica_hash = self.server_hash_func(SERVER_ID_MULTIPLIER*server_id, REPLICA_ID_MULTIPLIER*i)
            replica_hash = self.linear_probing_delete(replica_hash, server_id)
            self.hash_array[replica_hash] = 0
            idx = bisect.bisect_left(self.hash_map, replica_hash)
            if idx >= len(self.hash_map) or self.hash_map[idx] != replica_hash:
                # Should never happen
                self.lock.release_writer()
                BufferError(f"consistent_hashing: Replica {i} of server {server_id} does not exist")
            del self.hash_map[idx]
            self.num_virtual_servers -= 1
        self.num_servers -= 1
        self.lock.release_writer()
        # self.__unique_checker()

    def remove_servers(self, server_hostnames: list):
        if len(server_hostnames) == 0:
            print("consistent_hashing: No servers to remove")
            return []
        
        servers_removed = []
        # removed_server_ctr = 0
        self.lock.acquire_writer()
        for server_hostname in server_hostnames:
            # Check if server exists
            if server_hostname not in self.hostname_to_id:
                print(f"consistent_hashing: Server {server_hostname} does not exist")
                continue
            server_id = self.hostname_to_id[server_hostname]
            del self.hostname_to_id[server_hostname]
            del self.id_to_hostname[server_id]
            for i in range(self.num_replicas):
                replica_hash = self.server_hash_func(SERVER_ID_MULTIPLIER*server_id, REPLICA_ID_MULTIPLIER*i)
                replica_hash = self.linear_probing_delete(replica_hash, server_id)
                self.hash_array[replica_hash] = 0
                idx = bisect.bisect_left(self.hash_map, replica_hash)
                if idx >= len(self.hash_map) or self.hash_map[idx] != replica_hash:
                    # Should never happen
                    self.lock.release_writer()
                    BufferError(f"consistent_hashing: Replica {i} of server {server_id} does not exist")
                del self.hash_map[idx]
                self.num_virtual_servers -= 1
            self.num_servers -= 1
            servers_removed.append(server_hostname)
            # removed_server_ctr += 1
        self.lock.release_writer()
        
        return servers_removed

    def print_hash_map(self):
        self.lock.acquire_reader()
        print(f"consistent_hashing: Number of virtual servers: {self.num_virtual_servers}\nHash map: ", end="")
        print(self.hash_map)
        # print(self.hash_array)
        self.lock.release_reader()

    def list_servers(self):
        self.lock.acquire_reader()
        servers = list(self.hostname_to_id.keys())
        self.lock.release_reader()
        return servers

    def __unique_checker(self):
        # Check if sorted list is unique
        print("consistent_hashing: Checking uniqueness of hash map: ", end="")
        self.lock.acquire_reader()
        for i in range(len(self.hash_map)-1):
            if self.hash_map[i] == self.hash_map[i+1]:
                print("consistent_hashing: Not unique")
                print(f"consistent_hashing: Duplicate: {self.hash_map[i]} at index {i} and {i+1}")
                print(self.hash_map)
                self.lock.release_reader()
                exit()
        self.lock.release_reader()
        print("consistent_hashing: Unique")


# if __name__ == "__main__":
#     server_hostnames = ["server1", "server2", "server3"]
#     consistent_hashing = ConsistentHashing(server_hostnames)
#     consistent_hashing.print_hash_map()
#     # time.sleep(5)
#     consistent_hashing.add_server("server4")
#     consistent_hashing.print_hash_map()
#     # time.sleep(5)
#     consistent_hashing.remove_server("server2")
#     consistent_hashing.print_hash_map()
#     # time.sleep(5)
#     print(consistent_hashing.get_server(1))
#     print(consistent_hashing.get_server(2))
#     print(consistent_hashing.get_server(5))

#     # Advanced functionality
#     consistent_hashing.add_servers(["server5", "server7"])
#     consistent_hashing.print_hash_map()
#     consistent_hashing.remove_servers(["server3","server5"])
#     consistent_hashing.print_hash_map()
#     # consistent_hashing.add_server("server6")
#     # consistent_hashing.print_hash_map()
#     # consistent_hashing.remove_server("server1")
#     # consistent_hashing.print_hash_map()
#     print(consistent_hashing.get_server(1))
#     print(consistent_hashing.get_server(2))
#     print(consistent_hashing.get_server(7))
