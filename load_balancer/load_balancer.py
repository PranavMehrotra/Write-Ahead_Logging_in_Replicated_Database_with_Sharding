import random
import sys
import sys
import os
import re

# add the path to the parent directory to the sys.path list
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
# from ..consistent_hashing import RWLock
# from ..consistent_hashing import consistent_hashing
from RWLock import RWLock
from consistent_hashing import ConsistentHashing
from docker_utils import spawn_server_cntnr, kill_server_cntnr

SLEEP_AFTER_SERVER_ADDITION = 1

class LoadBalancer:
    def __init__(self):
        
        self.serv_to_shard = {} # dictionary to store the mapping of servers to shards
        self.servers = set()  # set of active servers
        self.rw_lock = RWLock()  # reader-writer lock to protect the self.servers set
        self.socket = None
        self.load_count = {}
        self.load_cnt_lock = RWLock()
        # # spawn the initial set of servers
        # for hostname in initial_servers:
        #     done = spawn_server_cntnr(hostname)
        #     if not done:
        #         print("load_balancer: <Error> Server: '" + hostname + "' could not be spawned!")
        #         return
        #     self.rw_lock.acquire_writer()
        #     # self.servers[hostname] = port
        #     self.servers.add(hostname)
        #     self.rw_lock.release_writer()
        #     # time.sleep(SLEEP_AFTER_SERVER_ADDITION)
        
        
        
        # self.consistent_hashing = ConsistentHashing(server_hostnames=initial_servers, num_servers=len(initial_servers))
        self.consistent_hashing: dict[str, ConsistentHashing] = {}

    def add_servers(self, num_add, serv_to_shard: dict, should_spawn: bool = True):
        # def check_hostname(hostname: str):
        #     pattern = r"Server\[\d+\]"
        #     print(re.findall(pattern, hostname))
        #     if re.findall(pattern, hostname):
        #         num = random.randint(10000, 99999)
        #         return f"Server{num}"
        #     return hostname
        
        error=""
        # temp_new_servers = set()
        # Make hostnames list unique(basically a set)
        hostnames = set(serv_to_shard.keys())
        # hostnames = set([check_hostname(hostname) for hostname in list(hostnames)])
        
        if (len(hostnames) != num_add):
            print(f"load_balancer: <Error> Length of servers dict is not equal to newly added instances")
            error = f"<Error> Length of servers dict is not equal to newly added instances"
            return -1, [], error
            
        else:
            # add the servers whose hostnames are provided, to the set
            for hostname in hostnames:
                self.rw_lock.acquire_reader()
                if (hostname in self.servers):
                    self.rw_lock.release_reader()
                    print("load_balancer: <Error> Hostname: '" + hostname + "' already exists in the active list of servers!") 
                    return -1, [], "<Error> Hostname: '" + hostname + "' already exists in the active list of servers!"
                self.rw_lock.release_reader()
                # temp_new_servers.add(hostname)
            
            # # add the remaining servers to the list by generating new random hostnames for them
            # for i in range(num_add - len(temp_new_servers)):
            #     new_hostname = generate_new_hostname()
            #     self.rw_lock.acquire_reader()
            #     while (new_hostname in self.servers or new_hostname in temp_new_servers):
            #         new_hostname = generate_new_hostname()
            #     self.rw_lock.release_reader()
            #     temp_new_servers.add(new_hostname)
        
        final_add_server_set = set()  # Use set instead of dictionary for faster additions and subtractions        
        
        if should_spawn:
            for server in hostnames:
                done = spawn_server_cntnr(server) ## function from docker_utils.py
                ### TO-DO: Add error handling here in case the server could not be spawned
                if not done:
                    print("load_balancer: <Error> Server: '" + server + "' could not be spawned!")

                else:     # add the newly spawned server to the dictionary of servers
                    final_add_server_set.add(server)
        else:
            final_add_server_set = hostnames
            # time.sleep(SLEEP_AFTER_SERVER_ADDITION)
        
        shard_to_server: dict[str, list] = {}
        for server in final_add_server_set:
            for shard in serv_to_shard[server]:
                if shard in shard_to_server:
                    shard_to_server[shard].append(server)
                else:
                    shard_to_server[shard] = [server]
        new_servers = set()
        for shard, servers in shard_to_server.items():
            if shard in self.consistent_hashing:
                tem_new_servers = set(self.consistent_hashing[shard].add_servers(servers))
                new_servers = new_servers.union(tem_new_servers)

        # send the temorary list of new servers to be added to the consistent hashing module
        # the consistent hasing module will finally return the list of servers that were finally added
        # new_servers = self.consistent_hashing.add_servers(list(final_add_server_set))
        # new_servers = set(new_servers)

        # # add the newly added servers to the dictionary of servers
        self.rw_lock.acquire_writer()
        # for server in new_servers:
        #     # self.servers[server] = final_add_server_dict[server] # port number
        #     self.servers.add(server)
        self.servers = self.servers.union(new_servers)
        for server in new_servers:
            self.serv_to_shard[server] = serv_to_shard[server]
        self.rw_lock.release_writer()
        
        ### TO-DO: For the servers that couldn't be added to the CH module (possibly due to lack of space), remove them from the list of servers to be added
        ### Also, close the docker containers and corresponding threads  
        for server in final_add_server_set - new_servers:
            kill_server_cntnr(server)
            
        # final_add_server_dict = {server: final_add_server_dict[server] for server in new_servers}
        
        return len(new_servers), list(new_servers), error
    
    def add_shards(self, shards: list):
        new_shards = []
        for val in shards:
            shard = val[1]
            if shard not in self.consistent_hashing:
                try:
                    self.rw_lock.acquire_writer()
                    self.consistent_hashing[shard] = ConsistentHashing(server_hostnames=[], num_servers=0)
                    self.rw_lock.release_writer()
                except Exception as e:
                    print("load_balancer: <Error> Could not add shard: " + str(shard) + " due to: " + str(e))
                    continue
                new_shards.append(val)
        return new_shards



    def remove_servers(self, num_rem, hostnames:list):
        error = ""
        self.rw_lock.acquire_reader()
        if (len(self.servers) == 0):
            print("load_balancer: <Error> No servers to remove!")
            self.rw_lock.release_reader()
            error = "<Error> No servers to remove!"
            return -1, [], error
        if (num_rem > len(self.servers)):
            print("load_balancer: <Error> Number of servers to remove is more than the number of active servers!")
            self.rw_lock.release_reader()
            error = "<Error> Number of servers to remove is more than the number of active servers!"
            return -1, [], error
        self.rw_lock.release_reader()
        
        temp_rm_servers = set()  # Use set instead of dictionary for faster additions and subtractions
        # Make hostname list unique(basically a set)
        hostnames = set(hostnames)
        if (len(hostnames) > num_rem):
            print("load_balancer: <Error> Length of hostname list is more than removable instances")
            error = "<Error> Length of hostname list is more than removable instances"
            return -1, [], error
        else:
            for hostname in hostnames:
                self.rw_lock.acquire_reader()
                if (hostname in self.servers):
                    temp_rm_servers.add(hostname)          ## FASTER
                self.rw_lock.release_reader()
                    
            ## VERY SLOW:
            # remove remaining servers from the list by randomly selecting them from the list of active servers
            # for i in range(num_rem - len(hostnames)):
            #     self.rw_lock.acquire_reader()
            #     if (len(self.servers) == 0):
            #         print("load_balancer: <Error> No active server left. Can't remove any more servers!")
            #         # error = "<Error> No active server left. Can't remove any more servers!"
            #         self.rw_lock.release_reader()
            #         break
            #     while(True):
            #         rm_hostname = random.choice(list(self.servers.keys() - temp_rm_servers.keys()))
            #         if (rm_hostname not in temp_rm_servers):
            #             temp_rm_servers[rm_hostname] = 1
            #             break
            #     self.rw_lock.release_reader()
            
            ## FASTER: (No need for random selection)
            if num_rem > len(temp_rm_servers):
                self.rw_lock.acquire_reader()
                if num_rem == len(self.servers):
                    # Extend the list of servers to be removed with all the remaining servers
                    # temp_rm_servers = set(self.servers.keys())
                    temp_rm_servers = set(self.servers)
                else:
                    left = num_rem - len(temp_rm_servers)
                    # tem_set = set(self.servers.keys()) - temp_rm_servers
                    tem_set = self.servers - temp_rm_servers
                    # Extend the list of servers to be removed with randomly selected servers

                    # temp_rm_servers = temp_rm_servers.union(random.sample(tem_set, left))  # This random.sample can also be removed, just take the first 'left' no. of servers :)
                    temp_rm_servers = temp_rm_servers.union(set(list(tem_set)[:left]))
                self.rw_lock.release_reader()
                
        # servers_rem_f = self.consistent_hashing.remove_servers([server for server in temp_rm_servers])
        servers_rem_f = set()
        for shard in self.consistent_hashing:
            tem_servers_rem_f = set(self.consistent_hashing[shard].remove_servers(temp_rm_servers))
            servers_rem_f = servers_rem_f.union(tem_servers_rem_f)
        # remove the newly removed servers from the dictionary of servers
        # self.rw_lock.acquire_writer()
        # for server in temp_rm_servers:
        #     if (server in servers_dne): # this is for the case when server got down before it could be removed
        #         # assert(server not in self.servers)
        #         if (server in self.servers): # this should never happen
        #             print("load_balancer: <Error> This shoudn't happen! Server should have already been removed!")
        #             self.servers.pop(server)
        #     else:
        #         self.servers.pop(server)
    
        # self.rw_lock.release_writer()
        
        # remove the final list of servers from the dictionary of servers
        self.rw_lock.acquire_writer()
        for server in servers_rem_f:
            try:
                # self.servers.pop(server)
                self.servers.remove(server)
                self.serv_to_shard.pop(server)
                # print("load_balancer: Server: " + server + " removed!")
            except KeyError:
                print("load_balancer: <Error> Server: '" + server + "' does not exist in the active list of servers!")
                continue
        self.rw_lock.release_writer()
        
        
        return len(servers_rem_f), list(servers_rem_f), error 
                
    def list_servers(self, send_shard_info: bool = False):
        if send_shard_info:            
            self.rw_lock.acquire_reader()
            servers_list = list(self.servers)
            serv_to_shard = self.serv_to_shard.copy()
            self.rw_lock.release_reader()
            return servers_list, serv_to_shard
        else:
            self.rw_lock.acquire_reader()
            servers_list = list(self.servers)
            self.rw_lock.release_reader()
            return servers_list

    def assign_server(self, shard_id, req_id):
        self.rw_lock.acquire_reader()
        if (len(self.servers) == 0):
            self.rw_lock.release_reader()
            print("load_balancer: <Error> No active server left. Can't assign any server!")
            return ""
        if (shard_id not in self.consistent_hashing):
            self.rw_lock.release_reader()
            print("load_balancer: <Error> Shard: " + str(shard_id) + " does not exist in the consistent hashing module!")
            return ""
        server = self.consistent_hashing[shard_id].get_server(req_id)
        self.rw_lock.release_reader()
        return server
    
    def list_shard_servers(self, shard_id):
        self.rw_lock.acquire_reader()
        if (shard_id not in self.consistent_hashing):
            print("load_balancer: <Error> Shard: " + str(shard_id) + " does not exist in the consistent hashing module!")
            self.rw_lock.release_reader()
            return []
        servers_list = self.consistent_hashing[shard_id].list_servers()
        self.rw_lock.release_reader()
        return servers_list
    
    def list_shards(self, list_servers: bool = False):
        if not list_servers:
            self.rw_lock.acquire_reader()
            shards_list = list(self.consistent_hashing.keys())
            self.rw_lock.release_reader()
            return shards_list
        self.rw_lock.acquire_reader()
        shard_to_servers = {shard: val.list_servers() for shard, val in self.consistent_hashing.items()}
        self.rw_lock.release_reader()
        return shard_to_servers

    def increment_server_req_count(self, server):
        self.load_cnt_lock.acquire_writer()
        if (server in self.load_count):
            self.load_count[server] += 1
        else:
            self.load_count[server] = 1
        self.load_cnt_lock.release_writer()
        
    def get_server_load_stats(self):
        self.load_cnt_lock.acquire_reader()
        load_count = self.load_count.copy()
        self.load_cnt_lock.release_reader()
        return load_count
    
    # def save_lb_analysis_csv(self):
    #     t = datetime.datetime.now()
    #     time_save = t.strftime("%d_%m_%Y_%H_%M_") + str(t.second)
    #     print(time_save)

    #     # check if an analysis folder exists
    #     if not os.path.exists("./lb_analysis"):
    #         os.mkdir("./lb_analysis")
    #     # save the csv file in the analysis folder

    #     try:
    #         self.load_cnt_lock.acquire_reader()
    #         with open("./lb_analysis/lb_analysis_" + time_save + ".csv", "w") as f:
    #             f.write("server,load\n")
    #             for server in self.load_count:
    #                 f.write(server + "," + str(self.load_count[server]) + "\n")
    #         self.load_cnt_lock.release_reader()
            
    #         return True
    
    #     except Exception as e:
    #         print("load_balancer: <Error> Could not save the load balancer analysis csv file due to: " + str(e))
    #         return False
            
# function to generate a new random hostname for a server
def generate_new_hostname():
        new_hostname = "S_"
        for i in range(6):
            new_hostname += str(random.randint(0, 9))

        return new_hostname
