import sys
import time
import os
# import threading
# import asyncio
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# from consistent_hashing import consistent_hashing
# from server import server
# from load_balancer import load_balancer
# from heartbeat import check_heartbeat
import client_handler
# import aiohttp
# import asyncio


if __name__ == "__main__":
    
    client_handler.run_load_balancer()
    # Lb = load_balancer.LoadBalancer()
    # Lb.add_servers(2, ['server1', 'server2'])
    # t1 = threading.Thread(target=asyncio.run(check_heartbeat(Lb, 'server1')), args=[])
    # t1.start()
    # t1.join()