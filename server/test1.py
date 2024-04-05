import asyncio
import aiohttp
import datetime
from helper import SQLHandler


sql_handler = SQLHandler(host='localhost',user='root',password='123654@123')

try:
    print("Connecting to database...")
    error, status=sql_handler.connect()
    if status!=200:
        print(status, flush=True)
        raise Exception(error)
    # res = sql_handler.query("USE server_database;")
    # res,status = sql_handler.query(f"UPDATE sh3 SET Stud_marks=40 WHERE Stud_id=65;")
    # res,status = sql_handler.query(f"SELECT COUNT(*) FROM sh3")
    # print(res[0][0])
    data = [{'Stud_id': 2255, 'Stud_name': 'sharma', 'Stud_marks': 27}, {'Stud_id': 2252, 'Stud_name': 'sharm', 'Stud_marks': 47}, {'Stud_id': 225, 'Stud_name': 'shar', 'Stud_marks': 127}]
    schema = '(Stud_id,Stud_name,Stud_marks)'
    res,status = sql_handler.Insert('sh3',data,schema)
    print(res,status)
    #res,status = sql_handler.query(f"SELECT * FROM sh3 WHERE Stud_id = 65;")
    
    print(res==[])
except Exception as e:
    print(e)
