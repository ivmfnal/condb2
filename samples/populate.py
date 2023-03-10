import psycopg2
import sys, random, time
from condb import ConDB

conn = psycopg2.connect(" ".join(sys.argv[1:]))
db = ConDB(connection=conn)
table = db.createFolder("data", 
    list({
        "data1":    "double precision",
        "data2":    "double precision"
    }.items())
, drop_existing=True)

t1 = 100.0
NC = 10

t0 = time.time()
time.sleep(0.1)
tr = 0

chunk = {}

for _ in range(5000):
    tv = int(random.random()*100)
    v1 = tv
    v2 = tv + (tr + 0.01)/10000.0 
    ch = int(random.random()*NC)
    if random.random() < 0.03 or (ch, tv) in chunk:
        if chunk:
            table.addData(list(chunk.values()), tr=tr)
            tr += 1
            chunk = {}
            print(_)  
    data = (v1, v2)
    row = (ch, tv) + data
    chunk[(ch, tv)] = row
    if _ and (_ % 10000) == 0:
        print(_)  

if chunk:
    table.addData(list(chunk.values()), tr=tr)
