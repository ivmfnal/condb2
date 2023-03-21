import sys, random, time
from condb import ConDB

#
# Sample code demonstrating direct access to a ConDB folder
#

Usage = """
python direct.py host=<dbhost> port=<dbport> dbname=... user=... password=...
"""

#
# Open the ConDB and create a folder
#

db = ConDB(connstr = " ".join(sys.argv[1:]))
folder = db.createFolder("test_data", 
    [   # data columns and their types
        ("x", "double precision"),
        ("y", "double precision"),
        ("n", "int")
    ],
    drop_existing=True          # drop the folder if it exists
)

print("Folder test_data created")

#
# Populate the folder with some data
#

NC = 20        # 100 channels
Tv_range = 200.0

for i in range(50):
    # upload data in 5 chunks
    chunk = []
    for channel in range(NC):
        if random.random() < 0.1:
            # not all channels have to be present in every chunk
            tv = random.random() * Tv_range      # random Tv
            x = random.random()             # some values for the data columns
            y = x*x + 10
            n = (i*(i+1)) % 7
            chunk.append((channel, tv, x, y, n))
    folder.addData(chunk)
    
    if i == 25:
        folder.tag("chunk_25")

print("Folder populated with data")

#
# Sample data for some Tv points
#

print("\nGetting data ...")

tvs = [random.random() * Tv_range for _ in range(5)]

for tv in tvs:
    print("\n--- tv=%.3f ---------------------------------------" % (tv,))
    data_at_tv = folder.getData(tv)
    for row in data_at_tv:
        (channel, tv, tr, data_type), data = row[:4], row[4:]
        print("%5d %.3f: %s" % (channel, tv, data))

print("\nGetting tagged data ...")

for tv in tvs:
    print("\n--- tv=%.3f (tagged) ------------------------------" % (tv,))
    data_at_tv = folder.getData(tv, tag="chunk_25")
    for row in data_at_tv:
        (channel, tv, tr, data_type), data = row[:4], row[4:]
        print("%5d %.3f: %s" % (channel, tv, data))

