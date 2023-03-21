import sys, random, time
from condb import ConDBClient

#
# Sample code demonstrating direct access to a ConDB folder
#

Usage = """
python populate.py <server url> <username> <password> <folder>
"""

server_url, username, password, folder = sys.argv[1:]
client = ConDBClient(server_url, username, password)
columns = ["channel", "tv", "x", "y", "n"]

#
# Populate the folder with some data
#

NC = 20        # 100 channels
Tv_range = 200.0

for i in range(10):
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
    if chunk:
        client.put_data(folder, chunk, columns)
    
    if i == 5:
        client.tag_state(folder, "chunk_25", override=True)

print("Folder populated with data")

#
# Sample data for some Tv points
#

print("\nGetting data ...")

tvs = [random.random() * Tv_range for _ in range(5)]

for tv in tvs:
    print("\n--- tv=%.3f ---------------------------------------" % (tv,))
    columns, data_at_tv = client.get_data(folder, tv)
    print("columns:", ','.join(columns))
    for row in data_at_tv:
        print(row)

print("\nGetting tagged data ...")

for tv in tvs:
    print("\n--- tv=%.3f (tagged) ------------------------------" % (tv,))
    columns, data_at_tv = client.get_data(folder, tv, tag="chunk_25")
    print("columns:", ','.join(columns))
    for row in data_at_tv:
        print(row)

