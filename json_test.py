import json

data = {'key1' : 'val1', 5 : 'val2'}

with open('data.txt', 'w') as outfile:
    json.dump(data, outfile)