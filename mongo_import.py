import json
from pymongo import MongoClient

client = MongoClient('localhost', 27017, username='admin', password='12345')
db = client['zakupki']
collection = db['purchases']

with open('3m/objects.json', encoding='utf-8') as f:
    file_data = json.load(f)

collection.insert_many(file_data)

client.close()
