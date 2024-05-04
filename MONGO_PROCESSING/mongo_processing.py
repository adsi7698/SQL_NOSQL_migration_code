import threading

from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure


def connect_mongo(collection_name):
	"Connecting mongodb"

	try:
		client = MongoClient('mongodb://localhost:27017/')
		db = client['encora_challenge']
		collection = db[collection_name]
	except ConnectionFailure:
		print("Failed to connect to MongoDB, check your server.")
		return None
	except OperationFailure as e:
		print("Operation failed:", e)
		return None

	return collection


def insert_in_batches(data, batch_size=1000):
	"Insert data in batches"

	try:
		collection = connect_mongo('movies')
		if collection is None:
			return False

		for i in range(0, len(data), batch_size):
			batch = data[i:i + batch_size]
			results = collection.insert_many(batch)
			print(f"Inserted batch {i//batch_size + 1} (ID range: {results.inserted_ids[0]} - {results.inserted_ids[-1]})")

		return True
	except Exception as e:
		print(f"An error occurred: {e}")
		return False
