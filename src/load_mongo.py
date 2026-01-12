from pymongo import MongoClient


def upload_to_mongo(data, db_name="text_etl_pipeline", collection_name="articles_data",
                    uri="mongodb://localhost:27017/", batch_size=500):
    # Connect to MongoDB
    client = MongoClient(uri)
    db = client[db_name]
    collection = db[collection_name]

    # insert all records
    total = len(data)
    inserted = 0

    for i in range(0, total, batch_size):
        batch = data[i:i + batch_size]
        collection.insert_many(batch)
        inserted += len(batch)
        print(f"Inserted {inserted}/{total} documents")

    client.close()
