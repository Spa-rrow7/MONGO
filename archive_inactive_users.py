import os
import json
from datetime import datetime, timedelta
from pymongo import MongoClient


client = MongoClient("mongodb://localhost:27017/")
db = client["my_database"]
source = db["user_events"]
archive = db["archived_users"]


today = datetime.now()
archive_date_str = today.strftime("%Y-%m-%d")
report_path = os.path.join(f"{archive_date_str}.json")

registration_cutoff = today - timedelta(days=30)
activity_cutoff = today - timedelta(days=14)

pipeline = [
    {"$match": {"user_info.registration_date": {"$lt": registration_cutoff}}},
    {"$group": {
        "_id": "$user_id",
        "last_event_time": {"$max": "$event_time"}
    }},
    {"$match": {"last_event_time": {"$lt": activity_cutoff}}}
]

cursors = list(source.aggregate(pipeline))
user_ids = [doc["_id"] for doc in cursors]

if not user_ids:
    report_data = {
        "date": archive_date_str,
        "archived_count": 0,
        "users_id": []
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report_data, f, indent=2)
    print("Нет пользователей для архивации")
    exit(0)

# Архивируем
for uid in user_ids:
    user_doc = source.find_one({"user_id": uid})
    if user_doc:
        user_doc.pop("_id", None)
        user_doc["_id"] = uid
        archive.update_one({"_id": uid}, {"$set": user_doc}, upsert=True)

# Удаляем из исходной коллекции
source.delete_many({"user_id": {"$in": user_ids}})

# Отчёт
report_data = {
    "date": archive_date_str,
    "archived_count": len(user_ids),
    "users_id": user_ids
}

with open(report_path, "w", encoding="utf-8") as f:
    json.dump(report_data, f, indent=2)

print(f"Архивировано: {len(user_ids)} пользователей")
print(f"Отчёт: {report_path}")