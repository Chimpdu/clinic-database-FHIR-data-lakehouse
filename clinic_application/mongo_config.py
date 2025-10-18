import os


MONGODB_URI = os.getenv("MONGODB_URI", "mongodb://localhost:27017")
MONGODB_DB  = os.getenv("MONGODB_DB", "clinic_app")

# Collection name
COLL_MESSAGES = "messages"
