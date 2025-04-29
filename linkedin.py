import pymongo
import csv
from datetime import datetime

connection_string = "CONNECTION_STRING"
db_name = "DB_NAME"
collection_name = "COLLECTION_NAME"

# Connect to MongoDB
client = pymongo.MongoClient(connection_string)
db = client[db_name]
collection = db[collection_name]

# Query to find students with LinkedIn URL but empty education records
query = {
    "contact_detail.linkedin_url": {"$exists": True, "$ne": None, "$ne": ""},
    "education_records": {"$exists": True, "$size": 0}
}

# Fields to retrieve
projection = {
    "_id": 1,
    "first_name": 1,
    "last_name": 1,
    "email": 1,
    "contact_detail.linkedin_url": 1
}

# Prepare CSV file
csv_filename = f"linkedin_students_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
csv_headers = ["Student ID", "First Name", "Last Name", "Email", "LinkedIn URL"]

# Execute query
students = list(collection.find(query, projection))
count = len(students)

# Write data to CSV
with open(csv_filename, mode='w', newline='', encoding='utf-8') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(csv_headers)
    
    for student in students:
        # Get LinkedIn URL safely (handling nested structure)
        linkedin_url = student.get('contact_detail', {}).get('linkedin_url', '')
        
        writer.writerow([
            student.get('_id'),
            student.get('first_name', ''),
            student.get('last_name', ''),
            student.get('email', ''),
            linkedin_url
        ])

print(f"Data exported successfully to {csv_filename}")
print(f"Total records found: {count}")
print(f"Query executed: {query}")