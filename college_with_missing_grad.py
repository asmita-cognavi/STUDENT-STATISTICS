import pymongo
import csv

# MongoDB connection string
connection_string = "CONNECTION_STRING"
db_name = "DB_NAME"
collection = "COLLECTION_NAME"
# Connect to MongoDB
client = pymongo.MongoClient(connection_string)


# Create aggregation pipeline to find and aggregate data by college
pipeline = [
    {
        "$unwind": "$education_records"
    },
    {
        "$match": {
            "education_records.is_primary": True,
            "$or": [
                {"education_records.end_year": ""},
                {"education_records.end_year": None},
                {"education_records.end_year": {"$exists": False}}
            ]
        }
    },
    {
        "$group": {
            "_id": {
                "college_id": "$education_records.college_id",
                "college_name": "$education_records.college_name",
                "is_college_registered": "$education_records.is_college_registered"
            },
            "student_count": {"$sum": 1}
        }
    },
    {
        "$project": {
            "_id": 0,
            "college_id": "$_id.college_id",
            "college_name": "$_id.college_name",
            "is_registered": {
                "$cond": [
                    "$_id.is_college_registered", 
                    "YES", 
                    "NO"
                ]
            },
            "students_missing_grad_year": "$student_count"
        }
    },
    {
        "$sort": {"students_missing_grad_year": -1}  # Sort by count in descending order
    }
]

# Execute the aggregation
results = list(collection.aggregate(pipeline))

# Write results to a CSV file
csv_filename = "colleges_with_missing_grad_years.csv"
csv_fields = ["college_id", "college_name", "is_registered", "students_missing_grad_year"]

try:
    with open(csv_filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=csv_fields)
        writer.writeheader()
        writer.writerows(results)
    
    print(f"CSV file '{csv_filename}' created successfully with {len(results)} colleges.")
    print(f"Total students with missing graduation year: {sum(r['students_missing_grad_year'] for r in results)}")
    
    # Print the top 5 colleges with the most missing graduation years
    if results:
        print("\nTop colleges with missing graduation years:")
        for i, college in enumerate(results[:5], 1):
            print(f"{i}. {college['college_name']}: {college['students_missing_grad_year']} students")
    
except Exception as e:
    print(f"Error writing CSV file: {e}")

finally:
    # Close the MongoDB connection
    client.close()