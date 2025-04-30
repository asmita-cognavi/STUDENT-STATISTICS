import pandas as pd
import pymongo
from pymongo import MongoClient
from bson.objectid import ObjectId

# Connection details
connection_string = "mongodb+srv://student-prod-user:qKNnFWzuRS5lX7ay@student-prod-db.qba4u.mongodb.net/?retryWrites=true&w=majority"

def process_skills_csv_to_mongodb():
    try:
        # Connect to MongoDB
        client = pymongo.MongoClient(connection_string)
        db = client["PROD_STUDENT"]
        collection = db["students"]
        
        print("Connected to MongoDB database")
        
        # Read the combined skills CSV
        df = pd.read_csv('combined_skill_data.csv')
        print(f"Read CSV with {len(df)} rows")
        
        # Counter for tracking updates
        update_count = 0
        error_count = 0
        
        # Process each row in the CSV
        for _, row in df.iterrows():
            try:
                student_id = row['student_id']
                skills_string = row['skills']
                
                # Skip if no skills data
                if pd.isna(skills_string) or not skills_string.strip():
                    continue
                
                # Parse the skills string (assuming skills are comma-separated)
                skill_names = [skill.strip() for skill in skills_string.split(',') if skill.strip()]
                
                # Create skills array in the required format
                skills_array = []
                for skill_name in skill_names:
                    skill_obj = {
                        "name": skill_name.lower(),
                   
                        "rating": 2,  # Default rating of 2 as requested
                        "composite": None,
                        "versions": []
                    }
                    skills_array.append(skill_obj)
                
                # Update the document in MongoDB
                # Convert string ID to ObjectId
                object_id = ObjectId(student_id)
                
                # Update the document
                result = collection.update_one(
                    {"_id": object_id},
                    {"$set": {"skills": skills_array}}
                )
                
                if result.modified_count > 0:
                    update_count += 1
                    print(f"Updated student {student_id} with {len(skills_array)} skills")
                else:
                    print(f"No document found for student ID {student_id}")
                    
            except Exception as e:
                error_count += 1
                print(f"Error processing student {student_id}: {str(e)}")
        
        print(f"Process completed. Updated {update_count} documents. Encountered {error_count} errors.")
        
    except Exception as e:
        print(f"Database connection error: {str(e)}")
    finally:
        # Close the connection
        if 'client' in locals():
            client.close()
            print("Database connection closed")

# Run the function
process_skills_csv_to_mongodb()