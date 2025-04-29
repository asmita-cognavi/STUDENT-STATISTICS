import pymongo
import pandas as pd
import logging
import time
import os
from datetime import datetime

# Set up logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_filename = f"{log_dir}/skills_graduation_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

connection_string = "CONNECTION_STRING"
db_name = "DB_NAME"
collection_name = "COLLECTION_NAME"

def connect_to_mongodb():
    """Establish connection to MongoDB and return collection"""
    try:
        logger.info("Connecting to MongoDB...")
        client = pymongo.MongoClient(connection_string, serverSelectionTimeoutMS=5000)
        # Force a connection to verify it works
        client.server_info()
        logger.info("MongoDB connection established successfully")
        db = client[db_name]
        collection = db[collection_name]
        return client, collection
    except Exception as e:
        logger.error(f"Failed to connect to MongoDB: {e}")
        raise

def main():
    start_time = time.time()
    
    try:
        # Connect to MongoDB
        client, collection = connect_to_mongodb()
        
        # Get total count for progress tracking
        total_documents = collection.count_documents({})
        logger.info(f"Found {total_documents} documents in the collection")
        
        # Initialize counters for overall skill categories
        skill_categories = {
            "0 skills": 0,
            "1-3 skills": 0,
            "4-6 skills": 0,
            "7-10 skills": 0,
            "10+ skills": 0
        }
        
        # Initialize counters for graduation years 2026 and 2027
        grad_year_2026_categories = {
            "0 skills": 0,
            "1-3 skills": 0,
            "4-6 skills": 0,
            "7-10 skills": 0,
            "10+ skills": 0
        }
        
        grad_year_2027_categories = {
            "0 skills": 0,
            "1-3 skills": 0,
            "4-6 skills": 0,
            "7-10 skills": 0,
            "10+ skills": 0
        }
        
        # Process in a single pass
        processed_count = 0
        grad_2026_count = 0
        grad_2027_count = 0
        
        for student in collection.find({}, {'skills': 1, 'education_records': 1}):
            # Get skills count
            skills = student.get('skills', [])
            skills_count = len(skills) if skills else 0
            
            # Determine skill category
            skill_category = ""
            if skills_count == 0:
                skill_category = "0 skills"
            elif 1 <= skills_count <= 3:
                skill_category = "1-3 skills"
            elif 4 <= skills_count <= 6:
                skill_category = "4-6 skills"
            elif 7 <= skills_count <= 10:
                skill_category = "7-10 skills"
            else:  # skills_count > 10
                skill_category = "10+ skills"
            
            # Update overall skill categories
            skill_categories[skill_category] += 1
            
            # Check graduation year from primary education record
            education_records = student.get('education_records', [])
            has_primary_2026 = False
            has_primary_2027 = False
            
            for edu_record in education_records:
                if edu_record.get('is_primary', False):
                    grad_year = edu_record.get('end_year')
                    if grad_year == 2026:
                        grad_2026_count += 1
                        has_primary_2026 = True
                        grad_year_2026_categories[skill_category] += 1
                    elif grad_year == 2027:
                        grad_2027_count += 1
                        has_primary_2027 = True
                        grad_year_2027_categories[skill_category] += 1
            
            # Update processed count
            processed_count += 1
            
            # Log progress every 10,000 records
            if processed_count % 10000 == 0:
                logger.info(f"Processed {processed_count}/{total_documents} documents ({processed_count/total_documents*100:.2f}%)")
                logger.info(f"Found {grad_2026_count} students graduating in 2026 and {grad_2027_count} students graduating in 2027 so far")
        
        # Convert overall results to DataFrame
        df_overall = pd.DataFrame({
            'Skills Category': list(skill_categories.keys()),
            'Student Count': list(skill_categories.values())
        })
        
        # Convert 2026 results to DataFrame
        df_2026 = pd.DataFrame({
            'Skills Category': list(grad_year_2026_categories.keys()),
            'Student Count (2026 Graduates)': list(grad_year_2026_categories.values())
        })
        
        # Convert 2027 results to DataFrame
        df_2027 = pd.DataFrame({
            'Skills Category': list(grad_year_2027_categories.keys()),
            'Student Count (2027 Graduates)': list(grad_year_2027_categories.values())
        })
        
        # Merge the DataFrames
        df_combined = pd.merge(df_overall, df_2026, on='Skills Category')
        df_combined = pd.merge(df_combined, df_2027, on='Skills Category')
        
        # Save to CSV
        output_file = f"skills_graduation_distribution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df_combined.to_csv(output_file, index=False)
        logger.info(f"Combined results saved to {output_file}")
        
        # Also save individual CSVs
        df_overall.to_csv(f"skills_overall_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False)
        df_2026.to_csv(f"skills_2026_graduates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False)
        df_2027.to_csv(f"skills_2027_graduates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", index=False)
        
        # Print summary statistics
        logger.info("\n===== SUMMARY STATISTICS =====")
        logger.info(f"Total records processed: {processed_count}")
        logger.info(f"Total 2026 graduates found: {grad_2026_count}")
        logger.info(f"Total 2027 graduates found: {grad_2027_count}")
        
        # Print overall skills distribution
        logger.info("\nSkills Distribution (Overall):")
        for category, count in skill_categories.items():
            percentage = (count / processed_count) * 100
            logger.info(f"- {category}: {count} students ({percentage:.2f}%)")
        
        # Print 2026 skills distribution
        if grad_2026_count > 0:
            logger.info("\nSkills Distribution (2026 Graduates):")
            for category, count in grad_year_2026_categories.items():
                percentage = (count / grad_2026_count) * 100
                logger.info(f"- {category}: {count} students ({percentage:.2f}%)")
        
        # Print 2027 skills distribution
        if grad_2027_count > 0:
            logger.info("\nSkills Distribution (2027 Graduates):")
            for category, count in grad_year_2027_categories.items():
                percentage = (count / grad_2027_count) * 100
                logger.info(f"- {category}: {count} students ({percentage:.2f}%)")
        
        # Calculate execution time
        execution_time = time.time() - start_time
        logger.info(f"Analysis completed in {execution_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"An error occurred: {e}", exc_info=True)
    finally:
        # Close MongoDB connection
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

if __name__ == "__main__":
    main()