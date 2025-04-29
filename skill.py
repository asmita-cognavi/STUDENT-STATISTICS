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

log_filename = f"{log_dir}/skills_count_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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
        
        # Initialize counters for each skill category
        skill_categories = {
            "0 skills": 0,
            "1-3 skills": 0,
            "4-6 skills": 0,
            "7-10 skills": 0,
            "10+ skills": 0
        }
        
        # Process in a single pass - for very large collections, consider using batching
        processed_count = 0
        
        for student in collection.find({}, {'skills': 1}):
            # Get skills count
            skills = student.get('skills', [])
            skills_count = len(skills) if skills else 0
            
            # Categorize skills count
            if skills_count == 0:
                skill_categories["0 skills"] += 1
            elif 1 <= skills_count <= 3:
                skill_categories["1-3 skills"] += 1
            elif 4 <= skills_count <= 6:
                skill_categories["4-6 skills"] += 1
            elif 7 <= skills_count <= 10:
                skill_categories["7-10 skills"] += 1
            else:  # skills_count > 10
                skill_categories["10+ skills"] += 1
            
            # Update processed count
            processed_count += 1
            
            # Log progress every 10,000 records
            if processed_count % 10000 == 0:
                logger.info(f"Processed {processed_count}/{total_documents} documents ({processed_count/total_documents*100:.2f}%)")
        
        # Convert results to DataFrame
        df = pd.DataFrame({
            'Skills Category': list(skill_categories.keys()),
            'Student Count': list(skill_categories.values())
        })
        
        # Save to CSV
        output_file = f"skills_count_distribution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        df.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
        
        # Print summary statistics
        logger.info("\n===== SUMMARY STATISTICS =====")
        logger.info(f"Total records processed: {processed_count}")
        
        # Print skills distribution
        logger.info("\nSkills Distribution:")
        for category, count in skill_categories.items():
            percentage = (count / processed_count) * 100
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