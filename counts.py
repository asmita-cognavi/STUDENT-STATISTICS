import pymongo
import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from functools import partial
import os
from datetime import datetime

# Set up logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_filename = f"{log_dir}/mongodb_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# MongoDB connection parameters
connection_string = "CONNECTION_STRING"
db_name = "DB_NAME"
collection_name = "COLLECTION_NAME"

# Counters for summary statistics
stats = {
    "total_processed": 0,
    "projects": {"have": 0, "have_not": 0},
    "work_experience": {"have": 0, "have_not": 0},
    "achievements": {"have": 0, "have_not": 0},
    "skills": {"have": 0, "have_not": 0},
    "grade": {"have": 0, "have_not": 0}
}

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

def analyze_student_record(student):
    """Analyze a single student record and return results"""
    result = {}
    
    # Check for projects
    if student.get('projects') and len(student.get('projects', [])) > 0:
        result["projects"] = "have"
    else:
        result["projects"] = "have_not"
    
    # Check for work experience
    if student.get('work_experiences') and len(student.get('work_experiences', [])) > 0:
        result["work_experience"] = "have"
    else:
        result["work_experience"] = "have_not"
    
    # Check for achievements or awards
    has_achievements = (student.get('achievements') and len(student.get('achievements', [])) > 0)
    has_awards = (student.get('awards') and len(student.get('awards', [])) > 0)
    if has_achievements or has_awards:
        result["achievements"] = "have"
    else:
        result["achievements"] = "have_not"
    
    # Check for skills
    if student.get('skills') and len(student.get('skills', [])) > 0:
        result["skills"] = "have"
    else:
        result["skills"] = "have_not"
    
    # Check for education performance where is_primary is true
    has_performance = False
    if student.get('education_records'):
        for record in student.get('education_records', []):
            if record.get('is_primary') is True and record.get('performance') is not None:
                has_performance = True
                break
    
    if has_performance:
        result["grade"] = "have"
    else:
        result["grade"] = "have_not"
    
    return result

def process_batch(batch):
    """Process a batch of student records"""
    results = []
    for student in batch:
        result = analyze_student_record(student)
        results.append(result)
    return results

def main():
    start_time = time.time()
    
    try:
        # Connect to MongoDB
        client, collection = connect_to_mongodb()
        
        # Get total count for progress tracking
        total_documents = collection.count_documents({})
        logger.info(f"Found {total_documents} documents in the collection")
        
        # For very large collections, use batching
        batch_size = 1000
        total_batches = (total_documents + batch_size - 1) // batch_size
        
        all_results = []
        processed_count = 0
        
        # Process data in batches with threading
        with ThreadPoolExecutor(max_workers=4) as executor:
            for batch_num in range(total_batches):
                logger.info(f"Processing batch {batch_num + 1}/{total_batches}")
                
                # Get batch of documents
                batch_cursor = collection.find().skip(batch_num * batch_size).limit(batch_size)
                batch = list(batch_cursor)
                
                # Process the batch
                batch_results = process_batch(batch)
                all_results.extend(batch_results)
                
                # Update processed count
                processed_count += len(batch)
                logger.info(f"Processed {processed_count}/{total_documents} documents ({processed_count/total_documents*100:.2f}%)")
        
        # Calculate statistics
        df = pd.DataFrame(all_results)
        stats["total_processed"] = len(df)
        
        # Get counts for each category
        for category in ["projects", "work_experience", "achievements", "skills", "grade"]:
            value_counts = df[category].value_counts().to_dict()
            stats[category]["have"] = value_counts.get("have", 0)
            stats[category]["have_not"] = value_counts.get("have_not", 0)
        
        # Create summary DataFrame for CSV output
        summary_data = {
            "field": ["projects", "experience", "achievements/awards", "skills", "grade"],
            "have": [
                stats["projects"]["have"],
                stats["work_experience"]["have"],
                stats["achievements"]["have"],
                stats["skills"]["have"],
                stats["grade"]["have"]
            ],
            "have_not": [
                stats["projects"]["have_not"],
                stats["work_experience"]["have_not"],
                stats["achievements"]["have_not"],
                stats["skills"]["have_not"],
                stats["grade"]["have_not"]
            ]
        }
        
        # Create and save the output DataFrame
        output_df = pd.DataFrame(summary_data)
        output_file = f"student_records_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        output_df.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
        
        # Print summary statistics
        logger.info("\n===== SUMMARY STATISTICS =====")
        logger.info(f"Total records processed: {stats['total_processed']}")
        
        for category in ["projects", "work_experience", "achievements", "skills", "grade"]:
            display_name = "achievements/awards" if category == "achievements" else category
            display_name = "experience" if category == "work_experience" else display_name
            logger.info(f"{display_name.capitalize()}: Have: {stats[category]['have']} | Have not: {stats[category]['have_not']}")
        
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