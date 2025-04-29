import pymongo
import pandas as pd
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import os
from datetime import datetime

# Set up logging
log_dir = "logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

log_filename = f"{log_dir}/mongodb_college_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

def get_college_name(student):
    """Extract college name from primary education record"""
    if student.get('education_records'):
        for record in student.get('education_records', []):
            if record.get('is_primary') is True and record.get('college_name'):
                return record.get('college_name')
    return "Unknown"

def analyze_student_record(student):
    """Analyze a single student record and return results with college name"""
    result = {}
    
    # Get college name
    college_name = get_college_name(student)
    result["college_name"] = college_name
    
    # Check for projects
    has_projects = bool(student.get('projects') and len(student.get('projects', [])) > 0)
    result["has_projects"] = has_projects
    
    # Check for work experience
    has_experience = bool(student.get('work_experiences') and len(student.get('work_experiences', [])) > 0)
    result["has_experience"] = has_experience
    
    # Check for achievements or awards
    has_achievements = bool(student.get('achievements') and len(student.get('achievements', [])) > 0)
    has_awards = bool(student.get('awards') and len(student.get('awards', [])) > 0)
    result["has_achievements"] = has_achievements or has_awards
    
    # Check for skills
    has_skills = bool(student.get('skills') and len(student.get('skills', [])) > 0)
    result["has_skills"] = has_skills
    
    # Check for education performance where is_primary is true
    has_performance = False
    if student.get('education_records'):
        for record in student.get('education_records', []):
            if record.get('is_primary') is True and record.get('performance') is not None:
                has_performance = True
                break
    
    result["has_grade"] = has_performance
    
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
        
        # Convert results to DataFrame
        df = pd.DataFrame(all_results)
        logger.info(f"Created DataFrame with {len(df)} records")
        
        # Create a summary DataFrame with counts by college
        summary_data = []
        colleges = df['college_name'].unique()
        
        for college in colleges:
            college_df = df[df['college_name'] == college]
            college_count = len(college_df)
            
            college_summary = {
                'College Name': college,
                'Total Students': college_count,
                'Have Projects': college_df['has_projects'].sum(),
                'No Projects': college_count - college_df['has_projects'].sum(),
                'Have Experience': college_df['has_experience'].sum(),
                'No Experience': college_count - college_df['has_experience'].sum(),
                'Have Achievements': college_df['has_achievements'].sum(),
                'No Achievements': college_count - college_df['has_achievements'].sum(),
                'Have Skills': college_df['has_skills'].sum(),
                'No Skills': college_count - college_df['has_skills'].sum(),
                'Have Grade': college_df['has_grade'].sum(),
                'No Grade': college_count - college_df['has_grade'].sum()
            }
            
            summary_data.append(college_summary)
        
        # Create the summary DataFrame
        summary_df = pd.DataFrame(summary_data)
        
        # Sort by college name for better readability
        summary_df = summary_df.sort_values('College Name')
        
        # Save to CSV
        output_file = f"student_records_by_college_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        summary_df.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
        
        # Print summary statistics
        logger.info("\n===== SUMMARY STATISTICS =====")
        logger.info(f"Total records processed: {len(df)}")
        logger.info(f"Number of colleges found: {len(colleges)}")
        
        # Print top 5 colleges by student count
        college_counts = df["college_name"].value_counts()
        logger.info("\nTop 5 colleges by student count:")
        for college, count in college_counts.head(5).items():
            logger.info(f"- {college}: {count} students")
        
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