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

log_filename = f"{log_dir}/mongodb_skills_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
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

def get_skills_count(student):
    """Get the number of skills for a student"""
    skills = student.get('skills', [])
    return len(skills) if skills else 0

def analyze_student_record(student):
    """Analyze a single student record and return college name and skills count"""
    result = {}
    
    # Get college name
    college_name = get_college_name(student)
    result["college_name"] = college_name
    
    # Get skills count
    skills_count = get_skills_count(student)
    result["skills_count"] = skills_count
    
    # Categorize skills count
    if skills_count == 0:
        result["skills_category"] = "0 skills"
    elif 1 <= skills_count <= 3:
        result["skills_category"] = "1-3 skills"
    elif 4 <= skills_count <= 6:
        result["skills_category"] = "4-6 skills"
    elif 7 <= skills_count <= 10:
        result["skills_category"] = "7-10 skills"
    else:  # skills_count > 10
        result["skills_category"] = "10+ skills"
    
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
        
        # Create a summary DataFrame with skills distribution by college
        summary_data = []
        colleges = df['college_name'].unique()
        
        # Define the skills categories in the desired order
        skills_categories = ["0 skills", "1-3 skills", "4-6 skills", "7-10 skills", "10+ skills"]
        
        for college in colleges:
            college_df = df[df['college_name'] == college]
            college_count = len(college_df)
            
            college_summary = {
                'College Name': college,
                'Total Students': college_count
            }
            
            # Add counts for each skills category
            for category in skills_categories:
                count = len(college_df[college_df['skills_category'] == category])
                college_summary[category] = count
                
            summary_data.append(college_summary)
        
        # Create the summary DataFrame
        summary_df = pd.DataFrame(summary_data)
        
        # Sort by college name for better readability
        summary_df = summary_df.sort_values('College Name')
        
        # Save to CSV
        output_file = f"student_skills_distribution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        summary_df.to_csv(output_file, index=False)
        logger.info(f"Results saved to {output_file}")
        
        # Create overall skills distribution summary
        overall_summary = {
            'Skills Category': skills_categories,
            'Student Count': [len(df[df['skills_category'] == category]) for category in skills_categories]
        }
        
        overall_df = pd.DataFrame(overall_summary)
        
        # Save overall summary to CSV
        overall_output_file = f"overall_skills_distribution_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        overall_df.to_csv(overall_output_file, index=False)
        logger.info(f"Overall skills distribution saved to {overall_output_file}")
        
        # Print summary statistics
        logger.info("\n===== SUMMARY STATISTICS =====")
        logger.info(f"Total records processed: {len(df)}")
        logger.info(f"Number of colleges found: {len(colleges)}")
        
        # Print skills distribution
        logger.info("\nSkills Distribution:")
        for category in skills_categories:
            count = len(df[df['skills_category'] == category])
            percentage = (count / len(df)) * 100
            logger.info(f"- {category}: {count} students ({percentage:.2f}%)")
        
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