import logging
import os
import csv
from pymongo import MongoClient
from datetime import datetime
import time

# Configure logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f"{log_dir}/education_resume_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# File paths for the CSV outputs
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
no_edu_file = f"{output_dir}/students_no_education_{timestamp}.csv"
no_primary_file = f"{output_dir}/students_no_primary_{timestamp}.csv"
no_end_year_file = f"{output_dir}/students_no_end_year_{timestamp}.csv"

def main():
    start_time = time.time()
    logger.info("===== STARTING EDUCATION RECORDS AND RESUME ANALYSIS =====")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Connect to MongoDB
        connection_string = "string"
        logger.info("Connecting to MongoDB...")
        client = MongoClient(connection_string)
        db = client["db_name"]
        students_collection = db["students"]
        resume_collection = db["resume"]
        
        # Create CSV files and write headers
        create_csv_files()
        
        # Initialize counters
        no_education_count = 0
        no_primary_count = 0
        no_end_year_count = 0
        
        # Process students with no education records
        logger.info("Processing students with no education records...")
        no_education_cursor = students_collection.find({
            "$or": [
                {"education_records": {"$exists": False}},
                {"education_records": {"$eq": []}}
            ]
        }, {"_id": 1})
        
        no_education_ids = [doc["_id"] for doc in no_education_cursor]
        no_education_count = len(no_education_ids)
        logger.info(f"Found {no_education_count} students with no education records")
        
        # Process resumes for students with no education records
        process_resumes(resume_collection, no_education_ids, no_edu_file)
        
        # Process students with education records but no primary
        logger.info("Processing students with education records but no primary education...")
        
        # Initialize lists to collect student IDs for each category
        no_primary_ids = []
        no_end_year_ids = []
        
        # Process in batches of 1000
        BATCH_SIZE = 1000
        processed_records = 0
        
        # Get total count for progress reporting
        total_with_edu = students_collection.count_documents({
            "education_records": {"$exists": True, "$ne": []}
        })
        logger.info(f"Found {total_with_edu} students with education records to process")
        
        # Process students with education records in batches
        cursor = students_collection.find({
            "education_records": {"$exists": True, "$ne": []}
        }, {"_id": 1, "education_records": 1})
        
        current_batch = []
        batch_count = 0
        
        for student in cursor:
            current_batch.append(student)
            
            if len(current_batch) >= BATCH_SIZE:
                batch_results = process_education_batch(batch_count, current_batch)
                no_primary_ids.extend(batch_results["no_primary_ids"])
                no_end_year_ids.extend(batch_results["no_end_year_ids"])
                
                processed_records += len(current_batch)
                percentage = (processed_records / total_with_edu) * 100 if total_with_edu > 0 else 0
                logger.info(f"Progress: {processed_records}/{total_with_edu} records ({percentage:.2f}%)")
                
                current_batch = []
                batch_count += 1
        
        # Process any remaining records
        if current_batch:
            batch_results = process_education_batch(batch_count, current_batch)
            no_primary_ids.extend(batch_results["no_primary_ids"])
            no_end_year_ids.extend(batch_results["no_end_year_ids"])
            processed_records += len(current_batch)
        
        no_primary_count = len(no_primary_ids)
        no_end_year_count = len(no_end_year_ids)
        
        # Process resumes for students with no primary education
        logger.info(f"Processing resumes for {no_primary_count} students with no primary education...")
        process_resumes(resume_collection, no_primary_ids, no_primary_file)
        
        # Process resumes for students with primary education but no end_year
        logger.info(f"Processing resumes for {no_end_year_count} students with primary education but no end_year...")
        process_resumes(resume_collection, no_end_year_ids, no_end_year_file)
        
        # Log final results
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info("\n===== ANALYSIS SUMMARY =====")
        logger.info(f"Students with no education records: {no_education_count}")
        logger.info(f"Students with education records but no primary: {no_primary_count}")
        logger.info(f"Students with primary education but no end_year: {no_end_year_count}")
        logger.info(f"Results exported to:")
        logger.info(f" - {no_edu_file}")
        logger.info(f" - {no_primary_file}")
        logger.info(f" - {no_end_year_file}")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        logger.info(f"Ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
    finally:
        # Close MongoDB connection
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def create_csv_files():
    """Create CSV files with headers"""
    for file_path in [no_edu_file, no_primary_file, no_end_year_file]:
        with open(file_path, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['student_id', 'resume_link'])
        logger.info(f"Created CSV file: {file_path}")

def process_education_batch(batch_id, batch):
    """Process a batch of student records to categorize them"""
    batch_start_time = time.time()
    batch_size = len(batch)
    
    logger.info(f"Processing education batch {batch_id} with {batch_size} records")
    
    no_primary_ids = []
    no_end_year_ids = []
    
    for student in batch:
        student_id = student["_id"]
        has_primary = False
        has_primary_with_end_year = False
        
        for edu_record in student.get("education_records", []):
            if edu_record.get("is_primary", False):
                has_primary = True
                if edu_record.get("end_year") is not None:
                    has_primary_with_end_year = True
        
        if not has_primary:
            no_primary_ids.append(student_id)
        elif has_primary and not has_primary_with_end_year:
            no_end_year_ids.append(student_id)
    
    batch_processing_time = time.time() - batch_start_time
    logger.info(f"Completed batch {batch_id} in {batch_processing_time:.2f} seconds - "
               f"No primary: {len(no_primary_ids)}, No end_year: {len(no_end_year_ids)}")
    
    return {
        "no_primary_ids": no_primary_ids,
        "no_end_year_ids": no_end_year_ids
    }

def process_resumes(resume_collection, student_ids, output_file):
    """Process resumes for a list of student IDs and write to CSV"""
    start_time = time.time()
    batch_size = 500  # Process resumes in smaller batches
    total_ids = len(student_ids)
    processed = 0
    found_resumes = 0
    
    for i in range(0, total_ids, batch_size):
        batch_ids = student_ids[i:i+batch_size]
        
        # Find resumes for this batch of student IDs
        # Using student _id to match with user_id in resume collection
        resumes = resume_collection.find(
            {"user_id": {"$in": batch_ids}},
            {"user_id": 1, "link": 1}
        )
        
        # Write resumes to CSV
        with open(output_file, 'a', newline='') as csvfile:
            writer = csv.writer(csvfile)
            batch_count = 0
            
            for resume in resumes:
                writer.writerow([resume.get("user_id", ""), resume.get("link", "")])
                batch_count += 1
        
        processed += len(batch_ids)
        found_resumes += batch_count
        percentage = (processed / total_ids) * 100 if total_ids > 0 else 0
        
        logger.info(f"Resume processing progress: {processed}/{total_ids} ({percentage:.2f}%) - Found {batch_count} resumes in this batch")
    
    processing_time = time.time() - start_time
    logger.info(f"Resume processing completed in {processing_time:.2f} seconds - Total resumes found: {found_resumes}")
    
    return found_resumes

if __name__ == "__main__":
    main()