import logging
import os
import csv
from pymongo import MongoClient
from datetime import datetime
import time

# Configure logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f"{log_dir}/zero_skills_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# File path for the CSV output
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
zero_skills_file = f"{output_dir}/students_zero_skills_{timestamp}.csv"

def main():
    start_time = time.time()
    logger.info("===== STARTING ZERO SKILLS STUDENTS ANALYSIS =====")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Connect to MongoDB
        connection_string = "string"
        logger.info("Connecting to MongoDB...")
        client = MongoClient(connection_string)
        db = client["db_name"]
        students_collection = db["students"]
        resume_collection = db["resume"]
        
        # Create CSV file and write header
        with open(zero_skills_file, 'w', newline='') as csvfile:
            writer = csv.writer(csvfile)
            writer.writerow(['student_id', 'resume_link'])
        logger.info(f"Created CSV file: {zero_skills_file}")
        
        # Initialize counters
        total_zero_skills = 0
        total_with_resumes = 0
        
        # Process in batches of 1000
        BATCH_SIZE = 1000
        processed_records = 0
        
        # Find students with zero skills
        logger.info("Finding students with zero skills...")
        
        # Query conditions for zero skills (either skills field doesn't exist or is empty array)
        zero_skills_query = {
            "$or": [
                {"skills": {"$exists": False}},
                {"skills": {"$eq": []}}
            ]
        }
        
        # Get total count for progress reporting
        total_zero_skills_students = students_collection.count_documents(zero_skills_query)
        logger.info(f"Found {total_zero_skills_students} students with zero skills")
        
        # Process students with zero skills in batches
        cursor = students_collection.find(zero_skills_query, {"_id": 1})
        
        student_ids_batch = []
        batch_count = 0
        
        for student in cursor:
            student_ids_batch.append(student["_id"])
            total_zero_skills += 1
            
            # Process when batch size reached
            if len(student_ids_batch) >= BATCH_SIZE:
                found_resumes = process_resume_batch(resume_collection, student_ids_batch, zero_skills_file)
                total_with_resumes += found_resumes
                
                processed_records += len(student_ids_batch)
                percentage = (processed_records / total_zero_skills_students) * 100 if total_zero_skills_students > 0 else 0
                logger.info(f"Progress: {processed_records}/{total_zero_skills_students} records ({percentage:.2f}%)")
                
                student_ids_batch = []
                batch_count += 1
        
        # Process any remaining records
        if student_ids_batch:
            found_resumes = process_resume_batch(resume_collection, student_ids_batch, zero_skills_file)
            total_with_resumes += found_resumes
            processed_records += len(student_ids_batch)
        
        # Log final results
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info("\n===== ANALYSIS SUMMARY =====")
        logger.info(f"Total students with zero skills: {total_zero_skills}")
        logger.info(f"Students with zero skills and resumes: {total_with_resumes}")
        resume_percentage = (total_with_resumes / total_zero_skills) * 100 if total_zero_skills > 0 else 0
        logger.info(f"Percentage of zero-skills students with resumes: {resume_percentage:.2f}%")
        logger.info(f"Results exported to: {zero_skills_file}")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        logger.info(f"Ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
    finally:
        # Close MongoDB connection
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def process_resume_batch(resume_collection, student_ids, output_file):
    """Process resumes for a batch of student IDs and write to CSV"""
    batch_start_time = time.time()
    
    logger.info(f"Processing resume batch with {len(student_ids)} student IDs")
    
    # Find resumes for this batch of student IDs
    # Using student _id to match with user_id in resume collection
    resumes = resume_collection.find(
        {"user_id": {"$in": student_ids}},
        {"user_id": 1, "link": 1}
    )
    
    # Write resumes to CSV
    resume_count = 0
    with open(output_file, 'a', newline='') as csvfile:
        writer = csv.writer(csvfile)
        
        for resume in resumes:
            writer.writerow([resume.get("user_id", ""), resume.get("link", "")])
            resume_count += 1
    
    batch_processing_time = time.time() - batch_start_time
    logger.info(f"Batch processing completed in {batch_processing_time:.2f} seconds - Found {resume_count} resumes")
    
    return resume_count

if __name__ == "__main__":
    main()