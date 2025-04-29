import logging
import os
from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime
import time

# Configure logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f"{log_dir}/education_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_filename),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def main():
    start_time = time.time()
    logger.info("===== STARTING EDUCATION RECORDS ANALYSIS =====")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Connect to MongoDB
        connection_string = "string"
        logger.info("Connecting to MongoDB...")
        client = MongoClient(connection_string)
        db = client["db_name"]
        students_collection = db["students"]
        
        # Initialize counters
        graduation_year_counts = defaultdict(int)
        special_count = 0  # Count for graduation year before 2023 or after 2027 + no graduation year
        no_education_records_count = 0
        education_records_but_no_primary_count = 0
        
        # Count total students
        total_students = students_collection.count_documents({})
        logger.info(f"Total number of students found: {total_students}")
        
        # Count students with no education_records
        logger.info("Counting students with no education records...")
        no_education_records_count = students_collection.count_documents({
            "$or": [
                {"education_records": {"$exists": False}},
                {"education_records": {"$eq": []}}
            ]
        })
        logger.info(f"Students with no education records: {no_education_records_count}")
        
        # Prepare to process students with education records
        logger.info("Preparing to process students with education records...")
        cursor = students_collection.find({
            "education_records": {"$exists": True, "$ne": []}
        })
        
        total_records_to_process = students_collection.count_documents({
            "education_records": {"$exists": True, "$ne": []}
        })
        logger.info(f"Found {total_records_to_process} students with education records")
        
        # Process in simple batches of 100
        BATCH_SIZE = 100
        processed_records = 0
        current_batch = []
        batch_count = 0
        
        # Process each student
        for student in cursor:
            current_batch.append(student)
            
            # Process when batch size reached
            if len(current_batch) >= BATCH_SIZE:
                batch_results = process_batch(batch_count, current_batch)
                
                # Update counters with batch results
                for year, count in batch_results["year_counts"].items():
                    graduation_year_counts[year] += count
                special_count += batch_results["special_count"]
                education_records_but_no_primary_count += batch_results["no_primary_count"]
                
                # Update progress
                processed_records += len(current_batch)
                percentage = (processed_records / total_records_to_process) * 100 if total_records_to_process > 0 else 0
                logger.info(f"Progress: {processed_records}/{total_records_to_process} records ({percentage:.2f}%)")
                
                # Clear batch for next iteration
                current_batch = []
                batch_count += 1
        
        # Process any remaining records
        if current_batch:
            batch_results = process_batch(batch_count, current_batch)
            for year, count in batch_results["year_counts"].items():
                graduation_year_counts[year] += count
            special_count += batch_results["special_count"]
            education_records_but_no_primary_count += batch_results["no_primary_count"]
            processed_records += len(current_batch)
        
        # Log final results
        logger.info("===== ANALYSIS RESULTS =====")
        logger.info(f"Total students: {total_students}")
        logger.info(f"Students graduating in 2026 (primary education): {graduation_year_counts['2026']}")
        logger.info(f"Students graduating in 2027 (primary education): {graduation_year_counts['2027']}")
        logger.info(f"Students with graduation year before 2023 or after 2027 + No graduation year: {special_count}")
        logger.info(f"Students with no education records: {no_education_records_count}")
        logger.info(f"Students with education records but no primary education: {education_records_but_no_primary_count}")
        
        # Generate summary statistics
        total_primary_education = graduation_year_counts['2026'] + graduation_year_counts['2027'] + special_count
        logger.info("\n===== SUMMARY STATISTICS =====")
        
        # Safe division to prevent errors with zero
        def safe_percentage(num, denom):
            return (num/denom)*100 if denom > 0 else 0
        
        logger.info(f"Students with primary education records: {total_primary_education} ({safe_percentage(total_primary_education, total_students):.2f}% of total)")
        logger.info(f"Students graduating in 2026: {graduation_year_counts['2026']} ({safe_percentage(graduation_year_counts['2026'], total_students):.2f}% of total)")
        logger.info(f"Students graduating in 2027: {graduation_year_counts['2027']} ({safe_percentage(graduation_year_counts['2027'], total_students):.2f}% of total)")
        logger.info(f"Students with special graduation cases: {special_count} ({safe_percentage(special_count, total_students):.2f}% of total)")
        logger.info(f"Students with no education records: {no_education_records_count} ({safe_percentage(no_education_records_count, total_students):.2f}% of total)")
        logger.info(f"Students with education records but no primary: {education_records_but_no_primary_count} ({safe_percentage(education_records_but_no_primary_count, total_students):.2f}% of total)")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        # Avoid division by zero
        records_per_second = processed_records / total_time if total_time > 0 else 0
        
        logger.info(f"\n===== ANALYSIS COMPLETED =====")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        logger.info(f"Average processing speed: {records_per_second:.1f} records/second")
        logger.info(f"Ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
    finally:
        # Close MongoDB connection
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def process_batch(batch_id, batch):
    """Process a batch of student records to collect statistics"""
    batch_start_time = time.time()
    batch_size = len(batch)
    
    logger.info(f"Starting batch {batch_id} with {batch_size} records")
    
    local_counts = defaultdict(int)
    local_special_count = 0
    local_no_primary_count = 0
    
    for student in batch:
        has_primary = False
        for edu_record in student.get("education_records", []):
            if edu_record.get("is_primary", False):
                has_primary = True
                end_year = edu_record.get("end_year")
                
                # Make sure end_year is an integer before comparison
                if end_year == 2026:
                    local_counts["2026"] += 1
                elif end_year == 2027:
                    local_counts["2027"] += 1
                elif end_year is None or not isinstance(end_year, int) or end_year < 2023 or end_year > 2027:
                    local_special_count += 1
        
        if not has_primary and student.get("education_records"):
            local_no_primary_count += 1
    
    batch_processing_time = time.time() - batch_start_time
    logger.info(f"Completed batch {batch_id} in {batch_processing_time:.2f} seconds - "
               f"Stats: 2026: {local_counts['2026']}, 2027: {local_counts['2027']}, "
               f"Special: {local_special_count}, No Primary: {local_no_primary_count}")
    
    return {
        "batch_id": batch_id,
        "year_counts": dict(local_counts),
        "special_count": local_special_count,
        "no_primary_count": local_no_primary_count,
        "processing_time": batch_processing_time,
        "records_processed": batch_size
    }

if __name__ == "__main__":
    main()