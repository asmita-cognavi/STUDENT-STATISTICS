import logging
import os
from pymongo import MongoClient
from collections import defaultdict
from datetime import datetime
import time

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
        connection_string = "string"
        logger.info("Connecting to MongoDB...")
        client = MongoClient(connection_string)
        db = client["db_name"]
        students_collection = db["students"]
        
        graduation_year_counts = defaultdict(int)
        no_education_records_count = 0
        education_records_but_no_primary_count = 0
        no_end_year_count = 0
        
        total_students = students_collection.count_documents({})
        logger.info(f"Total number of students found: {total_students}")
        
        logger.info("Counting students with no education records...")
        no_education_records_count = students_collection.count_documents({
            "$or": [
                {"education_records": {"$exists": False}},
                {"education_records": {"$eq": []}}
            ]
        })
        logger.info(f"Students with no education records: {no_education_records_count}")
        
        logger.info("Preparing to process students with education records...")
        cursor = students_collection.find({
            "education_records": {"$exists": True, "$ne": []}
        })
        
        total_records_to_process = students_collection.count_documents({
            "education_records": {"$exists": True, "$ne": []}
        })
        logger.info(f"Found {total_records_to_process} students with education records")
        
        # Process in batches of 1000
        BATCH_SIZE = 1000
        processed_records = 0
        current_batch = []
        batch_count = 0
        
        # Process each student
        for student in cursor:
            current_batch.append(student)
            
            if len(current_batch) >= BATCH_SIZE:
                batch_results = process_batch(batch_count, current_batch)
                
                for year, count in batch_results["year_counts"].items():
                    graduation_year_counts[year] += count
                education_records_but_no_primary_count += batch_results["no_primary_count"]
                no_end_year_count += batch_results["no_end_year_count"]
                
                processed_records += len(current_batch)
                percentage = (processed_records / total_records_to_process) * 100 if total_records_to_process > 0 else 0
                logger.info(f"Progress: {processed_records}/{total_records_to_process} records ({percentage:.2f}%)")
                
                current_batch = []
                batch_count += 1
        
        if current_batch:
            batch_results = process_batch(batch_count, current_batch)
            for year, count in batch_results["year_counts"].items():
                graduation_year_counts[year] += count
            education_records_but_no_primary_count += batch_results["no_primary_count"]
            no_end_year_count += batch_results["no_end_year_count"]
            processed_records += len(current_batch)
        
        sorted_years = sorted(graduation_year_counts.keys())
        
        # Log final results
        logger.info("===== ANALYSIS RESULTS =====")
        logger.info(f"Total students: {total_students}")
        logger.info(f"Distribution of graduation years (primary education):")
        for year in sorted_years:
            if isinstance(year, int):
                logger.info(f"  Year {year}: {graduation_year_counts[year]} students")
        
        logger.info(f"Students with no end_year in primary education: {no_end_year_count}")
        logger.info(f"Students with no education records: {no_education_records_count}")
        logger.info(f"Students with education records but no primary education: {education_records_but_no_primary_count}")
        
        # Generate summary statistics
        total_with_primary_education = sum(graduation_year_counts.values()) + no_end_year_count
        logger.info("\n===== SUMMARY STATISTICS =====")
        
        def safe_percentage(num, denom):
            return (num/denom)*100 if denom > 0 else 0
        
        logger.info(f"Students with primary education records: {total_with_primary_education} ({safe_percentage(total_with_primary_education, total_students):.2f}% of total)")
        
        for year in sorted_years:
            if isinstance(year, int):
                logger.info(f"Students graduating in {year}: {graduation_year_counts[year]} ({safe_percentage(graduation_year_counts[year], total_students):.2f}% of total)")
        
        logger.info(f"Students with no end_year in primary education: {no_end_year_count} ({safe_percentage(no_end_year_count, total_students):.2f}% of total)")
        logger.info(f"Students with no education records: {no_education_records_count} ({safe_percentage(no_education_records_count, total_students):.2f}% of total)")
        logger.info(f"Students with education records but no primary: {education_records_but_no_primary_count} ({safe_percentage(education_records_but_no_primary_count, total_students):.2f}% of total)")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        records_per_second = processed_records / total_time if total_time > 0 else 0
        
        logger.info(f"\n===== ANALYSIS COMPLETED =====")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        logger.info(f"Average processing speed: {records_per_second:.1f} records/second")
        logger.info(f"Ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def process_batch(batch_id, batch):
    """Process a batch of student records to collect statistics"""
    batch_start_time = time.time()
    batch_size = len(batch)
    
    logger.info(f"Starting batch {batch_id} with {batch_size} records")
    
    local_year_counts = defaultdict(int)
    local_no_primary_count = 0
    local_no_end_year_count = 0
    
    for student in batch:
        has_primary = False
        for edu_record in student.get("education_records", []):
            if edu_record.get("is_primary", False):
                has_primary = True
                end_year = edu_record.get("end_year")
                
                if end_year is not None and isinstance(end_year, int):
                    local_year_counts[end_year] += 1
                else:
                    local_no_end_year_count += 1
        
        if not has_primary and student.get("education_records"):
            local_no_primary_count += 1
    
    batch_processing_time = time.time() - batch_start_time
    
    sorted_years = sorted(local_year_counts.keys())[:5]  # Limit to first 5 years for logging
    year_stats = ", ".join([f"{year}: {local_year_counts[year]}" for year in sorted_years])
    if len(local_year_counts) > 5:
        year_stats += f", ... ({len(local_year_counts) - 5} more years)"
    
    logger.info(f"Completed batch {batch_id} in {batch_processing_time:.2f} seconds - "
               f"Years: {year_stats}, "
               f"No end_year: {local_no_end_year_count}, No Primary: {local_no_primary_count}")
    
    return {
        "batch_id": batch_id,
        "year_counts": dict(local_year_counts),
        "no_end_year_count": local_no_end_year_count,
        "no_primary_count": local_no_primary_count,
        "processing_time": batch_processing_time,
        "records_processed": batch_size
    }

if __name__ == "__main__":
    main()