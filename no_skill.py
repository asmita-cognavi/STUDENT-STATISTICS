import logging
import os
from pymongo import MongoClient
from datetime import datetime
import time

# Configure logging
log_dir = "logs"
os.makedirs(log_dir, exist_ok=True)
log_filename = f"{log_dir}/student_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"

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
    logger.info("===== STARTING STUDENT ANALYSIS - ZERO SKILLS =====")
    logger.info(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    try:
        # Connect to MongoDB
        connection_string = "string"
        logger.info("Connecting to MongoDB...")
        client = MongoClient(connection_string)
        db = client["db_name"]
        students_collection = db["students"]
        resumes_collection = db["resume"]
        
        # Count total students
        logger.info("Counting total students...")
        total_students = students_collection.count_documents({})
        logger.info(f"Total students found: {total_students}")
        
        # Fetch all resumes and create a lookup dictionary
        logger.info("Fetching all resumes...")
        all_resumes = list(resumes_collection.find({}, {'user_id': 1, 'link': 1}))
        resume_dict = {}
        for resume in all_resumes:
            if 'user_id' in resume and 'link' in resume:
                # Properly handle ObjectId by using str() for comparison later
                user_id = str(resume['user_id'])
                resume_dict[user_id] = resume['link']
        
        logger.info(f"Found {len(all_resumes)} resumes")
        
        # Initialize counters
        total_no_skills_with_education = 0
        total_no_skills_with_projects = 0
        total_no_skills_with_work_exp = 0
        total_no_skills_with_resume = 0
        processed_records = 0
        
        # Process in batches of 100
        BATCH_SIZE = 100
        
        logger.info(f"Processing students in batches of {BATCH_SIZE}")
        
        # Use cursor to avoid loading all records into memory
        cursor = students_collection.find({})
        batch = []
        
        for student in cursor:
            batch.append(student)
            if len(batch) >= BATCH_SIZE:
                # Process this batch
                batch_results = process_batch(batch, resume_dict)
                
                # Update counters
                total_no_skills_with_education += batch_results["no_skills_with_education"]
                total_no_skills_with_projects += batch_results["no_skills_with_projects"]
                total_no_skills_with_work_exp += batch_results["no_skills_with_work_exp"]
                total_no_skills_with_resume += batch_results["no_skills_with_resume"]
                processed_records += len(batch)
                
                # Log progress
                logger.info(f"Progress: {processed_records}/{total_students} records "
                           f"({(processed_records/total_students)*100:.2f}%) completed")
                
                # Clear batch for next iteration
                batch = []
        
        # Process remaining records
        if batch:
            batch_results = process_batch(batch, resume_dict)
            total_no_skills_with_education += batch_results["no_skills_with_education"]
            total_no_skills_with_projects += batch_results["no_skills_with_projects"]
            total_no_skills_with_work_exp += batch_results["no_skills_with_work_exp"]
            total_no_skills_with_resume += batch_results["no_skills_with_resume"]
            processed_records += len(batch)
        
        # Log final results
        logger.info("\n===== ANALYSIS RESULTS =====")
        logger.info(f"Total students: {total_students}")
        logger.info(f"Students with 0 skills but with education records: {total_no_skills_with_education}")
        logger.info(f"Students with 0 skills but with projects: {total_no_skills_with_projects}")
        logger.info(f"Students with 0 skills but with work experience: {total_no_skills_with_work_exp}")
        logger.info(f"Students with 0 skills but with resume: {total_no_skills_with_resume}")
        
        # Calculate percentages
        logger.info("\n===== PERCENTAGE BREAKDOWN =====")
        logger.info(f"0 skills but with education: {(total_no_skills_with_education/total_students)*100:.2f}% of total students")
        logger.info(f"0 skills but with projects: {(total_no_skills_with_projects/total_students)*100:.2f}% of total students")
        logger.info(f"0 skills but with work experience: {(total_no_skills_with_work_exp/total_students)*100:.2f}% of total students")
        logger.info(f"0 skills but with resume: {(total_no_skills_with_resume/total_students)*100:.2f}% of total students")
        
        end_time = time.time()
        total_time = end_time - start_time
        
        logger.info(f"\n===== ANALYSIS COMPLETED =====")
        logger.info(f"Total execution time: {total_time:.2f} seconds")
        logger.info(f"Average processing speed: {total_students/total_time:.1f} records/second")
        logger.info(f"Ended at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except Exception as e:
        logger.error(f"Error occurred: {str(e)}", exc_info=True)
    finally:
        # Close MongoDB connection
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def process_batch(students, resume_dict):
    """Process a batch of student records"""
    batch_start_time = time.time()
    batch_size = len(students)
    
    # Initialize counters for this batch
    no_skills_with_education = 0
    no_skills_with_projects = 0
    no_skills_with_work_exp = 0
    no_skills_with_resume = 0
    
    for student in students:
        # Check if student has NO skills
        has_no_skills = not student.get('skills') or len(student.get('skills', [])) == 0
        
        if has_no_skills:
            # Check education records
            if student.get('education_records') and len(student.get('education_records', [])) > 0:
                no_skills_with_education += 1
            
            # Check work experiences
            if student.get('work_experiences') and len(student.get('work_experiences', [])) > 0:
                no_skills_with_work_exp += 1
            
            # Check projects
            if student.get('projects') and len(student.get('projects', [])) > 0:
                no_skills_with_projects += 1
            
            # Check resume - use string representation of ObjectId for comparison
            student_id = str(student.get('_id'))
            if student_id in resume_dict:
                no_skills_with_resume += 1
    
    batch_processing_time = time.time() - batch_start_time
    
    logger.info(f"Processed batch of {batch_size} records in {batch_processing_time:.2f} seconds - "
               f"Found: No skills with education: {no_skills_with_education}, "
               f"No skills with projects: {no_skills_with_projects}, "
               f"No skills with work exp: {no_skills_with_work_exp}, "
               f"No skills with resume: {no_skills_with_resume}")
    
    return {
        "no_skills_with_education": no_skills_with_education,
        "no_skills_with_projects": no_skills_with_projects,
        "no_skills_with_work_exp": no_skills_with_work_exp,
        "no_skills_with_resume": no_skills_with_resume
    }

if __name__ == "__main__":
    main()