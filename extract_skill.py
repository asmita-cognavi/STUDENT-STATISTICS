# !pip install PyPDF2
import pandas as pd
import requests
from PyPDF2 import PdfReader
from io import BytesIO
import re
import os

def normalize_text(text):
    if isinstance(text, str):
        text = re.sub(r'[^a-zA-Z0-9\s]', '', text).lower().strip()
    return text

def fetch_and_parse_resume(pdf_url, api_url, api_key):
    try:
        # Fetch PDF
        response = requests.get(pdf_url)
        response.raise_for_status()
        pdf_content = response.content

        # Extract text using PyPDF2
        pdf_reader = PdfReader(BytesIO(pdf_content))
        pdf_text = "\n".join([page.extract_text() or '' for page in pdf_reader.pages])
        print("PDF text extracted successfully")

        # Send to resume parser API
        headers = {
            "Content-Type": "application/json",
            "x-api-key": api_key
        }
        payload = {"resume": pdf_text}
        parser_response = requests.post(api_url, headers=headers, json=payload, timeout=30)

        if parser_response.status_code == 200:
            return parser_response.json().get('res', {})
        else:
            print(f"Error: {parser_response.status_code}, {parser_response.text}")
            return {}
    except Exception as e:
        print(f"Failed to parse resume from {pdf_url}: {e}")
        return {}

def extract_skills(parsed_data):
    # Combine skills from the skills section
    skills = set(normalize_text(skill) for skill in (parsed_data.get('skill', []) or []))

    # Add technologies used in projects
    tech_skills = set(normalize_text(tech) for proj in (parsed_data.get('projects', []) or [])
                      for tech in (proj.get('technologies_used', []) or []))

    # Combine all skills
    all_skills = skills.union(tech_skills)

    # Return as comma-separated string
    return ", ".join(all_skills) if all_skills else ""

def process_csv(input_csv, output_csv, api_url, api_key, start_idx=0, end_idx=None):
    # Read input CSV
    input_df = pd.read_csv(input_csv)

    # Check if output file exists and load it if so
    if os.path.exists(output_csv):
        print(f"Output file {output_csv} exists. Loading and appending to it.")
        output_df = pd.read_csv(output_csv)

        # Verify that output_df has the same structure as input_df
        if len(output_df) != len(input_df):
            # If row counts don't match, create a new output_df from input_df
            print("Output file has different row count. Creating new output dataframe.")
            output_df = input_df.copy()
            if 'skills' not in output_df.columns:
                output_df['skills'] = None
    else:
        print(f"Output file {output_csv} does not exist. Creating new dataframe.")
        # Create a copy of the input DataFrame for output
        output_df = input_df.copy()
        # Add skills column if it doesn't exist
        if 'skills' not in output_df.columns:
            output_df['skills'] = None

    # Get the slice of DataFrame to process
    if end_idx is not None:
        slice_indices = range(start_idx, min(end_idx, len(input_df)))
    else:
        slice_indices = range(start_idx, len(input_df))

    print(f"Processing rows {start_idx} to {end_idx if end_idx is not None else len(input_df)}")

    # Process each row in the specified slice
    for index in slice_indices:
        try:
            # Check if this row has already been processed
            if pd.notna(output_df.at[index, 'skills']):
                print(f"Row {index} already has skills data. Skipping.")
                continue

            # Get the row from input DataFrame
            row = input_df.iloc[index]

            # Get PDF URL and process it
            pdf_url = row.get('resume_link')
            if not pdf_url or pd.isnull(pdf_url):
                print(f"Skipping row {index} due to missing resume link")
                continue

            # Fetch and parse the resume
            parsed_data = fetch_and_parse_resume(pdf_url, api_url, api_key)

            # Extract skills and update the output DataFrame
            skills_string = extract_skills(parsed_data)
            output_df.at[index, 'skills'] = skills_string

            # Save after each row to avoid losing progress in case of errors
            output_df.to_csv(output_csv, index=False)
            print(f"Processed row {index} and saved. Skills: {skills_string}")

        except Exception as e:
            print(f"Error processing row {index}: {e}")

    # Final save to output CSV
    output_df.to_csv(output_csv, index=False)
    print(f"Processing complete. Output saved to {output_csv}")

# Parameters
input_csv = '/content/students_zero_skills_20250422_134633.csv'  # Your input CSV with 'link' column
output_csv = 'skill_data.csv'    # Output file name as requested
api_url = 'https://qybjmp1bnc.execute-api.ap-south-1.amazonaws.com/default/resume-parser-v2'
api_key = 'V1CK8aoZJS8THvYbfkVv12x8KZKv0kPm1v8nF3J1'

# Define your batch range here (e.g., 500-1000)
start_idx = 1000  # Starting row index (0-based)
end_idx = 1300   # Ending row index (exclusive)

# Process the CSV with specified batch range
process_csv(input_csv, output_csv, api_url, api_key, start_idx, end_idx)