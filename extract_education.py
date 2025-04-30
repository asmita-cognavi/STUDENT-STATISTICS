# !pip install PyPDF2
import pandas as pd
import requests
from PyPDF2 import PdfReader
from io import BytesIO
import re
import os

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

def extract_highest_education(parsed_data):
    """Extract the highest education based on year"""
    degrees = parsed_data.get('degree', [])

    if not degrees:
        return None, None, None

    # Find the degree with the highest year
    highest_degree = max(degrees, key=lambda x: x.get('year', 0) if x.get('year') is not None else 0, default=None)

    if highest_degree:
        return (
            highest_degree.get('degree_name', ''),
            highest_degree.get('institution', ''),
            highest_degree.get('year', '')
        )
    return None, None, None

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
            # Add education columns if they don't exist
            for col in ['degree_name', 'institution', 'year']:
                if col not in output_df.columns:
                    output_df[col] = None
    else:
        print(f"Output file {output_csv} does not exist. Creating new dataframe.")
        # Create a copy of the input DataFrame for output
        output_df = input_df.copy()
        # Add education columns if they don't exist
        for col in ['degree_name', 'institution', 'year']:
            if col not in output_df.columns:
                output_df[col] = None

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
            if pd.notna(output_df.at[index, 'degree_name']) and pd.notna(output_df.at[index, 'institution']) and pd.notna(output_df.at[index, 'year']):
                print(f"Row {index} already has education data. Skipping.")
                continue

            # Get the row from input DataFrame
            row = input_df.iloc[index]

            # Get resume URL
            pdf_url = row.get('resume_link')

            if not pdf_url or pd.isnull(pdf_url):
                print(f"Skipping row {index} due to missing resume link")
                continue

            # Fetch and parse the resume
            parsed_data = fetch_and_parse_resume(pdf_url, api_url, api_key)

            # Extract the highest education
            degree_name, institution, year = extract_highest_education(parsed_data)

            # Update the output DataFrame
            output_df.at[index, 'degree_name'] = degree_name
            output_df.at[index, 'institution'] = institution
            output_df.at[index, 'year'] = year

            # Save after each row to avoid losing progress in case of errors
            output_df.to_csv(output_csv, index=False)
            print(f"Processed row {index} - Highest Education: {degree_name}, Year: {year}")

        except Exception as e:
            print(f"Error processing row {index}: {e}")

    # Final save to output CSV
    output_df.to_csv(output_csv, index=False)
    print(f"Processing complete. Output saved to {output_csv}")

# Parameters
input_csv = 'combined_file.csv'  # Your input CSV with resume_link column
output_csv = 'education_data.csv'  # Output file name as requested
api_url = 'https://qybjmp1bnc.execute-api.ap-south-1.amazonaws.com/default/resume-parser-v2'
api_key = 'key'

# Define your batch range here (e.g., 500-1000)
start_idx = 3500  # Starting row index (0-based)
end_idx = 4300   # Ending row index (exclusive)

# Process the CSV with specified batch range
process_csv(input_csv, output_csv, api_url, api_key, start_idx, end_idx)
