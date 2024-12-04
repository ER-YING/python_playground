

import pdfplumber
import pandas as pd

# Function to extract text using pdfplumber
def extract_text_with_pdfplumber(pdf_path):
    text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text += page.extract_text()
    return text

# Function to process text and create a summary table for each PDF
def process_text_to_summary_plumber(text):
    # Split the text into lines
    lines = text.split('\n')
    
    # Initialize an empty list to hold the data
    data = []
    
    # Iterate through lines and extract No., Description, and Quantity
    for line in lines:
        parts = line.split()
        if len(parts) >= 4 and parts[0].startswith('10'):  # Checking if No. starts with '10'
            try:
                no = parts[0].strip()
                description = ' '.join(parts[1:-2]).strip()
                # Removing commas from the quantity before conversion
                quantity = int(parts[-2].replace(',', '').strip())
                data.append((no, description, quantity))
            except ValueError as e:
                print(f"Error processing line: {line}, {e}")
                continue
    
    # Create a dataframe from the extracted data
    df = pd.DataFrame(data, columns=["No.", "Description", "Quantity"])
    
    # Summarize the Quantity by No.
    summary = df.groupby("No.")["Quantity"].sum().reset_index()
    return summary

# Function to process multiple PDFs
def process_multiple_pdfs(pdf_paths):
    combined_data = pd.DataFrame(columns=["No.", "Description", "Quantity"])
    
    for pdf_path in pdf_paths:
        
        # Extract text from each PDF
        pdf_text = extract_text_with_pdfplumber('../Playwright_expercice/'+pdf_path)
        
        # Process the text to get a summary table
        summary_table = process_text_to_summary_plumber(pdf_text)
        
        # Append to the combined dataframe
        combined_data = pd.concat([combined_data, summary_table], ignore_index=True)
    
    # Summarize across all PDFs
    combined_summary = combined_data.groupby("No.")["Quantity"].sum().reset_index()
    return combined_summary

# List of PDF file paths for the uploaded files
pdf_paths = ['Inbound Receipt ER3438.pdf','Inbound Receipt ER3461.pdf','Inbound Receipt ER3539.pdf','Inbound Receipt ER3554.pdf','Inbound Receipt ER3566.pdf']

# Process all PDFs and create a combined summary
combined_summary_table = process_multiple_pdfs(pdf_paths)

# Display the combined summary table
print(combined_summary_table)

combined_summary_table
