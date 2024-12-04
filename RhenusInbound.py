import PyPDF2
import pandas as pd

# Function to extract text from PDF
def extract_text_from_pdf(pdf_path):
    with open(pdf_path, 'rb') as file:
        pdf_reader = PyPDF2.PdfReader(file)
        text = ""
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            text += page.extract_text()
    return text

# Function to process text and create a summary table
def process_text_to_summary_manual(text):
    # Split the text into lines
    lines = text.split('\n')
    
    # Initialize an empty list to hold the data
    data = []
    
    # Iterate through lines and extract TAG ID, QTY, and SKU/DESCRIPTION
    for line in lines:
        if 'PALLET' in line:
            parts = line.split()
            if len(parts) >= 4 and parts[3].startswith('10'):
                try:
                    qty = int(parts[2].strip())
                    sku_description = parts[3].strip()
                    data.append((qty, sku_description))
                except ValueError as e:
                    # Print the line causing the error for debugging
                    print(f"Error processing line: {line}, {e}")
                    continue
    
    # Create a dataframe from the extracted data
    df = pd.DataFrame(data, columns=["QTY", "SKU/DESCRIPTION"])
    
    # Summarize the quantity by SKU/DESCRIPTION
    summary = df.groupby("SKU/DESCRIPTION")["QTY"].sum().reset_index()
    return summary

# Extract text from the provided PDF
pdf_path = '../Playwright_expercice/Inbound Report EARTHRATED 1501.pdf'  # Update this to your actual PDF path
pdf_text = extract_text_from_pdf(pdf_path)

# Process the text and create the summary table
summary_table_manual = process_text_to_summary_manual(pdf_text)

# Display the summary table
print(summary_table_manual)

file2_path= 'NS Data.csv' 
file2_df = pd.read_csv(file2_path)
file2_df['Quantity'] = file2_df['Quantity'].astype(int)

# Compare data frames
def compare_dataframes(df_pdf, df_csv):
    merged_df = pd.merge(df_pdf, df_csv, on='SKU/DESCRIPTION', suffixes=('_pdf', '_csv'))
    merged_df['Quantity_match'] = merged_df['QTY_pdf'] == merged_df['QTY_csv']
    mismatches = merged_df[~merged_df['Quantity_match']]
    if not mismatches.empty:
        mismatches['Quantity_difference'] = mismatches['QTY_pdf'] - mismatches['QTY_csv']
        print("Mismatches found:")
        print(mismatches[['SKU/DESCRIPTION', 'QTY_pdf', 'QTY_csv', 'Quantity_difference']])
    else:
        print("All quantities match between PDF and CSV.")