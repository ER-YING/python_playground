import fitz  # PyMuPDF
import pandas as pd

# Paths to the PDF files
pdf_paths = ['1.pdf','2.pdf']

# Function to extract text from a PDF file
def extract_text_from_pdf(pdf_path):
    print(f"Processing {pdf_path}")
    pdf_document = fitz.open(pdf_path)
    text = ""
    for page_num in range(len(pdf_document)):
        page = pdf_document.load_page(page_num)
        text += page.get_text("text")
    pdf_document.close()
    return text

# Function to process text and extract specific fields
def process_text_to_fields(text):
    lines = text.split('\n')
    data = {
        "Version": "",
        "Receipt DATE": "",
        "Product ID": [],
        "Total Pieces Received": []
    }
    
    Version_found = False
    Receipt_Date_found = False

    current_product_ID = None
    for line in lines:
        line = line.strip()
        if not Version_found and line.startswith("Version"):
            data["Version"] = line.split()[-1]
            Version_found = True
        elif not Receipt_Date_found and line.startswith("Receipt DATE"):
            data["Receipt DATE"] = line.split()[-1]
            Receipt_Date_found = True
        elif line.startswith("10CA") or line.startswith("10US"):
            parts = line.split(maxsplit=1)
            if len(parts) > 0:
                current_product_ID = parts[0]
                data["Product ID"].append(current_product_ID)
        elif current_product_ID and line.isdigit():
            data["Total Pieces Received"].append(line)
            current_product_ID = None
    
    return data

# Extract text from both PDFs and combine the results
all_data = {
    "Version": [],
    "Receipt DATE": [],
    "Product ID": [],
    "Total Pieces Received": []
}

for pdf_path in pdf_paths:
    text = extract_text_from_pdf('../Playwright_expercice/'+pdf_path)
    extracted_data = process_text_to_fields(text)
    
    all_data["Version"].extend([extracted_data["Version"]] * len(extracted_data["Product ID"]))
    all_data["Receipt DATE"].extend([extracted_data["Receipt DATE"]] * len(extracted_data["Product ID"]))
    all_data["Product ID"].extend(extracted_data["Product ID"])
    all_data["Total Pieces Received"].extend(extracted_data["Total Pieces Received"])

# Create a DataFrame from the combined data
df = pd.DataFrame(all_data)

# Convert Quantity columns to integers
df['Total Pieces Received'] = df['Total Pieces Received'].astype(int)

# Load the second CSV file
file2_path = 'NS Data.csv'
file2_df = pd.read_csv(file2_path)

# Strip any extra spaces from the column names
file2_df.columns = file2_df.columns.str.strip()

# Display column names for debugging
print("File 1 DataFrame columns:", df.columns)
print("File 2 DataFrame columns:", file2_df.columns)

# Merge the two dataframes on Version and RECEIVE REFERENCE, and Product ID and Product
merged_df = pd.merge(df, file2_df, left_on=['Version', 'Product ID'], right_on=['RECEIVE REFERENCE', 'Product'], suffixes=('_file1', '_file2'))

# Display merged DataFrame columns for debugging
print("Merged DataFrame columns:", merged_df.columns)

# Compare the Total Pieces Received and Quantity columns
merged_df['Quantity_match'] = merged_df['Total Pieces Received'] == merged_df['Quantity']

# Check if all quantities match
all_match = merged_df['Quantity_match'].all()

# Create a result table to show the relevant columns
result_table = merged_df[['Version', 'Product ID', 'Total Pieces Received', 'Quantity', 'Quantity_match']]
print(result_table)

# Optionally, print whether all quantities match
if all_match:
    print("All quantities match.")
else:
    # Identify mismatches and calculate quantity difference
    mismatches = merged_df[~merged_df['Quantity_match']]
    mismatches['Quantity_difference'] = mismatches['Total Pieces Received'] - mismatches['Quantity']
    result = mismatches[['Version', 'Product ID', 'Total Pieces Received', 'Quantity', 'Quantity_difference']]
    print("Mismatched quantities:")
    print(result)
