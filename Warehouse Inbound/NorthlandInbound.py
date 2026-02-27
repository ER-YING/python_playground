import PyPDF2
import pandas as pd
import re

def extract_data_from_pdf(pdf_path):
    data = {
        "Document#": [],
        "SKU": [],
        "Received": []
    }
    
    with open(pdf_path, "rb") as file:
        reader = PyPDF2.PdfReader(file)
        document_num = None  # Initialize document number
        for page_num in range(len(reader.pages)):
            page = reader.pages[page_num]
            text = page.extract_text()
            
            # Combine all text into one line to handle multi-line issues
            text = text.replace("\n", " ")

            # Debug output to inspect the text
            print(f"Page {page_num+1} Text: {text[:500]}")  # Print first 500 characters of the page text

            # Extract Document#
            if document_num is None:
                doc_match = re.search(r"ER\d{4}", text)
                if doc_match:
                    document_num = doc_match.group(0).strip()
                    print(f"Document#: {document_num}")  # Debug info

            # Extract SKUs and Received quantities
            sku_matches = re.findall(r"(\b10CA\w+\b|\b10EU\w+\b|\b10US\w+\b).+?(\d{1,3}(?:,\d{3})*\.\d+)\s+(\d{1,3}(?:,\d{3})*\.\d+)", text)
            for match in sku_matches:
                sku, _, received = match
                received = float(received.replace(',', ''))
                print(f"SKU: {sku}, Received: {received}")  # Debug info
                data["Document#"].append(document_num)
                data["SKU"].append(sku)
                data["Received"].append(received)
    
    return data

# List of PDF files
pdf_files = ["20240612_PB01_ER3250.PDF","20240612_PB01_ER3268.PDF","20240612_PB01_ER3321.PDF"]

# Aggregate data from all PDFs
aggregated_data = {
    "Document#": [],
    "SKU": [],
    "Received": []
}

for pdf_file in pdf_files:
    print(f"Processing file: {pdf_file}")  # Debug info
    pdf_data = extract_data_from_pdf('../Playwright_expercice'+pdf_file)
    for key in aggregated_data:
        aggregated_data[key].extend(pdf_data[key])

# Create DataFrame
df = pd.DataFrame(aggregated_data)

# Summarize data
summary = df.groupby(["Document#", "SKU"]).sum().reset_index()

# Display the DataFrame
print("Extracted and Summarized Data:")
print(summary)

# Save summary to CSV for further inspection
summary.to_csv("extracted_table_combined.csv", index=False)

# Load the second CSV file
file2_path = 'NS Data.csv'
file2_df = pd.read_csv(file2_path)

# Rename columns for easier comparison
file2_df.rename(columns={"RECEIVE REFERENCE": "Document#", "Product": "SKU", "Quantity": "Quantity_ns"}, inplace=True)

# Print the loaded NS Data for debugging
print("NS Data:")
print(file2_df)

# Convert Quantity column in the second CSV file to float
file2_df['Quantity_ns'] = file2_df['Quantity_ns'].astype(float)

# Merge the two dataframes on Document# and SKU
merged_df = pd.merge(summary, file2_df, on=['Document#', 'SKU'], suffixes=('_extracted', '_ns'))

# Print merged data for debugging
print("Merged Data:")
print(merged_df)

# Compare the Quantity columns
merged_df['Quantity_difference'] = merged_df['Received'] - merged_df['Quantity_ns']
merged_df['Quantity_match'] = merged_df['Quantity_difference'] == 0

# Check if all quantities match
all_match = merged_df['Quantity_match'].all()

# Print the result table
result_table = merged_df[['Document#', 'SKU', 'Received', 'Quantity_ns', 'Quantity_difference']]
print(result_table)

if all_match:
    print("100% received")

# Save mismatches to a CSV file if there are any
if not all_match:
    mismatches = merged_df[~merged_df['Quantity_match']]
    mismatches.to_csv('mismatches.csv', index=False)
