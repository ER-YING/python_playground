import pdfplumber
import pandas as pd
from collections import defaultdict
import re

# Path to PDF
pdf_path = "ReceiptSummary-638814255355960057.pdf"

# Initialize
normal_summary = defaultdict(float)
damaged_summary = defaultdict(float)

# Decimal pattern to match Quantity (must be like 12.00, 24.00, etc.)
quantity_pattern = re.compile(r'\b\d+\.\d{2}\b')

with pdfplumber.open(pdf_path) as pdf:
    for page in pdf.pages:
        text = page.extract_text()
        if text:
            for line in text.split('\n'):
                parts = line.strip().split()

                if len(parts) < 5:
                    continue

                product_id = parts[0]
                if len(product_id) < 10:
                    continue

                # Find first decimal number = quantity
                quantity = None
                matches = quantity_pattern.findall(line)
                if matches:
                    quantity = float(matches[0])  # FIRST decimal number = Quantity

                if quantity is None:
                    continue

                # Check if line ends with "Damaged"
                disposition = "Damaged" if line.strip().lower().endswith("damaged") else ""

                if disposition == "Damaged":
                    damaged_summary[product_id] += quantity
                else:
                    normal_summary[product_id] += quantity

# Output results
print("\n📦 Normal Products (Disposition empty):")
for product_id, total_qty in normal_summary.items():
    print(f"{product_id}: {total_qty}")

print("\n❌ Damaged Products (Disposition 'Damaged'):")
for product_id, total_qty in damaged_summary.items():
    print(f"{product_id}: {total_qty}")

# Save to CSV
normal_df = pd.DataFrame(list(normal_summary.items()), columns=["Product ID", "Normal Quantity"])
damaged_df = pd.DataFrame(list(damaged_summary.items()), columns=["Product ID", "Damaged Quantity"])

normal_df.to_csv("normal_products_summary.csv", index=False)
damaged_df.to_csv("damaged_products_summary.csv", index=False)
