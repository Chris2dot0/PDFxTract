import os
from PyPDF2 import PdfReader
import pandas as pd

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file."""
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            text += page.extract_text()
        return text
    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {str(e)}")
        return ""

def process_pdfs(input_folder):
    """Process all PDFs in the input folder and extract information."""
    results = []
    
    # Get all PDF files from the input folder
    pdf_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.pdf')]
    
    for pdf_file in pdf_files:
        pdf_path = os.path.join(input_folder, pdf_file)
        print(f"Processing: {pdf_file}")
        
        # Extract text from PDF
        text = extract_text_from_pdf(pdf_path)
        
        # For now, we'll just store the filename and first 100 characters
        # This can be expanded later to extract specific information
        results.append({
            'Filename': pdf_file,
            'Content Preview': text[:100] + '...' if len(text) > 100 else text
        })
    
    return results

def save_to_excel(data, output_file='output.xlsx'):
    """Save the extracted data to an Excel file."""
    df = pd.DataFrame(data)
    df.to_excel(output_file, index=False)
    print(f"Data saved to {output_file}")

def main():
    input_folder = 'Input'
    
    # Check if input folder exists
    if not os.path.exists(input_folder):
        print(f"Error: {input_folder} folder not found!")
        return
    
    # Process PDFs
    results = process_pdfs(input_folder)
    
    if results:
        # Save results to Excel
        save_to_excel(results)
    else:
        print("No PDF files found to process.")

if __name__ == "__main__":
    main() 