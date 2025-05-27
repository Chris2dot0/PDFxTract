# Simple app to extract info from PDF files

## Plan
1. Simple app to load a PDF file
2. Finds the correct info to extract
3. Extract the info
4. Write the info into a XLS file
5. Save the XLS file
6. Add specific data extraction based on patterns in your datasheets
7. Add error handling for corrupted PDFs
8. Add support for different output formats
9. Add a GUI interface
10. Add support for specific fields you want to extract from the datasheets

## Usage

1. First, install the required dependencies:
```bash
pip install -r requirements.txt
```

2. Place your PDF files in the Input folder

3. Run the script:
```bash
python pdf_extractor.py
```

The script will:
- Process all PDFs in the Input folder
- Create an output.xlsx file with the extracted information
- Show progress in the console