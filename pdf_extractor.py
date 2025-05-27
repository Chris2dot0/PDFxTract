import os
import re
from PyPDF2 import PdfReader
import pandas as pd

# List of fields to extract: (Display Name, Search Pattern)
FIELDS = [
    ("Tag No.", "Tag No."),
    ("Service", "Service"),
    ("Line No.", "Line No."),
    ("Area Classification", "Area Classification"),
    ("Ambient Temperature", "Ambient Temperature"),
    ("Allowable Sound Pressure Level", "Allowable Sound Pressure Level"),
    ("Tightness Requirements", "Tightness Requirements"),
    ("Available Air Supply Pressure", "Available Air Supply Pressure"),
    ("Power Failure Position", "Power Failure Position"),
    ("spec_udf_c13", "spec_udf_c13"),
    ("Pipe Material", "Pipe Material"),
    ("Line Size and Schedule", "Line Size and Schedule"),
    ("Pipe Insulation", "Pipe Insulation"),
    ("Process Fluid", "Process Fluid"),
    ("Upstream Condition", "Upstream Condition"),
    ("Differential Pressure", "Differential Pressure"),
    ("Flow Rate", "Flow Rate"),
    ("Inlet Pressure", "Inlet Pressure"),
    ("Pressure Drop", "Pressure Drop"),
    ("Inlet Temperature", "Inlet Temperature"),
    ("Inlet Density / Specific Gravity / Molecular Mass", "Inlet Density / Specific Gravity / Molecular Mass"),
    ("Inlet Compressibility Factor", "Inlet Compressibility Factor"),
    ("Inlet Viscosity", "Inlet Viscosity"),
    ("Inlet Specific Heats Ratio", "Inlet Specific Heats Ratio"),
    ("Inlet Vapour Pressure", "Inlet Vapour Pressure"),
    ("spec_udf_c32", "spec_udf_c32"),
    ("Flow Coefficient Cv", "Flow Coefficient Cv"),
    ("Travel", "Travel"),
    ("Sound Pressure Level @ Maximum Flow", "Sound Pressure Level @ Maximum Flow"),
    ("MFR", "MFR"),
    ("Model", "Model"),
    ("Body Type", "Body Type"),
    ("Body Size Trim Size", "Body Size Trim Size"),
    ("Rated Cv Characteristics", "Rated Cv Characteristics"),
    ("End Connec. & Rating", "End Connec. & Rating"),
    ("Body Material", "Body Material"),
    ("Bonnet Type", "Bonnet Type"),
    ("Flow Direction", "Flow Direction"),
    ("Lubricator Isolat. Valve", "Lubricator Isolat. Valve"),
    ("Guiding No. of Ports", "Guiding No. of Ports"),
    ("Trim Type", "Trim Type"),
    ("Rated Travel", "Rated Travel"),
    ("Plug/Ball/ Disk Material", "Plug/Ball/ Disk Material"),
    ("Seat Material", "Seat Material"),
    ("Cage Stem Material", "Cage Stem Material"),
    ("Gasket Material", "Gasket Material"),
    ("spec_udf_c70", "spec_udf_c70"),
    ("MFR (Actuator)", "MFR (Actuator)"),
    ("Model (Actuator)", "Model (Actuator)"),
    ("Type", "Type"),
    ("Size", "Size"),
    ("Air Fail Valve", "Air Fail Valve"),
    ("Handwheel Location", "Handwheel Location"),
    ("Bench Range", "Bench Range"),
    ("spec_udf_c59", "spec_udf_c59"),
    ("spec_udf_c58", "spec_udf_c58"),
    ("spec_udf_c61", "spec_udf_c61"),
    ("spec_udf_c62", "spec_udf_c62"),
    ("spec_udf_c63", "spec_udf_c63"),
    ("spec_udf_c64", "spec_udf_c64"),
    ("spec_udf_c65", "spec_udf_c65"),
    ("spec_udf_c66", "spec_udf_c66"),
    ("spec_udf_c67", "spec_udf_c67"),
    ("spec_udf_c68", "spec_udf_c68"),
    ("spec_udf_c69", "spec_udf_c69"),
    ("spec_udf_c71", "spec_udf_c71"),
    ("spec_udf_c72", "spec_udf_c72"),
    ("spec_udf_c73", "spec_udf_c73"),
    ("spec_udf_c74", "spec_udf_c74"),
    ("spec_udf_c75", "spec_udf_c75"),
    ("spec_udf_c76", "spec_udf_c76"),
    ("spec_udf_c77", "spec_udf_c77"),
    ("spec_udf_c78", "spec_udf_c78"),
    ("spec_udf_c79", "spec_udf_c79"),
    ("spec_udf_c80", "spec_udf_c80"),
    ("spec_udf_c81", "spec_udf_c81"),
    ("spec_udf_c82", "spec_udf_c82"),
    ("spec_udf_c83", "spec_udf_c83"),
    ("spec_udf_c84", "spec_udf_c84"),
    ("spec_udf_c85", "spec_udf_c85"),
    ("spec_udf_c86", "spec_udf_c86"),
    ("spec_udf_c87", "spec_udf_c87"),
    ("spec_udf_c88", "spec_udf_c88"),
    ("spec_udf_c89", "spec_udf_c89"),
    ("spec_udf_c90", "spec_udf_c90"),
    ("spec_udf_c91", "spec_udf_c91"),
    ("spec_udf_c92", "spec_udf_c92"),
    ("spec_udf_c93", "spec_udf_c93"),
    ("spec_udf_c94", "spec_udf_c94"),
    ("spec_udf_c95", "spec_udf_c95"),
    ("spec_udf_c96", "spec_udf_c96"),
    ("spec_udf_c97", "spec_udf_c97"),
    ("spec_udf_c98", "spec_udf_c98"),
    ("spec_udf_c99", "spec_udf_c99"),
    ("spec_udf_c100", "spec_udf_c100"),
    ("Serial Number", "Serial Number"),
]

def extract_text_from_pdf(pdf_path):
    """Extract text from a PDF file."""
    try:
        reader = PdfReader(pdf_path)
        text = ""
        for page in reader.pages:
            page_text = page.extract_text()
            if page_text:
                text += page_text + "\n"
        return text
    except Exception as e:
        print(f"Error reading PDF {pdf_path}: {str(e)}")
        return ""

def extract_fields_from_text(text):
    data = {}
    for display_name, pattern in FIELDS:
        match = re.search(rf"{re.escape(pattern)}\s*[:\-]?\s*(.*)", text)
        if match:
            value = match.group(1).split('\n')[0].strip()
        else:
            value = ""
        data[display_name] = value
    return data

def process_pdfs(input_folder):
    results = []
    pdf_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.pdf')]
    for pdf_file in pdf_files:
        pdf_path = os.path.join(input_folder, pdf_file)
        print(f"Processing: {pdf_file}")
        text = extract_text_from_pdf(pdf_path)
        fields_data = extract_fields_from_text(text)
        fields_data['Filename'] = pdf_file
        results.append(fields_data)
    return results

def get_next_output_filename(output_folder, base_name="output", ext="xlsx"):
    os.makedirs(output_folder, exist_ok=True)
    existing = [f for f in os.listdir(output_folder) if f.startswith(base_name) and f.endswith(f'.{ext}')]
    nums = [int(re.findall(r'_(\d+)\.', f)[0]) for f in existing if re.findall(r'_(\d+)\.', f)]
    next_num = max(nums) + 1 if nums else 1
    return os.path.join(output_folder, f"{base_name}_{next_num:03d}.{ext}")

def save_to_excel(data, output_folder='Output'):
    output_file = get_next_output_filename(output_folder)
    df = pd.DataFrame(data)
    df.to_excel(output_file, index=False)
    print(f"Data saved to {output_file}")

def main():
    input_folder = 'Input'
    output_folder = 'Output'
    if not os.path.exists(input_folder):
        print(f"Error: {input_folder} folder not found!")
        return
    results = process_pdfs(input_folder)
    if results:
        save_to_excel(results, output_folder)
    else:
        print("No PDF files found to process.")

if __name__ == "__main__":
    main() 