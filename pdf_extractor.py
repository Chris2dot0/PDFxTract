import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from PyPDF2 import PdfReader
import pandas as pd
from typing import Dict, List, Tuple, Optional
import logging

# --- NEW: Import Camelot for table extraction ---
try:
    import camelot
    camelot_available = True
except ImportError:
    camelot_available = False

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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

def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract text from a PDF file with improved error handling."""
    try:
        reader = PdfReader(pdf_path)
        text = ""
        total_pages = len(reader.pages)
        
        for page_num, page in enumerate(reader.pages, 1):
            try:
                page_text = page.extract_text()
                if page_text:
                    text += f"--- PAGE {page_num} ---\n{page_text}\n"
                logger.debug(f"Extracted text from page {page_num}/{total_pages}")
            except Exception as e:
                logger.warning(f"Error extracting text from page {page_num}: {e}")
                continue
                
        return text
    except Exception as e:
        logger.error(f"Error reading PDF {pdf_path}: {str(e)}")
        return ""

def extract_text_from_pdf_chunked(pdf_path: str, chunk_size: int = 5) -> str:
    """Extract text from PDF in chunks to reduce memory usage."""
    try:
        reader = PdfReader(pdf_path)
        text = ""
        total_pages = len(reader.pages)
        
        for start_page in range(0, total_pages, chunk_size):
            end_page = min(start_page + chunk_size, total_pages)
            chunk_text = ""
            
            for page_num in range(start_page, end_page):
                try:
                    page = reader.pages[page_num]
                    page_text = page.extract_text()
                    if page_text:
                        chunk_text += f"--- PAGE {page_num + 1} ---\n{page_text}\n"
                except Exception as e:
                    logger.warning(f"Error extracting text from page {page_num + 1}: {e}")
                    continue
            
            text += chunk_text
            logger.debug(f"Processed pages {start_page + 1}-{end_page}/{total_pages}")
            
        return text
    except Exception as e:
        logger.error(f"Error reading PDF {pdf_path}: {str(e)}")
        return ""

def extract_fields_from_text_optimized(text: str) -> Dict[str, str]:
    """Optimized text-based field extraction with better pattern matching."""
    data = {}
    lines = text.splitlines()
    
    # Create a more efficient search pattern
    field_patterns = {pattern: display_name for display_name, pattern in FIELDS}
    
    # Process lines in batches for better performance
    for i, line in enumerate(lines):
        # Skip empty lines
        if not line.strip():
            continue
            
        # Try to find field patterns in current line
        for pattern, display_name in field_patterns.items():
            if display_name in data:  # Skip if already found
                continue
                
            if pattern in line:
                # Try to extract value from the same line
                value = extract_value_from_line(line, pattern)
                
                # If not found on same line, try next line
                if not value and i + 1 < len(lines):
                    value = extract_value_from_line(lines[i + 1], pattern, is_next_line=True)
                
                if value:
                    data[display_name] = value
                    logger.debug(f"Found {display_name}: {value}")
    
    return data

def extract_value_from_line(line: str, pattern: str, is_next_line: bool = False) -> str:
    """Extract value from a line containing a pattern."""
    try:
        # Try different column separators
        separators = [r"\s{2,}", r"\t+", r"\s*\|\s*", r"\s*;\s*"]
        
        for sep in separators:
            columns = re.split(sep, line)
            for j, col in enumerate(columns):
                if pattern in col:
                    # Try to get value from next column
                    if j + 1 < len(columns):
                        value = columns[j + 1].strip()
                        if value and value != pattern:
                            return value
                    
                    # If no value in next column, try the current column after the pattern
                    if pattern != col:
                        value = col.replace(pattern, "").strip()
                        if value:
                            return value
        
        # If it's a next line and no pattern found, return first non-empty value
        if is_next_line:
            for sep in separators:
                columns = re.split(sep, line)
                for col in columns:
                    value = col.strip()
                    if value and value != pattern:
                        return value
                        
    except Exception as e:
        logger.debug(f"Error extracting value from line: {e}")
    
    return ""

def extract_fields_from_table_improved(df: pd.DataFrame) -> Dict[str, str]:
    """Improved table-based field extraction with better matching."""
    data = {}
    
    # Convert DataFrame to string for easier searching
    df_str = df.astype(str)
    
    for display_name, pattern in FIELDS:
        if display_name in data:  # Skip if already found
            continue
            
        value = ""
        
        # Search in all cells
        for i, row in df_str.iterrows():
            for j, cell in enumerate(row):
                if pattern in cell:
                    # Try to get value from adjacent cells
                    value = extract_value_from_table_cell(df_str, i, j, pattern)
                    if value:
                        break
            if value:
                break
        
        data[display_name] = value
        if value:
            logger.debug(f"Found {display_name}: {value} in table")
    
    return data

def extract_value_from_table_cell(df: pd.DataFrame, row_idx: int, col_idx: int, pattern: str) -> str:
    """Extract value from table cell and its neighbors."""
    try:
        # Try right neighbor
        if col_idx + 1 < len(df.columns):
            value = str(df.iloc[row_idx, col_idx + 1]).strip()
            if value and value != pattern and value != "nan":
                return value
        
        # Try bottom neighbor
        if row_idx + 1 < len(df):
            value = str(df.iloc[row_idx + 1, col_idx]).strip()
            if value and value != pattern and value != "nan":
                return value
        
        # Try diagonal neighbor
        if row_idx + 1 < len(df) and col_idx + 1 < len(df.columns):
            value = str(df.iloc[row_idx + 1, col_idx + 1]).strip()
            if value and value != pattern and value != "nan":
                return value
        
        # Extract from current cell if it contains more than just the pattern
        current_cell = str(df.iloc[row_idx, col_idx])
        if pattern != current_cell:
            value = current_cell.replace(pattern, "").strip()
            if value and value != "nan":
                return value
                
    except Exception as e:
        logger.debug(f"Error extracting value from table cell: {e}")
    
    return ""

def escape_excel_formula(val):
    """Escape Excel formulas to prevent execution."""
    if isinstance(val, str) and val and val[0] in ('=', '+', '-', '@'):
        return "'" + val
    return val

def try_camelot_extraction(pdf_path: str) -> Tuple[bool, Optional[pd.DataFrame], str]:
    """Try multiple Camelot extraction methods for better table detection."""
    if not camelot_available:
        return False, None, "Camelot not available"
    
    # Try different Camelot flavors and parameters
    extraction_methods = [
        ('stream', {'pages': 'all', 'edge_tol': 500}),
        ('lattice', {'pages': 'all'}),
        ('stream', {'pages': 'all', 'edge_tol': 300}),
        ('stream', {'pages': '1-3', 'edge_tol': 500}),
    ]
    
    for flavor, params in extraction_methods:
        try:
            logger.debug(f"Trying Camelot with flavor: {flavor}, params: {params}")
            tables = camelot.read_pdf(pdf_path, flavor=flavor, **params)
            
            if tables and len(tables) > 0:
                # Find the best table (most rows/columns)
                best_table = max(tables, key=lambda t: len(t.df) * len(t.df.columns))
                df = best_table.df
                
                # Clean the DataFrame
                df = df.applymap(escape_excel_formula)
                df = df.replace('', pd.NA).dropna(how='all').dropna(axis=1, how='all')
                
                if len(df) > 0 and len(df.columns) > 0:
                    return True, df, f"Success with {flavor} flavor"
                    
        except Exception as e:
            logger.debug(f"Camelot extraction failed with {flavor}: {e}")
            continue
    
    return False, None, "All Camelot methods failed"

def process_single_pdf(pdf_path: str, output_folder: str) -> Dict[str, any]:
    """Process a single PDF file with comprehensive extraction methods."""
    pdf_file = os.path.basename(pdf_path)
    result = {
        'filename': pdf_file,
        'success': False,
        'method': 'none',
        'output_file': '',
        'error': '',
        'extraction_time': 0
    }
    
    start_time = time.time()
    
    try:
        logger.info(f"Processing: {pdf_file}")
        
        # Try Camelot table extraction first
        camelot_success, table_df, camelot_message = try_camelot_extraction(pdf_path)
        
        if camelot_success and table_df is not None:
            # Extract fields from table
            fields_data = extract_fields_from_table_improved(table_df)
            fields_data['Filename'] = pdf_file
            
            # Save both table and extracted fields
            base_name = os.path.splitext(pdf_file)[0]
            
            # Save full table
            table_output = os.path.join(output_folder, f"{base_name}_table.xlsx")
            table_df.to_excel(table_output, index=False, header=False)
            
            # Save extracted fields
            fields_df = pd.DataFrame([fields_data])
            fields_output = os.path.join(output_folder, f"{base_name}_fields.xlsx")
            fields_df.to_excel(fields_output, index=False)
            
            result.update({
                'success': True,
                'method': 'camelot',
                'output_file': fields_output,
                'table_file': table_output,
                'fields_found': len([v for v in fields_data.values() if v]),
                'total_fields': len(FIELDS)
            })
            
            logger.info(f"[Camelot] Successfully processed {pdf_file} - Found {result['fields_found']}/{result['total_fields']} fields")
            
        else:
            # Fallback to text extraction
            logger.info(f"[Fallback] Using text extraction for {pdf_file}")
            text = extract_text_from_pdf_chunked(pdf_path)
            
            if text:
                fields_data = extract_fields_from_text_optimized(text)
                fields_data['Filename'] = pdf_file
                
                df = pd.DataFrame([fields_data])
                output_file = os.path.join(output_folder, f"{os.path.splitext(pdf_file)[0]}_fields.xlsx")
                df.to_excel(output_file, index=False)
                
                result.update({
                    'success': True,
                    'method': 'text',
                    'output_file': output_file,
                    'fields_found': len([v for v in fields_data.values() if v]),
                    'total_fields': len(FIELDS)
                })
                
                logger.info(f"[Text] Successfully processed {pdf_file} - Found {result['fields_found']}/{result['total_fields']} fields")
            else:
                result['error'] = 'No text extracted from PDF'
                logger.error(f"No text extracted from {pdf_file}")
                
    except Exception as e:
        result['error'] = str(e)
        logger.error(f"Error processing {pdf_file}: {e}")
    
    result['extraction_time'] = time.time() - start_time
    return result

def process_pdfs_parallel(input_folder: str, output_folder: str = 'Output', max_workers: int = 4) -> List[Dict]:
    """Process multiple PDFs in parallel for improved speed."""
    os.makedirs(output_folder, exist_ok=True)
    pdf_files = [f for f in os.listdir(input_folder) if f.lower().endswith('.pdf')]
    
    if not pdf_files:
        logger.warning(f"No PDF files found in {input_folder}")
        return []
    
    logger.info(f"Found {len(pdf_files)} PDF files to process")
    results = []
    
    # Process files in parallel
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_file = {
            executor.submit(process_single_pdf, os.path.join(input_folder, pdf_file), output_folder): pdf_file
            for pdf_file in pdf_files
        }
        
        # Collect results as they complete
        for i, future in enumerate(as_completed(future_to_file), 1):
            pdf_file = future_to_file[future]
            try:
                result = future.result()
                results.append(result)
                logger.info(f"Completed {i}/{len(pdf_files)}: {pdf_file}")
            except Exception as e:
                logger.error(f"Error processing {pdf_file}: {e}")
                results.append({
                    'filename': pdf_file,
                    'success': False,
                    'error': str(e),
                    'extraction_time': 0
                })
    
    return results

def generate_summary_report(results: List[Dict], output_folder: str):
    """Generate a summary report of the extraction process."""
    if not results:
        return
    
    total_files = len(results)
    successful_files = len([r for r in results if r['success']])
    failed_files = total_files - successful_files
    
    # Calculate statistics
    total_time = sum(r.get('extraction_time', 0) for r in results)
    avg_time = total_time / total_files if total_files > 0 else 0
    
    camelot_success = len([r for r in results if r.get('method') == 'camelot'])
    text_success = len([r for r in results if r.get('method') == 'text'])
    
    # Create summary DataFrame
    summary_data = []
    for result in results:
        summary_data.append({
            'Filename': result['filename'],
            'Success': result['success'],
            'Method': result.get('method', 'none'),
            'Fields Found': result.get('fields_found', 0),
            'Total Fields': result.get('total_fields', len(FIELDS)),
            'Success Rate (%)': round((result.get('fields_found', 0) / len(FIELDS)) * 100, 1) if result['success'] else 0,
            'Processing Time (s)': round(result.get('extraction_time', 0), 2),
            'Error': result.get('error', '')
        })
    
    summary_df = pd.DataFrame(summary_data)
    
    # Save summary report
    summary_file = os.path.join(output_folder, 'extraction_summary.xlsx')
    summary_df.to_excel(summary_file, index=False)
    
    # Print summary
    logger.info("\n" + "="*60)
    logger.info("EXTRACTION SUMMARY")
    logger.info("="*60)
    logger.info(f"Total files processed: {total_files}")
    logger.info(f"Successful extractions: {successful_files}")
    logger.info(f"Failed extractions: {failed_files}")
    logger.info(f"Success rate: {(successful_files/total_files)*100:.1f}%")
    logger.info(f"Average processing time: {avg_time:.2f} seconds")
    logger.info(f"Camelot extractions: {camelot_success}")
    logger.info(f"Text extractions: {text_success}")
    logger.info(f"Summary saved to: {summary_file}")
    logger.info("="*60)

def main():
    input_folder = 'Input'
    output_folder = 'Output'
    
    if not os.path.exists(input_folder):
        logger.error(f"Error: {input_folder} folder not found!")
        return
    
    # Process PDFs with parallel processing
    results = process_pdfs_parallel(input_folder, output_folder, max_workers=4)
    
    # Generate summary report
    generate_summary_report(results, output_folder)

if __name__ == "__main__":
    main() 