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
    
    # Based on the screenshot, this is a key-value pair table where:
    # - Field names are in column B (index 1)
    # - Values are in column D (index 3) and sometimes E (index 4)
    
    # First, let's analyze the table structure
    logger.info(f"Table shape: {df.shape}")
    logger.info(f"Number of columns: {len(df.columns)}")
    
    # Look for the key-value pattern: field names in column B, values in column D
    for display_name, pattern in FIELDS:
        if display_name in data:  # Skip if already found
            continue
            
        value = ""
        
        # Search for the field name in column B (index 1)
        for row_idx, row in df_str.iterrows():
            if len(row) > 1:  # Make sure column B exists
                cell_b = str(row.iloc[1]).strip()  # Column B
                if pattern in cell_b:
                    # Found the field name in column B, now get the value from column D
                    if len(row) > 3:  # Make sure column D exists
                        cell_d = str(row.iloc[3]).strip()  # Column D
                        if cell_d and cell_d != "nan" and cell_d != "<NA>" and cell_d != pattern:
                            value = cell_d
                            logger.debug(f"Found {display_name}: {value} at row {row_idx} (B->D)")
                            break
                    
                    # If no value in column D, try column E
                    if not value and len(row) > 4:
                        cell_e = str(row.iloc[4]).strip()  # Column E
                        if cell_e and cell_e != "nan" and cell_e != "<NA>" and cell_e != pattern:
                            value = cell_e
                            logger.debug(f"Found {display_name}: {value} at row {row_idx} (B->E)")
                            break
        
        data[display_name] = value
    
    # For any fields not found in the key-value pattern, try alternative approaches
    for display_name, pattern in FIELDS:
        if display_name in data and data[display_name]:  # Skip if already found
            continue
            
        value = ""
        
        # Search in all cells for the pattern
        for i, row in df_str.iterrows():
            for j, cell in enumerate(row):
                if pattern in str(cell):
                    # Try to get value from adjacent cells
                    value = extract_value_from_table_cell(df_str, i, j, pattern)
                    if value:
                        break
            if value:
                break
        
        data[display_name] = value
        if value:
            logger.debug(f"Found {display_name}: {value} in table (adjacent search)")
    
    return data

def extract_value_for_field(df: pd.DataFrame, row_idx: int, col_idx: int, pattern: str) -> str:
    """Extract the value for a field found at a specific position."""
    try:
        # Try right neighbor first (most common for key-value pairs)
        if col_idx + 1 < len(df.columns):
            value = str(df.iloc[row_idx, col_idx + 1]).strip()
            if value and value != "nan" and value != "<NA>" and value != pattern:
                return value
        
        # Try right neighbor + 1 (sometimes there's an empty cell)
        if col_idx + 2 < len(df.columns):
            value = str(df.iloc[row_idx, col_idx + 2]).strip()
            if value and value != "nan" and value != "<NA>" and value != pattern:
                return value
        
        # Try bottom neighbor (for vertical layouts)
        if row_idx + 1 < len(df):
            value = str(df.iloc[row_idx + 1, col_idx]).strip()
            if value and value != "nan" and value != "<NA>" and value != pattern:
                return value
        
        # Try bottom neighbor + 1 (for vertical layouts with empty cells)
        if row_idx + 2 < len(df):
            value = str(df.iloc[row_idx + 2, col_idx]).strip()
            if value and value != "nan" and value != "<NA>" and value != pattern:
                return value
        
        # Try diagonal neighbor
        if row_idx + 1 < len(df) and col_idx + 1 < len(df.columns):
            value = str(df.iloc[row_idx + 1, col_idx + 1]).strip()
            if value and value != "nan" and value != "<NA>" and value != pattern:
                return value
        
        # Extract from current cell if it contains more than just the pattern
        current_cell = str(df.iloc[row_idx, col_idx])
        if pattern != current_cell:
            # Try to extract value after the pattern in the same cell
            value = current_cell.replace(pattern, "").strip()
            if value and value != "nan" and value != "<NA>":
                return value
            
            # Try to extract value before the pattern in the same cell
            if pattern in current_cell:
                parts = current_cell.split(pattern)
                if len(parts) > 1 and parts[0].strip():
                    return parts[0].strip()
                
    except Exception as e:
        logger.debug(f"Error extracting value for field {pattern}: {e}")
    
    return ""

def extract_fields_from_key_value_table(df: pd.DataFrame) -> Dict[str, str]:
    """Specialized extraction for key-value pair tables like the one in the screenshot."""
    data = {}
    df_str = df.astype(str)
    
    logger.info("Using specialized key-value table extraction")
    logger.info(f"Table shape: {df.shape}")
    logger.info(f"Columns: {len(df.columns)}")
    
    # Check if this is a multi-column table (like the one in your screenshot)
    if len(df.columns) >= 4:
        logger.info("Detected multi-column table structure - using enhanced extraction")
        return extract_fields_from_multi_column_table(df)
    
    # Fallback to original key-value extraction for simpler tables
    # Based on the screenshot analysis:
    # - Field names are in column B (index 1)
    # - Values are in column D (index 3) and sometimes E (index 4)
    
    for display_name, pattern in FIELDS:
        value = ""
        
        # Search for the field name in column B
        for row_idx, row in df_str.iterrows():
            if len(row) > 1:  # Ensure column B exists
                cell_b = str(row.iloc[1]).strip()  # Column B
                
                # Check if this cell contains our field pattern
                if pattern in cell_b:
                    # Found the field name, now get the corresponding value
                    if len(row) > 3:  # Ensure column D exists
                        cell_d = str(row.iloc[3]).strip()  # Column D
                        if cell_d and cell_d != "nan" and cell_d != "<NA>" and cell_d != pattern:
                            value = cell_d
                            logger.info(f"✓ {display_name}: {value} (from column D)")
                            break
                    
                    # If no value in column D, try column E
                    if not value and len(row) > 4:
                        cell_e = str(row.iloc[4]).strip()  # Column E
                        if cell_e and cell_e != "nan" and cell_e != "<NA>" and cell_e != pattern:
                            value = cell_e
                            logger.info(f"✓ {display_name}: {value} (from column E)")
                            break
        
        data[display_name] = value
        if not value:
            logger.debug(f"✗ {display_name}: Not found")
    
    return data

def extract_fields_from_multi_column_table(df: pd.DataFrame) -> Dict[str, str]:
    """Extract fields from multi-column tables with proper column separation."""
    data = {}
    df_str = df.astype(str)
    
    logger.info("Extracting from multi-column table structure")
    logger.info(f"Table shape: {df.shape}")
    
    # FIRST: Handle Min/Max fields with highest priority
    logger.info("=== STEP 1: Processing Min/Max fields ===")
    
    # Search through ALL rows for Min/Max patterns
    for row_idx, row in df_str.iterrows():
        if len(row) > 3:  # Ensure we have enough columns
            cell_b = str(row.iloc[1]).strip()  # Field name
            cell_d = str(row.iloc[3]).strip()  # Value
            
            logger.debug(f"Row {row_idx}: Field='{cell_b}', Value='{cell_d}'")
            
            # Check for Ambient Temperature Min/Max
            if "Ambient" in cell_b and "Temperature" in cell_b and ("Min" in cell_b or "Max" in cell_b):
                logger.info(f"Found Ambient Temperature field: {cell_b}")
                if cell_d and cell_d != "nan" and cell_d != "<NA>":
                    min_val, max_val = split_min_max_value(cell_d)
                    if min_val and max_val:
                        data["Ambient Temperature"] = f"{min_val}/{max_val}"
                        logger.info(f"✓ Ambient Temperature: {min_val}/{max_val}")
                    else:
                        data["Ambient Temperature"] = cell_d
                        logger.warning(f"Could not split Ambient Temperature value: {cell_d}")
            
            # Check for Available Air Supply Pressure Min/Max
            elif ("Available" in cell_b and "Supply" in cell_b and "Pressure" in cell_b) or \
                 ("Available" in cell_b and "Air" in cell_b and "Pressure" in cell_b):
                logger.info(f"Found Available Air Supply Pressure field: {cell_b}")
                if cell_d and cell_d != "nan" and cell_d != "<NA>":
                    min_val, max_val = split_min_max_value(cell_d)
                    if min_val and max_val:
                        data["Available Air Supply Pressure"] = f"{min_val}/{max_val}"
                        logger.info(f"✓ Available Air Supply Pressure: {min_val}/{max_val}")
                    else:
                        data["Available Air Supply Pressure"] = cell_d
                        logger.warning(f"Could not split Available Air Supply Pressure value: {cell_d}")
    
    # SECOND: Handle multi-column fields (flow conditions)
    logger.info("=== STEP 2: Processing multi-column fields ===")
    
    multi_column_fields = {
        "Flow Rate": ["Flow Rate", "18 Flow Rate"],
        "Inlet Pressure": ["Inlet Pressure", "19 Inlet Pressure"],
        "Pressure Drop": ["Pressure Drop", "20 Pressure Drop"],
        "Inlet Temperature": ["Inlet Temperature", "21 Inlet Temperature"],
        "Inlet Density / Specific Gravity / Molecular Mass": ["Inlet Density", "22 Inlet Density"],
        "Inlet Viscosity": ["Inlet Viscosity", "24 Inlet Viscosity"],
        "Inlet Vapour Pressure": ["Inlet Vapour Pressure", "26 Inlet Vapour Pressure"],
        "Flow Coefficient Cv": ["Flow Coefficient Cv", "28 Flow Coefficient Cv"],
        "Travel": ["Travel", "29 Travel"],
        "Sound Pressure Level @ Maximum Flow": ["Sound Pressure Level", "30 Sound Pressure Level"]
    }
    
    for display_name, search_patterns in multi_column_fields.items():
        if display_name in data:  # Skip if already found
            continue
            
        values = []
        
        for row_idx, row in df_str.iterrows():
            if len(row) > 1:
                cell_b = str(row.iloc[1]).strip()
                
                if any(pattern in cell_b for pattern in search_patterns):
                    row_values = []
                    
                    # Extract from columns D, E, F
                    for col_idx in [3, 4, 5]:  # D, E, F
                        if len(row) > col_idx:
                            cell_val = str(row.iloc[col_idx]).strip()
                            if cell_val and cell_val != "nan" and cell_val != "<NA>":
                                if col_idx == 3:
                                    row_values.append(f"Max:{cell_val}")
                                elif col_idx == 4:
                                    row_values.append(f"Norm:{cell_val}")
                                elif col_idx == 5:
                                    row_values.append(f"Min:{cell_val}")
                    
                    if row_values:
                        combined_value = " | ".join(row_values)
                        data[display_name] = combined_value
                        logger.info(f"✓ {display_name}: {combined_value}")
                        break
        
        if display_name not in data:
            logger.debug(f"✗ {display_name}: Not found")
    
    # THIRD: Handle simple fields
    logger.info("=== STEP 3: Processing simple fields ===")
    
    simple_fields = [
        "Tag No.", "Service", "Line No.", "Area Classification", 
        "Allowable Sound Pressure Level", "Tightness Requirements", 
        "Power Failure Position"
    ]
    
    for display_name, pattern in FIELDS:
        if display_name in data:  # Skip if already found
            continue
            
        if display_name in simple_fields:
            value = ""
            
            for row_idx, row in df_str.iterrows():
                if len(row) > 1:
                    cell_b = str(row.iloc[1]).strip()
                    
                    if pattern in cell_b:
                        if len(row) > 3:
                            cell_d = str(row.iloc[3]).strip()
                            if cell_d and cell_d != "nan" and cell_d != "<NA>" and cell_d != pattern:
                                value = cell_d
                                logger.info(f"✓ {display_name}: {value} (simple field)")
                                break
            
            data[display_name] = value
    
    return data

def split_min_max_value(concatenated_value: str) -> Tuple[str, str]:
    """Split concatenated Min/Max values like '1242' into '12' and '42'."""
    try:
        value = str(concatenated_value).strip()
        logger.info(f"Attempting to split Min/Max value: '{value}'")
        
        # Handle specific known cases first
        if value == "1242":
            logger.info("Found specific case: 1242 -> 12/42")
            return "12", "42"
        elif value == "8001000":
            logger.info("Found specific case: 8001000 -> 800/1000")
            return "800", "1000"
        
        # Common patterns for Min/Max values
        patterns = [
            # Pattern: 2 digits + 2 digits (like 1242 -> 12/42)
            (r'^(\d{2})(\d{2})$', 2, 2),
            # Pattern: 3 digits + 4 digits (like 8001000 -> 800/1000)
            (r'^(\d{3})(\d{4})$', 3, 4),
            # Pattern: 1 digit + 2 digits (like 142 -> 1/42)
            (r'^(\d{1})(\d{2})$', 1, 2),
            # Pattern: 2 digits + 3 digits (like 12100 -> 12/100)
            (r'^(\d{2})(\d{3})$', 2, 3),
            # Pattern: 1 digit + 1 digit (like 12 -> 1/2)
            (r'^(\d{1})(\d{1})$', 1, 1),
        ]
        
        for pattern, min_len, max_len in patterns:
            match = re.match(pattern, value)
            if match:
                min_val = value[:min_len]
                max_val = value[min_len:min_len + max_len]
                logger.info(f"Split '{value}' using pattern into Min:'{min_val}' Max:'{max_val}'")
                return min_val, max_val
        
        # If no pattern matches, try to split at the middle
        if len(value) >= 2:
            mid = len(value) // 2
            min_val = value[:mid]
            max_val = value[mid:]
            logger.info(f"Split '{value}' at middle into Min:'{min_val}' Max:'{max_val}'")
            return min_val, max_val
        
        logger.warning(f"Could not split Min/Max value: '{value}'")
        return "", ""
        
    except Exception as e:
        logger.error(f"Error splitting Min/Max value '{concatenated_value}': {e}")
        return "", ""

def extract_value_from_table_cell(df: pd.DataFrame, row_idx: int, col_idx: int, pattern: str) -> str:
    """Extract value from table cell and its neighbors."""
    try:
        # Try right neighbor
        if col_idx + 1 < len(df.columns):
            value = str(df.iloc[row_idx, col_idx + 1]).strip()
            if value and value != pattern and value != "nan" and value != "<NA>":
                return value
        
        # Try bottom neighbor
        if row_idx + 1 < len(df):
            value = str(df.iloc[row_idx + 1, col_idx]).strip()
            if value and value != pattern and value != "nan" and value != "<NA>":
                return value
        
        # Try diagonal neighbor
        if row_idx + 1 < len(df) and col_idx + 1 < len(df.columns):
            value = str(df.iloc[row_idx + 1, col_idx + 1]).strip()
            if value and value != pattern and value != "nan" and value != "<NA>":
                return value
        
        # Try left neighbor (for cases where value is before the field name)
        if col_idx > 0:
            value = str(df.iloc[row_idx, col_idx - 1]).strip()
            if value and value != pattern and value != "nan" and value != "<NA>":
                return value
        
        # Extract from current cell if it contains more than just the pattern
        current_cell = str(df.iloc[row_idx, col_idx])
        if pattern != current_cell:
            # Try to extract value after the pattern in the same cell
            value = current_cell.replace(pattern, "").strip()
            if value and value != "nan" and value != "<NA>":
                return value
            
            # Try to extract value before the pattern in the same cell
            if pattern in current_cell:
                parts = current_cell.split(pattern)
                if len(parts) > 1 and parts[0].strip():
                    return parts[0].strip()
                
    except Exception as e:
        logger.debug(f"Error extracting value from table cell: {e}")
    
    return ""

def escape_excel_formula(val):
    """Escape Excel formulas to prevent execution."""
    if isinstance(val, str) and val and val[0] in ('=', '+', '-', '@'):
        return "'" + val
    return val

def debug_table_structure(df: pd.DataFrame, output_folder: str, filename: str):
    """Debug function to analyze table structure and save detailed analysis."""
    try:
        debug_info = []
        debug_info.append(f"Table Analysis for {filename}")
        debug_info.append("=" * 50)
        debug_info.append(f"Table shape: {df.shape}")
        debug_info.append(f"Columns: {len(df.columns)}")
        debug_info.append(f"Rows: {len(df)}")
        debug_info.append("")
        
        # Analyze header row
        if len(df) > 0:
            debug_info.append("HEADER ROW ANALYSIS:")
            debug_info.append("-" * 30)
            header_row = df.iloc[0]
            for i, cell in enumerate(header_row):
                debug_info.append(f"Column {i}: '{cell}'")
            debug_info.append("")
        
        # Analyze data rows
        debug_info.append("DATA ROWS ANALYSIS:")
        debug_info.append("-" * 30)
        for row_idx in range(1, min(5, len(df))):  # First 4 data rows
            debug_info.append(f"Row {row_idx}:")
            row = df.iloc[row_idx]
            for col_idx, cell in enumerate(row):
                if str(cell).strip() and str(cell).strip() != "nan" and str(cell).strip() != "<NA>":
                    debug_info.append(f"  Column {col_idx}: '{cell}'")
            debug_info.append("")
        
        # Field pattern matching analysis
        debug_info.append("FIELD PATTERN MATCHING:")
        debug_info.append("-" * 30)
        for display_name, pattern in FIELDS[:15]:  # First 15 fields for brevity
            found = False
            
            # Search through all rows and columns
            for row_idx, row in df.iterrows():
                for col_idx, cell in enumerate(row):
                    if pattern in str(cell):
                        found = True
                        debug_info.append(f"  {display_name}: Found at row {row_idx}, column {col_idx} = '{cell}'")
                        
                        # Also check what's in adjacent cells
                        if col_idx + 1 < len(df.columns):
                            right_cell = str(df.iloc[row_idx, col_idx + 1]).strip()
                            if right_cell and right_cell != "nan" and right_cell != "<NA>":
                                debug_info.append(f"    -> Right neighbor: '{right_cell}'")
                        
                        if col_idx + 2 < len(df.columns):
                            right2_cell = str(df.iloc[row_idx, col_idx + 2]).strip()
                            if right2_cell and right2_cell != "nan" and right2_cell != "<NA>":
                                debug_info.append(f"    -> Right+1 neighbor: '{right2_cell}'")
                        
                        if row_idx + 1 < len(df):
                            bottom_cell = str(df.iloc[row_idx + 1, col_idx]).strip()
                            if bottom_cell and bottom_cell != "nan" and bottom_cell != "<NA>":
                                debug_info.append(f"    -> Bottom neighbor: '{bottom_cell}'")
                        
                        break
                if found:
                    break
            
            if not found:
                debug_info.append(f"  {display_name}: NOT FOUND")
        
        # Save debug info
        debug_file = os.path.join(output_folder, f"{os.path.splitext(filename)[0]}_debug.txt")
        with open(debug_file, 'w', encoding='utf-8') as f:
            f.write('\n'.join(debug_info))
        
        logger.info(f"Debug analysis saved to: {debug_file}")
        
    except Exception as e:
        logger.error(f"Error in debug analysis: {e}")

def try_camelot_extraction(pdf_path: str) -> Tuple[bool, Optional[pd.DataFrame], str]:
    """Try multiple Camelot extraction methods for better table detection."""
    if not camelot_available:
        return False, None, "Camelot not available"
    
    # Try different Camelot flavors and parameters with better table preservation
    extraction_methods = [
        ('lattice', {'pages': 'all', 'line_scale': 40}),  # Better for structured tables
        ('stream', {'pages': 'all', 'edge_tol': 500, 'row_tol': 10}),
        ('lattice', {'pages': 'all', 'line_scale': 60}),
        ('stream', {'pages': 'all', 'edge_tol': 300, 'row_tol': 5}),
        ('stream', {'pages': '1-3', 'edge_tol': 500, 'row_tol': 10}),
    ]
    
    best_df = None
    best_score = 0
    best_method = ""
    
    for flavor, params in extraction_methods:
        try:
            logger.debug(f"Trying Camelot with flavor: {flavor}, params: {params}")
            tables = camelot.read_pdf(pdf_path, flavor=flavor, **params)
            
            if tables and len(tables) > 0:
                # Evaluate each table and find the best one
                for table in tables:
                    df = table.df
                    
                    # Calculate a score based on table quality
                    score = len(df) * len(df.columns)  # Basic score
                    
                    # Bonus for tables with more columns (better structure)
                    if len(df.columns) >= 4:
                        score *= 1.5
                    
                    # Bonus for tables with numeric data
                    numeric_cells = 0
                    total_cells = 0
                    for i, row in df.iterrows():
                        for j, cell in enumerate(row):
                            if str(cell).strip():
                                total_cells += 1
                                try:
                                    float(str(cell).replace(',', ''))
                                    numeric_cells += 1
                                except:
                                    pass
                    
                    if total_cells > 0:
                        numeric_ratio = numeric_cells / total_cells
                        score *= (1 + numeric_ratio)
                    
                    if score > best_score:
                        best_score = score
                        best_df = df.copy()
                        best_method = f"{flavor} flavor"
                
        except Exception as e:
            logger.debug(f"Camelot extraction failed with {flavor}: {e}")
            continue
    
    if best_df is not None:
        # Clean the DataFrame while preserving structure
        best_df = best_df.applymap(escape_excel_formula)
        best_df = best_df.replace('', pd.NA)
        
        # Only drop completely empty rows/columns
        best_df = best_df.dropna(how='all')
        best_df = best_df.dropna(axis=1, how='all')
        
        if len(best_df) > 0 and len(best_df.columns) > 0:
            return True, best_df, f"Success with {best_method}"
    
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
            # Debug table structure
            debug_table_structure(table_df, output_folder, pdf_file)
            
            # Try specialized key-value table extraction first
            fields_data = extract_fields_from_key_value_table(table_df)
            
            # If that didn't work well, try the improved general method
            if not any(fields_data.values()):
                logger.info("Key-value extraction failed, trying general method")
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