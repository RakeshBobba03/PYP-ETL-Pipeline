import os
import re
import csv
import openpyxl
import requests
import html
import zipfile
from sqlalchemy.exc import SQLAlchemyError
from rapidfuzz import process, fuzz, utils
from flask import current_app
from app import db
from app.models import MemberSubmission, Member, NewItem, MatchReview

BATCH_SIZE = 1000
# Configurable thresholds
FUZZY_MATCH_THRESHOLD = float(os.getenv('FUZZY_MATCH_THRESHOLD', '80.0'))
AUTO_RESOLVE_THRESHOLD = float(os.getenv('AUTO_RESOLVE_THRESHOLD', '95.0'))  # Higher threshold for auto-resolution

# Enhanced scoring penalty configuration
LENGTH_PENALTY_MULTIPLIER = float(os.getenv('LENGTH_PENALTY_MULTIPLIER', '30.0'))  # Length difference penalty
WORD_COUNT_PENALTY_MULTIPLIER = float(os.getenv('WORD_COUNT_PENALTY_MULTIPLIER', '10.0'))  # Word count difference penalty
DIETARY_TERMS_PENALTY = float(os.getenv('DIETARY_TERMS_PENALTY', '20.0'))  # Dietary terms mismatch penalty
SPECIAL_CHARS_PENALTY = float(os.getenv('SPECIAL_CHARS_PENALTY', '15.0'))  # Special characters mismatch penalty
NUMBERS_PENALTY = float(os.getenv('NUMBERS_PENALTY', '15.0'))  # Numbers mismatch penalty
ALGORITHM_DISAGREEMENT_PENALTY = float(os.getenv('ALGORITHM_DISAGREEMENT_PENALTY', '15.0'))  # Algorithm disagreement penalty
ALGORITHM_DISAGREEMENT_THRESHOLD = float(os.getenv('ALGORITHM_DISAGREEMENT_THRESHOLD', '20.0'))  # Threshold for algorithm disagreement

def validate_excel_file(file_path):
    """Validate Excel file before processing to prevent zip file errors"""
    try:
        # Check if file exists and has content
        if not os.path.exists(file_path):
            return False, "File does not exist"
        
        if os.path.getsize(file_path) == 0:
            return False, "File is empty"
        
        # For .xlsx files, verify it's a valid zip archive
        if file_path.lower().endswith('.xlsx'):
            try:
                with zipfile.ZipFile(file_path, 'r') as zip_file:
                    # Check if it contains the expected Excel structure
                    file_list = zip_file.namelist()
                    if not any(name.startswith('xl/') for name in file_list):
                        return False, "File does not contain valid Excel structure"
            except zipfile.BadZipFile:
                return False, "File is not a valid Excel file (corrupted or wrong format)"
        
        # Try to open with openpyxl to verify it's readable
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        if not wb.active:
            return False, "Excel file has no active sheet"
        wb.close()
        
        return True, "File is valid"
    except Exception as e:
        return False, f"File validation failed: {str(e)}"

def get_fuzzy_match_threshold():
    """Get fuzzy matching threshold from environment or use default"""
    return FUZZY_MATCH_THRESHOLD

def get_auto_resolve_threshold():
    """Get auto-resolution threshold from environment or use default"""
    return AUTO_RESOLVE_THRESHOLD

def apply_match_penalties(text_sanitized, match_name, raw_score):
    """
    Apply consistent penalties to any match score based on the enhanced scoring system.
    This ensures both best matches and alternatives use the same penalty logic.
    """
    adjusted_score = raw_score
    
    # Penalty 1: Length difference penalty
    length_diff = abs(len(text_sanitized) - len(match_name))
    max_length = max(len(text_sanitized), len(match_name))
    length_penalty = (length_diff / max_length) * LENGTH_PENALTY_MULTIPLIER
    adjusted_score -= length_penalty
    
    # Penalty 2: Word count difference penalty
    text_word_count = len(text_sanitized.split())
    match_word_count = len(match_name.split())
    word_diff = abs(text_word_count - match_word_count)
    word_penalty = min(word_diff * WORD_COUNT_PENALTY_MULTIPLIER, 25)
    adjusted_score -= word_penalty
    
    # Penalty 3: Dietary terms penalty
    dietary_terms = ['gluten-free', 'organic', 'natural', 'raw', 'extra virgin', 'whole grain']
    text_has_dietary = any(term in text_sanitized.lower() for term in dietary_terms)
    match_has_dietary = any(term in match_name.lower() for term in dietary_terms)
    if text_has_dietary != match_has_dietary:
        adjusted_score -= DIETARY_TERMS_PENALTY
    
    # Penalty 4: Special characters penalty
    text_special_chars = sum(1 for c in text_sanitized if c in '!@#$%^&*()')
    match_special_chars = sum(1 for c in match_name if c in '!@#$%^&*()')
    if text_special_chars != match_special_chars:
        adjusted_score -= SPECIAL_CHARS_PENALTY
    
    # Penalty 5: Numbers penalty
    text_has_numbers = any(c.isdigit() for c in text_sanitized)
    match_has_numbers = any(c.isdigit() for c in match_name)
    if text_has_numbers != match_has_numbers:
        adjusted_score -= NUMBERS_PENALTY
    
    # Ensure score doesn't go below 0
    adjusted_score = max(0.0, adjusted_score)
    
    return adjusted_score

def sanitize_string(value):
    """Sanitize string input to prevent XSS and injection attacks"""
    if not value:
        return value
    # Remove HTML tags and escape special characters
    value = str(value).strip()
    value = re.sub(r'<[^>]+>', '', value)  # Remove HTML tags
    value = html.escape(value)  # Escape HTML entities
    return value

def validate_business_name(name):
    """Validate business name for security and data quality"""
    if not name or len(name.strip()) < 2:
        return False, "Business name must be at least 2 characters long"
    if len(name.strip()) > 200:
        return False, "Business name must be less than 200 characters"
    if re.search(r'[<>"\']', name):  # Check for dangerous characters
        return False, "Business name contains invalid characters"
    return True, None

def validate_email(email):
    """Basic email validation"""
    if not email:
        return True, None  # Email is optional
    email_pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if not re.match(email_pattern, email):
        return False, "Invalid email format"
    return True, None

def process_submission_file(filename):
    """
    Load a new submission, validate each row, skip bad ones,
    return (count, validation_errors, valid_row_indices)
    """
    # Use database transaction to prevent race conditions
    with db.session.begin():
        # Check if already processed within transaction
        existing = MemberSubmission.query.filter_by(name=filename).with_for_update().first()
        if existing:
            current_app.logger.info(f"[etl] skipping {filename}: already processed")
            return 0, [], []

        # Create submission record immediately to prevent race conditions
        sub = MemberSubmission(name=filename)
        db.session.add(sub)
        db.session.flush()  # Get the ID but don't commit yet
        
        try:
            # Process the file
            result = _process_file_content(filename, sub)
            # If we get here, processing succeeded, so commit
            return result
        except Exception as e:
            # If any error occurs, the transaction will rollback automatically
            current_app.logger.error(f"[etl] Error processing {filename}: {e}")
            raise

def _process_file_content(filename, submission):
    """Process the actual file content (separated for better error handling)"""
    data_dir = os.path.join(os.getcwd(), 'seed_data', 'new_submissions')
    fp = os.path.join(data_dir, filename)
    ext = filename.lower().rsplit('.', 1)[1]

    # Prepare reader + helper - process row by row to avoid memory issues
    if ext == 'csv':
        f = open(fp, encoding='utf-8', newline='')
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        get = lambda r, c: r.get(c)
        # Process CSV row by row
        return _process_csv_rows(reader, headers, get, submission)
    elif ext in ['xlsx', 'xls']:
        return _process_excel_file_safe(fp, filename, submission)
    else:
        raise ValueError(f"Unsupported file format: {ext}. Only .csv, .xlsx, and .xls files are supported.")

def _process_excel_file_safe(file_path, filename, submission):
    """Safely process Excel files with multiple fallback methods"""
    # First, try to validate the file
    is_valid, validation_msg = validate_excel_file(file_path)
    if not is_valid:
        raise ValueError(f"Excel file validation failed: {validation_msg}")
    
    try:
        # Method 1: Try with openpyxl (most reliable for .xlsx)
        wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
        sheet = wb.active
        if not sheet:
            raise ValueError("Excel file has no active sheet")
            
        header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
        headers = [h if h else '' for h in header_row]
        get = lambda r, c: r[headers.index(c)] if c in headers else None
        
        # Process Excel row by row
        result = _process_excel_rows(sheet, headers, get, submission)
        wb.close()
        return result
        
    except Exception as e:
        error_msg = str(e)
        
        # Provide specific guidance based on error type
        if "File is not a zip file" in error_msg or "not a zip file" in error_msg:
            raise ValueError(
                f"The file '{filename}' cannot be read as an Excel file. This usually means:\n"
                f"1. The file is corrupted\n"
                f"2. The file was saved in an unsupported format\n"
                f"3. The file extension doesn't match its actual format\n\n"
                f"Please try:\n"
                f"• Opening the file in Excel and re-saving it as .xlsx\n"
                f"• Converting it to CSV format\n"
                f"• Checking if the file is actually an Excel file"
            )
        elif "BadZipFile" in error_msg:
            raise ValueError(
                f"The file '{filename}' appears to be corrupted or not a valid Excel file. "
                f"Please try re-saving it from Excel or convert it to CSV format."
            )
        else:
            raise ValueError(f"Error reading Excel file '{filename}': {error_msg}")

def _process_csv_rows(reader, headers, get, submission):
    """Process CSV rows one by one to avoid memory issues"""
    return _process_rows_generator(reader, headers, get, submission, is_csv=True)

def _process_excel_rows(sheet, headers, get, submission):
    """Process Excel rows one by one to avoid memory issues"""
    return _process_rows_generator(sheet.iter_rows(min_row=2, values_only=True), headers, get, submission, is_csv=False)

def _process_rows_generator(rows_generator, headers, get, submission, is_csv=True):
    """Generic row processor that handles both CSV and Excel data"""
    # Must have these columns in the file
    for col in ('businessName', 'country1', 'products', 'ingredients'):
        if col not in headers:
            current_app.logger.error(f"[etl] File missing required column: {col}")
            raise ValueError(f"Missing required column: {col}")

    current_app.logger.info(f"[etl] Starting processing (columns: {headers})")

    url   = current_app.config.get('DGRAPH_URL')
    token = current_app.config.get('DGRAPH_API_TOKEN')
    
    # Handle case where Dgraph is not configured
    if not url or not token:
        current_app.logger.warning("[etl] Dgraph not configured - skipping canonical data fetch")
        data = {}
    else:
        gql = """
        query {
          products: queryProduct { title productID }
          ingredients: queryIngredients { title ingredientID }
        }
        """
        try:
            current_app.logger.info("[etl] Fetching canonical products/ingredients from Dgraph…")
            resp = requests.post(url, json={"query": gql}, headers={"Content-Type": "application/json", "Dg-Auth": token}, timeout=10)
            resp.raise_for_status()
            data = resp.json().get("data", {})
            current_app.logger.info(f"[etl] Canonical products: {len(data.get('products',[]))}, ingredients: {len(data.get('ingredients',[]))}")
        except Exception as e:
            current_app.logger.error(f"[etl] Could not fetch canonical: {e}")
            data = {}

    prod_map   = {p['title']: p['productID']   for p in data.get("products", [])}
    ing_map    = {i['title']: i['ingredientID'] for i in data.get("ingredients", [])}
    prod_lower = {t.lower(): ext for t,ext in prod_map.items()}
    ing_lower  = {t.lower(): ext for t,ext in ing_map.items()}
    prod_names = list(prod_map.keys())
    ing_names  = list(ing_map.keys())

    def is_valid(v):
        if v is None: return False
        if isinstance(v, str) and v.strip().lower() in ('', 'null', 'none', 'n/a', 'na', 'nan'):
            return False
        return True

    validation_errors = []
    valid_row_indices = []
    counter = 0

    # Process rows one by one to avoid memory issues
    for idx, row in enumerate(rows_generator, start=2):
        current_app.logger.info(f"\n[etl] Processing row {idx}…")
        
        try:
            biz     = get(row, 'businessName')
            country = get(row, 'country1')
            row_errors = []
            if not is_valid(biz):
                row_errors.append("Missing or empty businessName")
                current_app.logger.warning(f"[etl] Row {idx}: Missing or empty businessName, skipping row.")
            if not is_valid(country):
                row_errors.append("Missing or empty country1")
                current_app.logger.warning(f"[etl] Row {idx}: Missing or empty country1, skipping row.")
            if row_errors:
                validation_errors.append({'row': idx, 'error': "; ".join(row_errors)})
                continue

            # Only add valid rows to DB!
            valid_row_indices.append(idx)
            current_app.logger.info(f"[etl] Row {idx}: Creating Member for '{biz}', country: '{country}'")
            
            # Sanitize and validate all inputs
            biz_sanitized = sanitize_string(biz)
            email_sanitized = sanitize_string(get(row, 'contactEmail'))
            address_sanitized = sanitize_string(get(row, 'streetAddress1'))
            city_sanitized = sanitize_string(get(row, 'city1'))
            country_sanitized = sanitize_string(country)
            bio_sanitized = sanitize_string(get(row, 'companyBio'))
            
            # Validate business name
            is_valid_name, name_error = validate_business_name(biz_sanitized)
            if not is_valid_name:
                validation_errors.append({'row': idx, 'error': f"Business name validation failed: {name_error}"})
                continue
            
            # Validate email if provided
            is_valid_email, email_error = validate_email(email_sanitized)
            if not is_valid_email:
                validation_errors.append({'row': idx, 'error': f"Email validation failed: {email_error}"})
                continue
            
            member = Member(
                name=biz_sanitized,
                contact_email=email_sanitized or None,
                street_address1=address_sanitized or None,
                city1=city_sanitized or None,
                country1=country_sanitized,
                company_bio=bio_sanitized or None,
                submission=submission
            )
            db.session.add(member)
            db.session.flush()

            def handle(kind, cell):
                nonlocal counter
                kindstr = "Product" if kind == "product" else "Ingredient"
                current_app.logger.info(f"[etl] Row {idx}: Handling {kindstr}s for member '{biz}'…")
                if not is_valid(cell):
                    current_app.logger.info(f"[etl] Row {idx}: No {kindstr}s listed.")
                    return
                fragments = re.split(r'[;,]', str(cell))
                for raw in fragments:
                    text = raw.strip()
                    if not is_valid(text):
                        current_app.logger.info(f"[etl] Row {idx}: Skipping blank/invalid {kindstr}.")
                        continue
                    
                    # Sanitize item name
                    text_sanitized = sanitize_string(text)
                    ni = NewItem(name=text_sanitized, type=kind, member=member)
                    lower_map = prod_lower if kind == 'product' else ing_lower
                    pool      = prod_names if kind == 'product' else ing_names

                    # Exact match
                    ext_id = lower_map.get(text_sanitized.lower())
                    if ext_id:
                        ni.matched_canonical_id = ext_id
                        ni.score = 100.0
                        ni.resolved = True
                        current_app.logger.info(f"[etl] Row {idx}: '{text_sanitized}' ({kindstr}) exact matched existing canonical [{ext_id}]")
                    else:
                        # Enhanced fuzzy matching with penalty-based ranking
                        # Get all potential matches with raw scores
                        all_matches = process.extract(text_sanitized, pool, scorer=fuzz.token_set_ratio, processor=utils.default_process, limit=10)
                        
                        # Apply penalties to all matches and find the best one
                        penalized_matches = []
                        best_match = None
                        best_adjusted_score = 0.0
                        
                        for match_name, raw_score, _ in all_matches:
                            # Additional scoring algorithms for cross-validation (only for the best raw match)
                            if match_name == all_matches[0][0]:  # Only for the raw best match
                                best_ratio = process.extractOne(text_sanitized, [match_name], scorer=fuzz.ratio, processor=utils.default_process)
                                ratio_score = float(best_ratio[1]) if best_ratio else 0.0
                                
                                best_partial = process.extractOne(text_sanitized, [match_name], scorer=fuzz.partial_ratio, processor=utils.default_process)
                                partial_score = float(best_partial[1]) if best_partial else 0.0
                                
                                # Apply penalties including algorithm disagreement
                                adjusted_score = apply_match_penalties(text_sanitized, match_name, float(raw_score))
                                
                                # Additional penalty for algorithm disagreement
                                score_variance = max(abs(float(raw_score) - ratio_score), abs(float(raw_score) - partial_score))
                                if score_variance > ALGORITHM_DISAGREEMENT_THRESHOLD:
                                    adjusted_score -= ALGORITHM_DISAGREEMENT_PENALTY
                                
                                current_app.logger.info(f"[etl] Row {idx}: Raw best '{match_name}' raw score: {raw_score:.1f}%, adjusted score: {adjusted_score:.1f}%")
                            else:
                                # Apply penalties without algorithm disagreement check
                                adjusted_score = apply_match_penalties(text_sanitized, match_name, float(raw_score))
                                current_app.logger.info(f"[etl] Row {idx}: Alternative '{match_name}' raw score: {raw_score:.1f}%, adjusted score: {adjusted_score:.1f}%")
                            
                            # Ensure score doesn't go below 0
                            adjusted_score = max(0.0, adjusted_score)
                            
                            penalized_matches.append((match_name, adjusted_score))
                            
                            # Track the best adjusted score
                            if adjusted_score > best_adjusted_score:
                                best_adjusted_score = adjusted_score
                                best_match = match_name
                        
                        # Now use the best match after penalties
                        name0 = best_match
                        final_score = best_adjusted_score
                        
                        current_app.logger.info(f"[etl] Row {idx}: '{text_sanitized}' ({kindstr}) best match after penalties: '{name0}' (score {final_score:.1f}%)")
                        
                        # Use configurable threshold
                        threshold = get_fuzzy_match_threshold()
                        auto_resolve_threshold = get_auto_resolve_threshold()
                        
                        # Simplified logic using adjusted scores - the penalties already handle most edge cases
                        if final_score >= auto_resolve_threshold:
                            # High confidence - auto-resolve (penalties already applied)
                            ni.matched_canonical_id = (prod_map if kind == 'product' else ing_map)[name0]
                            ni.score = final_score
                            ni.resolved = True
                            current_app.logger.info(f"[etl] Row {idx}: '{text_sanitized}' ({kindstr}) auto-resolved with '{name0}' (score {final_score:.1f}%)")
                        elif final_score >= threshold:
                            # Medium confidence - create review but mark as suggested
                            ni.matched_canonical_id = (prod_map if kind == 'product' else ing_map)[name0]
                            ni.score = final_score
                            ni.resolved = False  # Don't auto-resolve, require review
                            current_app.logger.info(f"[etl] Row {idx}: '{text_sanitized}' ({kindstr}) suggested match '{name0}' (score {final_score:.1f}%) - requires review")
                            
                            # Create review for suggested match with alternatives
                            # Use the already calculated penalized matches as alternatives
                            alts = []
                            ext_map = prod_map if kind == 'product' else ing_map
                            
                            # Use penalized_matches but exclude the best match
                            for alt_name, alt_score in penalized_matches:
                                if alt_name != name0:  # Skip the best match
                                    alt_ext_id = ext_map.get(alt_name)
                                    alts.append({"name": alt_name, "score": alt_score, "ext_id": alt_ext_id})
                                    # Stop when we have 3 alternatives
                                    if len(alts) >= 3:
                                        break
                            
                            mr = MatchReview(
                                new_item=ni, suggested_name=name0,
                                suggested_ext_id=(prod_map if kind == 'product' else ing_map)[name0],
                                score=final_score, alternatives=alts, approved=None
                            )
                            db.session.add(mr)
                        else:
                            # Low confidence - require manual review
                            alts = []
                            ext_map = prod_map if kind == 'product' else ing_map
                            suggested_ext_id = ext_map.get(name0) if name0 else None
                            
                            # Use penalized_matches but exclude the best match
                            for alt_name, alt_score in penalized_matches:
                                if alt_name != name0:  # Skip the best match
                                    alt_ext_id = ext_map.get(alt_name)
                                    alts.append({"name": alt_name, "score": alt_score, "ext_id": alt_ext_id})
                                    # Stop when we have 3 alternatives
                                    if len(alts) >= 3:
                                        break
                                    
                            current_app.logger.info(
                                f"[etl] Row {idx}: '{text_sanitized}' ({kindstr}) no good match found (score {final_score:.1f}%), will require review. Top guess: '{name0}'."
                            )
                            mr = MatchReview(
                                new_item=ni, suggested_name=name0 or text_sanitized,
                                suggested_ext_id=suggested_ext_id,
                                score=final_score, alternatives=alts, approved=None
                            )
                            db.session.add(mr)
                    db.session.add(ni)
                    counter += 1
                    if counter % BATCH_SIZE == 0:
                        db.session.flush()  # Use flush instead of commit for better performance
                        current_app.logger.info(f"[etl] processed {counter} items…")

            handle('product', get(row, 'products'))
            handle('ingredient', get(row, 'ingredients'))
        except SQLAlchemyError as ex:
            err_msg = f"DB error: {ex}"
            current_app.logger.error(f"[etl] Row {idx}: {err_msg}")
            validation_errors.append({'row': idx, 'error': err_msg})
            continue

    current_app.logger.info(f"[etl] Finished processing → {counter} unique items; {len(validation_errors)} rows skipped")
    if validation_errors:
        for err in validation_errors:
            current_app.logger.warning(f"[etl] Validation Error Row {err['row']}: {err['error']}")
    else:
        current_app.logger.info(f"[etl] No validation errors.")
    return counter, validation_errors, valid_row_indices

def convert_excel_to_csv_suggestion(filename):
    """Provide helpful suggestion for converting problematic Excel files to CSV"""
    return (
        f"💡 **Excel File Conversion Tip**\n\n"
        f"The file '{filename}' cannot be processed. Here's how to fix it:\n\n"
        f"**Option 1: Re-save as Excel (.xlsx)**\n"
        f"1. Open the file in Microsoft Excel or Google Sheets\n"
        f"2. Go to File → Save As\n"
        f"3. Choose 'Excel Workbook (.xlsx)'\n"
        f"4. Save and try uploading again\n\n"
        f"**Option 2: Convert to CSV (Recommended)**\n"
        f"1. Open the file in Excel/Google Sheets\n"
        f"2. Go to File → Save As\n"
        f"3. Choose 'CSV (Comma delimited) (*.csv)'\n"
        f"4. Upload the CSV file instead\n\n"
        f"**Why this happens:**\n"
        f"• File corruption during download/transfer\n"
        f"• Saved in an unsupported Excel format\n"
        f"• File extension doesn't match actual format\n"
        f"• File was created by non-Excel software"
    )
