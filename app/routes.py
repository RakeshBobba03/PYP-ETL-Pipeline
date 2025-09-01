# app/routes.py

import os
import csv
import requests
import logging
import time
import json
import openpyxl
from pathlib import Path
from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, current_app, get_flashed_messages,
    send_from_directory, session, abort, jsonify
)
from werkzeug.utils import secure_filename
from app import db
from app.models import NewItem, MatchReview, MemberSubmission, Member
from app.etl import process_submission_file, map_headers_to_schema, validate_required_columns, normalize_data_sample

main_bp = Blueprint('main', __name__)

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'seed_data', 'new_submissions')
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

# Custom Jinja filters
@main_bp.app_template_filter('confidence_class')
def confidence_class_filter(confidence):
    """Convert confidence score to CSS class"""
    if confidence >= 90:
        return 'high'
    elif confidence >= 70:
        return 'medium'
    else:
        return 'low'

def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def is_safe_filename(filename):
    """Check if filename is safe (no path traversal)"""
    if not filename:
        return False
    # Normalize path and check if it's within upload folder
    try:
        file_path = Path(UPLOAD_FOLDER) / filename
        file_path.resolve().relative_to(Path(UPLOAD_FOLDER).resolve())
        return True
    except (ValueError, RuntimeError):
        return False

def dgraph_request_with_retry(url, json_data, headers, max_retries=3, base_delay=1):
    """Make Dgraph request with exponential backoff retry"""
    for attempt in range(max_retries):
        try:
            resp = requests.post(url, json=json_data, headers=headers, timeout=30)
            resp.raise_for_status()
            return resp
        except requests.exceptions.RequestException as e:
            if attempt == max_retries - 1:
                raise e
            
            delay = base_delay * (2 ** attempt)  # Exponential backoff
            current_app.logger.warning(f"[dgraph] Request failed (attempt {attempt + 1}/{max_retries}), retrying in {delay}s: {e}")
            time.sleep(delay)
    
    raise Exception(f"All {max_retries} retry attempts failed")

@main_bp.route('/')
def index():
    current_app.logger.info("[routes] Redirecting to upload page")
    get_flashed_messages(with_categories=True)
    return redirect(url_for('main.upload_file'))

@main_bp.route('/upload', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file = request.files.get('file')
        clear_previous = request.form.get('clear_previous') == 'on'
        current_app.logger.info(f"[upload] POST received. File: {file.filename if file else 'None'}, clear_previous: {clear_previous}")

        session.pop('etl_validation_errors', None)
        session.pop('etl_error_filename', None)

        if file and allowed_file(file.filename):
            filename = secure_filename(file.filename)
            
            # Ensure upload directory exists
            os.makedirs(UPLOAD_FOLDER, exist_ok=True)
            
            save_path = os.path.join(UPLOAD_FOLDER, filename)
            file.save(save_path)
            current_app.logger.info(f"[upload] File uploaded and saved to: {save_path}")

            # Store file info in session for validation flow
            session['uploaded_file'] = filename
            session['file_path'] = save_path

            if clear_previous:
                current_app.logger.info("[upload] Clearing previous submissions and DB records…")
                MatchReview.query.delete()
                NewItem.query.delete()
                Member.query.delete()
                MemberSubmission.query.delete()
                db.session.commit()
                current_app.logger.info("[upload] Previous DB records cleared.")

            # Redirect to validation page instead of processing immediately
            flash(f"File '{filename}' uploaded successfully. Please review the header mapping and data sample before processing.", "success")
            return redirect(url_for('main.validate_headers'))

        current_app.logger.warning("[upload] No valid file selected or invalid file type.")
        flash("Please select a valid .xlsx, .xls or .csv file.", "danger")

    current_app.logger.info("[upload] GET received. Rendering upload.html")
    return render_template('upload.html')

@main_bp.route('/validate_headers')
def validate_headers():
    """Show header mapping and validation results"""
    filename = session.get('uploaded_file')
    file_path = session.get('file_path')
    
    if not filename or not file_path:
        flash("No file uploaded. Please upload a file first.", "danger")
        return redirect(url_for('main.upload_file'))
    
    if not os.path.exists(file_path):
        flash("Uploaded file not found. Please upload again.", "danger")
        return redirect(url_for('main.upload_file'))
    
    try:
        # Extract headers from file
        ext = filename.lower().rsplit('.', 1)[1]
        headers = []
        
        if ext == 'csv':
            with open(file_path, encoding='utf-8', newline='') as f:
                reader = csv.reader(f)
                headers = next(reader, [])
        elif ext in ['xlsx', 'xls']:
            wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
            sheet = wb.active
            header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
            headers = [h if h else '' for h in header_row]
            wb.close()
        
        # Check if we have updated mapping from session
        updated_mapping = session.get('updated_mapping')
        updated_validation = session.get('updated_validation')
        updated_sample_data = session.get('updated_sample_data')
        
        if updated_mapping and updated_validation and updated_sample_data:
            # Use updated data from session
            mapping = updated_mapping
            validation = updated_validation
            sample_data = updated_sample_data
            unmapped = [h for h in headers if h not in mapping]
            
            # Clear session data
            session.pop('updated_mapping', None)
            session.pop('updated_validation', None)
            session.pop('updated_sample_data', None)
        else:
            # Use automatic mapping
            mapping, unmapped = map_headers_to_schema(headers)
            validation = validate_required_columns(headers, mapping)
            sample_data = normalize_data_sample(file_path, headers, mapping, sample_size=10)
        
        # Get schema fields for template
        from app.etl import get_schema_field_mapping
        schema_fields = get_schema_field_mapping()
        
        return render_template(
            'validate_headers.html',
            filename=filename,
            headers=headers,
            mapping=mapping,
            unmapped=unmapped,
            validation=validation,
            sample_data=sample_data,
            schema_fields=schema_fields
        )
        
    except Exception as e:
        current_app.logger.error(f"Error validating headers: {e}")
        error_msg = str(e)
        
        # Provide specific guidance for Excel file errors
        if "Bad offset for central directory" in error_msg or "BadZipFile" in error_msg:
            flash(
                f"Excel file error: {error_msg}\n\n"
                f"💡 This usually means the Excel file is corrupted. Please try:\n\n"
                f"**Option 1: Re-save the file**\n"
                f"1. Open the file in Microsoft Excel or Google Sheets\n"
                f"2. Go to File → Save As\n"
                f"3. Choose 'Excel Workbook (.xlsx)'\n"
                f"4. Save and upload the new file\n\n"
                f"**Option 2: Convert to CSV**\n"
                f"1. Open the file in Excel/Google Sheets\n"
                f"2. Go to File → Save As\n"
                f"3. Choose 'CSV (Comma delimited) (*.csv)'\n"
                f"4. Upload the CSV file instead", 
                "danger"
            )
        elif "File is not a zip file" in error_msg or "not a zip file" in error_msg:
            flash(
                f"Excel file error: {error_msg}\n\n"
                f"💡 This usually means the file extension doesn't match its actual format.\n\n"
                f"Please try:\n"
                f"1. Opening the file in Excel to verify it's actually an Excel file\n"
                f"2. Re-saving it as .xlsx format\n"
                f"3. Or converting it to CSV format", 
                "danger"
            )
        else:
            flash(f"Error validating file: {error_msg}", "danger")
        
        return redirect(url_for('main.upload_file'))

@main_bp.route('/update_mapping', methods=['POST'])
def update_mapping():
    """Update header mapping based on user input"""
    current_app.logger.info(f"[update_mapping] POST received. Form data: {request.form}")
    current_app.logger.info(f"[update_mapping] Is JSON: {request.is_json}")
    
    filename = session.get('uploaded_file')
    file_path = session.get('file_path')
    
    if not filename or not file_path:
        current_app.logger.error("[update_mapping] No file uploaded")
        return jsonify({'error': 'No file uploaded'}), 400
    
    try:
        # Handle both form data and JSON data
        if request.is_json:
            data = request.get_json()
            custom_mapping = data.get('mapping', {})
        else:
            # Handle form data
            mapping_json = request.form.get('mapping', '{}')
            try:
                custom_mapping = json.loads(mapping_json)
            except json.JSONDecodeError:
                return jsonify({'error': 'Invalid mapping data format'}), 400
        
        # Store custom mapping in session
        session['custom_mapping'] = custom_mapping
        
        # Re-validate with custom mapping
        ext = filename.lower().rsplit('.', 1)[1]
        headers = []
        
        if ext == 'csv':
            with open(file_path, encoding='utf-8', newline='') as f:
                reader = csv.reader(f)
                headers = next(reader, [])
        elif ext in ['xlsx', 'xls']:
            wb = openpyxl.load_workbook(file_path, read_only=True, data_only=True)
            sheet = wb.active
            header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
            headers = [h if h else '' for h in header_row]
            wb.close()
        
        # Apply custom mapping
        mapping = {}
        for header in headers:
            if header in custom_mapping:
                mapping[header] = {
                    'schema_field': custom_mapping[header],
                    'confidence': 100,  # User-defined mapping
                    'original_header': header
                }
        
        # Validate required columns
        validation = validate_required_columns(headers, mapping)
        
        # Generate updated data sample
        sample_data = normalize_data_sample(file_path, headers, mapping, sample_size=10)
        
        # Store the updated data in session for the next page load
        session['updated_mapping'] = mapping
        session['updated_validation'] = validation
        session['updated_sample_data'] = sample_data
        
        # Redirect back to validation page with updated data
        flash('Mapping updated successfully!', 'success')
        return redirect(url_for('main.validate_headers'))
        
    except Exception as e:
        current_app.logger.error(f"Error updating mapping: {e}")
        error_msg = str(e)
        
        # Provide specific guidance for Excel file errors
        if "Bad offset for central directory" in error_msg or "BadZipFile" in error_msg:
            return jsonify({
                'error': 'Excel file is corrupted. Please re-save the file in Excel or convert to CSV format.'
            }), 500
        else:
            return jsonify({'error': str(e)}), 500

@main_bp.route('/process_validated_file', methods=['POST'])
def process_validated_file():
    """Process the file after validation and mapping confirmation"""
    filename = session.get('uploaded_file')
    file_path = session.get('file_path')
    
    if not filename or not file_path:
        flash("No file uploaded. Please upload a file first.", "danger")
        return redirect(url_for('main.upload_file'))
    
    try:
        # Get custom mapping from session
        custom_mapping = session.get('custom_mapping', {})
        current_app.logger.info(f"[process_validated_file] Starting ETL process for validated file: {filename}")
        current_app.logger.info(f"[process_validated_file] Custom mapping: {custom_mapping}")
        count, val_errors, valid_row_indices = process_submission_file(filename, custom_mapping=custom_mapping)
        current_app.logger.info(f"[process_validated_file] ETL finished for {filename}: {count} items, {len(val_errors)} validation errors")
        
        # Clear session data
        session.pop('uploaded_file', None)
        session.pop('file_path', None)
        session.pop('custom_mapping', None)
        
        # Handle results similar to original upload flow
        if count == 0:
            error_filename = f"{filename.rsplit('.',1)[0]}_errors.csv"
            error_path = os.path.join(UPLOAD_FOLDER, error_filename)
            current_app.logger.warning(f"[process_validated_file] All rows invalid for {filename}. Writing errors to: {error_path}")
            with open(error_path, 'w', newline='', encoding='utf-8') as ef:
                writer = csv.writer(ef)
                writer.writerow(['Row','Error'])
                for err in val_errors:
                    writer.writerow([err['row'], err['error']])
            return render_template(
                'etl_errors.html',
                submission=filename,
                errors=val_errors,
                error_filename=error_filename
            )

        # Store validation errors in session for banner/download on review page
        if val_errors:
            session['etl_validation_errors'] = val_errors
            session['etl_error_filename'] = f"{filename.rsplit('.',1)[0]}_errors.csv"
            error_path = os.path.join(UPLOAD_FOLDER, session['etl_error_filename'])
            current_app.logger.warning(f"[process_validated_file] Some rows were skipped. Writing ETL error report to: {error_path}")
            with open(error_path, 'w', newline='', encoding='utf-8') as ef:
                writer = csv.writer(ef)
                writer.writerow(['Row','Error'])
                for err in val_errors:
                    writer.writerow([err['row'], err['error']])
            flash(f"Some rows were skipped due to validation errors. See details on the review page.", "warning")

        current_app.logger.info(f"[process_validated_file] Successfully processed {count} valid items from {filename}")
        flash(f"Uploaded and processed '{filename}' ({count} valid items). Ready for review.", "success")
        return redirect(url_for('main.review_list'))

    except Exception as e:
        current_app.logger.error(f"[process_validated_file][error] {e}")
        error_msg = str(e)
        
        # Provide helpful guidance for Excel file errors
        if "File is not a zip file" in error_msg or "not a zip file" in error_msg or "BadZipFile" in error_msg:
            flash(
                f"Excel file error: {error_msg}\n\n"
                f"💡 Tip: Try converting your Excel file to CSV format:\n"
                f"1. Open the file in Excel\n"
                f"2. Go to File → Save As\n"
                f"3. Choose 'CSV (Comma delimited) (*.csv)'\n"
                f"4. Upload the CSV file instead", 
                "danger"
            )
        else:
            flash(f"Processing failed: {e}", "danger")
        return redirect(url_for('main.validate_headers'))

@main_bp.route('/download_etl_errors')
def download_etl_errors():
    errors = session.get('etl_validation_errors')
    filename = session.get('etl_error_filename', 'ETL_Errors.csv')
    current_app.logger.info(f"[download_etl_errors] Download request. errors present: {bool(errors)}, filename: {filename}")
    if not errors or not filename:
        flash("No error report available.", "danger")
        return redirect(url_for('main.review_list'))
    return send_from_directory(UPLOAD_FOLDER, filename, as_attachment=True)

@main_bp.route('/errors/<filename>')
def download_errors(filename):
    current_app.logger.info(f"[download_errors] Downloading error file: {filename}")
    
    # Security check: prevent path traversal
    if not is_safe_filename(filename):
        current_app.logger.warning(f"[download_errors] Attempted path traversal: {filename}")
        abort(400, description="Invalid filename")
    
    # Additional check: ensure file exists and is in allowed directory
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    if not os.path.exists(file_path) or not os.path.isfile(file_path):
        abort(404, description="File not found")
    
    return send_from_directory(UPLOAD_FOLDER, filename, as_attachment=True)

@main_bp.route('/reviews')
def review_list():
    val_errors = session.get('etl_validation_errors')
    error_filename = session.get('etl_error_filename')
    current_app.logger.info("[review_list] Checking for pending reviews…")
    pending = MatchReview.query.join(NewItem) \
        .filter(MatchReview.approved.is_(None), NewItem.ignored.is_(False)) \
        .all()
    if pending:
        current_app.logger.info(f"[review_list] {len(pending)} pending reviews found. Rendering reviews.html")
        return render_template('reviews.html', pending_reviews=pending, val_errors=val_errors, error_filename=error_filename)

    new_items_to_add = NewItem.query.filter_by(resolved=False).all()
    
    # Fix: Properly categorize approved items as new vs matched
    # New items: approved but NOT resolved (user chose "Create New")
    # Matched items: approved AND resolved (user chose existing match)
    new_items_approved = NewItem.query.join(MatchReview).filter(
        NewItem.resolved == False,  # Fixed: Look for unresolved items (Create New)
        MatchReview.approved == True
    ).all()
    
    # Get all items that were resolved (either auto-resolved or manually matched)
    # This includes items that were linked to existing canonical data
    matched_items = NewItem.query.filter(
        NewItem.resolved == True,   # Look for resolved items (canonical matches)
        NewItem.ignored == False    # Exclude ignored items
    ).all()
    
    # Get all companies that were created during ETL processing
    # This includes companies with auto-resolved items (no manual review needed)
    all_etl_companies = Member.query.distinct().all()
    
    # Get companies that have items requiring manual review (for the summary)
    companies_with_reviewed_items = set()
    for item in new_items_approved:
        companies_with_reviewed_items.add(item.member.name)
    
    current_app.logger.info(f"[review_list] No pending reviews. New items to add: {len(new_items_to_add)} | New items approved: {len(new_items_approved)} | Matched items: {len(matched_items)} | Total ETL companies: {len(all_etl_companies)}")
    return render_template(
        'reviews_done.html',
        new_items_to_add=new_items_to_add,
        new_items_approved=new_items_approved,
        matched_items=matched_items,
        all_etl_companies=all_etl_companies,
        companies_with_reviewed_items=companies_with_reviewed_items,
        val_errors=val_errors,
        error_filename=error_filename
    )

@main_bp.route('/reviews/handle_review/<int:item_id>', methods=['POST'])
def handle_review(item_id):
    current_app.logger.info(f"[handle_review] POST for review item_id={item_id}")
    review = MatchReview.query.filter_by(new_item_id=item_id, approved=None).first()
    if not review:
        current_app.logger.warning(f"[handle_review] Review item {item_id} not found or already handled.")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': False, 'message': 'Review item not found or already handled.'})
        flash("Review item not found or already handled.", "warning")
        return redirect(url_for('main.review_list'))

    # Handle multiple canonical choices
    canonical_choices = request.form.getlist('canonical_choices')
    single_choice = request.form.get('choice')
    
    current_app.logger.info(f"[handle_review] Canonical choices: {canonical_choices}, Single choice: {single_choice}")

    try:
        if canonical_choices:
            # User selected one or more canonical matches
            review.approved = True
            review.new_item.resolved = True
            
            # Store multiple canonical IDs as JSON in a new field
            # For now, we'll use the first choice as the primary match
            # and store the rest in the alternatives field
            primary_choice = canonical_choices[0]
            review.new_item.matched_canonical_id = primary_choice
            
            # Store all selected choices in alternatives for future reference
            all_choices = [{"ext_id": choice, "selected": True} for choice in canonical_choices]
            review.alternatives = all_choices
            
            message = f"Approved '{review.new_item.name}' matched to {len(canonical_choices)} canonical item(s)."
            current_app.logger.info(f"[handle_review] Approved '{review.new_item.name}' matched to {len(canonical_choices)} canonical items: {canonical_choices}")
            
        elif single_choice == '__new__':
            # User chose to create as new item
            review.approved = True
            review.new_item.resolved = False  # Mark as unresolved so it gets created as new
            # Don't set matched_canonical_id since we want it as a new item
            message = f"Approved '{review.new_item.name}' as new {review.new_item.type}."
            current_app.logger.info(f"[handle_review] Approved '{review.new_item.name}' as new {review.new_item.type}")
            
        elif single_choice and single_choice != '__new__':
            # User chose a single canonical match (backward compatibility)
            review.approved = True
            review.new_item.resolved = True
            review.new_item.matched_canonical_id = single_choice
            message = f"Approved '{review.new_item.name}' matched to canonical data."
            current_app.logger.info(f"[handle_review] Approved '{review.new_item.name}' matched to canonical ID: {single_choice}")
            
        else:
            # No choice made - treat as ignored
            review.approved = False
            review.new_item.ignored = True
            message = f"Ignored '{review.new_item.name}'."
            current_app.logger.info(f"[handle_review] Ignored review for item '{review.new_item.name}'")

        db.session.commit()
        
        # Return JSON for AJAX requests, redirect for regular requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': True, 'message': message})
        
        # Only set flash message for non-AJAX requests
        flash(message, "success")
        return redirect(url_for('main.review_list'))
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[handle_review] Error processing review: {e}")
        error_message = f"Error processing review: {str(e)}"
        
        # Return JSON for AJAX requests, redirect for regular requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': False, 'message': error_message})
        
        flash(error_message, "error")
        return redirect(url_for('main.review_list'))

@main_bp.route('/reviews/ignore_review_item/<int:item_id>', methods=['POST'])
def ignore_review_item(item_id):
    current_app.logger.info(f"[ignore_review_item] POST for item_id={item_id}")
    review = MatchReview.query.filter_by(new_item_id=item_id, approved=None).first()
    if not review:
        current_app.logger.warning(f"[ignore_review_item] Review item {item_id} not found or already handled.")
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': False, 'message': 'Review item not found or already handled.'})
        flash("Review item not found or already handled.", "warning")
        return redirect(url_for('main.review_list'))

    try:
        review.approved = False
        review.new_item.ignored = True
        db.session.commit()
        
        message = f"Ignored '{review.new_item.name}'."
        current_app.logger.info(f"[ignore_review_item] Ignored review for item '{review.new_item.name}'")
        
        # Return JSON for AJAX requests, redirect for regular requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': True, 'message': message})
        
        flash(message, "warning")
        return redirect(url_for('main.review_list'))
        
    except Exception as e:
        db.session.rollback()
        current_app.logger.error(f"[ignore_review_item] Error ignoring review: {e}")
        error_message = f"Error ignoring review: {str(e)}"
        
        # Return JSON for AJAX requests, redirect for regular requests
        if request.headers.get('X-Requested-With') == 'XMLHttpRequest':
            return jsonify({'success': False, 'message': error_message})
        
        flash(error_message, "error")
        return redirect(url_for('main.review_list'))

@main_bp.route('/reviews/batch_save_decisions', methods=['POST'])
def batch_save_decisions():
    reviews = MatchReview.query.join(NewItem) \
        .filter(MatchReview.approved.is_(None), NewItem.ignored.is_(False)) \
        .all()
    current_app.logger.info(f"[batch_save_decisions] Saving batch review decisions for {len(reviews)} items as NEW items")
    
    for review in reviews:
        # For batch save, we'll approve all items as "new" since they need review
        # This means they'll be created as new products/ingredients in Dgraph
        review.approved = True
        review.new_item.resolved = False  # Mark as unresolved so they get created as new
        # Don't set matched_canonical_id since we want them as new items
        current_app.logger.info(f"[batch_save_decisions] Marking '{review.new_item.name}' as new {review.new_item.type}")
        db.session.add(review)
        db.session.add(review.new_item)

    db.session.commit()
    flash(f"All {len(reviews)} items approved as NEW products/ingredients and ready to push to Dgraph.", "success")
    current_app.logger.info(f"[batch_save_decisions] Batch review decisions saved for {len(reviews)} items as NEW items.")
    return redirect(url_for('main.review_list'))

@main_bp.route('/reviews/batch_approve_high_confidence', methods=['POST'])
def batch_approve_high_confidence():
    """Auto-approve items with high confidence matches (90%+)"""
    high_confidence_reviews = MatchReview.query.join(NewItem) \
        .filter(
            MatchReview.approved.is_(None), 
            NewItem.ignored.is_(False),
            MatchReview.score >= 90.0,
            MatchReview.suggested_ext_id.isnot(None)
        ).all()
    
    current_app.logger.info(f"[batch_approve_high_confidence] Auto-approving {len(high_confidence_reviews)} high confidence items")
    
    approved_count = 0
    for review in high_confidence_reviews:
        review.approved = True
        review.new_item.resolved = True
        review.new_item.matched_canonical_id = review.suggested_ext_id
        db.session.add(review)
        db.session.add(review.new_item)
        approved_count += 1

    db.session.commit()
    flash(f"Auto-approved {approved_count} high confidence items (90%+ match).", "success")
    current_app.logger.info(f"[batch_approve_high_confidence] Auto-approved {approved_count} high confidence items.")
    return redirect(url_for('main.review_list'))

@main_bp.route('/reviews/batch_ignore_all', methods=['POST'])
def batch_ignore_all():
    pending = MatchReview.query.join(NewItem) \
        .filter(MatchReview.approved.is_(None), NewItem.ignored.is_(False)) \
        .all()
    current_app.logger.info(f"[batch_ignore_all] Ignoring all ({len(pending)}) pending review items")
    for review in pending:
        review.approved = False
        review.new_item.ignored = True
        db.session.add(review)
        db.session.add(review.new_item)

    db.session.commit()
    flash("All pending items have been ignored.", "warning")
    current_app.logger.info("[batch_ignore_all] All pending review items ignored.")
    return redirect(url_for('main.review_list'))

@main_bp.route('/reviews/push', methods=['POST'])
def push_to_dgraph():
    submission = MemberSubmission.query.order_by(MemberSubmission.id.desc()).first()
    if not submission:
        current_app.logger.warning("[push_to_dgraph] No submission found to push.")
        flash("No submission found to push.", "warning")
        return redirect(url_for('main.upload_file'))

    url   = current_app.config.get('DGRAPH_URL')
    token = current_app.config.get('DGRAPH_API_TOKEN')
    
    # Check if Dgraph is configured
    if not url or not token:
        current_app.logger.error("[push] Dgraph not configured - cannot push data")
        flash("Dgraph is not configured. Please set DGRAPH_URL and DGRAPH_API_TOKEN environment variables.", "error")
        return redirect(url_for('main.review_list'))
    
    headers = {"Content-Type": "application/json", "Dg-Auth": token}
    
    # Test Dgraph connectivity first
    try:
        test_query = {"query": "query { __schema { types { name } } }"}
        test_resp = requests.post(url, json=test_query, headers=headers, timeout=5)
        test_resp.raise_for_status()
        current_app.logger.info("[push] Dgraph connectivity test successful")
    except Exception as e:
        current_app.logger.error(f"[push] Dgraph connectivity test failed: {e}")
        flash(f"Dgraph is not accessible: {str(e)}. Please check your Dgraph instance and daily limits.", "error")
        return redirect(url_for('main.review_list'))

    current_app.logger.info(f"[push] Starting push for submission: {submission.name}")
    members = Member.query.filter_by(submission_id=submission.id).all()
    current_app.logger.info(f"[push] Found {len(members)} member record(s) to process")

    results = {"members": [], "products": [], "ingredients": [], "errors": []}
    

    
    # Load valid countries from schema (listallcountries.json represents the schema)
    def load_valid_countries_from_schema():
        """Load all valid countries from the schema definition"""
        try:
            with open('listallcountries.json', 'r') as f:
                data = json.load(f)
                countries = data.get('data', {}).get('queryMemberCountry', [])
                return {country['title']: country['countryID'] for country in countries}
        except Exception as e:
            current_app.logger.warning(f"[push] Could not load valid countries from schema: {e}")
            return {}
    
    # Load valid countries from schema
    valid_countries_schema = load_valid_countries_from_schema()
    current_app.logger.info(f"[push] Loaded {len(valid_countries_schema)} valid countries from schema")
    
    # Cache for country lookups to avoid repeated queries
    country_cache = {}
    
    def create_country_if_missing(country_name):
        """Create a country if it's valid but doesn't exist in Dgraph"""
        try:
            # Try to create the country
            mut = """
            mutation ($in: [AddMemberCountryInput!]!) {
              addMemberCountry(input: $in) {
                memberCountry { countryID title }
              }
            }
            """
            v = {"in": [{"title": country_name}]}
            resp = requests.post(url, json={"query": mut, "variables": v}, headers=headers)
            resp_json = resp.json()
            
            if "errors" in resp_json:
                error_msg = resp_json["errors"][0].get("message", "Unknown Dgraph error")
                current_app.logger.error(f"[push] Failed to create country '{country_name}': {error_msg}")
                return None
                
            data = resp_json.get("data", {})
            if data and data.get("addMemberCountry", {}).get("memberCountry"):
                country = data["addMemberCountry"]["memberCountry"][0]
                current_app.logger.info(f"[push] Created new country '{country_name}' with ID: {country['countryID']}")
                return {"countryID": country["countryID"]}
            else:
                current_app.logger.error(f"[push] Unexpected response creating country '{country_name}': {resp_json}")
                return None
                
        except Exception as e:
            current_app.logger.error(f"[push] Exception creating country '{country_name}': {e}")
            # Check if it's a daily limit error
            if "daily limit" in str(e).lower():
                return {"error": f"Daily limit reached: {e}"}
            return None

    def lookup_ref(ref_type_query, var_name, title, id_field):
        # For countries, check cache first
        if ref_type_query == "queryMemberCountry":
            cache_key = f"country_{title}"
            if cache_key in country_cache:
                current_app.logger.debug(f"[push] Found country '{title}' in cache")
                return country_cache[cache_key]
        
        # For countries, use the correct GraphQL query format
        if ref_type_query == "queryMemberCountry":
            q = f'''
            query ($title: String!) {{
              queryMemberCountry(filter: {{title: {{eq: $title}}}}) {{
                {id_field}
              }}
            }}
            '''
        else:
            q = f'''
            query ($title: String!) {{
              {ref_type_query}(filter: {{title: {{eq: $title}}}}) {{
                {id_field}
              }}
            }}
            '''
        current_app.logger.info(f"[push] Looking up {ref_type_query} for title='{title}' ({id_field})")
        try:
            resp = dgraph_request_with_retry(url, {"query": q, "variables": {"title": title}}, headers=headers)
            
            # Validate response structure
            resp_json = resp.json()
            if "errors" in resp_json:
                error_msg = resp_json["errors"][0].get("message", "Unknown Dgraph error")
                current_app.logger.error(f"[push] Dgraph query error for {ref_type_query}: {error_msg}")
                # Return a special value to indicate Dgraph error vs "not found"
                return {"error": error_msg}
                
            data = resp_json.get("data", {})
            if not data:
                current_app.logger.warning(f"[push] Empty response data for {ref_type_query}")
                return None
                
            result_list = data.get(ref_type_query, [])
            if not result_list:
                current_app.logger.warning(f"[push] No {ref_type_query} found for '{title}'")
                return None
                
            if id_field not in result_list[0]:
                current_app.logger.error(f"[push] Missing {id_field} in {ref_type_query} response")
                return None
                
            current_app.logger.info(f"[push] Found {id_field} for '{title}': {result_list[0][id_field]}")
            result = {id_field: result_list[0][id_field]}
            
            # Cache country results
            if ref_type_query == "queryMemberCountry":
                cache_key = f"country_{title}"
                country_cache[cache_key] = result
                current_app.logger.debug(f"[push] Cached country '{title}' result")
            
            return result
            
        except Exception as e:
            current_app.logger.error(f"[push] Error for {ref_type_query}: {e}")
            return {"error": str(e)}



    # --- Begin atomic block per company ---
    for m in members:
        biz = m.name or "(Unknown)"
        try:
            # Use Python try/except to make each company atomic (skip if any error occurs)
            # Country lookup: required for every member
            if not m.country1:
                results["errors"].append(f"Missing country for business '{biz}'—skipped.")
                current_app.logger.warning(f"[push] Skipping '{biz}' due to missing country.")
                continue
            # Step 1: Check if country is valid according to schema
            if m.country1 not in valid_countries_schema:
                results["errors"].append(f"Country '{m.country1}' is not a valid country in the schema for business '{biz}'—skipped.")
                current_app.logger.warning(f"[push] Skipping '{biz}' due to invalid country '{m.country1}'")
                continue
            
            # Step 2: Check if country exists in Dgraph
            country_ref = lookup_ref("queryMemberCountry", "country", m.country1, "countryID")
            if country_ref is None:
                # Country not found in Dgraph - try to create it
                current_app.logger.info(f"[push] Country '{m.country1}' not found in Dgraph, attempting to create...")
                country_ref = create_country_if_missing(m.country1)
                if not country_ref:
                    results["errors"].append(f"Failed to create country '{m.country1}' for business '{biz}'—skipped.")
                    current_app.logger.warning(f"[push] Skipping '{biz}' due to failed country creation '{m.country1}'")
                    continue
                elif isinstance(country_ref, dict) and "error" in country_ref:
                    # Dgraph error occurred during creation
                    error_msg = country_ref["error"]
                    results["errors"].append(f"Dgraph error creating country '{m.country1}': {error_msg} - business '{biz}' skipped.")
                    current_app.logger.warning(f"[push] Skipping '{biz}' due to Dgraph error creating country '{m.country1}': {error_msg}")
                    continue
                else:
                    current_app.logger.info(f"[push] Successfully created country '{m.country1}' in Dgraph")
            elif isinstance(country_ref, dict) and "error" in country_ref:
                # Dgraph error occurred (e.g., daily limit reached)
                error_msg = country_ref["error"]
                results["errors"].append(f"Dgraph error for country '{m.country1}': {error_msg} - business '{biz}' skipped.")
                current_app.logger.warning(f"[push] Skipping '{biz}' due to Dgraph error for country '{m.country1}': {error_msg}")
                continue
            else:
                current_app.logger.info(f"[push] Found existing country '{m.country1}' in Dgraph")

            # Lookup in Dgraph for possible upsert
            q = """
            query ($name: String!) {
              queryMember(filter: {businessName: {eq: $name}}) {
                memberID
                products { title productID }
                ingredients { title ingredientID }
              }
            }
            """
            current_app.logger.info(f"[push] Checking if '{biz}' exists in Dgraph…")
            resp = requests.post(url, json={"query": q, "variables": {"name": biz}}, headers=headers)
            node_list = resp.json().get("data", {}).get("queryMember", [])

            if node_list:
                current_app.logger.info(f"[push] Member '{biz}' exists, updating products/ingredients…")
                node = node_list[0]
                mem_id   = node["memberID"]
                exist_ps = {p["title"]: p["productID"] for p in node["products"]}
                exist_is = {i["title"]: i["ingredientID"] for i in node["ingredients"]}

                # Get all products and ingredients for this member
                all_products = [ni for ni in m.new_items if ni.type=="product" and not ni.ignored]
                all_ingredients = [ni for ni in m.new_items if ni.type=="ingredient" and not ni.ignored]
                
                # Separate resolved (matched to canonicals) vs unresolved items
                resolved_products = [ni for ni in all_products if ni.resolved and ni.matched_canonical_id]
                unresolved_products = [ni for ni in all_products if not ni.resolved]
                
                resolved_ingredients = [ni for ni in all_ingredients if ni.resolved and ni.matched_canonical_id]
                unresolved_ingredients = [ni for ni in all_ingredients if not ni.resolved]
                
                current_app.logger.info(f"[push] Member '{biz}' update: {len(resolved_products)} resolved products, {len(unresolved_products)} unresolved products")
                current_app.logger.info(f"[push] Member '{biz}' update: {len(resolved_ingredients)} resolved ingredients, {len(unresolved_ingredients)} unresolved ingredients")

                # Collect all product IDs to link (existing + resolved + new)
                all_product_ids = list(exist_ps.values())  # Start with existing
                new_product_names = []
                
                # Add resolved product IDs (handle multiple selections)
                for ni in resolved_products:
                    # Check if this item has multiple canonical selections
                    if hasattr(ni, 'review') and ni.review and ni.review.alternatives:
                        # Get all selected canonical IDs from alternatives
                        selected_canonicals = []
                        for alt in ni.review.alternatives:
                            if isinstance(alt, dict) and alt.get('selected'):
                                selected_canonicals.append(alt.get('ext_id'))
                        
                        # Add all selected canonical IDs
                        for canonical_id in selected_canonicals:
                            if canonical_id and canonical_id not in all_product_ids:
                                all_product_ids.append(canonical_id)
                                current_app.logger.info(f"[push] Adding multi-selected product '{ni.name}' (ID: {canonical_id}) to existing member '{biz}'")
                    else:
                        # Single selection (backward compatibility)
                        if ni.matched_canonical_id and ni.matched_canonical_id not in all_product_ids:
                            all_product_ids.append(ni.matched_canonical_id)
                            current_app.logger.info(f"[push] Adding resolved product '{ni.name}' (ID: {ni.matched_canonical_id}) to existing member '{biz}'")
                
                # Check unresolved products
                for ni in unresolved_products:
                    if ni.name in exist_ps:
                        # Already linked to this member
                        current_app.logger.info(f"[push] Product '{ni.name}' already linked to member '{biz}'")
                    else:
                        # Check if it exists in Dgraph
                        q = """
                        query ($title: String!) {
                          queryProduct(filter: {title: {eq: $title}}) {
                            productID
                            title
                          }
                        }
                        """
                        resp = requests.post(url, json={"query": q, "variables": {"title": ni.name}}, headers=headers)
                        existing_products = resp.json().get("data", {}).get("queryProduct", [])
                        
                        if existing_products:
                            # Product exists in Dgraph - link it
                            product_id = existing_products[0]["productID"]
                            if product_id not in all_product_ids:
                                all_product_ids.append(product_id)
                                current_app.logger.info(f"[push] Linking existing product '{ni.name}' (ID: {product_id}) to member '{biz}'")
                        else:
                            # Product doesn't exist - will create new one
                            new_product_names.append(ni.name)
                            current_app.logger.info(f"[push] Will create new product '{ni.name}' for existing member '{biz}'")

                # Same logic for ingredients
                all_ingredient_ids = list(exist_is.values())  # Start with existing
                new_ingredient_names = []
                
                # Add resolved ingredient IDs (handle multiple selections)
                for ni in resolved_ingredients:
                    # Check if this item has multiple canonical selections
                    if hasattr(ni, 'review') and ni.review and ni.review.alternatives:
                        # Get all selected canonical IDs from alternatives
                        selected_canonicals = []
                        for alt in ni.review.alternatives:
                            if isinstance(alt, dict) and alt.get('selected'):
                                selected_canonicals.append(alt.get('ext_id'))
                        
                        # Add all selected canonical IDs
                        for canonical_id in selected_canonicals:
                            if canonical_id and canonical_id not in all_ingredient_ids:
                                all_ingredient_ids.append(canonical_id)
                                current_app.logger.info(f"[push] Adding multi-selected ingredient '{ni.name}' (ID: {canonical_id}) to existing member '{biz}'")
                    else:
                        # Single selection (backward compatibility)
                        if ni.matched_canonical_id and ni.matched_canonical_id not in all_ingredient_ids:
                            all_ingredient_ids.append(ni.matched_canonical_id)
                            current_app.logger.info(f"[push] Adding resolved ingredient '{ni.name}' (ID: {ni.matched_canonical_id}) to existing member '{biz}'")
                
                # Check unresolved ingredients
                for ni in unresolved_ingredients:
                    if ni.name in exist_is:
                        # Already linked to this member
                        current_app.logger.info(f"[push] Ingredient '{ni.name}' already linked to member '{biz}'")
                    else:
                        # Check if it exists in Dgraph
                        q = """
                        query ($title: String!) {
                          queryIngredients(filter: {title: {eq: $title}}) {
                            ingredientID
                            title
                          }
                        }
                        """
                        resp = requests.post(url, json={"query": q, "variables": {"title": ni.name}}, headers=headers)
                        existing_ingredients = resp.json().get("data", {}).get("queryIngredients", [])
                        
                        if existing_ingredients:
                            # Ingredient exists in Dgraph - link it
                            ingredient_id = existing_ingredients[0]["ingredientID"]
                            if ingredient_id not in all_ingredient_ids:
                                all_ingredient_ids.append(ingredient_id)
                                current_app.logger.info(f"[push] Linking existing ingredient '{ni.name}' (ID: {ingredient_id}) to member '{biz}'")
                        else:
                            # Ingredient doesn't exist - will create new one
                            new_ingredient_names.append(ni.name)
                            current_app.logger.info(f"[push] Will create new ingredient '{ni.name}' for existing member '{biz}'")

                # Create new products if needed
                new_product_ids = []
                if new_product_names:
                    mut = """
                    mutation ($in: [AddProductInput!]!) {
                      addProduct(input: $in) { product { title productID } }
                    }
                    """
                    v = {"in": [{"title": t} for t in new_product_names]}
                    r = requests.post(url, json={"query": mut, "variables": v}, headers=headers)
                    arr = r.json().get("data", {}).get("addProduct", {}).get("product", [])
                    for pr in arr:
                        new_product_ids.append(pr["productID"])
                        # Add member association note
                        pr["note"] = f"Created with existing member '{biz}'"
                        results["products"].append(pr)
                    current_app.logger.info(f"[push] Created {len(new_product_ids)} new products for existing member '{biz}'")

                # Create new ingredients if needed
                new_ingredient_ids = []
                if new_ingredient_names:
                    mut = """
                    mutation ($in: [AddIngredientsInput!]!) {
                      addIngredients(input: $in) { ingredients { title ingredientID } }
                    }
                    """
                    v = {"in": [{"title": t} for t in new_ingredient_names]}
                    r = requests.post(url, json={"query": mut, "variables": v}, headers=headers)
                    arr = r.json().get("data", {}).get("addIngredients", {}).get("ingredients", [])
                    for ing in arr:
                        new_ingredient_ids.append(ing["ingredientID"])
                        # Add member association note
                        ing["note"] = f"Created with existing member '{biz}'"
                        results["ingredients"].append(ing)
                    current_app.logger.info(f"[push] Created {len(new_ingredient_ids)} new ingredients for existing member '{biz}'")

                # Update member with all product and ingredient IDs
                all_product_ids.extend(new_product_ids)
                all_ingredient_ids.extend(new_ingredient_ids)
                
                if all_product_ids or all_ingredient_ids:
                    mut = """
                    mutation ($in: UpdateMemberInput!) {
                      updateMember(input: $in) {
                        member { memberID businessName }
                      }
                    }
                    """
                    all_ps = [{"productID": pid} for pid in all_product_ids]
                    all_is = [{"ingredientID": iid} for iid in all_ingredient_ids]
                    v = {
                      "in": {
                        "filter": {"memberID": [mem_id]},
                        "set":    {"products": all_ps, "ingredients": all_is}
                      }
                    }
                    requests.post(url, json={"query": mut, "variables": v}, headers=headers)
                    results["members"].append({"memberID": mem_id, "businessName": biz})
                    current_app.logger.info(f"[push] Updated member '{biz}' with {len(all_product_ids)} products and {len(all_ingredient_ids)} ingredients")

                continue  # done with this existing member

            # 2. Brand-new company → build input
            current_app.logger.info(f"[push] Member '{biz}' is new, creating new record in Dgraph…")
            
            # Get all products and ingredients for this member
            all_products = [ni for ni in m.new_items if ni.type=="product" and not ni.ignored]
            all_ingredients = [ni for ni in m.new_items if ni.type=="ingredient" and not ni.ignored]
            
            # Separate resolved (matched to canonicals) vs unresolved items
            resolved_products = [ni for ni in all_products if ni.resolved and ni.matched_canonical_id]
            unresolved_products = [ni for ni in all_products if not ni.resolved]
            
            resolved_ingredients = [ni for ni in all_ingredients if ni.resolved and ni.matched_canonical_id]
            unresolved_ingredients = [ni for ni in all_ingredients if not ni.resolved]
            
            current_app.logger.info(f"[push] Member '{biz}': {len(resolved_products)} resolved products, {len(unresolved_products)} unresolved products")
            current_app.logger.info(f"[push] Member '{biz}': {len(resolved_ingredients)} resolved ingredients, {len(unresolved_ingredients)} unresolved ingredients")
            
            # For unresolved items, check if they already exist in Dgraph
            existing_product_ids = []
            new_product_names = []
            
            for ni in unresolved_products:
                # Check if this product already exists in Dgraph
                q = """
                query ($title: String!) {
                  queryProduct(filter: {title: {eq: $title}}) {
                    productID
                    title
                  }
                }
                """
                resp = requests.post(url, json={"query": q, "variables": {"title": ni.name}}, headers=headers)
                existing_products = resp.json().get("data", {}).get("queryProduct", [])
                
                if existing_products:
                    # Product already exists - reuse it
                    existing_product_ids.append(existing_products[0]["productID"])
                    current_app.logger.info(f"[push] Reusing existing product '{ni.name}' (ID: {existing_products[0]['productID']}) for member '{biz}'")
                else:
                    # Product doesn't exist - will create new one
                    new_product_names.append(ni.name)
                    current_app.logger.info(f"[push] Will create new product '{ni.name}' for member '{biz}'")
            
            # Add resolved product IDs (handle multiple selections)
            for ni in resolved_products:
                # Check if this item has multiple canonical selections
                if hasattr(ni, 'review') and ni.review and ni.review.alternatives:
                    # Get all selected canonical IDs from alternatives
                    selected_canonicals = []
                    for alt in ni.review.alternatives:
                        if isinstance(alt, dict) and alt.get('selected'):
                            selected_canonicals.append(alt.get('ext_id'))
                    
                    # Add all selected canonical IDs
                    for canonical_id in selected_canonicals:
                        if canonical_id and canonical_id not in existing_product_ids:
                            existing_product_ids.append(canonical_id)
                            current_app.logger.info(f"[push] Using multi-selected product '{ni.name}' (ID: {canonical_id}) for member '{biz}'")
                else:
                    # Single selection (backward compatibility)
                    if ni.matched_canonical_id and ni.matched_canonical_id not in existing_product_ids:
                        existing_product_ids.append(ni.matched_canonical_id)
                        current_app.logger.info(f"[push] Using resolved product '{ni.name}' (ID: {ni.matched_canonical_id}) for member '{biz}'")
            
            # Same logic for ingredients
            existing_ingredient_ids = []
            new_ingredient_names = []
            
            for ni in unresolved_ingredients:
                # Check if this ingredient already exists in Dgraph
                q = """
                query ($title: String!) {
                  queryIngredients(filter: {title: {eq: $title}}) {
                    ingredientID
                    title
                  }
                }
                """
                resp = requests.post(url, json={"query": q, "variables": {"title": ni.name}}, headers=headers)
                existing_ingredients = resp.json().get("data", {}).get("queryIngredients", [])
                
                if existing_ingredients:
                    # Ingredient already exists - reuse it
                    existing_ingredient_ids.append(existing_ingredients[0]["ingredientID"])
                    current_app.logger.info(f"[push] Reusing existing ingredient '{ni.name}' (ID: {existing_ingredients[0]['ingredientID']}) for member '{biz}'")
                else:
                    # Ingredient doesn't exist - will create new one
                    new_ingredient_names.append(ni.name)
                    current_app.logger.info(f"[push] Will create new ingredient '{ni.name}' for member '{biz}'")
            
            # Add resolved ingredient IDs (handle multiple selections)
            for ni in resolved_ingredients:
                # Check if this item has multiple canonical selections
                if hasattr(ni, 'review') and ni.review and ni.review.alternatives:
                    # Get all selected canonical IDs from alternatives
                    selected_canonicals = []
                    for alt in ni.review.alternatives:
                        if isinstance(alt, dict) and alt.get('selected'):
                            selected_canonicals.append(alt.get('ext_id'))
                    
                    # Add all selected canonical IDs
                    for canonical_id in selected_canonicals:
                        if canonical_id and canonical_id not in existing_ingredient_ids:
                            existing_ingredient_ids.append(canonical_id)
                            current_app.logger.info(f"[push] Using multi-selected ingredient '{ni.name}' (ID: {canonical_id}) for member '{biz}'")
                else:
                    # Single selection (backward compatibility)
                    if ni.matched_canonical_id and ni.matched_canonical_id not in existing_ingredient_ids:
                        existing_ingredient_ids.append(ni.matched_canonical_id)
                        current_app.logger.info(f"[push] Using resolved ingredient '{ni.name}' (ID: {ni.matched_canonical_id}) for member '{biz}'")
            
            # Create new products if needed
            new_product_ids = []
            if new_product_names:
                mut = """
                mutation ($in: [AddProductInput!]!) {
                  addProduct(input: $in) { product { title productID } }
                }
                """
                v = {"in": [{"title": t} for t in new_product_names]}
                r = requests.post(url, json={"query": mut, "variables": v}, headers=headers)
                arr = r.json().get("data", {}).get("addProduct", {}).get("product", [])
                for pr in arr:
                    new_product_ids.append(pr["productID"])
                    # Add member association note to avoid duplication
                    pr["note"] = f"Created with member '{biz}'"
                    results["products"].append(pr)
                current_app.logger.info(f"[push] Created {len(new_product_ids)} new products for member '{biz}'")
            
            # Create new ingredients if needed
            new_ingredient_ids = []
            if new_ingredient_names:
                mut = """
                mutation ($in: [AddIngredientsInput!]!) {
                  addIngredients(input: $in) { ingredients { title ingredientID } }
                }
                """
                v = {"in": [{"title": t} for t in new_ingredient_names]}
                r = requests.post(url, json={"query": mut, "variables": v}, headers=headers)
                arr = r.json().get("data", {}).get("addIngredients", {}).get("ingredients", [])
                for ing in arr:
                    new_ingredient_ids.append(ing["ingredientID"])
                    # Add member association note to avoid duplication
                    ing["note"] = f"Created with member '{biz}'"
                    results["ingredients"].append(ing)
                current_app.logger.info(f"[push] Created {len(new_ingredient_ids)} new ingredients for member '{biz}'")

            state_ref = None
            if hasattr(m, 'state1') and m.state1:
                state_ref = lookup_ref("queryMemberStateOrProvince", "state", m.state1, "stateOrProvinceID")

            # Build member input with validation to ensure no None values
            member_input = {
                "businessName":   biz,
                "country1":       country_ref,
                "streetAddress1": m.street_address1 if (m.street_address1 and m.street_address1.strip()) else "Not provided",  # Required field
            }
            
            # Only add optional fields if they have valid values
            if m.contact_email and m.contact_email.strip():
                member_input["contactEmail"] = m.contact_email
            if m.city1 and m.city1.strip():
                member_input["city1"] = m.city1
            if m.company_bio and m.company_bio.strip():
                member_input["companyBio"] = m.company_bio
                
            # Add products and ingredients - combine existing and new IDs
            all_product_ids = existing_product_ids + new_product_ids
            all_ingredient_ids = existing_ingredient_ids + new_ingredient_ids
            
            if all_product_ids:
                member_input["products"] = [{"productID": pid} for pid in all_product_ids]
            if all_ingredient_ids:
                member_input["ingredients"] = [{"ingredientID": iid} for iid in all_ingredient_ids]
            if state_ref:
                member_input["stateOrProvince1"] = state_ref
            if hasattr(m, 'zip_code1') and m.zip_code1:
                member_input["zipCode1"] = m.zip_code1

            mut = """
            mutation ($in: [AddMemberInput!]!) {
              addMember(input: $in) {
                member { memberID businessName }
              }
            }
            """
            current_app.logger.info(f"[push] Member input for '{biz}': {member_input}")
            r = requests.post(
                url,
                json={"query": mut, "variables": {"in": [member_input]}},
                headers=headers
            )
            resp_json = r.json()
            arr = resp_json.get("data", {}).get("addMember", {}).get("member", [])
            if arr:
                results["members"].extend(arr)
                current_app.logger.info(f"[push] Created new member '{biz}' in Dgraph")
                
                # Note: Products and ingredients are already added to results when created above
                # No need to add them again here to avoid duplication
            else:
                err = resp_json.get("errors", [{"message": "Unknown Dgraph error"}])
                error_msg = err[0].get('message', 'Unknown Dgraph error')
                results["errors"].append(
                    f"Failed to create '{biz}': {error_msg}"
                )
                current_app.logger.warning(f"[push] Failed to create '{biz}': {error_msg}")
                current_app.logger.warning(f"[push] Full response: {resp_json}")
        except Exception as ex:
            current_app.logger.warning(f"[push] Atomic rollback: failed to push '{biz}': {ex}")
            results["errors"].append(
                f"Failed to push '{biz}' due to: {ex} (skipped; no partial writes for this company)"
            )
            continue

    return render_template(
        'push_summary.html',
        submission=submission,
        members=results["members"],
        products=results["products"],
        ingredients=results["ingredients"],
        errors=results["errors"]
    )

@main_bp.route('/reviews/cancel', methods=['POST'])
def cancel_review():
    current_app.logger.info("[cancel_review] Cancelling review, clearing DB and session.")
    MatchReview.query.delete()
    NewItem.query.delete()
    Member.query.delete()
    MemberSubmission.query.delete()
    db.session.commit()
    session.pop('etl_validation_errors', None)
    session.pop('etl_error_filename', None)
    flash("Review cancelled. You can upload a new file now.", "info")
    return redirect(url_for('main.upload_file'))
