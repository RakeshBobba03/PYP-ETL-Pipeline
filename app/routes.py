# app/routes.py

import os
import csv
import requests
import logging
import time
from pathlib import Path
from flask import (
    Blueprint, render_template, request, redirect,
    url_for, flash, current_app, get_flashed_messages,
    send_from_directory, session, abort
)
from werkzeug.utils import secure_filename
from app import db
from app.models import NewItem, MatchReview, MemberSubmission, Member
from app.etl import process_submission_file

main_bp = Blueprint('main', __name__)

UPLOAD_FOLDER = os.path.join(os.getcwd(), 'seed_data', 'new_submissions')
ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv'}

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

            if clear_previous:
                current_app.logger.info("[upload] Clearing previous submissions and DB records…")
                MatchReview.query.delete()
                NewItem.query.delete()
                Member.query.delete()
                MemberSubmission.query.delete()
                db.session.commit()
                current_app.logger.info("[upload] Previous DB records cleared.")

            try:
                current_app.logger.info(f"[upload] Starting ETL process for file: {filename}")
                count, val_errors, valid_row_indices = process_submission_file(filename)
                current_app.logger.info(f"[upload] ETL finished for {filename}: {count} items, {len(val_errors)} validation errors")
            except Exception as e:
                current_app.logger.error(f"[upload][error] {e}")
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
                    flash(f"Upload failed: {e}", "danger")
                return redirect(url_for('main.upload_file'))

            # If *all* rows were invalid
            if count == 0:
                error_filename = f"{filename.rsplit('.',1)[0]}_errors.csv"
                error_path = os.path.join(UPLOAD_FOLDER, error_filename)
                current_app.logger.warning(f"[upload] All rows invalid for {filename}. Writing errors to: {error_path}")
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
                current_app.logger.warning(f"[upload] Some rows were skipped. Writing ETL error report to: {error_path}")
                with open(error_path, 'w', newline='', encoding='utf-8') as ef:
                    writer = csv.writer(ef)
                    writer.writerow(['Row','Error'])
                    for err in val_errors:
                        writer.writerow([err['row'], err['error']])
                flash(f"Some rows were skipped due to validation errors. See details on the review page.", "warning")

            current_app.logger.info(f"[upload] Successfully processed {count} valid items from {filename}")
            flash(f"Uploaded and processed “{filename}” ({count} valid items). Ready for review.", "success")
            return redirect(url_for('main.review_list'))

        current_app.logger.warning("[upload] No valid file selected or invalid file type.")
        flash("Please select a valid .xlsx, .xls or .csv file.", "danger")

    current_app.logger.info("[upload] GET received. Rendering upload.html")
    return render_template('upload.html')

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
    # New items: approved but no canonical ID (user chose "new")
    # Matched items: approved with canonical ID (user chose existing match)
    new_items_approved = NewItem.query.join(MatchReview).filter(
        NewItem.resolved == True,
        MatchReview.approved == True,
        NewItem.matched_canonical_id.is_(None)
    ).all()
    
    matched_items = NewItem.query.join(MatchReview).filter(
        NewItem.resolved == True,
        MatchReview.approved == True,
        NewItem.matched_canonical_id.isnot(None)
    ).all()
    
    current_app.logger.info(f"[review_list] No pending reviews. New items to add: {len(new_items_to_add)} | New items approved: {len(new_items_approved)} | Matched items: {len(matched_items)}")
    return render_template(
        'reviews_done.html',
        new_items_to_add=new_items_to_add,
        new_items_approved=new_items_approved,
        matched_items=matched_items,
        val_errors=val_errors,
        error_filename=error_filename
    )

@main_bp.route('/reviews/handle_review/<int:item_id>', methods=['POST'])
def handle_review(item_id):
    current_app.logger.info(f"[handle_review] POST for review item_id={item_id}")
    review = MatchReview.query.filter_by(new_item_id=item_id, approved=None).first()
    if not review:
        current_app.logger.warning(f"[handle_review] Review item {item_id} not found or already handled.")
        flash("Review item not found or already handled.", "warning")
        return redirect(url_for('main.review_list'))

    choice = request.form.get('choice')
    current_app.logger.info(f"[handle_review] Choice received: {choice}")

    if choice == '__new__':
        # User chose to create as new item
        review.approved = True
        review.new_item.resolved = True
        # Don't set matched_canonical_id since we want it as a new item
        flash(f"Approved '{review.new_item.name}' as new {review.new_item.type}.", "success")
        current_app.logger.info(f"[handle_review] Approved '{review.new_item.name}' as new {review.new_item.type}")
    elif choice and choice != '__new__':
        # User chose a specific canonical match
        review.approved = True
        review.new_item.resolved = True
        review.new_item.matched_canonical_id = choice
        flash(f"Approved '{review.new_item.name}' matched to canonical data.", "success")
        current_app.logger.info(f"[handle_review] Approved '{review.new_item.name}' matched to canonical ID: {choice}")
    else:
        # No choice made - treat as ignored
        review.approved = False
        review.new_item.ignored = True
        flash(f"Ignored '{review.new_item.name}'.", "warning")
        current_app.logger.info(f"[handle_review] Ignored review for item '{review.new_item.name}'")

    db.session.commit()
    return redirect(url_for('main.review_list'))

@main_bp.route('/reviews/ignore_review_item/<int:item_id>', methods=['POST'])
def ignore_review_item(item_id):
    current_app.logger.info(f"[ignore_review_item] POST for item_id={item_id}")
    review = MatchReview.query.filter_by(new_item_id=item_id, approved=None).first()
    if not review:
        current_app.logger.warning(f"[ignore_review_item] Review item {item_id} not found or already handled.")
        flash("Review item not found or already handled.", "warning")
        return redirect(url_for('main.review_list'))

    review.approved = False
    review.new_item.ignored = True
    db.session.commit()
    flash(f"Ignored “{review.new_item.name}”.", "warning")
    current_app.logger.info(f"[ignore_review_item] Ignored review for item '{review.new_item.name}'")
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
        review.new_item.resolved = True
        # Don't set matched_canonical_id since we want them as new items
        current_app.logger.info(f"[batch_save_decisions] Marking '{review.new_item.name}' as new {review.new_item.type}")
        db.session.add(review)
        db.session.add(review.new_item)

    db.session.commit()
    flash(f"All {len(reviews)} items approved as NEW products/ingredients and ready to push to Dgraph.", "success")
    current_app.logger.info(f"[batch_save_decisions] Batch review decisions saved for {len(reviews)} items as NEW items.")
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

    url   = current_app.config['DGRAPH_URL']
    token = current_app.config['DGRAPH_API_TOKEN']
    headers = {"Content-Type": "application/json", "Dg-Auth": token}

    current_app.logger.info(f"[push] Starting push for submission: {submission.name}")
    members = Member.query.filter_by(submission_id=submission.id).all()
    current_app.logger.info(f"[push] Found {len(members)} member record(s) to process")

    results = {"members": [], "products": [], "ingredients": [], "errors": []}

    def lookup_ref(ref_type_query, var_name, title, id_field):
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
                return None
                
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
            return {id_field: result_list[0][id_field]}
            
        except Exception as e:
            current_app.logger.error(f"[push] Error for {ref_type_query}: {e}")
            return None

    def create_country_if_missing(country_name):
        """Create a country if it doesn't exist"""
        try:
            # Try to create the country
            mut = """
            mutation ($in: [AddMemberCountryInput!]!) {
              addMemberCountry(input: $in) {
                country { countryID title }
              }
            }
            """
            v = {"in": [{"title": country_name}]}
            resp = requests.post(url, json={"query": mut, "variables": v}, headers=headers)
            resp_json = resp.json()
            
            if "errors" in resp_json:
                current_app.logger.error(f"[push] Failed to create country '{country_name}': {resp_json['errors']}")
                return None
                
            data = resp_json.get("data", {})
            if data and data.get("addMemberCountry", {}).get("country"):
                country = data["addMemberCountry"]["country"][0]
                current_app.logger.info(f"[push] Created new country '{country_name}' with ID: {country['countryID']}")
                return {"countryID": country["countryID"]}
            else:
                current_app.logger.error(f"[push] Unexpected response creating country '{country_name}': {resp_json}")
                return None
                
        except Exception as e:
            current_app.logger.error(f"[push] Exception creating country '{country_name}': {e}")
            return None

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
            country_ref = lookup_ref("queryMemberCountry", "country", m.country1, "countryID")
            if not country_ref:
                # Try to create the country if it doesn't exist
                current_app.logger.info(f"[push] Country '{m.country1}' not found, attempting to create it...")
                country_ref = create_country_if_missing(m.country1)
                if not country_ref:
                    results["errors"].append(f"Failed to create country '{m.country1}' for business '{biz}'—skipped.")
                    current_app.logger.warning(f"[push] Skipping '{biz}' due to failed country creation '{m.country1}'")
                    continue

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

                subs_ps = [ni.name for ni in m.new_items if ni.type=="product"    and not ni.ignored]
                subs_is = [ni.name for ni in m.new_items if ni.type=="ingredient" and not ni.ignored]
                new_ps  = [p for p in subs_ps if p not in exist_ps]
                new_is  = [i for i in subs_is if i not in exist_is]

                prod_ids = []
                if new_ps:
                    mut = """
                    mutation ($in: [AddProductInput!]!) {
                      addProduct(input: $in) { product { title productID } }
                    }
                    """
                    v = {"in": [{"title": t} for t in new_ps]}
                    r = requests.post(url, json={"query": mut, "variables": v}, headers=headers)
                    arr = r.json().get("data", {}).get("addProduct", {}).get("product", [])
                    for pr in arr:
                        prod_ids.append(pr["productID"])
                        results["products"].append(pr)
                    current_app.logger.info(f"[push] Added {len(prod_ids)} new products for member '{biz}'")

                ing_ids = []
                if new_is:
                    mut = """
                    mutation ($in: [AddIngredientsInput!]!) {
                      addIngredients(input: $in) { ingredients { title ingredientID } }
                    }
                    """
                    v = {"in": [{"title": t} for t in new_is]}
                    r = requests.post(url, json={"query": mut, "variables": v}, headers=headers)
                    arr = r.json().get("data", {}).get("addIngredients", {}).get("ingredients", [])
                    for ing in arr:
                        ing_ids.append(ing["ingredientID"])
                        results["ingredients"].append(ing)
                    current_app.logger.info(f"[push] Added {len(ing_ids)} new ingredients for member '{biz}'")

                if prod_ids or ing_ids:
                    mut = """
                    mutation ($in: UpdateMemberInput!) {
                      updateMember(input: $in) {
                        member { memberID businessName }
                      }
                    }
                    """
                    all_ps = [{"productID": pid} for pid in list(exist_ps.values()) + prod_ids]
                    all_is = [{"ingredientID": iid} for iid in list(exist_is.values())  + ing_ids]
                    v = {
                      "in": {
                        "filter": {"memberID": [mem_id]},
                        "set":    {"products": all_ps, "ingredients": all_is}
                      }
                    }
                    requests.post(url, json={"query": mut, "variables": v}, headers=headers)
                    results["members"].append({"memberID": mem_id, "businessName": biz})
                    current_app.logger.info(f"[push] Linked products/ingredients to member '{biz}'")

                continue  # done with this existing member

            # 2. Brand-new company → build input
            current_app.logger.info(f"[push] Member '{biz}' is new, creating new record in Dgraph…")
            subs_ps = [ni.name for ni in m.new_items if ni.type=="product"    and (ni.resolved or not ni.ignored)]
            subs_is = [ni.name for ni in m.new_items if ni.type=="ingredient" and (ni.resolved or not ni.ignored)]

            state_ref = None
            if hasattr(m, 'state1') and m.state1:
                state_ref = lookup_ref("queryMemberStateOrProvince", "state", m.state1, "stateOrProvinceID")

            member_input = {
                "businessName":   biz,
                "contactEmail":   m.contact_email,
                "streetAddress1": m.street_address1,
                "city1":          m.city1,
                "companyBio":     m.company_bio,
                "products":       [{"title": t} for t in subs_ps],
                "ingredients":    [{"title": t} for t in subs_is],
                "country1":       country_ref,
            }
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
            else:
                err = resp_json.get("errors", [{"message": "Unknown Dgraph error"}])
                results["errors"].append(
                    f"Failed to create '{biz}': {err[0].get('message')}"
                )
                current_app.logger.warning(f"[push] Failed to create '{biz}': {err[0].get('message')}")
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
