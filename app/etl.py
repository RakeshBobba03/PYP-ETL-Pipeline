import os
import re
import csv
import openpyxl
import requests
from sqlalchemy.exc import SQLAlchemyError
from rapidfuzz import process, fuzz, utils
from flask import current_app
from app import db
from app.models import MemberSubmission, Member, NewItem, MatchReview

BATCH_SIZE = 1000

def process_submission_file(filename):
    """
    Load a new submission, validate each row, skip bad ones,
    return (count, validation_errors, valid_row_indices)
    """
    # Prevent re-processing
    if MemberSubmission.query.filter_by(name=filename).first():
        current_app.logger.info(f"[etl] skipping {filename}: already processed")
        return 0, [], []

    data_dir = os.path.join(os.getcwd(), 'seed_data', 'new_submissions')
    fp = os.path.join(data_dir, filename)
    ext = filename.lower().rsplit('.', 1)[1]

    # Prepare reader + helper
    if ext == 'csv':
        f = open(fp, encoding='utf-8', newline='')
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)
        get = lambda r, c: r.get(c)
    else:
        wb = openpyxl.load_workbook(fp, read_only=True)
        sheet = wb.active
        header_row = next(sheet.iter_rows(min_row=1, max_row=1, values_only=True))
        headers = [h if h else '' for h in header_row]
        rows = list(sheet.iter_rows(min_row=2, values_only=True))
        get = lambda r, c: r[headers.index(c)] if c in headers else None

    # Must have these columns in the file
    for col in ('businessName', 'country1', 'products', 'ingredients'):
        if col not in headers:
            current_app.logger.error(f"[etl] File missing required column: {col}")
            raise ValueError(f"Missing required column: {col}")

    current_app.logger.info(f"[etl] Starting {filename} (columns: {headers})")

    sub = MemberSubmission(name=filename)
    db.session.add(sub)
    db.session.flush()

    url   = current_app.config['DGRAPH_URL']
    token = current_app.config['DGRAPH_API_TOKEN']
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

    for idx, row in enumerate(rows, start=2):
        current_app.logger.info(f"\n[etl] Processing row {idx}…")
        # --- Begin atomic block for this row ---
        from sqlalchemy.orm import Session
        session: Session = db.session
        try:
            with session.begin_nested():
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
                    # Nested block ensures rollback for just this row, then continue
                    continue

                # Only add valid rows to DB!
                valid_row_indices.append(idx)
                current_app.logger.info(f"[etl] Row {idx}: Creating Member for '{biz}', country: '{country}'")
                member = Member(
                    name=str(biz).strip(),
                    contact_email=str(get(row, 'contactEmail') or '').strip() or None,
                    street_address1=str(get(row, 'streetAddress1') or '').strip() or None,
                    city1=str(get(row, 'city1') or '').strip() or None,
                    country1=str(country).strip(),
                    company_bio=str(get(row, 'companyBio') or '').strip() or None,
                    submission=sub
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
                        ni = NewItem(name=text, type=kind, member=member)
                        lower_map = prod_lower if kind == 'product' else ing_lower
                        pool      = prod_names if kind == 'product' else ing_names

                        # Exact match
                        ext_id = lower_map.get(text.lower())
                        if ext_id:
                            ni.matched_canonical_id = ext_id
                            ni.score = 100.0
                            ni.resolved = True
                            current_app.logger.info(f"[etl] Row {idx}: '{text}' ({kindstr}) exact matched existing canonical [{ext_id}]")
                        else:
                            best = process.extractOne(text, pool, scorer=fuzz.token_set_ratio, processor=utils.default_process)
                            name0, score0 = (best[0], float(best[1])) if best else (None, 0.0)
                            if score0 >= 80.0:
                                ni.matched_canonical_id = (prod_map if kind == 'product' else ing_map)[name0]
                                ni.score = score0
                                ni.resolved = True
                                current_app.logger.info(f"[etl] Row {idx}: '{text}' ({kindstr}) fuzzy matched '{name0}' (score {score0:.1f}%)")
                            else:
                                alts = []
                                ext_map = prod_map if kind == 'product' else ing_map
                                for alt_nm, alt_sc, _ in process.extract(text, pool, scorer=fuzz.token_set_ratio, processor=utils.default_process, limit=3):
                                    alts.append({"name": alt_nm, "score": float(alt_sc), "ext_id": ext_map.get(alt_nm)})
                                current_app.logger.info(
                                    f"[etl] Row {idx}: '{text}' ({kindstr}) no good match found (score {score0:.1f}%), will require review. Top guess: '{name0}'."
                                )
                                mr = MatchReview(
                                    new_item=ni, suggested_name=name0 or text,
                                    suggested_ext_id=(ext_map.get(name0) if name0 else None),
                                    score=score0, alternatives=alts, approved=None
                                )
                                db.session.add(mr)
                        db.session.add(ni)
                        counter += 1
                        if counter % BATCH_SIZE == 0:
                            db.session.commit()
                            current_app.logger.info(f"[etl] committed {counter} items…")

                handle('product', get(row, 'products'))
                handle('ingredient', get(row, 'ingredients'))
        except SQLAlchemyError as ex:
            db.session.rollback()
            err_msg = f"DB error: {ex}"
            current_app.logger.error(f"[etl] Row {idx}: {err_msg}")
            validation_errors.append({'row': idx, 'error': err_msg})
            continue

    db.session.commit()
    current_app.logger.info(f"[etl] Finished {filename} → {counter} unique items; {len(validation_errors)} rows skipped")
    if validation_errors:
        for err in validation_errors:
            current_app.logger.warning(f"[etl] Validation Error Row {err['row']}: {err['error']}")
    else:
        current_app.logger.info(f"[etl] No validation errors.")
    return counter, validation_errors, valid_row_indices
