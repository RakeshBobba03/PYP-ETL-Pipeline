# app/report_utils.py
import csv
import io
import json
from datetime import datetime
from typing import Dict, List, Any, Optional
from flask import make_response, current_app
from app.models import Member, NewItem, MemberSubmission, MatchReview
from app import db

class ReportGenerator:
    """Generate downloadable CSV reports for ETL processing results"""
    
    def __init__(self):
        self.reports = {}
    
    def generate_processed_rows_csv(self, submission_id: int) -> str:
        """Generate processed_rows.csv with original + normalized + matched IDs + status"""
        try:
            submission = MemberSubmission.query.get(submission_id)
            if not submission:
                raise ValueError(f"Submission {submission_id} not found")
            
            members = Member.query.filter_by(submission_id=submission_id).all()
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header
            writer.writerow([
                'Submission Name',
                'Business Name',
                'Item Type',
                'Original Item Name',
                'Normalized Item Name',
                'Decision Status',
                'Matched Canonical ID',
                'Canonical Name',
                'Confidence Score',
                'Review Status',
                'Ignored',
                'Created At',
                'Review Timestamp',
                'Reviewer Action'
            ])
            
            # Write data rows
            for member in members:
                for item in member.new_items:
                    # Get review information
                    review = MatchReview.query.filter_by(new_item_id=item.id).first()
                    
                    # Determine decision status
                    if item.ignored:
                        decision_status = 'Ignored'
                    elif item.resolved and item.matched_canonical_id:
                        decision_status = 'Matched to Existing'
                    elif not item.resolved:
                        decision_status = 'Created as New'
                    else:
                        decision_status = 'Unknown'
                    
                    # Get canonical name if available
                    canonical_name = ''
                    if item.matched_canonical_id:
                        canonical_name = self._get_canonical_name(item.matched_canonical_id, item.type)
                    
                    # Get review information
                    review_status = 'Not Reviewed'
                    reviewer_action = ''
                    review_timestamp = ''
                    
                    if review:
                        if review.approved is True:
                            review_status = 'Approved'
                            reviewer_action = 'Approved'
                        elif review.approved is False:
                            review_status = 'Rejected'
                            reviewer_action = 'Rejected'
                        else:
                            review_status = 'Pending'
                            reviewer_action = 'Pending'
                        
                        if review.created_at:
                            review_timestamp = review.created_at.strftime('%Y-%m-%d %H:%M:%S')
                    
                    writer.writerow([
                        submission.name,
                        member.name,
                        item.type.title(),
                        item.name,  # Original name
                        item.name,  # Normalized name (same as original in current implementation)
                        decision_status,
                        item.matched_canonical_id or '',
                        canonical_name,
                        f"{item.score:.2f}" if item.score else '',
                        review_status,
                        'Yes' if item.ignored else 'No',
                        item.member.submission.created_at.strftime('%Y-%m-%d %H:%M:%S') if item.member.submission.created_at else '',
                        review_timestamp,
                        reviewer_action
                    ])
            
            csv_content = output.getvalue()
            output.close()
            
            current_app.logger.info(f"[report_generator] Generated processed_rows.csv for submission {submission_id} with {len(members)} members")
            return csv_content
            
        except Exception as e:
            current_app.logger.error(f"[report_generator] Error generating processed_rows.csv: {e}")
            raise
    
    def generate_errors_csv(self, submission_id: int, push_errors: List[Dict[str, Any]] = None) -> str:
        """Generate errors.csv with rows that had injection failures and error messages"""
        try:
            submission = MemberSubmission.query.get(submission_id)
            if not submission:
                raise ValueError(f"Submission {submission_id} not found")
            
            members = Member.query.filter_by(submission_id=submission_id).all()
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header
            writer.writerow([
                'Submission Name',
                'Business Name',
                'Error Type',
                'Error Category',
                'Error Message',
                'Item Type',
                'Item Name',
                'Row Number',
                'Timestamp',
                'Operation ID',
                'Retry Count',
                'Context'
            ])
            
            # Add ETL validation errors
            for member in members:
                for item in member.new_items:
                    if item.ignored:
                        writer.writerow([
                            submission.name,
                            member.name,
                            'Validation Error',
                            'Item Ignored',
                            'Item was marked as ignored during review',
                            item.type.title(),
                            item.name,
                            '',  # Row number not available
                            item.member.submission.created_at.strftime('%Y-%m-%d %H:%M:%S') if item.member.submission.created_at else '',
                            '',  # Operation ID
                            0,   # Retry count
                            'ETL Processing'
                        ])
            
            # Add push errors if provided
            if push_errors:
                for error in push_errors:
                    writer.writerow([
                        submission.name,
                        error.get('business_name', 'Unknown'),
                        error.get('error_type', 'Push Error'),
                        error.get('category', 'Unknown'),
                        error.get('error_message', 'Unknown error'),
                        error.get('item_type', ''),
                        error.get('item_name', ''),
                        error.get('row_number', ''),
                        error.get('timestamp', ''),
                        error.get('operation_id', ''),
                        error.get('retry_count', 0),
                        error.get('context', '')
                    ])
            
            csv_content = output.getvalue()
            output.close()
            
            current_app.logger.info(f"[report_generator] Generated errors.csv for submission {submission_id}")
            return csv_content
            
        except Exception as e:
            current_app.logger.error(f"[report_generator] Error generating errors.csv: {e}")
            raise
    
    def generate_created_nodes_csv(self, submission_id: int, created_products: List[Dict[str, Any]] = None, 
                                 created_ingredients: List[Dict[str, Any]] = None) -> str:
        """Generate created_nodes.csv with new products/ingredients and their returned UIDs"""
        try:
            submission = MemberSubmission.query.get(submission_id)
            if not submission:
                raise ValueError(f"Submission {submission_id} not found")
            
            output = io.StringIO()
            writer = csv.writer(output)
            
            # Write header
            writer.writerow([
                'Submission Name',
                'Node Type',
                'Node ID',
                'Title',
                'Created At',
                'Associated Business',
                'Source',
                'Notes'
            ])
            
            # Add created products
            if created_products:
                for product in created_products:
                    writer.writerow([
                        submission.name,
                        'Product',
                        product.get('productID', ''),
                        product.get('title', ''),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        product.get('associated_business', ''),
                        'Dgraph Mutation',
                        product.get('note', '')
                    ])
            
            # Add created ingredients
            if created_ingredients:
                for ingredient in created_ingredients:
                    writer.writerow([
                        submission.name,
                        'Ingredient',
                        ingredient.get('ingredientID', ''),
                        ingredient.get('title', ''),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        ingredient.get('associated_business', ''),
                        'Dgraph Mutation',
                        ingredient.get('note', '')
                    ])
            
            # Add new items that were created (from database)
            members = Member.query.filter_by(submission_id=submission_id).all()
            for member in members:
                for item in member.new_items:
                    if not item.resolved and not item.ignored:
                        # This item was created as new
                        writer.writerow([
                            submission.name,
                            item.type.title(),
                            '',  # Node ID not available in database
                            item.name,
                            item.member.submission.created_at.strftime('%Y-%m-%d %H:%M:%S') if item.member.submission.created_at else '',
                            member.name,
                            'ETL Processing',
                            'Created as new item during ETL processing'
                        ])
            
            csv_content = output.getvalue()
            output.close()
            
            current_app.logger.info(f"[report_generator] Generated created_nodes.csv for submission {submission_id}")
            return csv_content
            
        except Exception as e:
            current_app.logger.error(f"[report_generator] Error generating created_nodes.csv: {e}")
            raise
    
    def _get_canonical_name(self, canonical_id: str, item_type: str) -> str:
        """Get canonical name from Dgraph (simplified version)"""
        try:
            # This is a simplified version - in a real implementation,
            # you might want to cache canonical names or query Dgraph
            return f"Canonical {item_type} {canonical_id}"
        except Exception:
            return ''
    
    def create_csv_response(self, csv_content: str, filename: str) -> Any:
        """Create Flask response for CSV download"""
        response = make_response(csv_content)
        response.headers['Content-Type'] = 'text/csv'
        response.headers['Content-Disposition'] = f'attachment; filename="{filename}"'
        return response
    
    def generate_all_reports(self, submission_id: int, push_results: Dict[str, Any] = None) -> Dict[str, str]:
        """Generate all three CSV reports"""
        try:
            reports = {}
            
            # Generate processed rows report
            reports['processed_rows'] = self.generate_processed_rows_csv(submission_id)
            
            # Generate errors report
            push_errors = push_results.get('errors', []) if push_results else []
            reports['errors'] = self.generate_errors_csv(submission_id, push_errors)
            
            # Generate created nodes report
            created_products = push_results.get('products', []) if push_results else []
            created_ingredients = push_results.get('ingredients', []) if push_results else []
            reports['created_nodes'] = self.generate_created_nodes_csv(
                submission_id, created_products, created_ingredients
            )
            
            current_app.logger.info(f"[report_generator] Generated all reports for submission {submission_id}")
            return reports
            
        except Exception as e:
            current_app.logger.error(f"[report_generator] Error generating all reports: {e}")
            raise

# Global report generator instance
report_generator = ReportGenerator()
