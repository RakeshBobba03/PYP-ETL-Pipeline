from app import db
from sqlalchemy.dialects.postgresql import JSON

class Product(db.Model):
    __tablename__ = 'products'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String, nullable=False)
    ext_id = db.Column(db.String, unique=True)
    def __repr__(self):
        return f"<Product {self.title}>"

class Ingredient(db.Model):
    __tablename__ = 'ingredients'
    id = db.Column(db.Integer, primary_key=True)
    title = db.Column(db.String, nullable=False)
    ext_id = db.Column(db.String, unique=True)
    def __repr__(self):
        return f"<Ingredient {self.title}>"

class MemberSubmission(db.Model):
    __tablename__ = 'member_submissions'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    members = db.relationship('Member', backref='submission', cascade="all, delete-orphan")

class Member(db.Model):
    __tablename__ = 'members'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)   # business name
    contact_email = db.Column(db.String)
    street_address1 = db.Column(db.String)
    city1 = db.Column(db.String)
    country1 = db.Column(db.String)
    company_bio = db.Column(db.String)
    submission_id = db.Column(db.Integer, db.ForeignKey('member_submissions.id'), nullable=False)
    new_items = db.relationship('NewItem', backref='member', cascade="all, delete-orphan")

class NewItem(db.Model):
    __tablename__ = 'new_items'
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String, nullable=False)
    type = db.Column(db.String, nullable=False)    # 'product' or 'ingredient'
    member_id = db.Column(db.Integer, db.ForeignKey('members.id'), nullable=False)
    matched_canonical_id = db.Column(db.String, nullable=True)
    score = db.Column(db.Float, nullable=True)
    resolved = db.Column(db.Boolean, default=False)
    review = db.relationship('MatchReview', back_populates='new_item', uselist=False)
    ignored = db.Column(db.Boolean, default=False)

class MatchReview(db.Model):
    __tablename__ = 'match_reviews'
    id = db.Column(db.Integer, primary_key=True)
    new_item_id = db.Column(db.Integer, db.ForeignKey('new_items.id'), nullable=False, unique=True)
    suggested_name = db.Column(db.String, nullable=False)
    suggested_ext_id = db.Column(db.String, nullable=True)
    score = db.Column(db.Float, nullable=False)
    alternatives = db.Column(JSON)
    approved = db.Column(db.Boolean, nullable=True)
    new_item = db.relationship('NewItem', back_populates='review')
