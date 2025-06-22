import pandas as pd
from pathlib import Path
from sqlalchemy import (
    create_engine, Column, Integer, String, Float, Text, DateTime, ForeignKey
)
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.exc import IntegrityError

# Setup paths
BASE_DIR = Path(__file__).resolve().parent.parent
DB_DIR = BASE_DIR / "db"
DB_DIR.mkdir(exist_ok=True)
DB_PATH = DB_DIR / "my_local_db.sqlite"
folder_path = BASE_DIR / 'sample_data'

# Setup database
Base = declarative_base()
engine = create_engine(f'sqlite:///{DB_PATH}', echo=False)
Session = sessionmaker(bind=engine)
session = Session()

# Define schema
class Creator(Base):
    __tablename__ = 'creators'
    id = Column(Integer, primary_key=True)
    first_name = Column(Text)
    last_name = Column(Text)
    email = Column(Text)
    join_date = Column(DateTime)
    last_post_date = Column(DateTime)

class Customer(Base):
    __tablename__ = 'customers'
    id = Column(Integer, primary_key=True)
    first_name = Column(Text)
    last_name = Column(Text)
    email = Column(Text)
    join_date = Column(DateTime)

class Transaction(Base):
    __tablename__ = 'transactions'
    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey('customers.id'))
    creator_id = Column(Integer, ForeignKey('creators.id'))
    transaction_date = Column(DateTime)
    amount_usd = Column(Float)
    transaction_type = Column(Text)

# Create tables
Base.metadata.create_all(engine)

# Map CSVs to models and their date columns
files = {
    "creators_2023": (Creator, ["join_date", "last_post_date"]),
    "customers_2023": (Customer, ["join_date"]),
    "transactions_2023_2024": (Transaction, ["transaction_date"]),
}

# Load and insert data
for file_name, (model, date_cols) in files.items():
    csv_path = folder_path / f"{file_name}.csv"
    df = pd.read_csv(csv_path)

    # Normalize column names
    df.columns = [c.strip().lower() for c in df.columns]

    # Convert date columns
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], format="%m/%d/%Y", errors="coerce")

    # Drop rows with invalid dates or missing required fields
    df = df.dropna(subset=date_cols)

    # Convert rows to model instances
    rows = [model(**row.to_dict()) for _, row in df.iterrows()]
    try:
        session.bulk_save_objects(rows)
        session.commit()
        print(f"✅ Loaded {len(rows)} rows into '{model.__tablename__}' table.")
    except IntegrityError as e:
        session.rollback()
        print(f"❌ Error loading {file_name}: {e}")

# Done
print("🎉 All data loaded into SQLite!")
