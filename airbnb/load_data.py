import pyarrow.parquet as pq
from pyarrow import parquet as pq
from uuid import uuid4
from sqlalchemy.orm import scoped_session, sessionmaker, declarative_base
import sqlalchemy as sa
from crate.client.sqlalchemy import types
import os

cratedb_pw = os.environ["CRATEDB_PASSWORD"]
CRATE_URI = f"crate://admin:{cratedb_pw}@data-engineering-meetup.aks1.westeurope.azure.cratedb.net:4200?ssl=true"

engine = sa.create_engine(CRATE_URI, echo=True)
session = scoped_session(sessionmaker())
Base = declarative_base()

session.configure(bind=engine, autoflush=False, expire_on_commit=False)

parquet_path = "data/part{}.parquet"

dataset = pq.ParquetFile(parquet_path.format(1))


class Airbnb(Base):
    __tablename__ = "airbnb_raw"
    device = sa.Column(sa.String)
    ecommerce = sa.Column(types.ObjectType)
    event_name = sa.Column(sa.String)
    event_previous_timestamp = sa.Column(sa.BigInteger)
    event_timestamp = sa.Column(sa.BigInteger, primary_key=True)
    geo = sa.Column(types.ObjectType)
    items = sa.Column(types.ObjectArray)
    traffic_source = sa.Column(sa.String)
    user_first_touch_timestamp = sa.Column(sa.BigInteger)
    user_id = sa.Column(sa.String)


Base.metadata.create_all(engine)

BATCH_SIZE = 60000

for batch in dataset.iter_batches(batch_size=BATCH_SIZE):
    batch_row = batch.to_pylist()
    session.execute(Airbnb.__table__.insert(), batch_row)
    session.commit()
