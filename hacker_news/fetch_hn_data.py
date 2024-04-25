from sqlalchemy.ext import declarative
from crate.client.sqlalchemy import types
import os
import sqlalchemy as sa


from hackernews_api import HackerNewsAPI


hn_api = HackerNewsAPI()

ID_MIN = 1
ID_MAX = 2

cratedb_pw = os.environ["CRATEDB_PASSWORD"]
CRATE_URI = f"crate://admin:{cratedb_pw}@data-engineering-meetup.aks1.westeurope.azure.cratedb.net:4200?ssl=true"


for _id in range(ID_MIN, ID_MAX):
    data = hn_api.fetch_data(_id)
    print(data)

engine = sa.create_engine(CRATE_URI, echo=True)
Base = declarative.declarative_base(bind=engine)


class HackerNewsItems(Base):
    __tablename__ = "items"
    # __table_args__ = {"crate_number_of_shards": 3}
    id = sa.Column(sa.Integer)
    deleted = sa.Column(sa.String)
    type = sa.Column(sa.String)
    by = sa.Column(sa.String)
    time = sa.Column(sa.Integer)
    text = sa.Column(sa.String)
    dead = sa.Column(sa.String)
    parent = sa.Column(sa.Integer)
    poll = sa.Column(sa.Integer)
    kids = sa.Column(sa.ARRAY(sa.Integer))
    url = sa.Column(sa.String)
    score = sa.Column(sa.Integer)
    title = sa.Column(sa.String)
    parts = sa.Column(sa.ARRAY(sa.Integer))
    descendats = sa.Column(sa.Integer)


# class HackerNewsUsers(Base):
#     __tablename__ = "users"
