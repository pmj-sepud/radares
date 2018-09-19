import os
import json
from sqlalchemy import create_engine, MetaData
from sqlalchemy.engine.url import URL


def connect():
    DATABASE = {
        'drivername': os.environ.get("RADARS_DRIVERNAME"),
        'host': os.environ.get("RADARS_HOST"), 
        'port': os.environ.get("RADARS_PORT"),
        'username': os.environ.get("RADARS_USERNAME"),
        'password': os.environ.get("RADARS_PASSWORD"),
        'database': os.environ.get("RADARS_DATABASE"),
        }

    db_url = URL(**DATABASE)
    engine = create_engine(db_url)
    meta = MetaData()
    meta.bind = engine
    meta.reflect(schema="radars")
    return meta
