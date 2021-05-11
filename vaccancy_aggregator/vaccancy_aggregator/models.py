import sqlalchemy

from sqlalchemy import create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import Column, Integer, String, ForeignKey, DateTime, JSON
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.sql import func

from sqlalchemy.engine.url import URL

import settings


DeclarativeBase = declarative_base()

def db_connect():
    """
    Performs database connection using database settings from settings.py.
    Returns sqlalchemy engine instance
    """
    return create_engine(URL(**settings.DATABASE))


def create_deals_table(engine):
    """"""
    DeclarativeBase.metadata.create_all(engine)


class Vaccancies(DeclarativeBase):
    """Sqlalchemy deals model"""
    __tablename__ = "vaccancy"

    id = Column(Integer, primary_key=True)
    vaccancy = Column(JSON())
    timestamp = Column(DateTime(timezone=True), server_default=func.now())
