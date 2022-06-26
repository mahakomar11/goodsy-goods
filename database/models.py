"""
Module with models of tables in database and errors.
"""
from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID as postgressUUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Item(Base):
    __tablename__ = "item"

    id = Column(postgressUUID(as_uuid=True), primary_key=True)
    name = Column(String(180), nullable=False)
    type = Column(String(180), nullable=False)
    price = Column(Integer, nullable=True)
    date = Column(String(180), nullable=False)


class Parent(Base):
    __tablename__ = "parent"

    id = Column(postgressUUID(as_uuid=True), ForeignKey(Item.id), primary_key=True)
    parentId = Column(postgressUUID(as_uuid=True), ForeignKey(Item.id))


class Stats(Base):
    __tablename__ = "stats"

    stat_id = Column(Integer, primary_key=True, autoincrement=True)
    id = Column(postgressUUID(as_uuid=True))
    name = Column(String(180), nullable=False)
    type = Column(String(180), nullable=False)
    price = Column(Integer, nullable=True)
    date = Column(String(180), nullable=False)
    parentId = Column(postgressUUID(as_uuid=True), nullable=True)


class DatabaseErrorInternal(Exception):
    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail
