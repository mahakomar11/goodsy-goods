from sqlalchemy import Column, DateTime, ForeignKey, Integer, String
from sqlalchemy.dialects.postgresql import UUID as postgressUUID
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class Item(Base):
    __tablename__ = "item"

    id = Column(postgressUUID(as_uuid=True), primary_key=True)
    name = Column(String(180), nullable=False)
    type = Column(String(180), nullable=False)
    price = Column(Integer, nullable=True)
    date = Column(DateTime, nullable=False)

    def dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Parent(Base):
    __tablename__ = "parent"

    id = Column(postgressUUID(as_uuid=True), ForeignKey(Item.id), primary_key=True)
    parentId = Column(postgressUUID(as_uuid=True), ForeignKey(Item.id))

    def dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class DatabaseErrorInternal(Exception):
    def __init__(self, detail: str):
        super().__init__(detail)
        self.detail = detail
