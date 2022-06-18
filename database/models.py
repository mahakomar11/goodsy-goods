from sqlalchemy import Column, String, Integer, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.dialects.postgresql import UUID as postgressUUID

Base = declarative_base()


class Item(Base):
    __tablename__ = 'item'

    id = Column(postgressUUID(as_uuid=True), primary_key=True)
    name = Column(String(180), nullable=False)
    type = Column(String(180), nullable=False)
    price = Column(Integer, nullable=True)
    date = Column(DateTime, nullable=False)

    def dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Parent(Base):
    __tablename__ = 'parent'

    id = Column(postgressUUID(as_uuid=True), ForeignKey(Item.id), primary_key=True)
    parentId = Column(postgressUUID(as_uuid=True), ForeignKey(Item.id))

    def dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}
