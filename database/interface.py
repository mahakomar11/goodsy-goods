from math import floor
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.dialects.postgresql import insert
import contextlib
from dotenv import load_dotenv
import os
from dateutil import parser
from datetime import datetime, timedelta
from statistics import mean

from models import Item, Parent, Base

from unit_test import IMPORT_BATCHES


class DBInterface:
    def __init__(self, user, password, database_name, host, port):
        self.engine = create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
        )
        self.global_session = sessionmaker()
        self.global_session.configure(bind=self.engine)

    @contextlib.contextmanager
    def open_session(self, global_session: sessionmaker) -> Session:
        """
        Context manager that opens session with database

        :param global_session: sessionmaker binded with engine
        :return: session
        """
        session = global_session()
        yield session
        session.close()

    @staticmethod
    def _prepare_items_data(items, date):
        items_data = list()
        parents_data = list()
        for item in items:
            if 'price' not in item.keys():
                item['price'] = None
            item['date'] = parser.parse(date)
            items_data.append({k: v for k, v in item.items() if k != 'parentId'})
            parents_data.append(dict(id=item['id'], parentId=item['parentId']))
        return items_data, parents_data

    def post_items(self, items: list, date: str) -> None:
        """
        Put items to database.

        :param items: list of dict with keys: id, parentId,
        :param date: string of date in ISO 8601 format
        """
        items_data, parents_data = self._prepare_items_data(items, date)

        with self.open_session(self.global_session) as session:
            insertion = insert(Item).values(items_data)
            session.execute(insertion.on_conflict_do_update(index_elements=[Item.id], set_=insertion.excluded))

            if len(parents_data) != 0:
                insertion = insert(Parent).values(parents_data)
                session.execute(insertion.on_conflict_do_update(index_elements=[Parent.id], set_=insertion.excluded))
            session.commit()

    def delete_item(self, id: str) -> None:
        """
        Delete item with all its children from database.

        :param id: UUID of element to delete
        """
        with self.open_session(self.global_session) as session:
            self._delete_with_children(session, id)
            session.commit()

    def _delete_with_children(self, session: Session, parent_id: str) -> None:
        """
        Recursive method for deleting item and its subtree.

        :param session: opened db-session
        :param parent_id: UUID of element to delete
        """
        parent = session.get(Parent, parent_id)
        if parent:
            session.delete(parent)
        children_query = session.query(Parent.id).filter_by(parentId=parent_id)
        for row in children_query.all():
            self._delete_with_children(session, row[0])
        session.delete(session.get(Item, parent_id))

    def get_item(self, id: str) -> dict:
        """
        Get dict with item and all its subtree.

        :param id: UUID of element
        :return: item and all its subtree
        """
        with self.open_session(self.global_session) as session:
            item = session.get(Item, id)
            if item.type == 'OFFER':
                return item.dict()
            tree, prices = self._get_children(session, item.id, item.dict())
            tree['parentId'] = session.get(Parent, id).parentId
            return tree

    def _get_children(self, session: Session, parent_id: str, tree: dict) -> (dict, list):
        """
        Recursive method for getting subtree of item and calculating mean price for categories.

        :param session: opened db-session
        :param parent_id: UUID of element
        :param tree: tree, calculated at the previous step
        :return:    tree - tree with children and calculated price
                    prices - list of prices of child offers
        """
        children_query = (session
                          .query(Parent.id, Parent.parentId, Item.name, Item.type, Item.date, Item.price)
                          .filter_by(parentId=parent_id)
                          .join(Item, Parent.id == Item.id)
                          )

        children: list[dict] = list()
        prices: list[int] = list()
        for row in children_query.all():
            subtree = dict(row._mapping)
            if subtree['type'] == 'OFFER':
                children.append(subtree)
                prices.append(subtree['price'])
            else:
                child_subtree, child_prices = self._get_children(session, row[0], subtree)
                children.append(child_subtree)
                prices.extend(child_prices)

        tree['children'] = children
        if len(prices) > 0:
            tree['price'] = mean(prices)
        return tree, prices

    def get_updated_items(self, date: str) -> list[dict]:
        """
        Get offers that were updated in interval [date - 1d, date].

        :param date: string of date in ISO 8601 format
        :return: list with offers
        """
        date: datetime = parser.parse(date)
        date_since: datetime = date - timedelta(days=1)
        with self.open_session(self.global_session) as session:
            updated_items = (session
                             .query(Item.id, Item.name, Item.type, Item.price, Item.date, Parent.parentId)
                             .filter(Item.type == 'OFFER',
                                     Item.date >= date_since,
                                     Item.date <= date)
                             .join(Parent, Parent.id == Item.id, isouter=True)
                             ).all()
            return [dict(row._mapping) for row in updated_items]




load_dotenv()
DB = DBInterface(user=os.getenv('POSTGRES_USER'),
                 password=os.getenv('POSTGRES_PASSWORD'),
                 database_name=os.getenv('POSTGRES_DB'),
                 host='localhost',
                 port='5432'
                 )

engine = DB.engine
if not engine.dialect.has_table(engine.connect(), Item):
    Base.metadata.create_all(engine)


DB.post_items(IMPORT_BATCHES[0]['items'], IMPORT_BATCHES[0]['updateDate'])
DB.post_items(IMPORT_BATCHES[1]['items'], IMPORT_BATCHES[1]['updateDate'])
DB.post_items(IMPORT_BATCHES[2]['items'], IMPORT_BATCHES[2]['updateDate'])
DB.post_items(IMPORT_BATCHES[3]['items'], IMPORT_BATCHES[3]['updateDate'])
DB.post_items(IMPORT_BATCHES[4]['items'], IMPORT_BATCHES[4]['updateDate'])
# DB.delete_item('d515e43f-f3f6-4471-bb77-6b455017a2d2')
tree = DB.get_item('069cb8d7-bbdd-47d3-ad8f-82ef4c269df1')
updated_items = DB.get_updated_items('2022-02-02 13:01:00.000Z')
print('ok')