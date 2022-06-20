import contextlib
from datetime import datetime, timedelta
from statistics import mean

from dateutil import parser
from sqlalchemy import create_engine
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session, sessionmaker

from database.models import Base, DatabaseErrorInternal, Item, Parent


class DBInterface:
    def __init__(self, user, password, database_name, host, port):
        engine = create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
        )
        self.global_session = sessionmaker()
        self.global_session.configure(bind=engine)
        if not engine.dialect.has_table(engine.connect(), Item):
            Base.metadata.create_all(engine)

    @contextlib.contextmanager
    def open_session(self, global_session: sessionmaker) -> Session:
        """
        Context manager that opens session with database

        :param global_session: sessionmaker binded with engine
        :return: session
        """
        session: Session = global_session()
        yield session
        session.close()

    def check_items_type(self, items_data: list):
        with self.open_session(self.global_session) as session:
            for item in items_data:
                item_in_db: Item = session.get(Item, item["id"])
                if not item_in_db:
                    continue
                if item["type"] != item_in_db.type:
                    print(item["type"])
                    print(item_in_db.type)
                    raise DatabaseErrorInternal(
                        f"Item with {item_in_db.id} exists in database as {item_in_db.type}. "
                        f"Changing type is prohibited."
                    )

    def post_items(self, items_data: list, parents_data: list) -> None:
        """
        Put items to database.

        :param items_data: list of dict with keys id, name, type, price, date
        :param parents_data: list of dict with keys: id, parentId
        :param date: string of date in ISO 8601 format
        """
        with self.open_session(self.global_session) as session:
            insertion = insert(Item).values(items_data)
            session.execute(
                insertion.on_conflict_do_update(
                    index_elements=[Item.id], set_=insertion.excluded
                )
            )

            if len(parents_data) != 0:
                insertion = insert(Parent).values(parents_data)
                session.execute(
                    insertion.on_conflict_do_update(
                        index_elements=[Parent.id], set_=insertion.excluded
                    )
                )
            session.commit()

    def delete_item(self, id: str) -> None:
        """
        Delete item with all its children from database.

        :param id: UUID of element to delete
        """
        with self.open_session(self.global_session) as session:
            item: Item = session.get(Item, id)
            if not item:
                raise DatabaseErrorInternal(f"Item {id} not found in database")

            self._delete_with_children(session, id)
            session.commit()

    def _delete_with_children(self, session: Session, parent_id: str) -> None:
        """
        Recursive method for deleting item and its subtree.

        :param session: opened db-session
        :param parent_id: UUID of element to delete
        """
        parent: Parent = session.get(Parent, parent_id)
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
            item: Item = session.get(Item, id)
            if not item:
                raise DatabaseErrorInternal(f"Item {id} not found in database")

            if item.type == "OFFER":
                return item.dict()
            tree, prices = self._get_children(session, item.id, item.dict())
            tree["parentId"] = session.get(Parent, id).parentId
            return tree

    def _get_children(
        self, session: Session, parent_id: str, tree: dict
    ) -> (dict, list):
        """
        Recursive method for getting subtree of item and calculating mean price for categories.

        :param session: opened db-session
        :param parent_id: UUID of element
        :param tree: tree, calculated at the previous step
        :return:    tree - tree with children and calculated price
                    prices - list of prices of child offers
        """
        children_query = (
            session.query(
                Parent.id, Parent.parentId, Item.name, Item.type, Item.date, Item.price
            )
            .filter_by(parentId=parent_id)
            .join(Item, Parent.id == Item.id)
        )

        children: list[dict] = list()
        prices: list[int] = list()
        for row in children_query.all():
            subtree = dict(row._mapping)
            if subtree["type"] == "OFFER":
                children.append(subtree)
                prices.append(subtree["price"])
            else:
                child_subtree, child_prices = self._get_children(
                    session, row[0], subtree
                )
                children.append(child_subtree)
                prices.extend(child_prices)

        tree["children"] = children
        if len(prices) > 0:
            tree["price"] = mean(prices)
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
            updated_items = (
                session.query(
                    Item.id,
                    Item.name,
                    Item.type,
                    Item.price,
                    Item.date,
                    Item.datetime,
                    Parent.parentId,
                )
                .filter(
                    Item.type == "OFFER",
                    Item.datetime >= date_since,
                    Item.datetime <= date,
                )
                .join(Parent, Parent.id == Item.id, isouter=True)
            ).all()
            return [dict(row._mapping) for row in updated_items]
