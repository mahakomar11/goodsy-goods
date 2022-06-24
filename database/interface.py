"""
Module with database interface class.
"""
import contextlib
from datetime import datetime, timedelta
from math import floor
from statistics import mean
from typing import Optional, Union
from uuid import UUID

from dateutil import parser
from sqlalchemy import create_engine, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql.dml import Insert
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.orm.query import Query
from sqlalchemy.sql.selectable import Subquery

from database.models import Base, DatabaseErrorInternal, Item, Parent, Stats


class DBInterface:
    """
    Class that provides methods for handling data in database.
    """

    def __init__(
        self,
        user: str,
        password: str,
        database_name: str,
        host: str,
        port: Union[str, int],
    ):
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

    def check_items_type(self, items_data: list[dict]) -> None:
        """
        Check if any of items from items_data exists in table Item with different type.

        :param items_data: list of dict with keys id, name, type, price, date
        """
        with self.open_session(self.global_session) as session:
            for item in items_data:
                item_in_db: Optional[Item] = session.get(Item, item["id"])
                if not item_in_db:
                    continue
                if item["type"] != item_in_db.type:
                    raise DatabaseErrorInternal(
                        f"Item with {item_in_db.id} exists in database as {item_in_db.type}. "
                        f"Changing type is prohibited."
                    )

    def post_items(self, items_data: list, parents_data: list) -> None:
        """
        Put items to database.

        :param items_data: list of dict with keys id, name, type, price, date
        :param parents_data: list of dict with keys: id, parentId
        """
        date: str = items_data[0]["date"]
        with self.open_session(self.global_session) as session:
            items_ids: set[UUID] = {item["id"] for item in items_data}
            parents_ids: set[UUID] = {
                p["parentId"] for p in parents_data if p["parentId"]
            }

            ancestors_ids: set[UUID] = set()
            if len(parents_ids) > 0:
                ancestors_ids: set[UUID] = self._get_all_ancestors(
                    session, parents_ids, parents_ids
                )
            # Update Item table
            insertion: Insert = insert(Item).values(items_data)
            session.execute(
                insertion.on_conflict_do_update(
                    index_elements=[Item.id], set_=insertion.excluded
                )
            )
            # Update Parent table
            if len(parents_data) != 0:
                insertion: Insert = insert(Parent).values(parents_data)
                session.execute(
                    insertion.on_conflict_do_update(
                        index_elements=[Parent.id], set_=insertion.excluded
                    )
                )
            # Update date of all ancestors
            if len(ancestors_ids) != 0:
                (
                    session.query(Item)
                    .filter(Item.id.in_(ancestors_ids))
                    .update({Item.date: date})
                )
            # Add all rows that changed to statistics
            changed_ids: set[UUID] = ancestors_ids | items_ids
            select_changed: Query = (
                session.query(
                    Item.id,
                    Item.name,
                    Item.type,
                    Item.price,
                    Item.date,
                    Parent.parentId,
                )
                .filter(Item.id.in_(changed_ids))
                .join(Parent, Parent.id == Item.id, isouter=True)
            )
            session.execute(
                insert(Stats).from_select(
                    ["id", "name", "type", "price", "date", "parentId"], select_changed
                )
            )
            session.commit()

    def _get_all_ancestors(
        self, session, current_parents_ids: set[UUID], all_ancestors: set[UUID]
    ) -> set[UUID]:
        """
        Recursive method for getting ids of all ancestors of parents_ids.

        :param session: opened db-session
        :param parents_ids: set of UUIDs of
        :return:
        """
        next_parents: list[Parent] = (
            session.query(Parent).filter(Parent.id.in_(current_parents_ids)).all()
        )
        next_parents_ids: set[UUID] = {p.parentId for p in next_parents if p.parentId}
        new_ids: set[UUID] = next_parents_ids - all_ancestors
        if len(new_ids) > 0:
            all_ancestors.update(new_ids)
            self._get_all_ancestors(session, new_ids, all_ancestors)
        return all_ancestors

    def delete_item(self, id: UUID) -> None:
        """
        Delete item with all its children from database.

        :param id: UUID of element to delete
        """
        with self.open_session(self.global_session) as session:
            item: Optional[Item] = session.get(Item, id)
            if not item:
                raise DatabaseErrorInternal(f"Item {id} not found in database")

            self._delete_with_children(session, id)
            session.commit()

    def _delete_with_children(self, session: Session, parent_id: UUID) -> None:
        """
        Recursive method for deleting item and its subtree.

        :param session: opened db-session
        :param parent_id: UUID of element to delete
        """
        parent: Optional[Parent] = session.get(Parent, parent_id)
        if parent:
            session.delete(parent)
        children_query: Query = session.query(Parent.id).filter_by(parentId=parent_id)
        for row in children_query.all():
            self._delete_with_children(session, row[0])
        session.delete(session.get(Item, parent_id))
        session.query(Stats).filter_by(id=parent_id).delete()

    def get_item(self, id: UUID) -> dict:
        """
        Get dict with item and all its subtree.

        :param id: UUID of element
        :return: item and all its subtree
        """
        with self.open_session(self.global_session) as session:
            item: Optional[Item] = session.get(Item, id)
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
        children_query: Query = (
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
            tree["price"] = floor(mean(prices))
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
            updated_stats: list[Stats] = (
                session.query(Stats.id)
                .group_by(Stats.id, Stats.type)
                .having(
                    (func.count(Stats.id) > 1)
                    & (Stats.type == "OFFER")
                    & (
                        text(
                            f"MAX(CAST({Stats.date} AS TIMESTAMP WITH TIME ZONE)) >= "
                            f"CAST('{date_since}' AS TIMESTAMP WITH TIME ZONE)"
                        )
                    )
                    & (
                        text(
                            f"MAX(CAST({Stats.date} AS TIMESTAMP WITH TIME ZONE)) <= "
                            f"CAST('{date}' AS TIMESTAMP WITH TIME ZONE)"
                        )
                    )
                )
                .all()
            )
            updated_ids: list[UUID] = [row.id for row in updated_stats]
            updated_items: list[Item] = (
                session.query(
                    Item.id,
                    Item.name,
                    Item.type,
                    Item.date,
                    Item.price,
                    Parent.parentId,
                )
                .where(Item.id.in_(updated_ids))
                .join(Parent, Item.id == Parent.id, isouter=True)
                .all()
            )
            return [dict(row._mapping) for row in updated_items]

    def get_statistics(
        self, id: UUID, date_start: Optional[str], date_end: Optional[str]
    ) -> list[dict]:
        """
        Collect statistic of item with changes of price.

        :param id: UUID of element
        :param date_start: date statistics is collecting from, if None, collect from the most beginning
        :param date_end: date statistics is collecting to, if None, collect to now
        :return: list of item's info from different date
        """
        with self.open_session(self.global_session) as session:
            item: Optional[Item] = session.get(Item, id)
            if not item:
                raise DatabaseErrorInternal(f"Item {id} not found in database")

            # Get all rows in Stats table for item with id and date in interval
            if date_start and date_end:
                item_rows: list[Stats] = (
                    session.query(Stats)
                    .filter(
                        Stats.id == id,
                        text(
                            f"CAST({Stats.date} AS TIMESTAMP WITH TIME ZONE) >= "
                            f"CAST('{date_start}' AS TIMESTAMP WITH TIME ZONE)"
                        ),
                        text(
                            f"CAST({Stats.date} AS TIMESTAMP WITH TIME ZONE) < "
                            f"CAST('{date_end}' AS TIMESTAMP WITH TIME ZONE)"
                        ),
                    )
                    .all()
                )
            elif date_start:
                item_rows: list[Stats] = (
                    session.query(Stats)
                    .filter(
                        Stats.id == id,
                        text(
                            f"CAST({Stats.date} AS TIMESTAMP WITH TIME ZONE) >= "
                            f"CAST('{date_start}' AS TIMESTAMP WITH TIME ZONE)"
                        ),
                    )
                    .all()
                )
            elif date_end:
                item_rows: list[Stats] = (
                    session.query(Stats)
                    .filter(
                        Stats.id == id,
                        text(
                            f"CAST({Stats.date} AS TIMESTAMP WITH TIME ZONE) < "
                            f"CAST('{date_end}' AS TIMESTAMP WITH TIME ZONE)"
                        ),
                    )
                    .all()
                )
            else:
                item_rows: list[Stats] = (
                    session.query(Stats).filter(Stats.id == id).all()
                )

            # Calculate mean price for category
            answer: list[dict] = list()
            for row in item_rows:
                row_dict = row.dict()
                if not row.price:
                    prices: list[int] = self._get_child_prices(
                        session, row.id, row.date
                    )
                    if len(prices) > 0:
                        row_dict["price"] = floor(mean(prices))
                answer.append(row_dict)
            return answer

    def _get_child_prices(
        self, session: Session, parent_id: str, date: str
    ) -> list[int]:
        """
        Recursive method for getting prices of child items.

        :param session: opened db-session
        :param parent_id: UUID of element
        :param date: date, when parent_id added
        :return: list of child prices
        """
        # Get last update date for all direct children of parent_id that is not later than parent's date
        sub_query: Subquery = (
            session.query(
                Stats.id,
                func.max(text("CAST(stats.date AS TIMESTAMP WITH TIME ZONE)")).label(
                    "last_date"
                ),
            )
            .filter(
                Stats.parentId == parent_id,
                text(
                    f"CAST({Stats.date} AS TIMESTAMP WITH TIME ZONE) <= "
                    f"CAST('{date}' AS TIMESTAMP WITH TIME ZONE)"
                ),
            )
            .group_by(Stats.id)
            .subquery()
        )
        # Get all fields of direct children that were updated the closest to parent's date
        children: list[Stats] = (
            session.query(Stats)
            .join(
                sub_query,
                (Stats.id == sub_query.c.id)
                & (
                    text(f"CAST({Stats.date} AS TIMESTAMP WITH TIME ZONE)")
                    == sub_query.c.last_date
                ),
            )
            .all()
        )
        # Collect prices of all successor
        prices: list[int] = list()
        for child in children:
            price: Optional[int] = child.price
            if price:
                prices.append(price)
            else:
                child_prices: list[int] = self._get_child_prices(
                    session, child.id, child.date
                )
                prices.extend(child_prices)

        return prices
