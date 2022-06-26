"""
Module with database interface class.
"""
import contextlib
from datetime import datetime, timedelta
from math import floor
from statistics import mean
from typing import Optional, Union
from uuid import UUID

import databases
from databases.backends.postgres import Record
from dateutil import parser
from sqlalchemy import create_engine, func, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql.dml import Insert
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.sql.expression import bindparam
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
        self.db = databases.Database(
            f"postgresql+asyncpg://{user}:{password}@{host}:{port}/{database_name}"
        )
        engine = create_engine(
            f"postgresql://{user}:{password}@{host}:{port}/{database_name}"
        )
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

    async def check_items_type(self, items_data: list[dict]) -> None:
        """
        Check if any of items from items_data exists in table Item with different type.

        :param items_data: list of dict with keys id, name, type, price, date
        """

        for item in items_data:
            query = "SELECT * FROM item WHERE id = :id"
            item_in_db: Optional[Item] = await self.db.fetch_one(
                query=query, values={"id": item["id"]}
            )
            if not item_in_db:
                continue
            if item["type"] != item_in_db.type:
                raise DatabaseErrorInternal(
                    f"Item with {item_in_db.id} exists in database as {item_in_db.type}. "
                    f"Changing type is prohibited."
                )

    async def post_items(self, items_data: list, parents_data: list) -> None:
        """
        Put items to database.

        :param items_data: list of dict with keys id, name, type, price, date
        :param parents_data: list of dict with keys: id, parentId
        """
        date: str = items_data[0]["date"]
        items_ids: set[UUID] = {item["id"] for item in items_data}
        parents_ids: set[UUID] = {p["parentId"] for p in parents_data if p["parentId"]}

        ancestors_ids: set[UUID] = set()
        if len(parents_ids) > 0:
            ancestors_ids: set[UUID] = await self._get_all_ancestors(
                parents_ids, parents_ids
            )
        # Update Item table
        insertion: Insert = insert(Item).values(items_data)
        await self.db.execute(
            insertion.on_conflict_do_update(
                index_elements=[Item.id], set_=insertion.excluded
            )
        )
        # Update Parent table
        if len(parents_data) != 0:
            insertion: Insert = insert(Parent).values(parents_data)
            await self.db.execute(
                insertion.on_conflict_do_update(
                    index_elements=[Parent.id], set_=insertion.excluded
                )
            )
        # Update date of all ancestors
        if len(ancestors_ids) != 0:
            query = "UPDATE item SET date = :date WHERE id in :ancestors_ids"
            query = text(query).bindparams(
                bindparam("ancestors_ids", value=list(ancestors_ids), expanding=True)
            )
            query = query.bindparams(date=date)
            await self.db.execute(query=query)
        # Add all rows that changed to statistics
        changed_ids: set[UUID] = ancestors_ids | items_ids
        query = """
            INSERT INTO stats (id, name, type, price, date, "parentId")
            SELECT item.id, name, type, price, date, parent."parentId"
            FROM item LEFT JOIN parent ON item.id = parent.id
            WHERE item.id in :changed_ids
        """
        query = text(query).bindparams(
            bindparam("changed_ids", value=list(changed_ids), expanding=True)
        )
        await self.db.execute(query=query)

    async def _get_all_ancestors(
        self, current_parents_ids: set[UUID], all_ancestors: set[UUID]
    ) -> set[UUID]:
        """
        Recursive method for getting ids of all ancestors of parents_ids.

        :param parents_ids: set of UUIDs of current items for which to look ancestors
        :param all_ancestors: set of UUIDs of all ancestors (incude collected on previous steps)
        :return: all_ancestors - set of UUIDs of all ancestors (include found on that step)
        """
        query = "SELECT * FROM parent WHERE id in :current_parents_ids"
        query = text(query).bindparams(
            bindparam(
                "current_parents_ids", value=list(current_parents_ids), expanding=True
            )
        )
        next_parents: list[Parent] = await self.db.fetch_all(query=query)
        next_parents_ids: set[UUID] = {p.parentId for p in next_parents if p.parentId}
        new_ids: set[UUID] = next_parents_ids - all_ancestors
        if len(new_ids) > 0:
            all_ancestors.update(new_ids)
            await self._get_all_ancestors(new_ids, all_ancestors)
        return all_ancestors

    async def delete_item(self, id: UUID) -> None:
        """
        Delete item with all its children from database.

        :param id: UUID of element to delete
        """
        query = "SELECT * FROM item WHERE id = :id"
        item: Optional[Item] = await self.db.fetch_one(query=query, values={"id": id})
        if not item:
            raise DatabaseErrorInternal(f"Item {id} not found in database")

        await self._delete_with_children(id)

    async def _delete_with_children(self, parent_id: UUID) -> None:
        """
        Recursive method for deleting item and its subtree.

        :param session: opened db-session
        :param parent_id: UUID of element to delete
        """
        # Delete element from "parent" table
        query = "DELETE FROM parent WHERE id = :parent_id"
        await self.db.execute(query=query, values={"parent_id": parent_id})

        # Find children of the element and delete them
        query = """SELECT id FROM parent WHERE "parentId" = :parent_id"""
        children_rows: list[Parent] = await self.db.fetch_all(
            query=query, values={"parent_id": parent_id}
        )
        for child in children_rows:
            await self._delete_with_children(child.id)

        # Delete element from "item" table
        query = "DELETE FROM item WHERE id = :parent_id"
        await self.db.execute(query=query, values={"parent_id": parent_id})

        # Delete element from "stats" table
        query = "DELETE FROM stats WHERE id = :parent_id"
        await self.db.execute(query=query, values={"parent_id": parent_id})

    async def get_item(self, id: UUID) -> dict:
        """
        Get dict with item and all its subtree.

        :param id: UUID of element
        :return: item and all its subtree
        """
        # Check if element is in database
        query = "SELECT * FROM item WHERE id = :id"
        item: Optional[Record] = await self.db.fetch_one(query=query, values={"id": id})
        if not item:
            raise DatabaseErrorInternal(f"Item {id} not found in database")

        if item.type == "OFFER":
            return dict(item._mapping)
        tree, prices = await self._get_children(item.id, dict(item._mapping))
        query = """SELECT "parentId" FROM parent WHERE id = :id"""
        tree["parentId"] = (
            await self.db.fetch_one(query=query, values={"id": id})
        ).parentId
        return tree

    async def _get_children(self, parent_id: str, tree: dict) -> (dict, list):
        """
        Recursive method for getting subtree of item and calculating mean price for categories.

        :param parent_id: UUID of element
        :param tree: tree, calculated at the previous step
        :return:    tree - tree with children and calculated price
                    prices - list of prices of child offers
        """
        query = """
        SELECT parent.id as id, "parentId", name, type, date, price
        FROM item
        JOIN parent ON parent.id = item.id
        WHERE "parentId" = :parent_id
        """
        children_rows: list[Record] = await self.db.fetch_all(
            query=query, values={"parent_id": parent_id}
        )

        children: list[dict] = list()
        prices: list[int] = list()
        for row in children_rows:
            subtree = dict(row._mapping)
            if subtree["type"] == "OFFER":
                children.append(subtree)
                prices.append(subtree["price"])
            else:
                child_subtree, child_prices = await self._get_children(row.id, subtree)
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
