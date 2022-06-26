"""
Module with database interface class.
"""
from datetime import datetime, timedelta
from math import floor
from statistics import mean
from typing import Optional, Union
from uuid import UUID

import databases
from databases.backends.postgres import Record
from dateutil import parser
from sqlalchemy import create_engine, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.dialects.postgresql.dml import Insert
from sqlalchemy.sql.elements import TextClause
from sqlalchemy.sql.expression import bindparam

from database.models import Base, DatabaseErrorInternal, Item, Parent


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

    async def check_items_type(self, items_data: list[dict]) -> None:
        """
        Check if any of items from items_data exists in table Item with different type.

        :param items_data: list of dict with keys id, name, type, price, date
        """

        for item in items_data:
            query = "SELECT * FROM item WHERE id = :id"
            item_in_db: Optional[Record] = await self.db.fetch_one(
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
            query: TextClause = text(query).bindparams(
                bindparam("ancestors_ids", value=list(ancestors_ids), expanding=True)
            )
            query: TextClause = query.bindparams(date=date)
            await self.db.execute(query=query)
        # Add all rows that changed to statistics
        changed_ids: set[UUID] = ancestors_ids | items_ids
        query = """
            INSERT INTO stats (id, name, type, price, date, "parentId")
            SELECT item.id, name, type, price, date, parent."parentId"
            FROM item LEFT JOIN parent ON item.id = parent.id
            WHERE item.id in :changed_ids
        """
        query: TextClause = text(query).bindparams(
            bindparam("changed_ids", value=list(changed_ids), expanding=True)
        )
        await self.db.execute(query=query)

    async def _get_all_ancestors(
        self, current_parents_ids: set[UUID], all_ancestors: set[UUID]
    ) -> set[UUID]:
        """
        Recursive method for getting ids of all ancestors of parents_ids.

        :param current_parents_ids: set of UUIDs of current items for which to look ancestors
        :param all_ancestors: set of UUIDs of all ancestors (incude collected on previous steps)
        :return: all_ancestors - set of UUIDs of all ancestors (include found on that step)
        """
        query = "SELECT * FROM parent WHERE id in :current_parents_ids"
        query: TextClause = text(query).bindparams(
            bindparam(
                "current_parents_ids", value=list(current_parents_ids), expanding=True
            )
        )
        next_parents: list[Record] = await self.db.fetch_all(query=query)
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
        item: Optional[Record] = await self.db.fetch_one(query=query, values={"id": id})
        if not item:
            raise DatabaseErrorInternal(f"Item {id} not found in database")

        await self._delete_with_children(id)

    async def _delete_with_children(self, parent_id: UUID) -> None:
        """
        Recursive method for deleting item and its subtree.

        :param parent_id: UUID of element to delete
        """
        # Delete element from "parent" table
        query = "DELETE FROM parent WHERE id = :parent_id"
        await self.db.execute(query=query, values={"parent_id": parent_id})

        # Find children of the element and delete them
        query = """SELECT id FROM parent WHERE "parentId" = :parent_id"""
        children_rows: list[Record] = await self.db.fetch_all(
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

    async def get_updated_items(self, date: str) -> list[dict]:
        """
        Get offers that were updated in interval [date - 1d, date].

        :param date: string of date in ISO 8601 format
        :return: list with offers
        """
        date_to: datetime = parser.parse(date)
        date_since: datetime = date_to - timedelta(days=1)

        # Get ids of offers that were updated (count(id) > 1) before date_to
        # and the last date of that update is after date_since
        # then retrieve offers with these ids from table "item" and "parent"
        query = """
            WITH updated_ids AS
                (SELECT id
                FROM stats
                WHERE date::timestamp with time zone <= :date_to
                GROUP BY id, type
                HAVING count(id) > 1 AND type = 'OFFER'
                    AND max(date::timestamp with time zone) >= :date_since)
            SELECT item.id AS id, name, type, price, date, "parentId"
            FROM item
            JOIN updated_ids ON item.id = updated_ids.id
            LEFT JOIN parent ON item.id = parent.id
        """
        updated_items: list[Record] = await self.db.fetch_all(
            query=query, values={"date_to": date_to, "date_since": date_since}
        )
        return [dict(row._mapping) for row in updated_items]

    async def get_statistics(
        self, id: UUID, date_start: Optional[str], date_end: Optional[str]
    ) -> list[dict]:
        """
        Collect statistic of item with changes of price.

        :param id: UUID of element
        :param date_start: date statistics is collecting from, if None, collect from the most beginning
        :param date_end: date statistics is collecting to, if None, collect to now
        :return: list of item's info from different date
        """
        # Check if item in database
        query = "SELECT * FROM item WHERE id = :id"
        item: Optional[Record] = await self.db.fetch_one(query=query, values={"id": id})
        if not item:
            raise DatabaseErrorInternal(f"Item {id} not found in database")

        # Get all rows in Stats table for item with id and date in interval
        if date_start and date_end:
            query = """
                SELECT * FROM stats
                WHERE id = :id
                    AND date::timestamp with time zone >= :date_start
                    AND date::timestamp with time zone < :date_end
            """
            values = {
                "id": id,
                "date_start": parser.parse(date_start),
                "date_end": parser.parse(date_end),
            }
        elif date_start:
            query = """
                SELECT * FROM stats
                WHERE id = :id
                    AND date::timestamp with time zone >= :date_start
            """
            values = {"id": id, "date_start": parser.parse(date_start)}
        elif date_end:
            query = """
                SELECT * FROM stats
                WHERE id = :id
                    AND date::timestamp with time zone < :date_end
            """
            values = {"id": id, "date_end": parser.parse(date_end)}
        else:
            query = "SELECT * FROM stats WHERE id = :id"
            values = {"id": id}

        item_rows: list[Record] = await self.db.fetch_all(query=query, values=values)

        # Calculate mean price for category
        answer: list[dict] = list()
        for row in item_rows:
            row_dict = dict(row._mapping)
            if not row.price:
                prices: list[int] = await self._get_child_prices(row.id, row.date)
                if len(prices) > 0:
                    row_dict["price"] = floor(mean(prices))
            answer.append(row_dict)
        return answer

    async def _get_child_prices(self, parent_id: str, parent_date: str) -> list[int]:
        """
        Recursive method for getting prices of child items.

        :param parent_id: UUID of element
        :param parent_date: date, when parent_id added
        :return: list of child prices
        """
        # Get last update date (not later than parent_date) for all direct children of parent_id,
        # then get all fields of direct children the closest to parent's date
        query = """
            WITH last_updated_children AS (
                SELECT id, max(date::timestamp with time zone) as last_date
                FROM stats
                WHERE "parentId" = :parent_id
                    AND date::timestamp with time zone <= :date
                GROUP BY id
            )
            SELECT stats.id AS id, price, date
            FROM stats
            JOIN last_updated_children ON stats.id = last_updated_children.id
                AND stats.date::timestamp with time zone = last_updated_children.last_date
        """
        children: list[Record] = await self.db.fetch_all(
            query=query,
            values={"parent_id": parent_id, "date": parser.parse(parent_date)},
        )
        # Collect prices of all successor
        prices: list[int] = list()
        for child in children:
            price: Optional[int] = child.price
            if price:
                prices.append(price)
            else:
                child_prices: list[int] = await self._get_child_prices(
                    child.id, child.date
                )
                prices.extend(child_prices)

        return prices
