from datetime import datetime
from typing import Union
from uuid import UUID

from fastapi.responses import JSONResponse

from api.models import BadRequestError, NotFoundError, PostImportsRequest
from database.interface import DBInterface
from database.models import DatabaseErrorInternal


def validate_isoformat(date: str):
    if not isinstance(date, str):
        raise TypeError("Date should be string")
    try:
        datetime.fromisoformat(date.replace("Z", "+00:00").replace("z", "+00:00"))
    except ValueError:
        raise ValueError("Date should be in ISO 8601 format")


class Core:
    """Core class.

    Interface with handlers for endpoints.
    """

    def __init__(self, database: DBInterface):
        """
        :param database: instance of DBInterface
        """
        self.database: DBInterface = database

    @staticmethod
    def _prepare_items_data(items: list[dict], date: str) -> (list[dict], list[dict]):
        """
        Static method for preparing dict of items for uploading to database.

        :param items: list of dicts like {'id': , 'name': , 'price': , 'type': , 'parentId'}
        :param date: datetime in ISO 8601 format
        :return:
            items_data - list of dicts with keys 'id', 'name', 'price', 'type'
            parents_data - list of dict with keys 'id', 'parentId'
        """
        items_data = list()
        parents_data = list()
        for item in items:
            if "price" not in item.keys():
                item["price"] = None
            item["date"] = date
            item["type"] = item["type"].value

            items_data.append({k: v for k, v in item.items() if k != "parentId"})
            parents_data.append(dict(id=item["id"], parentId=item["parentId"]))
        return items_data, parents_data

    def post_imports(
        self, items_to_post: PostImportsRequest
    ) -> Union[None, JSONResponse]:
        """
        Handler for posting imports.

        :param items_to_post: Pydantic model of {'item': list of items, 'updateTime': datetime in ISO 8601 format}
        :return:
            None, if posting succeeded,
                or BadRequestError, if there are duplicated ids in request or attempt to update type of item
        """
        items_to_post = items_to_post.dict()
        items: list[dict] = items_to_post["items"]
        date: str = items_to_post["updateDate"]

        try:
            validate_isoformat(date)
        except (TypeError, ValueError) as e:
            return JSONResponse(
                status_code=400, content=BadRequestError(message=str(e)).dict()
            )

        items_ids: list[str] = [item["id"] for item in items]
        if len(items_ids) != len(set(items_ids)):
            return JSONResponse(
                status_code=400,
                content=BadRequestError(message="Items contain duplicated ids"),
            )

        items_data, parents_data = self._prepare_items_data(items, date)

        try:
            self.database.check_items_type(items_data)
        except DatabaseErrorInternal as e:
            return JSONResponse(
                status_code=400, content=BadRequestError(message=e.detail).dict()
            )

        # TODO: catch exceptions
        self.database.post_items(items_data, parents_data)

    def delete_id(self, id: UUID) -> Union[None, JSONResponse]:
        """
        Handler for deleting item by id.

        :param id: UUID of item
        :return: None, if deleting succeeded,
            or NotFoundError, if there is no item with id in database
        """
        try:
            self.database.delete_item(str(id))
        except DatabaseErrorInternal:
            return JSONResponse(
                status_code=404, content=NotFoundError(message="Item not found").dict()
            )

    def get_node(self, id: UUID) -> Union[dict, JSONResponse]:
        """
        Handler for getting item by id.

        :param id: UUID of item
        :return: tree with item information and information about its successors,
            or NotFoundError, if there is no item with id in database
        """
        try:
            return self.database.get_item(str(id))
        except DatabaseErrorInternal:
            return JSONResponse(
                status_code=404, content=NotFoundError(message="Item not found").dict()
            )

    def get_sales(self, date: str) -> Union[dict, JSONResponse]:
        """
        Handler for getting items updated in interval [date - 1d, date].

        :param date: datetime in ISO 8601 format
        :return: dict like {'items': list of dicts with items info}
        """
        try:
            validate_isoformat(date)
        except (TypeError, ValueError) as e:
            return JSONResponse(
                status_code=400, content=BadRequestError(message=str(e)).dict()
            )
        return {"items": self.database.get_updated_items(str(date))}
