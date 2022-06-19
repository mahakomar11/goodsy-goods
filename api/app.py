import os
from typing import Optional, Union
from uuid import UUID

import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from api.models import (
    BadRequestError,
    CategoryWithChildren,
    GetSalesResponse,
    GetStatisticsResponse,
    NotFoundError,
    OfferWithChildren,
    PostImportsRequest,
)
from database.interface import DBInterface
from database.models import Base, Item
from src.core import Core

app = FastAPI(
    description="Вступительное задание в Летнюю Школу Бэкенд Разработки Яндекса 2022",
    title="Goodsy Goods",
    version="1.0",
)

load_dotenv()
DB = DBInterface(
    user=os.getenv("POSTGRES_USER"),
    password=os.getenv("POSTGRES_PASSWORD"),
    database_name=os.getenv("POSTGRES_DB"),
    host="localhost",
    port="5432",
)

engine = DB.engine
if not engine.dialect.has_table(engine.connect(), Item):
    Base.metadata.create_all(engine)

core = Core(DB)
# core.delete_id('d515e43f-f3f6-4471-bb77-6b455017a2d4')


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc: RequestValidationError):
    return JSONResponse(
        status_code=400,
        content=jsonable_encoder(
            {"code": 400, "message": f"Validation Failed: {exc.errors()}"}
        ),
    )


@app.post(
    "/imports",
    response_model=None,
    responses={
        "200": {
            "description": "Вставка или обновление прошли успешно.",
            "content": None,
        },
        "400": {
            "description": "Невалидная схема документа или входные данные не верны.",
            "model": BadRequestError,
        },
        "422": {"description": "Не поддерживается.", "model": None},
    },
    tags=["Базовые задачи"],
)
def post_imports(items_to_post: PostImportsRequest) -> Union[None, BadRequestError]:
    return core.post_imports(items_to_post)


@app.delete(
    "/delete/{id}",
    response_model=None,
    responses={
        "200": {"description": "Удаление прошло успешно.", "content": None},
        "400": {
            "description": "Невалидная схема документа или входные данные не верны.",
            "model": BadRequestError,
        },
        "404": {"description": "Категория/товар не найден.", "model": NotFoundError},
        "422": {"description": "Не поддерживается.", "model": None},
    },
    tags=["Базовые задачи"],
)
def delete_id(id: UUID) -> Union[None, BadRequestError, NotFoundError]:
    return core.delete_id(id)


@app.get(
    "/nodes/{id}",
    response_model=Union[CategoryWithChildren, OfferWithChildren],
    responses={
        "200": {"description": "Информация об элементе."},
        "400": {
            "description": "Невалидная схема документа или входные данные не верны.",
            "model": BadRequestError,
        },
        "404": {"description": "Категория/товар не найден.", "model": NotFoundError},
        "422": {"description": "Не поддерживается.", "model": None},
    },
    tags=["Базовые задачи"],
)
def get_node(
    id: UUID,
) -> Union[CategoryWithChildren, OfferWithChildren, BadRequestError, NotFoundError]:
    return core.get_node(id)


@app.get(
    "/sales",
    response_model=GetSalesResponse,
    responses={
        "200": {"description": "Список товаров, цена которых была обновлена."},
        "400": {
            "description": "Невалидная схема документа или входные данные не верны.",
            "model": BadRequestError,
        },
        "422": {"description": "Не поддерживается.", "model": None},
    },
    tags=["Дополнительные задачи"],
)
def get_sales(date: str):
    return core.get_sales(date)


@app.get(
    "/node/{id}/statistic",
    response_model=GetStatisticsResponse,
    responses={
        "200": {"description": "Статистика по элементу."},
        "400": {
            "description": "Некорректный формат запроса или некорректные даты интервала.",
            "model": BadRequestError,
        },
        "404": {"description": "Категория/товар не найден.", "model": NotFoundError},
        "422": {"description": "Не поддерживается.", "model": None},
    },
    tags=["Дополнительные задачи"],
)
def get_node_statistic(
    id: UUID,
    date_start: Optional[str] = Query(None, alias="dateStart"),
    date_end: Optional[str] = Query(None, alias="dateEnd"),
) -> Union[GetStatisticsResponse, BadRequestError, NotFoundError]:
    pass


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
