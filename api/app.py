from typing import Optional, Union
from uuid import UUID

import uvicorn
from fastapi import FastAPI, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from api.models import (
    BadRequestResponse,
    CategoryWithChildren,
    GetSalesResponse,
    GetStatisticsResponse,
    NotFoundResponse,
    OfferWithChildren,
    PostImportsRequest,
    ValidateResponse,
)
from database.interface import DBInterface
from src.constants import DB_SETTINGS
from src.core import Core

app = FastAPI(
    description="Вступительное задание в Летнюю Школу Бэкенд Разработки Яндекса 2022",
    title="Goodsy Goods",
    version="1.0",
)

DB = DBInterface(**DB_SETTINGS)
core = Core(DB)


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
        "400": dict(BadRequestResponse()),
        "422": dict(ValidateResponse()),
    },
    tags=["Базовые задачи"],
)
def post_imports(items_to_post: PostImportsRequest):
    return core.post_imports(items_to_post)


@app.delete(
    "/delete/{id}",
    response_model=None,
    responses={
        "200": {"description": "Удаление прошло успешно.", "content": None},
        "400": dict(BadRequestResponse()),
        "404": dict(NotFoundResponse()),
        "422": dict(ValidateResponse()),
    },
    tags=["Базовые задачи"],
)
def delete_id(id: UUID):
    return core.delete_id(id)


@app.get(
    "/nodes/{id}",
    response_model=Union[CategoryWithChildren, OfferWithChildren],
    responses={
        "200": {"description": "Информация об элементе."},
        "400": dict(BadRequestResponse()),
        "404": dict(NotFoundResponse()),
        "422": dict(ValidateResponse()),
    },
    tags=["Базовые задачи"],
)
def get_node(id: UUID):
    return core.get_node(id)


@app.get(
    "/sales",
    response_model=GetSalesResponse,
    responses={
        "200": {"description": "Список товаров, цена которых была обновлена."},
        "400": dict(BadRequestResponse()),
        "422": dict(ValidateResponse()),
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
        "400": dict(
            BadRequestResponse(
                description="Некорректный формат запроса или некорректные даты интервала."
            )
        ),
        "404": dict(NotFoundResponse()),
        "422": dict(ValidateResponse()),
    },
    tags=["Дополнительные задачи"],
)
def get_node_statistic(
    id: UUID,
    date_start: Optional[str] = Query(None, alias="dateStart"),
    date_end: Optional[str] = Query(None, alias="dateEnd"),
):
    pass


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
