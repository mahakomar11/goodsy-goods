from datetime import datetime
from typing import Optional, Union
from uuid import UUID

from fastapi import FastAPI, Query
from fastapi.encoders import jsonable_encoder
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse

from app.models import (BadRequestError,
                        NotFoundError,
                        PostImportsRequest,
                        CategoryWithChildren,
                        OfferWithChildren,
                        GetSalesResponse,
                        GetStatisticsResponse
                        )

app = FastAPI(
    description='Вступительное задание в Летнюю Школу Бэкенд Разработки Яндекса 2022',
    title='Goodsy Goods',
    version='1.0',
)


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request, exc: RequestValidationError):
    return JSONResponse(
        status_code=400,
        content=jsonable_encoder({"code": 400, "message": 'Validation Failed'}),
    )


@app.post(
    '/imports',
    response_model=None,
    responses={'200': {'description': 'Вставка или обновление прошли успешно.',
                       'content': None},
               '400': {'description': 'Невалидная схема документа или входные данные не верны.',
                       'model': BadRequestError},
               '422': {'description': 'Не поддерживается.',
                       'model': None}},
    tags=['Базовые задачи']
)
def post_imports(body: PostImportsRequest) -> Union[None, BadRequestError]:
    pass


@app.delete(
    '/delete/{id}',
    response_model=None,
    responses={'200': {'description': 'Удаление прошло успешно.',
                       'content': None},
               '400': {'description': 'Невалидная схема документа или входные данные не верны.',
                       'model': BadRequestError},
               '404': {'description': 'Категория/товар не найден.',
                       'model': NotFoundError},
               '422': {'description': 'Не поддерживается.',
                       'model': None}
               },
    tags=['Базовые задачи']
)
def delete_id(id: UUID) -> Union[None, BadRequestError, NotFoundError]:
    pass


@app.get(
    '/nodes/{id}',
    response_model=Union[CategoryWithChildren, OfferWithChildren],
    responses={'200': {'description': 'Информация об элементе.'},
               '400': {'description': 'Невалидная схема документа или входные данные не верны.',
                       'model': BadRequestError},
               '404': {'description': 'Категория/товар не найден.',
                       'model': NotFoundError},
               '422': {'description': 'Не поддерживается.',
                       'model': None}
               },
    tags=['Базовые задачи']
)
def get_node(id: UUID) -> Union[CategoryWithChildren, OfferWithChildren, BadRequestError, NotFoundError]:
    pass


@app.get(
    '/sales',
    response_model=GetSalesResponse,
    responses={'200': {'description': 'Список товаров, цена которых была обновлена.'},
               '400': {'description': 'Невалидная схема документа или входные данные не верны.',
                       'model': BadRequestError},
               '422': {'description': 'Не поддерживается.',
                       'model': None}
               },
    tags=['Дополнительные задачи']
)
def get_sales(date: datetime) -> Union[GetSalesResponse, BadRequestError]:
    pass


@app.get(
    '/node/{id}/statistic',
    response_model=GetStatisticsResponse,
    responses={'200': {'description': 'Статистика по элементу.'},
               '400': {'description': 'Некорректный формат запроса или некорректные даты интервала.',
                       'model': BadRequestError},
               '404': {'description': 'Категория/товар не найден.',
                       'model': NotFoundError},
               '422': {'description': 'Не поддерживается.',
                       'model': None}
               },
    tags=['Дополнительные задачи']
)
def get_node_statistic(
        id: UUID,
        date_start: Optional[datetime] = Query(None, alias='dateStart'),
        date_end: Optional[datetime] = Query(None, alias='dateEnd'),
) -> Union[GetStatisticsResponse, BadRequestError, NotFoundError]:
    pass
