from datetime import datetime
from enum import Enum
from typing import ForwardRef, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field, NonNegativeInt


class ShopUnitType(Enum):
    OFFER = 'OFFER'
    CATEGORY = 'CATEGORY'


class OfferType(Enum):
    OFFER = 'OFFER'


class CategoryType(Enum):
    CATEGORY = 'CATEGORY'


class OfferImport(BaseModel):
    id: UUID = Field(
        ...,
        description='Уникальный идентификатор',
        example='3fa85f64-5717-4562-b3fc-2c963f66a333',
    )
    name: str = Field(..., description='Имя элемента')
    parentId: Optional[UUID] = Field(
        None,
        description='UUID родительской категории',
        example='3fa85f64-5717-4562-b3fc-2c963f66a333',
    )
    type: OfferType = Field(
        ...,
        description='OFFER'
    )
    price: NonNegativeInt = Field(
        ...,
        description='Целое число'
    )


class CategoryImport(BaseModel):
    id: UUID = Field(
        ...,
        description='Уникальный идентификатор',
        example='3fa85f64-5717-4562-b3fc-2c963f66a333',
    )
    name: str = Field(..., description='Имя элемента')
    parentId: Optional[UUID] = Field(
        None,
        description='UUID родительской категории',
        example='3fa85f64-5717-4562-b3fc-2c963f66a333',
    )
    type: CategoryType = Field(
        ...,
        description='CATEGORY'
    )
    price: None = Field(None, example="null")


class OfferGet(OfferImport):
    date: datetime = Field(
        ...,
        description='Последнее время обновления товара'
    )


class CategoryGet(CategoryImport):
    price: Optional[NonNegativeInt] = Field(
        None,
        description='Средняя цена товаров категории, включая товары дочерних категорий'
    )
    date: datetime = Field(
        ...,
        description='Последнее время обновления категории'
    )


class OfferWithChildren(OfferGet):
    children: None = Field(
        None,
        description='У товара нет children',
        example='null'
    )


CategoryWithChildren = ForwardRef("CategoryWithChildren")


class CategoryWithChildren(CategoryGet):
    children: List[Union[OfferGet, CategoryWithChildren]] = Field(
        [],
        description='Дочерние товары и категории'
    )


CategoryWithChildren.update_forward_refs()

# class ShopUnitImport(BaseModel):
#     id: UUID = Field(
#         ...,
#         description='Уникальный идентфикатор',
#         example='3fa85f64-5717-4562-b3fc-2c963f66a333',
#     )
#     name: str = Field(..., description='Имя элемента.')
#     parentId: Optional[UUID] = Field(
#         None,
#         description='UUID родительской категории',
#         example='3fa85f64-5717-4562-b3fc-2c963f66a333',
#     )
#     type: ShopUnitType
#     price: Optional[int] = Field(
#         None, description='Целое число, для категорий поле должно содержать null.'
#     )


class PostImportsRequest(BaseModel):
    items: List[Union[OfferImport, CategoryImport]] = Field(
        ...,
        description='Импортируемые элементы'
    )
    updateDate: datetime = Field(
        ...,
        description='Время обновления добавляемых товаров/категорий',
        example='2022-05-28T21:12:01.000Z',
    )


class GetSalesResponse(BaseModel):
    items: List[OfferGet] = Field(
        [],
        description='История в произвольном порядке'
    )


class GetStatisticsResponse(BaseModel):
    items: Union[List[OfferGet], List[CategoryGet]] = Field(
        [],
        description='История в произвольном порядке'
    )


# class ShopUnit(BaseModel):
#     id: UUID = Field(
#         ...,
#         description='Уникальный идентификатор',
#         example='3fa85f64-5717-4562-b3fc-2c963f66a333',
#     )
#     name: str = Field(..., description='Имя категории')
#     date: datetime = Field(
#         ...,
#         description='Время последнего обновления элемента.',
#         example='2022-05-28T21:12:01.000Z',
#     )
#     parentId: Optional[UUID] = Field(
#         None,
#         description='UUID родительской категории',
#         example='3fa85f64-5717-4562-b3fc-2c963f66a333',
#     )
#     type: ShopUnitType
#     price: Optional[int] = Field(
#         None,
#         description='Целое число, для категории - это средняя цена всех дочерних товаров(включая товары подкатегорий). Если цена является не целым числом, округляется в меньшую сторону до целого числа. Если категория не содержит товаров цена равна null.',
#     )
#     children: Optional[List[ShopUnit]] = Field(
#         None,
#         description='Список всех дочерних товаров\\категорий. Для товаров поле равно null.',
#     )
#
#
# class ShopUnitStatisticUnit(BaseModel):
#     id: UUID = Field(
#         ...,
#         description='Уникальный идентфикатор',
#         example='3fa85f64-5717-4562-b3fc-2c963f66a333',
#     )
#     name: str = Field(..., description='Имя элемента')
#     parentId: Optional[UUID] = Field(
#         None,
#         description='UUID родительской категории',
#         example='3fa85f64-5717-4562-b3fc-2c963f66a333',
#     )
#     type: ShopUnitType
#     price: Optional[int] = Field(
#         None,
#         description='Целое число, для категории - это средняя цена всех дочерних товаров(включая товары подкатегорий). Если цена является не целым числом, округляется в меньшую сторону до целого числа. Если категория не содержит товаров цена равна null.',
#     )
#     date: datetime = Field(..., description='Время последнего обновления элемента.')
#
#
# class ShopUnitStatisticResponse(BaseModel):
#     items: Optional[List[ShopUnitStatisticUnit]] = Field(
#         None, description='История в произвольном порядке.'
#     )


class BadRequestError(BaseModel):
    code: int = Field(..., example=400)
    message: str = Field(..., example='Validation Failed')


class NotFoundError(BaseModel):
    code: int = Field(..., example=404)
    message: str = Field(..., example='Item not found')