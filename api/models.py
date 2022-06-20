from enum import Enum
from typing import ForwardRef, List, Optional, Union
from uuid import UUID

from pydantic import BaseModel, Field, NonNegativeInt


class OfferType(Enum):
    OFFER = "OFFER"


class CategoryType(Enum):
    CATEGORY = "CATEGORY"


class OfferImport(BaseModel):
    id: UUID = Field(
        ...,
        description="Уникальный идентификатор",
        example="3fa85f64-5717-4562-b3fc-2c963f66a333",
    )
    name: str = Field(..., description="Имя элемента")
    parentId: Optional[UUID] = Field(
        None,
        description="UUID родительской категории",
        example="3fa85f64-5717-4562-b3fc-2c963f66a333",
    )
    type: OfferType = Field(..., description="OFFER")
    price: NonNegativeInt = Field(..., description="Целое число")


class CategoryImport(BaseModel):
    id: UUID = Field(
        ...,
        description="Уникальный идентификатор",
        example="3fa85f64-5717-4562-b3fc-2c963f66a333",
    )
    name: str = Field(..., description="Имя элемента")
    parentId: Optional[UUID] = Field(
        None,
        description="UUID родительской категории",
        example="3fa85f64-5717-4562-b3fc-2c963f66a333",
    )
    type: CategoryType = Field(..., description="CATEGORY")
    price: None = Field(None, example="null")


class OfferGet(OfferImport):
    date: str = Field(..., description="Последнее время обновления товара")


class CategoryGet(CategoryImport):
    price: Optional[NonNegativeInt] = Field(
        None,
        description="Средняя цена товаров категории, включая товары дочерних категорий",
    )
    date: str = Field(..., description="Последнее время обновления категории")


class OfferWithChildren(OfferGet):
    children: None = Field(None, description="У товара нет children", example="null")


CategoryWithChildren = ForwardRef("CategoryWithChildren")


class CategoryWithChildren(CategoryGet):
    children: List[Union[OfferWithChildren, CategoryWithChildren]] = Field(
        [], description="Дочерние товары и категории"
    )


CategoryWithChildren.update_forward_refs()


class PostImportsRequest(BaseModel):
    items: List[Union[OfferImport, CategoryImport]] = Field(
        ..., description="Импортируемые элементы"
    )
    updateDate: str = Field(
        ...,
        description="Время обновления добавляемых товаров/категорий",
        example="2022-05-28T21:12:01.000Z",
    )


class GetSalesResponse(BaseModel):
    items: List[OfferGet] = Field([], description="История в произвольном порядке")


class GetStatisticsResponse(BaseModel):
    items: Union[List[OfferGet], List[CategoryGet]] = Field(
        [], description="История в произвольном порядке"
    )


class BadRequestError(BaseModel):
    code: int = Field(400, example=400)
    message: str = Field(..., example="Validation Failed")


class NotFoundError(BaseModel):
    code: int = Field(404, example=404)
    message: str = Field(..., example="Item not found")


class BadRequestResponse(BaseModel):
    description: str = Field("Невалидная схема документа или входные данные не верны.")
    model: BadRequestError = Field(BadRequestError)


class ValidateResponse(BaseModel):
    description: str = Field("Не поддерживается.")
    model: None = Field(None)


class NotFoundResponse(BaseModel):
    description: str = Field("Категория/товар не найден.")
    model: NotFoundError = Field(NotFoundError)
