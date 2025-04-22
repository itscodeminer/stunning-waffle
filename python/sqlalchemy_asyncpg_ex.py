from abc import ABC
from logging import Logger
from typing import Any, Dict, Optional, Type, TypeVar, List
from uuid import UUID
from sqlalchemy import asc, desc, select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import Select

from core.models.fts_linq_base_model import FtsLinqBaseModel
from core.contracts.icurrent_user_provider import ICurrentUserProvider
from core.contracts.idatetime_provider import IDateTimeProvider
from core.base_data_filter import BaseDataFilter
from core.contracts.igeneric_repository import IGenericRepository
from core.schemas.pagination_response import PaginationResponse
from core.schemas.fts_linq_base_schemas import FtsLinqBaseSchema

T = TypeVar("T", bound=FtsLinqBaseSchema)  # domain model
D = TypeVar("D", bound=FtsLinqBaseModel)  # database model


class GenericSqlRepository(IGenericRepository[T], ABC):
    def __init__(
        self,
        logger: Logger,
        db_session: AsyncSession,
        current_user_provider: ICurrentUserProvider,
        date_time_provider: IDateTimeProvider,
        item_schema: Type[T],
        item_db_model: Type[D],
    ) -> None:
        self._logger: Logger = logger
        self._db_session: AsyncSession = db_session
        self._current_user_provider: ICurrentUserProvider = current_user_provider
        self._date_time_provider: IDateTimeProvider = date_time_provider
        self._item_schema = item_schema
        self._item_db_model = item_db_model

    def _create_base_query(self) -> Select:
        return select(self._item_db_model)

    async def _get_db_item_by_id(self, id: UUID) -> Optional[D]:
        stmt = select(self._item_db_model).where(self._item_db_model.id == id)
        result = await self._db_session.execute(stmt)
        return result.scalar_one_or_none()

    async def get_by_id(self, id: UUID) -> Optional[T]:
        db_item = await self._get_db_item_by_id(id)
        return self._to_schema(db_item)

    async def find(self, data_filter: BaseDataFilter) -> PaginationResponse[T]:
        stmt = self._create_base_query()
        stmt = self._apply_data_filter(stmt, data_filter)

        # Count matching items
        count_stmt = select(func.count()).select_from(stmt.subquery())
        count_result = await self._db_session.execute(count_stmt)
        total_count = count_result.scalar_one()

        # Sort & paginate
        stmt = self._apply_sort_on(stmt, data_filter.sort_on, data_filter.sort_ascending)
        stmt = self._apply_pagination(stmt, data_filter.page_index, data_filter.page_size)

        result = await self._db_session.execute(stmt)
        db_items = result.scalars().all()
        items = self._to_schema_list(db_items)

        return PaginationResponse[self._item_schema](
            page_size=data_filter.page_size,
            page_index=data_filter.page_index,
            match_count=total_count,
            total_count=total_count,
            items=items,
        )

    async def create(self, item: T) -> T:
        db_item = self._to_model(item)
        self._preprocess_resource(db_item, b_adding=True)
        self._db_session.add(db_item)
        await self._db_session.flush()
        return self._to_schema(db_item)

    async def update(self, item: T) -> Optional[T]:
        stmt = select(self._item_db_model).where(self._item_db_model.id == item.id)
        result = await self._db_session.execute(stmt)
        existing_entry = result.scalar_one_or_none()
        if not existing_entry:
            return None

        updated_data = self._get_updatable_properties(item)
        for key, value in updated_data.items():
            try:
                setattr(existing_entry, key, value)
            except Exception as ex:
                self._logger.error(f"Failed to set value for {key}")
                raise ex

        self._preprocess_resource(existing_entry, b_adding=False)
        await self._db_session.flush()
        return self._to_schema(existing_entry)

    async def delete(self, id: UUID) -> None:
        db_item = await self._get_db_item_by_id(id)
        if db_item:
            await self._db_session.delete(db_item)
            await self._db_session.flush()

    def _apply_data_filter(self, stmt: Select, data_filter: BaseDataFilter) -> Select:
        # Default is a no-op; override in subclasses
        return stmt

    def _apply_sort_on(self, stmt: Select, sort_on: str = None, sort_ascending: bool = False) -> Select:
        if sort_on and hasattr(self._item_db_model, sort_on):
            sort_attr = getattr(self._item_db_model, sort_on)
            order_clause = asc(sort_attr) if sort_ascending else desc(sort_attr)
            stmt = stmt.order_by(order_clause)
        else:
            self._logger.warning(
                f"No sorting applied. Model does not contain field '{sort_on}'"
            )
        return stmt

    def _apply_pagination(self, stmt: Select, page_index: int = 0, page_size: int = 10) -> Select:
        offset = page_index * page_size
        return stmt.offset(offset).limit(page_size)

    def _preprocess_resource(self, db_model: D, b_adding: bool) -> D:
        db_model.modified_by = self._current_user_provider.user_name
        db_model.modified_at = self._date_time_provider.get_current_utc_date_time()

        if b_adding:
            db_model.created_by = self._current_user_provider.user_name
            db_model.created_at = self._date_time_provider.get_current_utc_date_time()

        return db_model

    def _to_schema_list(self, db_items: List[D]) -> Optional[List[T]]:
        if db_items is None:
            return None
        return [self._to_schema(x) for x in db_items]

    def _to_schema(self, db_item: D) -> Optional[T]:
        if db_item is None:
            return None
        return self._item_schema.model_construct(**db_item.__dict__)

    def _to_model(self, item: T) -> Optional[D]:
        if item is None:
            return None
        return self._item_db_model(**item.__dict__)

    def _get_updatable_properties(self, db_item: D) -> Dict[str, Any]:
        return db_item.model_dump(exclude_unset=True)
