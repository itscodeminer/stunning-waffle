from abc import ABC
from logging import Logger
from typing import Any, Dict, Optional, Type, TypeVar, List
from uuid import UUID
from sqlalchemy.orm import Session, Query
from sqlalchemy import asc, desc, select

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
    """
    Generic sql repository contract.
    Ref: https://dev.to/manukanne/a-python-implementation-of-the-unit-of-work-and-repository-design-pattern-using-sqlmodel-3mb5
    """

    def __init__(
        self,
        logger: Logger,
        db_session: Session,
        current_user_provider: ICurrentUserProvider,
        date_time_provider: IDateTimeProvider,
        item_schema: Type[T],
        item_db_model: Type[D],
    ) -> None:
        """
        Constructor
        """

        self._logger: Logger = logger
        self._db_session: Session = db_session
        self._current_user_provider: ICurrentUserProvider = current_user_provider
        self._date_time_provider: IDateTimeProvider = date_time_provider
        self._item_schema = item_schema  # domain model
        self._item_db_model = item_db_model  # db model

    def _preprocess_resource(self, db_model: Type[D], b_adding: bool) -> D:
        """
        Protected helper to allow preprocessing DB model instance before we commit
        it to the database.

        Args:
            db_model (Type[D]): db model instance to be pre-processed.
            b_adding (bool): A boolean value that indicates if the instance will
            be inserted or updated.

        Returns:
            D: Preprocessed db model instance.
        """

        # Populate last modified_by and last modified_at
        
        db_model.modified_by = self._current_user_provider.user_name
        db_model.modified_at = self._date_time_provider.get_current_utc_date_time()

        if b_adding:
            db_model.created_by = self._current_user_provider.user_name
            db_model.created_at = self._date_time_provider.get_current_utc_date_time()

        return db_model

    def get_by_id(self, id: UUID) -> Optional[T]:
        db_item = self._get_db_item_by_id(id)
        item: T = self._to_schema(db_item)
        return item

    def _get_db_item_by_id(self, id: UUID) -> Optional[D]:
        """
        Protected helper to get the db_model instance corresponding
        to the specified id.
        """

        db_item: D = self._db_session.query(self._item_db_model).get(ident=id)
        return db_item

    def find(self, data_filter: BaseDataFilter) -> PaginationResponse[T]:
        """
        Public method to get paginated list of items matching the
        specified filter criterion.
        """
        # create base query
        query = self._create_base_query()

        query = self._apply_data_filter(query=query, data_filter=data_filter)

        # Get the total count of matching items before applying pagination
        total_count = query.count()

        # Apply sorting if sort_on and sort_ascending are provided
        query = self._apply_sort_on(
            query=query,
            sort_on=data_filter.sort_on,
            sort_ascending=data_filter.sort_ascending,
        )

        query = self._apply_pagination(
            query=query,
            page_index=data_filter.page_index,
            page_size=data_filter.page_size,
        )

        db_items: List[self._item_db_model] = query.all()

        items = self._to_schema_list(db_items)

        # plug into paginated result.
        response = PaginationResponse[self._item_schema](
            page_size=data_filter.page_size,
            page_index=data_filter.page_index,
            match_count=total_count,
            total_count=total_count,
            items=items,
        )

        return response

    def create(self, item: T) -> T:
        """
        Public method to add a new item to the persistent storage.
        """

        # convert domain model to db model
        db_item: D = self._to_model(item)

        # preprocess item
        self._preprocess_resource(db_item, True)

        # persist
        self._db_session.add(db_item)
        self._db_session.flush()

        item = self._to_schema(db_item=db_item)

        return item

    def update(self, item: T) -> Optional[T]:
        """
        Public method to update an existing item in the persistent storage.
        """

        existing_entry = self._db_session.query(self._item_db_model).get(item.id)

        # get list of properties from the resource
        updated_data = self._get_updatable_properties(item)
        for key, value in updated_data.items():
            try:
                setattr(existing_entry, key, value)
            except Exception as ex:
                self._logger.error(f"Failed to set value for {key}")
                raise ex

        existing_entry = self._preprocess_resource(existing_entry, b_adding=False)
        self._db_session.flush()  # push to db, but don't commit

        item = self._to_schema(db_item=existing_entry)
        return item

    def delete(self, id: UUID) -> None:
        """
        Public method to delete the item identified by specified id.
        """
        item = self._get_db_item_by_id(id)
        if item is not None:
            self._db_session.delete(item)
            self._db_session.flush()

    def _to_schema_list(self, db_items: List[D]) -> Optional[List[T]]:
        """
        Protected helper method to convert a list of model items
        to schema items.
        """
        if db_items is None:
            return None

        items = [self._to_schema(x) for x in db_items]
        return items

    def _to_schema(self, db_item: D) -> Optional[T]:
        """
        Protected helper method to convert a model item
        to schema item.
        """
        if db_item is None:
            return None

        item = self._item_schema.model_construct(**db_item.__dict__)
        return item

    def _create_base_query(self) -> Query:
        """
        Creates the base query for the item database model.

        Deriving class should override this and apply the  specific selection criteria.
        The default implementation returns a query that
        does not modify the base query.

        Returns:
            Query: The base query for the item database model.
        """
        return self._db_session.query(self._item_db_model)

    def _to_model_list(self, items: List[T]) -> Optional[List[D]]:
        """
        Protected helper method to convert list of model items
        to a list of schema items.
        """
        if items is None:
            return None

        db_items = [self._to_schema(x) for x in db_items]
        return db_items

    def _to_model(self, item: T) -> Optional[D]:
        """
        Protected helper method to convert a db model item
        to a schema item.
        """
        if item is None:
            return None

        db_item: D = self._item_db_model(**item.__dict__)
        return db_item

    def _apply_data_filter(self, query: Query, data_filter: BaseDataFilter) -> Query:
        """
        Deriving class should override this and apply the data filter criterion.
        Default implementation does not modify the query.
        """
        return query

    def _apply_pagination(
        self, query: Query, page_index: int = 0, page_size: int = 10
    ) -> Query:
        if page_size > 0 and page_index >= 0:
            # Apply pagination
            # The line `offset = data_filter.page_size * page_index` is calculating the offset value
            # for pagination in the `find` method of the `GenericSqlRepository` class.
            offset = page_size * page_index
            query = query.offset(offset).limit(page_size)

        return query

    def _apply_sort_on(
        self, query: Query, sort_on: str = None, sort_ascending: bool = False
    ) -> Query:
        """
        Deriving class can override this method (generally not required) to
        specify their onw sorting criterion. As long as the sort_on value matches
        the corresponding db_model property, you will not be required to override
        this method.
        """

        if sort_on and sort_ascending is not None:
            valid_sort_on = hasattr(self._item_db_model, sort_on)
            if valid_sort_on:
                sort_on_attr = getattr(self._item_db_model, sort_on)
                if sort_ascending:
                    query = query.order_by(asc(sort_on_attr))
                else:
                    query = query.order_by(desc(sort_on_attr))
            else:
                self._logger.warning(
                    f"No sorting done. Model does not contain field '{sort_on}'"
                )

        return query

    def _get_updatable_properties(self, db_item: D) -> Dict[str, Any]:
        """
        Protected helper method to get the properties and values that
        are updatable.

        Deriving class can override this to remove properties that
        are either computed or not updatable.
        """
        return db_item.model_dump(exclude_unset=True)
