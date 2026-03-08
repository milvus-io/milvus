from enum import Enum


class BaseError(Enum):
    pass


class VectorInsertError(BaseError):
    pass


class VectorSearchError(BaseError):
    pass


class VectorGetError(BaseError):
    pass


class VectorQueryError(BaseError):
    pass


class VectorDeleteError(BaseError):
    pass


class CollectionListError(BaseError):
    pass


class CollectionCreateError(BaseError):
    pass


class CollectionDropError(BaseError):
    pass


class CollectionDescribeError(BaseError):
    pass
