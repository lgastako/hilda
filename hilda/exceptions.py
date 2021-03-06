class HildaException(Exception):
    """Base class for all Hilda exceptions."""

    pass


class NoResultFound(HildaException):
    """Raised when a method expecting a result does not find one."""

    pass


class TooManyResultsFound(HildaException):
    """Raised when a method gets more results than it expected."""

    pass
