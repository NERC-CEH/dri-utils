"""Core classes used by writers"""

from abc import ABC, abstractmethod
from typing import Any, Self


class ContextClass:
    _connection: Any
    """Reference to the connection object"""

    def __enter__(self) -> Self:
        """Creates a connection when used in a context block"""
        return self

    def __exit__(self, *args) -> None:
        """Closes the connection when exiting the context"""
        self.close()

    def __del__(self):
        """Closes the connection when deleted"""
        self.close()

    def close(self) -> None:
        """Closes the connection"""
        self._connection.close()


class ReaderInterface(ABC):
    """Abstract implementation for a IO reader"""

    @abstractmethod
    def read(self, *args, **kwargs) -> Any:
        """Reads data from a source"""


class WriterInterface(ABC):
    """Interface for defining parquet writing objects"""

    @abstractmethod
    def write(self, *args, **kwargs) -> None:
        """Abstract method for read operations"""
