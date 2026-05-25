from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator

from ..models import RawMessage


class BaseCollector(ABC):
    @abstractmethod
    async def stream(self, limit: int | None = None) -> AsyncIterator[RawMessage]:
        raise NotImplementedError

