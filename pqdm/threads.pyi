from typing import Any, Iterable, TypeVar

T = TypeVar('T')
def pqdm(iterator: Iterable[T], *args: Any, **kwargs: Any) -> Iterable[T]: ...
