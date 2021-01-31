from typing import Any, Iterable, TypeVar

T = TypeVar('T')
def tqdm(iterator: Iterable[T], *args: Any, **kwargs: Any) -> Iterable[T]: ...
