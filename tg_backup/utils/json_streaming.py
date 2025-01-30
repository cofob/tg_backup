from __future__ import annotations

import json
from contextlib import contextmanager
from collections.abc import Sequence
from concurrent.futures import Executor
from io import TextIOBase
from typing import Callable, Generic, Iterable, Iterator, Mapping, ParamSpec, TypeAlias, TypeVar, TypeVarTuple, overload


T = TypeVar("T")
Ts = TypeVarTuple("Ts")
Params = ParamSpec("Params")
Return = TypeVar("Return")


JSON: TypeAlias = str | int | float | None | Sequence["JSON"] | Mapping[str, "JSON"]


@overload
@contextmanager
def list_writer(fp: TextIOBase, *, indent: int | str | None = None, default: None = None, ensure_ascii: bool = True, executor: Executor | None = None) -> Iterator[JSONListWriter[JSON]]: ...  # noqa
@overload
@contextmanager
def list_writer(fp: TextIOBase, *, indent: int | str | None = None, default: Callable[[T], JSON], ensure_ascii: bool = True, executor: Executor | None = None) -> Iterator[JSONListWriter[T]]: ...  # noqa


@contextmanager
def list_writer(
    fp: TextIOBase,
    *,
    indent: int | str | None = None,
    default: Callable[[T], JSON] | None = None,
    ensure_ascii: bool = True,
    executor: Executor | None = None,
) -> Iterator[JSONListWriter[T] | JSONListWriter[JSON]]:
    writer = JSONListWriter(fp, default=default, indent=indent, ensure_ascii=ensure_ascii, executor=executor)
    writer.start()

    yield writer

    writer.finalize()


class JSONListWriter(Generic[T]):
    @overload
    def __init__(self: JSONListWriter[JSON], fp: TextIOBase, *, indent: int | str | None, default: None = None, ensure_ascii: bool = True, executor: Executor | None = None): ...  # noqa
    @overload
    def __init__(self: JSONListWriter[T], fp: TextIOBase, *, indent: int | str | None, default: Callable[[T], JSON], ensure_ascii: bool = True, executor: Executor | None = None): ...  # noqa

    def __init__(
        self,
        fp: TextIOBase,
        *,
        indent: int | str | None,
        default: Callable[[T], JSON] | None = None,
        ensure_ascii: bool = True,
        executor: Executor | None = None
    ) -> None:
        self.fp = fp
        self.indent = indent
        self.default = default
        self.ensure_ascii = ensure_ascii
        self.executor = executor
        self._need_separator: bool = False

    @property
    def json_dumps(self) -> Callable[[T], str]:
        indent = self.indent
        default = self.default
        ensure_ascii = self.ensure_ascii

        def dumps(item: T) -> str:
            return json.dumps(item, indent=indent, default=default, ensure_ascii=ensure_ascii)

        return dumps

    def write_items(self, items: Iterable[T]) -> None:
        dumps = self.json_dumps

        if self.executor is None:
            dumped = [dumps(item) for item in items]
        else:
            dumped = list(self.executor.map(dumps, items))

        for item in dumped:
            if self._need_separator:
                self.fp.write(",\n")
            else:
                self._need_separator = True
            self.fp.write(item)

    def start(self):
        self.fp.write("[")

    def finalize(self):
        self.fp.write("]")


def jointo(fp: TextIOBase, iterable: Iterable[str], *, sep: str) -> None:
    iterator = iter(iterable)
    first = next(iterator)
    fp.write(first)
    for item in iterator:
        fp.write(sep)
        fp.write(item)
