from collections.abc import Iterable, Iterator, Mapping
from enum import Enum

from adaptix import Loader, Mediator, Retort, bound, loader
from adaptix._internal.morphing.provider_template import LoaderProvider
from adaptix._internal.morphing.request_cls import LoaderRequest
from adaptix._internal.provider.loc_stack_filtering import OriginSubclassLSC
from adaptix._internal.provider.located_request import for_predicate
from adaptix._internal.provider.location import TypeHintLoc
from pyrogram import enums, types
from pyrogram.enums.auto_name import AutoName
from pyrogram.types import Object


def load_enum_value(value: str) -> Enum:
    type_name, name = value.split(".", maxsplit=1)
    enum = getattr(enums, type_name)

    return getattr(enum, name)


def _get_pyrogram_types() -> dict[str, type]:
    objects = {name: getattr(types, name) for name in dir(types)}
    classes = {name: cls for name, cls in objects.items() if isinstance(cls, type)}
    return {name: cls for name, cls in classes.items() if cls.__module__.startswith("pyrogram.")}


@for_predicate(Object)
class PyrogramObjectsProvider(LoaderProvider):  # type: ignore[no-untyped-call]
    _pyrogram_types = _get_pyrogram_types()

    def provide_loader(self, mediator: Mediator[Loader[Object]], request: LoaderRequest) -> Loader[Object]:
        def pyrogram_object_loader(data: Mapping[str, object]) -> Object:
            raw_type_name = data.get("_")
            if not isinstance(raw_type_name, str):
                raise TypeError("Expected Pyrogram object payload to include string '_' type marker.")
            cls = self._pyrogram_types[raw_type_name]
            loader = mediator.mandatory_provide(request=LoaderRequest(request.loc_stack.replace_last(TypeHintLoc(cls))))
            return loader(data)

        return pyrogram_object_loader


_pyrogram_objects = Retort(
    recipe=[
        loader(OriginSubclassLSC(AutoName), load_enum_value),
        bound(OriginSubclassLSC(Object), PyrogramObjectsProvider()),
    ]
)


def load_object(raw: Mapping[str, object]) -> Object:
    return _pyrogram_objects.load(raw, Object)


def load_objects(iterable: Iterable[Mapping[str, object]]) -> Iterator[Object]:
    for item in iterable:
        yield load_object(item)
