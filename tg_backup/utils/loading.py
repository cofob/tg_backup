from collections.abc import Iterable, Iterator
from enum import Enum

from adaptix import Retort, bound, loader
from adaptix import Mediator, Loader
from adaptix._internal.provider.loc_stack_filtering import OriginSubclassLSC
from adaptix._internal.morphing.provider_template import LoaderProvider
from adaptix._internal.morphing.request_cls import LoaderRequest
from adaptix._internal.provider.located_request import for_predicate
from adaptix._internal.provider.location import TypeHintLoc

from pyrogram.enums.auto_name import AutoName
from pyrogram.types import Object


def load_enum_value(value: str) -> Enum:
    from pyrogram import enums

    type_name, name = value.split(".", maxsplit=1)
    enum = getattr(enums, type_name)

    return getattr(enum, name)


def _get_pyrogram_types() -> dict[str, type]:
    from pyrogram import types

    objects = {name: getattr(types, name) for name in dir(types)}
    classes = {name: cls for name, cls in objects.items() if isinstance(cls, type)}
    pyrogram_classes = {name: cls for name, cls in classes.items() if cls.__module__.startswith('pyrogram.')}

    return pyrogram_classes


@for_predicate(Object)
class PyrogramObjectsProvider(LoaderProvider):
    _pyrogram_types = _get_pyrogram_types()

    def provide_loader(self, mediator: Mediator[Loader], request: LoaderRequest) -> Loader:

        def pyrogram_object_loader(data):
            cls = self._pyrogram_types[data["_"]]
            loader = mediator.mandatory_provide(request=LoaderRequest(request.loc_stack.replace_last(TypeHintLoc(cls))))
            return loader(data)

        return pyrogram_object_loader


_pyrogram_objects = Retort(recipe=[
    loader(OriginSubclassLSC(AutoName), load_enum_value),
    bound(OriginSubclassLSC(Object), PyrogramObjectsProvider()),
])


def load_object(raw: dict) -> Object:
    return _pyrogram_objects.load(raw, Object)


def load_objects(iterable: Iterable[dict]) -> Iterator[Object]:
    for item in iterable:
        yield load_object(item)
