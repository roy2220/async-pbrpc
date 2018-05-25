import typing

from asyncio_toolkit.typing import Coroutine
from google.protobuf import message


class ServiceHandler:
    SERVICE_NAME: typing.ClassVar[bytes]
    REQUEST_CLASSES: typing.ClassVar[typing.Tuple[message.Message, ...]]
    RESPONSE_CLASSES: typing.ClassVar[typing.Tuple[message.Message, ...]]
    METHOD_NAMES: typing.ClassVar[typing.Tuple[str, ...]]
