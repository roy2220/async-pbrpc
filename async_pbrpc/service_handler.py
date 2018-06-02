import inspect
import typing

from asyncio_toolkit.typing import BytesLike, Coroutine
from google.protobuf import message

from . import errors


class ServiceHandler:
    SERVICE_NAME: typing.ClassVar[bytes]
    REQUEST_CLASSES: typing.ClassVar[typing.Tuple[message.Message, ...]]
    RESPONSE_CLASSES: typing.ClassVar[typing.Tuple[message.Message, ...]]
    METHOD_NAMES: typing.ClassVar[typing.Tuple[str, ...]]

    def call_method(self, channel: "channels.Channel", method_index: int
                    , request_data: BytesLike) -> typing.Union[bytes, Coroutine[bytes]]:
        if method_index >= len(self.METHOD_NAMES):
            raise errors.NotImplementedError()

        method = getattr(self, self.METHOD_NAMES[method_index])
        request_class = self.REQUEST_CLASSES[method_index]
        requests: typing.Union[typing.Tuple[()], typing.Tuple[message.Message]]

        if request_class is type(None):
            request = None
            requests = ()
        else:
            try:
                request = request_class.FromString(request_data)
            except message.DecodeError:
                raise errors.BadRequestError() from None

            requests = request,

        self.on_method_call(channel, method.__name__, request)
        response = method(channel, *requests)
        response_class = self.RESPONSE_CLASSES[method_index]

        if not inspect.isawaitable(response):
            assert isinstance(response, response_class), repr((type(response), response_class))
            return b"" if response_class is type(None) else response.SerializeToString()

        async def do() -> bytes:
            response2 = await response
            assert isinstance(response2, response_class), repr((type(response2), response_class))
            return b"" if response_class is type(None) else response2.SerializeToString()

        return do()

    def on_method_call(self, channel, method_name: str, request) -> None:
        pass


if typing.TYPE_CHECKING:
    from . import channels
