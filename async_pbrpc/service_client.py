import typing

from google.protobuf import message

from .channels import Channel


class ServiceClient:
    SERVICE_NAME: typing.ClassVar[bytes]
    REQUEST_CLASSES: typing.ClassVar[typing.Tuple[message.Message, ...]]
    RESPONSE_CLASSES: typing.ClassVar[typing.Tuple[message.Message, ...]]

    def __init__(self, channel: Channel) -> None:
        self._channel = channel

    async def call_method(self, method_index: int, request: typing.Optional[message.Message]
                          , auto_retry: bool) -> typing.Optional[message.Message]:
        request_class = self.REQUEST_CLASSES[method_index]
        assert isinstance(request, request_class), repr((type(request), request_class))
        request_data = b"" if request_class is type(None) else request.SerializeToString()
        response_data = await self._channel.call_method(self.SERVICE_NAME, method_index
                                                        , request_data, auto_retry)
        response_class = self.RESPONSE_CLASSES[method_index]
        response = None if response_class is type(None) else response_class\
                                                             .FromString(response_data)
        return response

    def call_method_without_return(self, method_index: int, request: typing.Optional[message\
        .Message], auto_retry: bool) -> bool:
        request_class = self.REQUEST_CLASSES[method_index]
        assert isinstance(request, request_class), repr((type(request), request_class))
        request_data = b"" if request_class is type(None) else request.SerializeToString()
        return self._channel.call_method_without_return(self.SERVICE_NAME, method_index
                                                        , request_data, auto_retry)
