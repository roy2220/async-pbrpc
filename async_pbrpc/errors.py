import typing

from . import protocol_pb2


class Error(Exception):
    CODE: typing.ClassVar[int] = protocol_pb2.ERROR_NO


_ERROR_CODE_2_ERROR_CLASS: typing.Dict[int, typing.Type[Error]] = {}


def _register_error(error_code: int) -> typing.Callable[[typing.Type[Error]], typing.Type[Error]]:
    def do(error_class: typing.Type[Error]) -> typing.Type[Error]:
        assert issubclass(error_class, Error), repr(error_class)
        assert error_class not in _ERROR_CODE_2_ERROR_CLASS.keys(), repr(error_class)
        error_class.CODE = error_code
        _ERROR_CODE_2_ERROR_CLASS[error_code] = error_class
        return error_class

    return do


@_register_error(-1)
class ChannelBrokenError(Error):
    pass


@_register_error(-2)
class ChannelTimedOutError(Error):
    pass


@_register_error(protocol_pb2.ERROR_CHANNEL_BUSY)
class ChannelBusyError(Error):
    pass


@_register_error(protocol_pb2.ERROR_NOT_IMPLEMENTED)
class NotImplementedError(Error):
    pass


@_register_error(protocol_pb2.ERROR_BAD_REQUEST)
class BadRequestError(Error):
    pass


@_register_error(protocol_pb2.ERROR_INTERNAL_SERVER)
class InternalServerError(Error):
    pass


USER_ERROR_CODE_OFFSET = protocol_pb2.ERROR_USER_DEFINED


def get_error_class(error_code: int) -> typing.Type[Error]:
    return _ERROR_CODE_2_ERROR_CLASS[error_code]


def register_user_error(user_error_code: int) -> typing.Callable[[typing.Type[Error]]
                                                                 , typing.Type[Error]]:
    assert user_error_code >= 0
    return _register_error(USER_ERROR_CODE_OFFSET + user_error_code)


def get_user_error_class(user_error_code: int) -> typing.Type[Error]:
    assert user_error_code >= 0
    return get_error_class(USER_ERROR_CODE_OFFSET + user_error_code)
