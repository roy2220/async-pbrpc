import asyncio
import enum
import inspect
import logging
import time
import typing
import uuid

from asyncio_toolkit import utils
from asyncio_toolkit.deque import Deque
from asyncio_toolkit.typing import BytesLike
from google.protobuf import message

from . import errors
from . import protocol_pb2
from .service_handler import ServiceHandler
from .transport import TransportPolicy, Transport


class _ChannelState(enum.IntEnum):
    CONNECTING = enum.auto()
    CONNECTED = enum.auto()
    ACCEPTING = enum.auto()
    ACCEPTED = enum.auto()
    CLOSED = enum.auto()


class _MethodCall(typing.NamedTuple):
    service_name: bytes
    method_index: int
    request_data: bytes
    auto_retry: bool
    response_data: typing.Optional["asyncio.Future[bytes]"]

    def __repr__(self) -> str:
        return _represent_method_call(self.service_name, self.method_index)


class ChannelImpl:
    def __init__(self, owner: "channels.Channel", loop: typing.Optional[asyncio.AbstractEventLoop]
                 , logger: typing.Optional[logging.Logger]
                 , transport_policy: typing.Optional[TransportPolicy], timeout: float
                 , outgoing_window_size: int, incoming_window_size: int) -> None:
        assert timeout >= 0.0, repr(timeout)
        assert outgoing_window_size >= 0, repr(outgoing_window_size)
        assert incoming_window_size >= 0, repr(incoming_window_size)
        self._owner = owner
        self._transport = Transport(loop, logger, transport_policy)
        self._timeout = timeout
        self._outgoing_window_size = outgoing_window_size
        self._incoming_window_size = incoming_window_size
        self._state = _ChannelState.CLOSED
        self._opening: asyncio.Future[None] = self.get_loop().create_future()
        self._id = b""
        self._next_sequence_number = 0
        self._pending_method_calls1: Deque[_MethodCall] = Deque(_MIN_CHANNEL_WINDOW_SIZE
                                                                , self.get_loop())
        self._pending_method_calls2: typing.Dict[int, _MethodCall] = {}
        self._request_count = 0
        self._service_handlers: typing.Dict[bytes, ServiceHandler] = {}
        self._calling_methods1: typing.Dict[int, asyncio.Task[None]] = {}
        self._calling_methods2: typing.Set[asyncio.Task[None]] = set()
        self._sending_response: asyncio.Future[typing.Optional[_MethodCall]] = utils\
            .make_done_future(self.get_loop())

    def add_service_handler(self, service_handler: ServiceHandler) -> None:
        assert service_handler.SERVICE_NAME not in self._service_handlers.keys()\
               , repr(service_handler.SERVICE_NAME)
        self._service_handlers[service_handler.SERVICE_NAME] = service_handler

    def remove_service_handler(self, service_handler: ServiceHandler) -> None:
        del self._service_handlers[service_handler.SERVICE_NAME]

    async def connect(self, host_name: str, port_number: int, connect_deadline: float):
        self._set_state(_ChannelState.CONNECTING)
        transport = Transport(self.get_loop(), self.get_logger(), self._transport.get_policy())
        connect_timeout = max(connect_deadline - time.monotonic(), 0.0)
        await transport.connect(host_name, port_number, connect_timeout)

        try:
            handshake = protocol_pb2.Handshake(
                timeout=int(self._timeout * 1000),
                outgoing_window_size=self._outgoing_window_size,
                incoming_window_size=self._incoming_window_size,
                id=self._id
            )

            data = handshake.SerializeToString()
            transport.write(protocol_pb2.MESSAGE_HANDSHAKE, data)
            connect_timeout = max(connect_deadline - time.monotonic(), 0.0)
            message_type, data = await transport.read(connect_timeout)
            assert message_type == protocol_pb2.MESSAGE_HANDSHAKE, repr(message_type)
            handshake = protocol_pb2.Handshake.FromString(data)
        except Exception:
            transport.close()
            raise

        if not self._transport.is_closed():
            self._transport.close()

        self._transport = transport
        self._timeout = handshake.timeout / 1000
        self._outgoing_window_size = handshake.outgoing_window_size
        self._incoming_window_size = handshake.incoming_window_size
        self._id = handshake.id
        real_outgoing_window_size = self._pending_method_calls1.get_max_length()

        if real_outgoing_window_size < self._outgoing_window_size:
            self._pending_method_calls1.commit_item_removals(self._outgoing_window_size
                                                             - real_outgoing_window_size)
        elif real_outgoing_window_size > self._outgoing_window_size:
            self.get_logger().warn("channel outgoing window overflow: channel_id={!r}"
                                   .format(self._id.hex(), self._state))

        self._set_state(_ChannelState.CONNECTED)

    async def accept(self, stream_rw: typing.Tuple[asyncio.StreamReader, asyncio.StreamWriter]
                     , accept_deadline: float) -> None:
        self._set_state(_ChannelState.ACCEPTING)
        transport = Transport(self.get_loop(), self.get_logger(), self._transport.get_policy())
        transport.accept(*stream_rw)

        try:
            accept_timeout = max(accept_deadline - time.monotonic(), 0.0)
            message_type, data = await transport.read(accept_timeout)
            assert message_type == protocol_pb2.MESSAGE_HANDSHAKE, repr(message_type)
            handshake = protocol_pb2.Handshake.FromString(data)
            timeout = handshake.timeout / 1000
            outgoing_window_size = handshake.outgoing_window_size
            incoming_window_size = handshake.incoming_window_size

            if timeout > self._timeout:
                timeout = self._timeout

            if timeout < _MIN_CHANNEL_TIMEOUT:
                timeout = _MIN_CHANNEL_TIMEOUT
            elif timeout > _MAX_CHANNEL_TIMEOUT:
                timeout = _MAX_CHANNEL_TIMEOUT

            if outgoing_window_size > self._incoming_window_size:
                outgoing_window_size = self._incoming_window_size

            if outgoing_window_size < _MIN_CHANNEL_WINDOW_SIZE:
                outgoing_window_size = _MIN_CHANNEL_WINDOW_SIZE
            elif outgoing_window_size > _MAX_CHANNEL_WINDOW_SIZE:
                outgoing_window_size = _MAX_CHANNEL_WINDOW_SIZE

            if incoming_window_size > self._outgoing_window_size:
                incoming_window_size = self._outgoing_window_size

            if incoming_window_size < _MIN_CHANNEL_WINDOW_SIZE:
                incoming_window_size = _MIN_CHANNEL_WINDOW_SIZE
            elif incoming_window_size > _MAX_CHANNEL_WINDOW_SIZE:
                incoming_window_size = _MAX_CHANNEL_WINDOW_SIZE

            handshake.timeout = int(timeout * 1000)
            handshake.outgoing_window_size = outgoing_window_size
            handshake.incoming_window_size = incoming_window_size

            if handshake.id == b"":
                handshake.id = uuid.uuid4().bytes

            data = handshake.SerializeToString()
            transport.write(message_type, data)
        except Exception:
            transport.close()
            raise

        if not self._transport.is_closed():
            self._transport.close()

        self._transport = transport
        self._timeout = handshake.timeout / 1000
        self._outgoing_window_size = handshake.incoming_window_size
        self._incoming_window_size = handshake.outgoing_window_size
        self._id = handshake.id
        real_outgoing_window_size = self._pending_method_calls1.get_max_length()

        if real_outgoing_window_size < self._outgoing_window_size:
            self._pending_method_calls1.commit_item_removals(self._outgoing_window_size
                                                             - real_outgoing_window_size)
        elif real_outgoing_window_size > self._outgoing_window_size:
            self.get_logger().warn("channel outgoing window overflow: channel_id={!r}"
                                   .format(self._id.hex(), self._state))

        self._set_state(_ChannelState.ACCEPTED)

    async def dispatch(self) -> None:
        assert self._state in (_ChannelState.CONNECTED, _ChannelState.ACCEPTED), repr(self._state)
        await utils.wait_for_any((self._send_requests_and_heartbeats(), self._receive_messages())
                                  , loop=self.get_loop())

    def close(self) -> None:
        assert not self.is_closed()
        self._set_state(_ChannelState.CLOSED)
        self._id = b""
        self._next_sequence_number = 0
        self._pending_method_calls1.reset(_MIN_CHANNEL_WINDOW_SIZE)

    async def call_method(self, service_name: bytes, method_index: int, request_data: BytesLike
                          , auto_retry: bool) -> bytes:
        if self.is_closed():
            error_message = "method_call: {}".format(_represent_method_call(service_name
                                                                            , method_index))
            raise errors.ChannelTimedOutError(error_message)

        method_call = _MethodCall(
            service_name=service_name,
            method_index=method_index,
            request_data=bytes(request_data),
            auto_retry=auto_retry,
            response_data=self.get_loop().create_future(),
        )

        try:
            await self._pending_method_calls1.insert_tail(method_call)
        except errors.Error as error:
            error_message = "method_call: {!r}".format(method_call)
            error.args = error_message,
            raise

        try:
            return await method_call.response_data  # type: ignore
        except Exception:
            if not self._pending_method_calls1.is_closed():
                self._pending_method_calls1.try_remove_item(method_call)

            raise

    def call_method_without_return(self, service_name: bytes, method_index: int
                                   , request_data: BytesLike, auto_retry: bool) -> bool:
        if self.is_closed():
            error_message = "method_call: {}".format(_represent_method_call(service_name
                                                                            , method_index))
            raise errors.ChannelTimedOutError(error_message)

        return self._pending_method_calls1.try_insert_tail(_MethodCall(
            service_name=service_name,
            method_index=method_index,
            request_data=bytes(request_data),
            auto_retry=auto_retry,
            response_data=None,
        ))

    def wait_for_opened_unsafely(self) -> "asyncio.Future[None]":
        return self._opening

    async def wait_for_called_methods_unsafely(self) -> None:
        while True:
            calling_method = next(iter(self._calling_methods2), None)

            if calling_method is None:
                break

            try:
                await calling_method
            except asyncio.CancelledError:
                pass

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._transport.get_loop()

    def get_logger(self) -> logging.Logger:
        return self._transport.get_logger()

    def get_timeout(self) -> float:
        timeout = self._timeout

        if timeout < _MIN_CHANNEL_TIMEOUT:
            timeout = _MIN_CHANNEL_TIMEOUT
        elif timeout > _MAX_CHANNEL_TIMEOUT:
            timeout = _MAX_CHANNEL_TIMEOUT

        return self._timeout

    def get_id(self) -> bytes:
        return self._id

    def is_closed(self) -> bool:
        return self._state is _ChannelState.CLOSED

    def _set_state(self, new_state: _ChannelState) -> None:
        old_state = self._state
        error_class: typing.Optional[typing.Type[errors.Error]] = None

        if old_state is _ChannelState.CONNECTING:
            if new_state is old_state:
                return

            if new_state is _ChannelState.CONNECTED:
                pass
            elif new_state is _ChannelState.CLOSED:
                error_class = errors.ChannelTimedOutError
            else:
                assert False, repr(new_state)
        elif old_state is _ChannelState.CONNECTED:
            if new_state is _ChannelState.CONNECTING:
                error_class = errors.ChannelBrokenError
            elif new_state is _ChannelState.CLOSED:
                error_class = errors.ChannelTimedOutError
            else:
                assert False, repr(new_state)
        elif old_state is _ChannelState.ACCEPTING:
            if new_state is _ChannelState.ACCEPTED:
                pass
            elif new_state is _ChannelState.CLOSED:
                error_class = errors.ChannelTimedOutError
            else:
                assert False, repr(new_state)
        elif old_state is _ChannelState.ACCEPTED:
            assert new_state is _ChannelState.CLOSED, repr(new_state)
            error_class = errors.ChannelTimedOutError
        elif old_state is _ChannelState.CLOSED:
            assert new_state in (_ChannelState.CONNECTING, _ChannelState.ACCEPTING), repr(new_state)
        else:
            assert False, repr(old_state)

        if error_class is not None:
            need_retry = error_class is errors.ChannelBrokenError
            method_call: typing.Optional[_MethodCall]

            if new_state is _ChannelState.CLOSED:
                error_class2 = errors.ChannelTimedOutError

                if not self._transport.is_closed():
                    self._transport.close()

                while True:
                    method_call = self._pending_method_calls1.try_remove_head()

                    if method_call is None:
                        break

                    if method_call.response_data is None or method_call.response_data.cancelled():
                        continue

                    error_message = "method_call: {!r}".format(method_call)
                    method_call.response_data.set_exception(error_class2(error_message))

                self._pending_method_calls1.close(error_class2)

                for method_call in self._pending_method_calls2.values():
                    if method_call.response_data is None or method_call.response_data.cancelled():
                        continue

                    error_message = "method_call: {!r}".format(method_call)

                    if need_retry and method_call.auto_retry:
                        method_call.response_data.set_exception(error_class2(error_message))
                    else:
                        method_call.response_data.set_exception(error_class(error_message))

                self._pending_method_calls2.clear()
            else:
                self._pending_method_calls1.commit_item_removals(len(self._pending_method_calls2))

                for method_call in self._pending_method_calls2.values():
                    if method_call.response_data is None or method_call.response_data.cancelled():
                        continue

                    if need_retry and method_call.auto_retry:
                        self._pending_method_calls1.try_insert_tail(method_call)
                    else:
                        error_message = "method_call: {!r}".format(method_call)
                        method_call.response_data.set_exception(error_class(error_message))

                self._pending_method_calls2.clear()

            for calling_method in self._calling_methods1.values():
                calling_method.cancel()

            self._calling_methods2.update(self._calling_methods1.values())
            self._calling_methods1.clear()

        self._state = new_state
        self.get_logger().info("channel state change: channel_id={!r} channel_state={!r}"
                               .format(self._id.hex(), self._state))

        if old_state is _ChannelState.CLOSED:
            self._opening.set_result(None)
        elif new_state is _ChannelState.CLOSED:
            self._opening = self.get_loop().create_future()

    async def _send_requests_and_heartbeats(self) -> None:
        loop = self.get_loop()

        while True:
            method_call = self._pending_method_calls1.try_remove_head(False)

            if method_call is None:
                waiting_for_method_call = self._sending_response = utils.wait_for1(
                    self._pending_method_calls1.remove_head(False),
                    self._get_min_heartbeat_interval(),
                    loop=loop,
                )

                try:
                    method_call = await waiting_for_method_call
                except asyncio.TimeoutError:
                    heartbeat = protocol_pb2.Heartbeat()
                    self._transport.write(protocol_pb2.MESSAGE_HEARTBEAT
                                          , heartbeat.SerializeToString())
                    continue

                if method_call is None:
                    continue

            request = protocol_pb2.Request()
            request_header = request.header
            sequence_number = self._get_sequence_number()
            request_header.sequence_number = sequence_number
            request_header.service_name = method_call.service_name
            request_header.method_index = method_call.method_index
            request.payload = method_call.request_data
            self._transport.write(protocol_pb2.MESSAGE_REQUEST, request.SerializeToString())
            self._pending_method_calls2[sequence_number] = method_call

    async def _receive_messages(self) -> None:
        loop = self.get_loop()

        while True:
            message_type, data = await self._transport.read(self._timeout)

            if message_type == protocol_pb2.MESSAGE_REQUEST:
                if self._request_count == 0:
                    self._request_count = len(self._calling_methods1) + 1

                    def reset_request_count() -> None:
                        self._request_count = 0

                    loop.call_soon(reset_request_count)
                else:
                    self._request_count += 1

                request = protocol_pb2.Request.FromString(data)
                request_header = request.header
                self._call_method(request_header.sequence_number, request_header.service_name
                                  , request_header.method_index, request.payload)
            elif message_type == protocol_pb2.MESSAGE_RESPONSE:
                response = protocol_pb2.Response.FromString(data)
                response_header = response.header
                method_call = self._pending_method_calls2.pop(response_header.sequence_number, None)

                if method_call is None:
                    self.get_logger().warn("missing method call: response_header={!r}"
                                           .format(response_header))
                    continue

                self._pending_method_calls1.commit_item_removals(1)

                if method_call.response_data is None or method_call.response_data.cancelled():
                    continue

                if response_header.error_code == protocol_pb2.ERROR_NO:
                    method_call.response_data.set_result(response.payload)
                else:
                    error_class = errors.get_error_class(response_header.error_code)
                    error_message = "method_call: {!r}".format(method_call)
                    method_call.response_data.set_exception(error_class(error_message
                                                                        , is_remote=True))
            elif message_type == protocol_pb2.MESSAGE_HEARTBEAT:
                heartbeat = protocol_pb2.Heartbeat.FromString(data)
            else:
                assert False, repr(message_type)

    def _call_method(self, sequence_number: int, service_name: bytes, method_index: int
                     , request_data: bytes) -> None:
        if sequence_number in self._calling_methods1:
            self.get_logger().warn("method call duplication: channel_id={!r} sequence_number={!r}"
                                   " method_call={}".format(self._id.hex(), sequence_number\
                , _represent_method_call(service_name, method_index)))
            return

        if self._request_count > self._incoming_window_size:
            self._send_response(sequence_number, protocol_pb2.ERROR_CHANNEL_BUSY, b"")
            return

        service_handler = self._service_handlers.get(service_name, None)

        if service_handler is None:
            self._send_response(sequence_number, protocol_pb2.ERROR_NOT_IMPLEMENTED, b"")
            return

        try:
            response_data = service_handler.call_method(self._owner, method_index, request_data)
        except Exception as exception:
            if isinstance(exception, errors.Error) and exception.CODE >= 1 and not exception\
                .is_remote():
                self._send_response(sequence_number, exception.CODE, b"")
            else:
                self.get_logger().exception("method call failure: channel_id={!r}"
                                            " sequence_number={!r} method_call={}".format(
                    self._id.hex(), sequence_number,
                    _represent_method_call(service_name, method_index),
                ))

                self._send_response(sequence_number, protocol_pb2.ERROR_INTERNAL_SERVER, b"")

            return

        if not inspect.isawaitable(response_data):
            self._send_response(sequence_number, protocol_pb2.ERROR_NO  # type: ignore
                                , response_data)
            return

        def cleanup(calling_method: "asyncio.Task[None]") -> None:
            self._calling_methods2.remove(calling_method)

        async def do() -> None:
            nonlocal cleanup
            calling_method = asyncio.Task.current_task(self.get_loop())
            calling_method.remove_done_callback(cleanup)
            del cleanup

            try:
                response_data2 = await response_data  # type: ignore
            except asyncio.CancelledError:
                self.get_logger().info("method call cancellation: channel_id={!r}"
                                       " sequence_number={!r} method_call={}".format(
                    self._id.hex(), sequence_number,
                    _represent_method_call(service_name, method_index),
                ))

                if calling_method in self._calling_methods2:
                    self._calling_methods2.remove(calling_method)
                    raise

                self._send_response(sequence_number, protocol_pb2.ERROR_INTERNAL_SERVER, b"")
            except Exception as exception:
                if isinstance(exception, errors.Error) and exception.CODE >= 1 and not exception\
                    .is_remote():
                    self._send_response(sequence_number, exception.CODE, b"")
                else:
                    self.get_logger().exception("method call failure: channel_id={!r}"
                                                " sequence_number={!r} method_call={}".format(
                        self._id.hex(), sequence_number,
                        _represent_method_call(service_name, method_index),
                    ))

                    self._send_response(sequence_number, protocol_pb2.ERROR_INTERNAL_SERVER, b"")
            else:
                self._send_response(sequence_number, protocol_pb2.ERROR_NO, response_data2)

            del self._calling_methods1[sequence_number]

        calling_method = self.get_loop().create_task(do())
        calling_method.add_done_callback(cleanup)
        self._calling_methods1[sequence_number] = calling_method

    def _send_response(self, sequence_number: int, error_code: int, response_data: bytes) -> None:
        response = protocol_pb2.Response()
        response_header = response.header
        response_header.sequence_number = sequence_number
        response_header.error_code = error_code
        response.payload = response_data
        self._transport.write(protocol_pb2.MESSAGE_RESPONSE, response.SerializeToString())

        if not self._sending_response.done():
            self._sending_response.set_result(None)

    def _get_sequence_number(self) -> int:
        sequence_number = self._next_sequence_number
        self._next_sequence_number = (sequence_number + 1) & 0xFFFFFFFF
        return sequence_number

    def _get_min_heartbeat_interval(self) -> float:
        return self._timeout / 3


_MIN_CHANNEL_TIMEOUT = 3.0
_MAX_CHANNEL_TIMEOUT = 7.0
_MIN_CHANNEL_WINDOW_SIZE = 1 << 8
_MAX_CHANNEL_WINDOW_SIZE = 1 << 16


def _represent_method_call(service_name: bytes, method_index: int) -> str:
    return "<{}[{}]>".format(service_name.decode("ascii"), method_index)


if typing.TYPE_CHECKING:
    from . import channels
