import asyncio
import logging
import ssl
import time
import typing

from asyncio_toolkit import utils
from asyncio_toolkit.delay_pool import DelayPool
from asyncio_toolkit.typing import BytesLike, Coroutine

from .channel_impl import ChannelImpl
from .service_handler import ServiceHandler


ChannelStopCallback = typing.Callable[[typing.Any], None]


class Channel:
    def __init__(self, impl: ChannelImpl) -> None:
        self._impl = impl
        done_future: asyncio.Future[None] = utils.make_done_future(self.get_loop())
        self._starting: asyncio.Future[None] = done_future
        self._running: asyncio.Future[None] = done_future
        self._is_stopping = False
        self._stop_callbacks: typing.List[ChannelStopCallback] = []

    def add_service_handler(self, service_handler: typing.Type[ServiceHandler]) -> None:
        self._impl.add_service_handler(service_handler)

    def remove_service_handler(self, service_handler: typing.Type[ServiceHandler]) -> None:
        self._impl.remove_service_handler(service_handler)

    def add_stop_callback(self, stop_callback: ChannelStopCallback) -> None:
        self._stop_callbacks.append(stop_callback)

    def remove_stop_callback(self, stop_callback: ChannelStopCallback) -> None:
        self._stop_callbacks.remove(stop_callback)

    async def start(self) -> None:
        assert not self.is_running()

        if not self._starting.done():
            await utils.shield(self._starting)
            return

        running = self.get_loop().create_task(self._run())
        self._starting = utils.Future(loop=self.get_loop())

        try:
            await utils.delay_cancellation(self._impl.wait_for_opened())
        except Exception:
            running.cancel()
            await utils.delay_cancellation(running)
            raise
        finally:
            self._starting.set_result(None)
            self._running = running

    def stop(self) -> None:
        assert self.is_running()

        if self._is_stopping:
            return

        self._running.cancel()
        self._is_stopping = True

    @property
    def call_method(self) -> typing.Callable[[bytes, int, BytesLike, bool], Coroutine[bytes]]:
        return self._impl.call_method

    @property
    def call_method_without_return(self) -> typing.Callable[[bytes, int, BytesLike, bool], bool]:
        return self._impl.call_method_without_return

    def wait_for_stopped(self) -> "asyncio.Future[None]":
         return self._running if self._running.done() else utils.shield(self._running)

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._impl.get_loop()

    def get_logger(self) -> logging.Logger:
        return self._impl.get_logger()

    def is_running(self) -> bool:
        return not self._running.done()

    def is_stopping(self) -> bool:
        return not self._is_stopping

    async def _run(self) -> None:
        raise NotImplementedError()


class ClientChannel(Channel):
    def __init__(self, host_name: str, port_number: int, *,
        loop: typing.Optional[asyncio.AbstractEventLoop]=None,
        logger: typing.Optional[logging.Logger]=None,
        timeout=5.0,
        outgoing_window_size=1 << 12,
        incoming_window_size=1 << 12,
        ssl_context: typing.Optional[ssl.SSLContext]=None,
    ) -> None:
        super().__init__(ChannelImpl(self, loop, logger, timeout, outgoing_window_size
                                     , incoming_window_size))
        server_address = host_name, port_number
        self._ssl_context = ssl_context
        self._server_addresses = DelayPool((server_address,), 3.0, self._impl.get_timeout()
                                           , self.get_loop(), self.get_logger())

    async def _run(self) -> None:
        channel_id = self._impl.get_id().hex()

        try:
            while True:
                server_address = await self._server_addresses.allocate_item()

                if server_address is None:
                    self.get_logger().error("client channel connection failure: channel_id={!r}"
                                            .format(channel_id))
                    break

                self.get_logger().info("client channel connection: channel_id={!r}"
                                       " server_address={!r}".format(channel_id, server_address))
                connect_deadline = self._server_addresses.when_next_item_allocable()

                try:
                    await self._impl.connect(*server_address, self._ssl_context, connect_deadline)
                    channel_id = self._impl.get_id().hex()
                    self._server_addresses.reset(None, self._impl.get_timeout())
                    await self._impl.dispatch()
                except (
                    ConnectionRefusedError,
                    ConnectionResetError,
                    TimeoutError,
                    asyncio.TimeoutError,
                    asyncio.IncompleteReadError,
                ):
                    pass
        except asyncio.CancelledError:
            pass
        except Exception:
            self.get_logger().error("client channel run failure: channel_id={!r}"
                                    .format(channel_id))

        if self._is_stopping:
            self.get_logger().info("client channel stop (passive): channel_id={!r}"
                                    .format(channel_id))
        else:
            self.get_logger().info("client channel stop (active): channel_id={!r}"
                                   .format(channel_id))
            self._is_stopping = True

        if not self._impl.is_closed():
            self._impl.close()

        await self._impl.wait_for_called_methods()
        self._server_addresses.reset(None, None)

        for stop_callback in self._stop_callbacks:
            try:
                stop_callback(self)
            except Exception:
                self.get_logger().exception("client channel stop callback failure: channel_id={!r}"
                                            .format(channel_id))

        self._stop_callbacks.clear()
        self._is_stopping = False


class ServerChannel(Channel):
    def __init__(self, stream_reader: asyncio.StreamReader, stream_writer: asyncio.StreamWriter, *,
        loop: typing.Optional[asyncio.AbstractEventLoop]=None,
        logger: typing.Optional[logging.Logger]=None,
        timeout=5.0,
        outgoing_window_size=1 << 12,
        incoming_window_size=1 << 12,
    ) -> None:
        super().__init__(ChannelImpl(self, loop, logger, timeout, outgoing_window_size
                                     , incoming_window_size))
        self._stream_rw = stream_reader, stream_writer

    async def _run(self) -> None:
        channel_id = self._impl.get_id().hex()

        try:
            accept_deadline = time.monotonic() + self._impl.get_timeout()
            await self._impl.accept(self._stream_rw, accept_deadline)
            channel_id = self._impl.get_id().hex()
            self.get_logger().info("server channel acceptance: channel_id={!r} client_address={!r}"\
                .format(channel_id, self._stream_rw[1].get_extra_info("peername")))
            await self._impl.dispatch()
        except (
            ConnectionResetError,
            TimeoutError,
            asyncio.CancelledError,
            asyncio.TimeoutError,
            asyncio.IncompleteReadError,
        ):
            pass
        except Exception:
            self.get_logger().exception("server channel run failure: channel_id={!r}"
                                        .format(channel_id))

        if self._is_stopping:
            self.get_logger().info("server channel stop (passive): channel_id={!r}"
                                    .format(channel_id))
        else:
            self.get_logger().info("server channel stop (active): channel_id={!r}"
                                   .format(channel_id))
            self._is_stopping = True

        if not self._impl.is_closed():
            self._impl.close()

        await self._impl.wait_for_called_methods()

        for stop_callback in self._stop_callbacks:
            try:
                stop_callback(self)
            except Exception:
                self.get_logger().exception("server channel stop callback failure: channel_id={!r}"
                                            .format(channel_id))

        self._stop_callbacks.clear()
        self._is_stopping = False
