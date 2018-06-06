import asyncio
import logging
import typing

from asyncio_toolkit import utils

from .channels import ServerChannel
from .transport import TransportPolicy, Transport


class Server:
    def __init__(self, host_name: str, port_number: int, *,
        loop: typing.Optional[asyncio.AbstractEventLoop]=None,
        logger: typing.Optional[logging.Logger]=None,
        transport_policy: typing.Optional[TransportPolicy]=None,
    ) -> None:
        dummy_transport = Transport(loop, logger, transport_policy)
        self._loop = dummy_transport.get_loop()
        self._logger = dummy_transport.get_logger()
        self._transport_policy = dummy_transport.get_policy()
        self._address = host_name, port_number
        self._channels: typing.Set[ServerChannel] = set()
        self._starting: typing.Optional[asyncio.Future[None]] = None
        self._running: asyncio.Future[None] = utils.make_done_future(self._loop)
        self._is_stopping = 0
        self._version = 0
        self.on_initialize()

    def on_initialize(self) -> None:
        pass

    async def start(self) -> None:
        assert not self.is_running()

        if self._starting is not None:
            await utils.shield(self._starting, loop=self._loop)
            return

        self._starting = self._loop.create_future()
        running = self._loop.create_task(self._run())

        try:
            await utils.delay_cancellation(self._starting, loop=self._loop)
        except Exception:
            running.cancel()
            await utils.delay_cancellation(running, loop=self._loop)
            raise
        finally:
            self._starting = None
            self._running = running

    def stop(self, force=False) -> None:
        assert self.is_running()

        if self.is_stopping():
            return

        self._running.cancel()

        if force:
            for channel in self._channels:
                channel.stop()

            self._is_stopping = -1
        else:
            self._is_stopping = 1

    def create_channel(self, stream_reader: asyncio.StreamReader
                       , stream_writer: asyncio.StreamWriter) -> ServerChannel:
        return ServerChannel(self._loop, self._logger, self._transport_policy, stream_reader
                             , stream_writer)

    def wait_for_stopped(self) -> "asyncio.Future[None]":
         return self._running if self._running.done() else utils.shield(self._running
                                                                        , loop=self._loop)

    def get_loop(self) -> asyncio.AbstractEventLoop:
        return self._loop

    def get_logger(self) -> logging.Logger:
        return self._logger

    def get_transport_policy(self) -> TransportPolicy:
        return self._transport_policy

    def is_running(self) -> bool:
        return not self._running.done()

    def is_stopping(self) -> bool:
        return self._is_stopping != 0

    async def _run(self) -> None:
        try:
            base = await asyncio.start_server(
                self._on_connection_accept,
                *self._address,
                loop=self._loop,
                ssl=self._transport_policy.ssl_context,
                limit=self._transport_policy.read_buffer_size,
            )
        except Exception as exception:
            self._starting.set_exception(exception)  # type: ignore
            return

        self._starting.set_result(None)  # type: ignore

        try:
            await self._loop.create_future()
        except asyncio.CancelledError:
            pass

        base.close()
        await base.wait_closed()

        while True:
            channel = next(iter(self._channels), None)

            if channel is None:
                break

            await channel.wait_for_stopped_unsafely()

        self._is_stopping = 0
        self._version += 1

    async def _on_connection_accept(self, stream_reader: asyncio.StreamReader
                                    , stream_writer: asyncio.StreamWriter) -> None:
        self._logger.info("connection acceptance: client_address={!r}"
                          .format(stream_writer.get_extra_info("peername")))
        channel = self.create_channel(stream_reader, stream_writer)
        version = self._version
        await channel.start()

        if not channel.is_running():
            return

        if self._version != version:
            channel.stop()
            return

        self._channels.add(channel)
        channel.add_stop_callback(self._channels.remove)

        if self._is_stopping < 0:
            channel.stop()
