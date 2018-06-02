import asyncio

import async_pbrpc

import services_pb2
import services_pbrpc


class ServerServiceHandler(services_pbrpc.ServerServiceHandler):
    async def say_hello(self, channel: async_pbrpc.ServerChannel
                        , request: services_pb2.SayHelloRequest) -> services_pb2.SayHelloResponse:
        service_client = services_pbrpc.ClientServiceClient(channel)
        response2 = await service_client.get_name()
        hello = "{}, {}!".format(request.hello, response2.name)
        response = services_pb2.SayHelloResponse(hello=hello)
        return response


channels = set()


async def handle_connection(stream_reader: asyncio.StreamReader
                            , stream_writer: asyncio.StreamWriter) -> None:
    channel = async_pbrpc.ServerChannel(stream_reader, stream_writer)
    channels.add(channel)
    channel.add_stop_callback(lambda channel: channels.remove(channel))
    channel.add_service_handler(ServerServiceHandler())
    await channel.start()


loop = asyncio.get_event_loop()
server = loop.run_until_complete(asyncio.start_server(handle_connection, "127.0.0.1", 8888))

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

server.close()
loop.run_until_complete(server.wait_closed())
loop.run_until_complete(asyncio.gather(*(channel.wait_for_stopped() for channel in channels)))
loop.close()
