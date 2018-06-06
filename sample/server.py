from asyncio_toolkit import monkey_patch
monkey_patch.apply()


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


class Server(async_pbrpc.Server):
    def create_channel(self, stream_reader: asyncio.StreamReader
                       , stream_writer: asyncio.StreamWriter) -> async_pbrpc.ServerChannel:
        channel = async_pbrpc.ServerChannel(
            self.get_loop(), self.get_logger(), self.get_transport_policy(),
            stream_reader,
            stream_writer,
        )

        channel.add_service_handler(ServerServiceHandler())
        return channel


loop = asyncio.get_event_loop()
server = Server("127.0.0.1", 8888)
loop.run_until_complete(server.start())

try:
    loop.run_forever()
except KeyboardInterrupt:
    pass

server.stop()
loop.run_until_complete(server.wait_for_stopped())
loop.close()
