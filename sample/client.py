from asyncio_toolkit import monkey_patch
monkey_patch.apply()


import asyncio

import async_pbrpc

import services_pb2
import services_pbrpc


class ClientServiceHandler(services_pbrpc.ClientServiceHandler):
    def get_name(self, channel: async_pbrpc.ClientChannel) -> services_pb2.GetNameResponse:
        name = "async-pbrpc"
        response = services_pb2.GetNameResponse(name=name)
        return response


async def make_connection() -> None:
    channel = async_pbrpc.ClientChannel("127.0.0.1", 8888)
    channel.add_service_handler(ClientServiceHandler())
    await channel.start()
    service_client = services_pbrpc.ServerServiceClient(channel)
    request = services_pb2.SayHelloRequest(hello="hello")
    response = await service_client.say_hello(request)
    print(response.hello)
    channel.stop()
    await channel.wait_for_stopped()


loop = asyncio.get_event_loop()
loop.run_until_complete(make_connection())
loop.close()
