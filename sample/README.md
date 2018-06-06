# Sample

### Source

- [services.proto](./services.proto)

    ```protobuf
    syntax = "proto3";

    import "async_pbrpc.proto";

    message GetNameResponse {
        string name = 1;
    }

    service ClientService {
        rpc get_name(async_pbrpc.Void) returns (GetNameResponse);
    }

    message SayHelloRequest {
        string hello = 1;
    }

    message SayHelloResponse {
        string hello = 1;
    }

    service ServerService {
        rpc say_hello(SayHelloRequest) returns (SayHelloResponse);
    }
    ```

- [client.py](./client.py)

  ```python
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
  ```

- [server.py](./server.py)

  ```python
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
  ```

### Startup

1. compile `services.proto` to  [services_pb2.py](./services_pb2.py) and [services_pbrpc.py](./services_pbrpc.py) (RPC stub)

   ```bash
   protoc --proto_path=. --proto_path="${VIRTUAL_ENV:-/usr/local}/include" --plugin=protoc-gen-pbrpc="$(which protoc-gen-pbrpc)" --python_out=. --pbrpc_out=. *.proto
   ```

2. run rpc server

   ```bash
   python3 server.py
   ```

3. run rpc client

4. ```bash
   python3 client.py
   # output: hello, async-pbrpc!
   ```
