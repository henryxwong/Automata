import argparse
import asyncio
import msgpack
import time
import zmq
from base_app import BaseApp, MessageType
from collections import deque


async def send_and_receive(req_socket, packed_message):
    # Send the request
    await req_socket.send(packed_message)

    # Await the reply
    reply = await req_socket.recv()
    return reply  # Return the reply for later processing


class Sequencer(BaseApp):
    def __init__(self, config_file):
        super().__init__(config_file)
        self.req_sockets = {}
        self.message_queue = deque()
        self.input_socket = None
        self.output_socket = None

    def _should_publish(self):
        return False

    async def post_start(self):
        self.input_socket = self.zmq_context.socket(zmq.PULL)
        self.input_socket.bind(self.config['ZeroMQ']['pull_endpoint'])
        self.output_socket = self.zmq_context.socket(zmq.PUB)
        self.output_socket.bind(self.config['ZeroMQ']['pub_endpoint'])

        task1 = asyncio.create_task(self.sequencing())
        self.tasks.update({task1})

    async def sequencing(self):
        while not self.shutdown_event.is_set():
            try:
                message = await self.input_socket.recv()
                unpacked_message = msgpack.unpackb(message, raw=False)
                self.logger.debug(f"Received message with type: {unpacked_message.get('msg_type')}")
            except Exception as e:
                self.logger.error(f"Failed to receive or unpack message: {e}")
                continue
            if unpacked_message.get('msg_type') == MessageType.CONNECT.value:
                connection_id = unpacked_message['connection_id']
                endpoint = f"{self.config['ZeroMQ']['req_endpoint_prefix']}_{connection_id}"
                req_socket = self.zmq_context.socket(zmq.REQ)
                req_socket.connect(endpoint)
                self.req_sockets[connection_id] = req_socket
                self.logger.info(f"Connected new request socket for connection_id: {connection_id}")
            elif unpacked_message.get('msg_type') == MessageType.DISCONNECT.value:
                connection_id = unpacked_message['connection_id']
                req_socket = self.req_sockets.pop(connection_id, None)
                if req_socket:
                    req_socket.close()
                    self.logger.info(f"Disconnected request socket for connection_id: {connection_id}")
            else:
                msg_time = time.time_ns()

                self.message_queue.append(unpacked_message)
                while len(self.message_queue) > 0:
                    queued_message = self.message_queue.popleft()
                    queued_message['msg_time'] = msg_time
                    try:
                        packed_message = msgpack.packb(queued_message)
                        # Create a list of coroutines for each REQ socket
                        tasks = [send_and_receive(req_socket, packed_message) for req_socket in
                                 self.req_sockets.values()]

                        # Run the coroutines concurrently and collect replies in order
                        replies = await asyncio.gather(*tasks)

                        # Process replies in order
                        for reply in replies:
                            unpacked_replies = msgpack.unpackb(reply, raw=False)
                            if len(unpacked_replies) > 0:
                                for unpacked_reply in unpacked_replies:
                                    self.message_queue.append(unpacked_reply)

                        await self.output_socket.send(packed_message)
                        self.logger.debug(f"Dispatched message with type: {queued_message.get('msg_type')}")
                    except Exception as e:
                        self.logger.error(f"Failed to process or dispatch message: {e}")
                        continue

    async def pre_stop(self):
        try:
            for socket in self.req_sockets.values():
                socket.close()
            self.req_sockets.clear()
            self.output_socket.close()
            self.input_socket.close()
            self.logger.info("Closed all sockets successfully during pre_stop.")
        except Exception as e:
            self.logger.error(f"An error occurred during pre_stop: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Sequencer app with the specified configuration")
    parser.add_argument('--config', type=str, help='Path to the configuration file', required=True)
    args = parser.parse_args()

    asyncio.run(Sequencer(args.config).run())
