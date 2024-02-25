import argparse
import asyncio
import msgpack
import time
import zmq
from base_app import BaseApp

class Sequencer(BaseApp):
    def _should_publish(self):
        return False

    def _should_subscribe(self):
        return False

    def post_start(self):
        self.input_socket = self.zmq_context.socket(zmq.PULL)
        self.input_socket.bind(self.config['ZeroMQ']['pull_endpoint'])
        self.output_socket = self.zmq_context.socket(zmq.PUB)
        self.output_socket.bind(self.config['ZeroMQ']['pub_endpoint'])

        task1 = asyncio.create_task(self.sequencing())
        self.tasks.update({task1})

    async def sequencing(self):
        while not self.shutdown_event.is_set():
            message = await self.input_socket.recv()
            unpacked_message = msgpack.unpackb(message, raw=False)
            self.logger.debug(f"Processing message with type: {unpacked_message.get('msg_type')}")
            unpacked_message['msg_time'] = time.time_ns()
            await self.output_socket.send(msgpack.packb(unpacked_message))

    async def pre_stop(self):
        self.output_socket.close()
        self.input_socket.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the Sequencer app with the specified configuration")
    parser.add_argument('--config', type=str, help='Path to the configuration file', required=True)
    args = parser.parse_args()

    asyncio.run(Sequencer(args.config).run())
