import msgpack
import zmq
from abc import ABC, abstractmethod
from base_app import BaseApp
from typing import final


class ProxyApp(BaseApp, ABC):

    def __init__(self, config_file):
        super().__init__(config_file)
        self.subscriber_socket = None

    async def post_start(self):
        self.subscriber_socket = self.zmq_context.socket(zmq.SUB)
        self.subscriber_socket.connect(self.config['ZeroMQ']['sub_endpoint'])
        self.subscriber_socket.subscribe('')

    async def pre_stop(self):
        if self.subscriber_socket:
            self.subscriber_socket.close()

    @final
    async def receive(self):
        if self.subscriber_socket:
            message = await self.subscriber_socket.recv()
            unpacked_msg = msgpack.unpackb(message, raw=False)
            self.virtual_time = unpacked_msg.get('msg_time', self.virtual_time)
            return unpacked_msg
        return None
