import asyncio
import msgpack
import zmq
from abc import ABC, abstractmethod
from core.base_app import BaseApp, MessageType


class Strategy(BaseApp, ABC):

    def __init__(self, config_file):
        super().__init__(config_file)
        self.connection_id = self.config['ZeroMQ']['connection_id']
        self.endpoint_prefix = self.config['ZeroMQ']['rep_endpoint_prefix']
        self.rep_socket = None
        self.replies = []

    async def post_start(self):
        self.rep_socket = self.zmq_context.socket(zmq.REP)
        endpoint = f"{self.endpoint_prefix}_{self.connection_id}"
        self.rep_socket.bind(endpoint)
        self.logger.info(f"REP socket bound to {endpoint} for connection_id: {self.connection_id}")

        # Send connect message
        connect_message = {
            'msg_type': MessageType.CONNECT.value,
            'connection_id': self.connection_id
        }
        await self.send(connect_message)
        self.tasks.update({asyncio.create_task(self.request_and_reply())})

    async def request_and_reply(self):
        while not self.shutdown_event.is_set():
            self.replies.clear()
            message = await self.rep_socket.recv()
            request = msgpack.unpackb(message, raw=False)
            self.logger.debug(f"Received request: {request}")
            self.virtual_time = request.get('msg_time', self.virtual_time)
            self.handle_request(request)
            self.logger.debug(f"Sending replies: {self.replies}")
            await self.rep_socket.send(msgpack.packb(self.replies))

    @abstractmethod
    def handle_request(self, request):
        pass

    async def pre_stop(self):
        # Send disconnect message
        disconnect_message = {
            'msg_type': MessageType.DISCONNECT.value,
            'connection_id': self.connection_id
        }
        await self.send(disconnect_message)

        # Close REP socket
        if self.rep_socket:
            self.rep_socket.close()
            self.rep_socket = None
            self.logger.info(f"REP socket for connection_id: {self.connection_id} has been closed.")

    def send_order(self, exchange, symbol, order_side, price, quantity, client_order_id, order_type='limit',
                         post_only=True):
        message = {
            'msg_type': MessageType.CREATE_ORDER.value,
            'exchange': exchange,
            'symbol': symbol,
            'data': {
                'symbol': symbol,
                'type': order_type,
                'side': order_side,
                'amount': quantity,
                'price': price,
                'params': {
                    'client_oid': client_order_id,  # ccxt doesn't map it correctly
                    'clientOrderId': client_order_id,
                    'postOnly': post_only
                }
            }
        }
        self.replies.append(message)

    def cancel_order(self, exchange, symbol, order_id, client_order_id):
        message = {
            'msg_type': MessageType.CANCEL_ORDER.value,
            'exchange': exchange,
            'symbol': symbol,
            'data': {
                'id': order_id,
                'params': {
                    'clientOrderId': client_order_id
                }
            }
        }
        self.replies.append(message)

    def cancel_all_orders(self, exchange, symbol):
        message = {
            'msg_type': MessageType.CANCEL_ALL_ORDER.value,
            'exchange': exchange,
            'symbol': symbol,
            'data': {
                'symbol': symbol
            }
        }
        self.replies.append(message)
