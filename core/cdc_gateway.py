import aiohttp
import argparse
import asyncio
import ccxt.pro
import hashlib
import hmac
import json
import math
import os
import time
import websockets
from base_app import MessageType
from dotenv import load_dotenv
from proxy_app import ProxyApp


class CdcGateway(ProxyApp):
    def __init__(self, config):
        super().__init__(config)
        load_dotenv()
        self.api_key = os.environ.get(self.config['API']['api_key_env'])
        self.api_secret = os.environ.get(self.config['API']['secret_env'])
        self.exchange_id = self.config['Instrument']['exchange']
        self.instruments = [instrument.strip() for instrument in self.config['Instrument']['instruments'].split(',')]
        self.instruments_map = None
        self.market_websocket = None
        self.user_websocket = None
        # use for message parsing
        self.exchange = ccxt.pro.cryptocom()

    async def post_start(self):
        await super().post_start()

        # Call REST API to get instruments
        rest_endpoint_prefix = self.config['API']['rest_endpoint_prefix']
        instruments_url = rest_endpoint_prefix + "public/get-instruments"
        async with aiohttp.ClientSession() as session:
            async with session.get(instruments_url) as response:
                if response.status == 200:
                    instruments_data = await response.json()
                    if instruments_data.get("code") == 0:
                        self.instruments_map = {
                            instrument["symbol"]: instrument
                            for instrument in instruments_data["result"]["data"]
                        }
                        self.logger.info(f"{self.app_name} - Instruments map updated.")
                    else:
                        self.logger.error(
                            f"{self.app_name} - Failed to get instruments: {instruments_data.get('message')}")
                else:
                    self.logger.error(f"{self.app_name} - Failed to get instruments, HTTP status: {response.status}")

        market_url = self.config['API']['market_url']
        self.market_websocket = await websockets.connect(market_url)
        self.logger.info(f"{self.app_name} - Connected to market data WebSocket at {market_url}")
        await self.subscribe_to_market_channels()

        user_url = self.config['API']['user_url']
        self.user_websocket = await websockets.connect(user_url)
        self.logger.info(f"{self.app_name} - Connected to user data WebSocket at {user_url}")
        await self.authenticate_user_websocket()
        await self.subscribe_to_user_channels()

        task1 = asyncio.create_task(self.command_message_handler())
        task2 = asyncio.create_task(self.market_data_handler())
        task3 = asyncio.create_task(self.user_data_handler())
        self.tasks.update({task1, task2, task3})

    async def pre_stop(self):
        await self.exchange.close()
        if self.market_websocket:
            await self.market_websocket.close()
        if self.user_websocket:
            await self.user_websocket.close()
        await super().pre_stop()

    async def subscribe_to_market_channels(self):
        book_depth = self.config['Instrument']['book_depth']
        channels = ",".join([f"book.{instrument}.{book_depth}" for instrument in self.instruments])
        order_book_payload = {
            "id": int(time.time() * 1000),
            "method": "subscribe",
            "params": {
                "channels": channels
            }
        }
        order_book_payload_json = json.dumps(order_book_payload)
        self.logger.info(
            f"{self.app_name} - Sending subscribe request for market channels with payload: {order_book_payload_json}")
        await self.market_websocket.send(order_book_payload_json)
        self.logger.info(f"{self.app_name} - Subscribed to market channels: {channels}")

    async def authenticate_user_websocket(self):
        nonce = int(time.time() * 1000)
        method = 'public/auth'
        payload_str = method + str(nonce) + self.api_key + str(nonce)
        signature = hmac.new(bytes(self.api_secret, 'utf-8'), msg=bytes(payload_str, 'utf-8'),
                             digestmod=hashlib.sha256).hexdigest()
        auth_payload = {
            "id": nonce,
            "method": method,
            "api_key": self.api_key,
            "sig": signature,
            "nonce": nonce
        }
        auth_payload_json = json.dumps(auth_payload)
        self.logger.info(f"{self.app_name} - Sending authentication request with payload: {auth_payload_json}")
        await self.user_websocket.send(auth_payload_json)
        auth_response = await self.user_websocket.recv()
        self.logger.info(f"{self.app_name} - Received authentication response: {auth_response}")
        auth_message = json.loads(auth_response)
        if auth_message.get('code') == 0:
            self.user_websocket.authenticated = True
        else:
            error_msg = f"Authentication failed: {auth_message.get('message')}"
            self.logger.error(f"{self.app_name} - {error_msg}")
            raise Exception(error_msg)

    async def subscribe_to_user_channels(self):
        if not getattr(self.user_websocket, 'authenticated', False):
            raise Exception("Cannot subscribe to user channels without successful authentication.")
        channels = ["user.order"]
        user_channels_payload = {
            "id": int(time.time() * 1000),
            "method": "subscribe",
            "params": {
                "channels": channels
            }
        }
        user_channels_payload_json = json.dumps(user_channels_payload)
        self.logger.info(
            f"{self.app_name} - Sending subscribe request for user channels with payload: {user_channels_payload_json}")
        await self.user_websocket.send(user_channels_payload_json)
        self.logger.info(f"{self.app_name} - Subscribed to user channels: {channels}")

    async def market_data_handler(self):
        while not self.shutdown_event.is_set():
            try:
                market_response = await self.market_websocket.recv()
                self.logger.debug(f"{self.app_name} - Received market data: {market_response}")
                market_message = json.loads(market_response)
                if 'method' in market_message:
                    method = market_message['method']
                    if method == 'public/heartbeat':
                        await self.handle_heartbeat(self.market_websocket, market_message)
                    elif method == 'subscribe':
                        if 'result' in market_message:
                            result = market_message['result']
                            if result['channel'] == 'book':
                                instrument_name = result['instrument_name']
                                data0 = result['data'][0]
                                order_book = self.exchange.parse_order_book(orderbook=data0,
                                                                            symbol=instrument_name,
                                                                            timestamp=data0['t'])
                                self.logger.debug(
                                    f"{self.app_name} - Sending order book for {instrument_name}: {order_book}")
                                message = {
                                    'msg_type': MessageType.ORDER_BOOK.value,
                                    'exchange': self.exchange_id,
                                    'symbol': instrument_name,
                                    'data': order_book
                                }
                                await self.send(message)
            except Exception as e:
                self.logger.error(f"{self.app_name} - An unexpected error occurred with market data WebSocket: {e}")

    async def user_data_handler(self):
        while not self.shutdown_event.is_set():
            try:
                user_response = await self.user_websocket.recv()
                self.logger.info(f"{self.app_name} - Received user data: {user_response}")
                user_message = json.loads(user_response)
                if 'method' in user_message:
                    method = user_message['method']
                    if method == 'public/heartbeat':
                        await self.handle_heartbeat(self.user_websocket, user_message)
                    elif method == 'private/create-order':
                        if user_message['code'] != 0:
                            self.logger.error(
                                f"{self.app_name} - create order reject for {user_message['result']['client_oid']}")
                            reject_message = {
                                'msg_type': MessageType.CREATE_ORDER_REJECT.value,
                                'exchange': self.exchange_id,
                                'data': {'params': {'clientOrderId': user_message['result']['client_oid']}}
                            }
                            await self.send(reject_message)
                    elif method == 'subscribe':
                        if 'result' in user_message:
                            result = user_message['result']
                            if result['channel'].startswith('user.order'):
                                orders = self.exchange.parse_orders(orders=result['data'])
                                for order in orders:
                                    self.logger.info(
                                        f"{self.app_name} - sending order update for {order['symbol']}[{order['clientOrderId']}]")
                                    message = {
                                        'msg_type': MessageType.ORDER_UPDATE.value,
                                        'exchange': self.exchange_id,
                                        'symbol': order['symbol'],
                                        'data': order
                                    }
                                    await self.send(message)
                            # TODO handle trades
            except Exception as e:
                self.logger.error(f"{self.app_name} - An unexpected error occurred with user data WebSocket: {e}")

    async def handle_heartbeat(self, websocket, message):
        self.logger.debug(f"{self.app_name} - Received heartbeat message: {message}")
        if 'id' in message:
            await websocket.send(json.dumps({
                "id": message['id'],
                "method": "public/respond-heartbeat"
            }))
        self.logger.debug(f"{self.app_name} - Responded to heartbeat with message ID: {message['id']}")

    async def command_message_handler(self):
        while not self.shutdown_event.is_set():
            message = await self.receive()
            if message.get('exchange') == self.exchange_id:
                if message['msg_type'] == MessageType.CREATE_ORDER.value:
                    self.logger.info(f"{self.app_name} - received create order for {message['symbol']}")
                    await self.create_order(**message['data'])
                elif message['msg_type'] == MessageType.CANCEL_ORDER.value:
                    self.logger.info(f"{self.app_name} - received cancel order for {message['data']['id']}")
                    await self.cancel_order(**message['data'])

    async def create_order(self, symbol, side, type, price, amount, params):
        instrument = self.instruments_map[symbol]
        price_tick_size = float(instrument['price_tick_size'])
        qty_tick_size = float(instrument['qty_tick_size'])
        side = side.upper()
        if side == 'BUY':
            price = math.floor(price / price_tick_size) * price_tick_size
        elif side == 'SELL':
            price = math.ceil(price / price_tick_size) * price_tick_size
        amount = math.floor(amount / qty_tick_size) * qty_tick_size

        client_order_id = params["clientOrderId"]
        exec_inst = ["POST_ONLY"] if params.get("postOnly") else []
        order_payload = {
            "id": int(time.time() * 1000),
            "method": "private/create-order",
            "params": {
                "instrument_name": symbol,
                "side": side,
                "type": type.upper(),
                "price": f"{price:.{instrument['quote_decimals']}f}",
                "quantity": f"{amount:.{instrument['quantity_decimals']}f}",
                "client_oid": client_order_id,
                "exec_inst": exec_inst
            }
        }
        order_payload_json = json.dumps(order_payload)
        self.logger.info(f"{self.app_name} - Sending place order request with payload: {order_payload_json}")
        await self.user_websocket.send(order_payload_json)
        self.logger.info(
            f"{self.app_name} - Sent request to place new sell order with ID: {client_order_id} at price: {price}")

    async def cancel_order(self, id=0, params={}):
        if 'clientOrderId' in params:
            cancel_payload = {
                "id": int(time.time() * 1000),
                "method": "private/cancel-order",
                "params": {
                    "client_oid": params['clientOrderId']
                }
            }
        else:
            cancel_payload = {
                "id": int(time.time() * 1000),
                "method": "private/cancel-order",
                "params": {
                    "order_id": id
                }
            }
        cancel_payload_json = json.dumps(cancel_payload)
        self.logger.info(f"{self.app_name} - Sending cancel order request with payload: {cancel_payload_json}")
        await self.user_websocket.send(cancel_payload_json)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the CdcGateway app with the specified configuration")
    parser.add_argument('--config', type=str, help='Path to the configuration file', required=True)
    args = parser.parse_args()

    asyncio.run(CdcGateway(args.config).run())
