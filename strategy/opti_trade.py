import sys
import os
import argparse
import asyncio
import math
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from core.base_app import MessageType
from core.strategy import Strategy

class OptiTrade(Strategy):
    def __init__(self, config_file):
        super().__init__(config_file)
        self.exchange = self.config['OptiTrade']['exchange']
        self.symbol = self.config['OptiTrade']['symbol']
        self.tick_size = float(self.config['OptiTrade']['tick_size'])
        self.order_quantity = int(self.config['OptiTrade']['order_quantity'])
        self.sleep_time = int(self.config['OptiTrade']['sleep_time_sec']) * 1_000_000_000
        self.client_order_id_prefix = self.config['OptiTrade']['client_order_id_prefix']
        self.order_side = self.config['OptiTrade']['order_side'].lower()
        self.exec_mode = self.config['OptiTrade'].get('exec_mode', 'MID')  # Default to 'MID' if not specified
        self.sequence_number = 0
        self.order_book = None
        self.last_order_time = None
        self.open_orders = {}
        self.pending_new = set()
        self.pending_cancel = set()

    def post_start(self):
        self.tasks.update({asyncio.create_task(self.handle_message())})

    async def pre_stop(self):
        try:
            await self.cancel_all_orders(self.exchange, self.symbol)
            self.logger.info("Sent cancel all orders request.")
        except Exception as e:
            self.logger.error(f"Failed to send cancel all orders request: {e}", exc_info=True)

    async def handle_message(self):
        while not self.shutdown_event.is_set():
            try:
                message = await self.receive()
                if message.get('exchange') == self.exchange and message.get('symbol') == self.symbol:
                    if message['msg_type'] == MessageType.ORDER_BOOK.value:
                        self.order_book = message['data']
                        await self.manage_orders()
                    elif message['msg_type'] == MessageType.ORDER_UPDATE.value:
                        order = message['data']
                        clientOrderId = order['clientOrderId']
                        if order['status'] == 'open':
                            # Add or update the order in the open_orders dict
                            self.open_orders[clientOrderId] = order
                        elif order['status'] in ['closed', 'canceled', 'expired', 'rejected']:
                            # Remove the order from open_orders
                            if clientOrderId in self.open_orders:
                                del self.open_orders[clientOrderId]
                            # Remove from pending_cancel if it's confirmed by the exchange
                            if clientOrderId in self.pending_cancel:
                                self.pending_cancel.discard(clientOrderId)
                                self.logger.info(f"Order {clientOrderId} removed from pending_cancel.")

                        # Remove from pending_new if it's confirmed by the exchange
                        if clientOrderId in self.pending_new:
                            self.pending_new.discard(clientOrderId)
                            self.logger.info(f"Order {clientOrderId} removed from pending_new.")
                    elif message['msg_type'] == MessageType.CREATE_ORDER_REJECT.value:
                        order = message['data']
                        clientOrderId = order['params']['clientOrderId']
                        if clientOrderId in self.pending_new:
                            self.pending_new.discard(clientOrderId)
                            self.logger.info(f"Order {clientOrderId} removed from pending_new due to rejection.")

            except Exception as e:
                self.logger.error(f"Failed to handle message: {e}", exc_info=True)

    async def manage_orders(self):
        # Check if it's time to place a new order or if no order has been sent before
        if (self.last_order_time is None or (self.virtual_time - self.last_order_time) >= self.sleep_time) and self.order_book:
            # Filter orders with the same client_order_id_prefix and sort them by price descendingly
            filtered_sorted_orders = sorted(
                [order for order in self.open_orders.values() if order['clientOrderId'].startswith(self.client_order_id_prefix)],
                key=lambda o: float(o['price']),
                reverse=True
            )

            # Cancel all but the last order
            for order in filtered_sorted_orders[:-1]:
                await self.try_cancel_order(order['clientOrderId'])

            # Cancel the last order if it exists and is not at the top of the book
            if len(filtered_sorted_orders) > 0:
                last_order_price = filtered_sorted_orders[-1]['price']
                best_ask_price = self.order_book['asks'][0][0] if self.order_book['asks'] else None
                if best_ask_price is not None and last_order_price != best_ask_price:
                    await self.try_cancel_order(filtered_sorted_orders[-1]['clientOrderId'])
                    self.logger.info("Placing a new order after cancellation.")
                    # Place a new order
                    await self.try_place_order()
                    self.last_order_time = self.virtual_time
            else:
                # If there are no orders, place a new order
                self.logger.info("No open orders found, placing a new order.")
                await self.try_place_order()
                self.last_order_time = self.virtual_time

    async def try_cancel_order(self, client_order_id):
        if client_order_id in self.pending_cancel:
            self.logger.info(f"Order {client_order_id} is already pending cancellation, skipping.")
            return

        try:
            # Add to pending_cancel list
            self.pending_cancel.add(client_order_id)
            self.logger.info(f"Order {client_order_id} added to pending_cancel and cancellation request is being sent.")
            await self.cancel_order(self.exchange, self.symbol, self.open_orders[client_order_id]['id'], client_order_id)
            self.logger.info(f"Order {client_order_id} cancellation request sent.")
        except Exception as e:
            self.logger.error(f"Failed to send cancellation for order {client_order_id}: {e}", exc_info=True)

    async def try_place_order(self):
        # Do not place a new order if there are pending new orders
        if self.pending_new:
            self.logger.info("New order placement is deferred due to pending new orders.")
            return

        self.sequence_number += 1
        client_order_id = f"{self.client_order_id_prefix}{self.sequence_number}"
        # Determine price based on execution mode
        if self.exec_mode == 'TOB':
            # Use top of book price for sell side
            if self.order_side == 'sell' and self.order_book and self.order_book['asks']:
                price = float(self.order_book['asks'][0][0])
            elif self.order_side == 'buy' and self.order_book and self.order_book['bids']:
                price = float(self.order_book['bids'][0][0])
            else:
                price = None
        elif self.exec_mode == 'MID':
            # Calculate mid price
            best_bid = float(self.order_book['bids'][0][0]) if self.order_book and self.order_book['bids'] else None
            best_ask = float(self.order_book['asks'][0][0]) if self.order_book and self.order_book['asks'] else None
            if best_bid and best_ask:
                price = (best_bid + best_ask) / 2
                mid_price = (best_bid + best_ask) / 2
                if self.order_side == 'sell':
                    # Round up to the nearest tick size for sell orders
                    price = math.ceil(mid_price / self.tick_size) * self.tick_size
                elif self.order_side == 'buy':
                    # Round down to the nearest tick size for buy orders
                    price = math.floor(mid_price / self.tick_size) * self.tick_size
            else:
                price = None
        else:
            self.logger.error(f"Execution mode '{self.exec_mode}' is invalid.")
            return

        if price is None:
            self.logger.error("Order placement failed: price is unavailable.")
            return

        try:
            # Add to pending_new list
            self.pending_new.add(client_order_id)
            self.logger.info(f"Order {client_order_id} added to pending_new and a new {self.order_side} order placement request is being sent at price: {price}")
            await self.send_order(self.exchange, self.symbol, self.order_side, price, self.order_quantity, client_order_id)
            self.logger.info(f"New {self.order_side} order {client_order_id} placement request sent at price: {price}")
        except Exception as e:
            self.logger.error(f"Failed to place new {self.order_side} order {client_order_id} at price {price}: {e}", exc_info=True)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the OptiTrade app with the specified configuration")
    parser.add_argument('--config', type=str, help='Path to the configuration file', required=True)
    args = parser.parse_args()

    asyncio.run(OptiTrade(args.config).run())
