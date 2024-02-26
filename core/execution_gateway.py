import argparse
import os
import asyncio
import ccxt.pro as ccxtpro
from base_app import BaseApp, MessageType
from ccxt.base.types import Order, Trade
from typing import List
from dotenv import load_dotenv

class ExecutionGateway(BaseApp):
    def __init__(self, config_file):
        super().__init__(config_file)
        self.exchange_id = self.config['Exchange']['id']
        self.exchange_params = {
            k[6:]: v for k, v in self.config['Exchange'].items() if k.startswith('param_')
        }
        load_dotenv()
        self.exchange_params['apiKey'] = os.environ.get(self.config['Exchange']['api_key_env'])
        self.exchange_params['secret'] = os.environ.get(self.config['Exchange']['secret_env'])

    def post_start(self):
        exchange_class = getattr(ccxtpro, self.exchange_id)
        self.exchange: ccxtpro.Exchange = exchange_class(self.exchange_params)
        task1 = asyncio.create_task(self.handle_message())
        task2 = asyncio.create_task(self.send_order_updates())
        task3 = asyncio.create_task(self.send_trade_executions())
        self.tasks.update({task1, task2, task3})

    async def pre_stop(self):
        await self.exchange.close()

    async def handle_message(self):
        while not self.shutdown_event.is_set():
            message = await self.receive()
            if message.get('exchange') == self.exchange_id:
                if message['msg_type'] == MessageType.CREATE_ORDER.value:
                    try:
                        self.logger.info(f"{self.app_name} - received create order for {message['symbol']}")
                        await self.exchange.create_order(**message['data'])
                    except Exception as e:
                        self.logger.error(f"{self.app_name} - create order reject for {message['symbol']}")
                        reject_message = {
                            'msg_type': MessageType.CREATE_ORDER_REJECT.value,
                            'exchange': self.exchange_id,
                            'symbol': message['symbol'],
                            'data': message['data']
                        }
                        await self.send(reject_message)
                elif message['msg_type'] == MessageType.CANCEL_ORDER.value:
                    self.logger.info(f"{self.app_name} - received cancel order instruction for {message['data']['id']}")
                    await self.exchange.cancel_order(**message['data'])
                elif message['msg_type'] == MessageType.CANCEL_ALL_ORDER.value:
                    self.logger.info(f"{self.app_name} - received cancel all orders instruction")
                    await self.exchange.cancel_all_orders(**message['data'])

    async def send_order_updates(self):
        while not self.shutdown_event.is_set():
            orders: List[Order] = await self.exchange.watch_orders()
            for order in orders:
                self.logger.info(f"{self.app_name} - sending order update for {order['id']}")
                message = {
                    'msg_type': MessageType.ORDER_UPDATE.value,
                    'exchange': self.exchange_id,
                    'symbol': order['symbol'],
                    'data': order
                }
                await self.send(message)

    async def send_trade_executions(self):
        while not self.shutdown_event.is_set():
            trades: List[Trade] = await self.exchange.watch_my_trades()
            for trade in trades:
                self.logger.info(f"{self.app_name} - sending trade execution for {trade['id']}")
                message = {
                    'msg_type': MessageType.TRADE_EXECUTION.value,
                    'exchange': self.exchange_id,
                    'symbol': trade['symbol'],
                    'data': trade
                }
                await self.send(message)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the ExecutionGateway app with the specified configuration")
    parser.add_argument('--config', type=str, help='Path to the configuration file', required=True)
    args = parser.parse_args()

    asyncio.run(ExecutionGateway(args.config).run())
