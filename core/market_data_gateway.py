import argparse
import asyncio
import ccxt.pro as ccxtpro
from base_app import BaseApp, MessageType


class MarketDataGateway(BaseApp):
    def __init__(self, config_file):
        super().__init__(config_file)
        self.exchange = None
        self.exchange_id = self.config['Exchange']['id']
        self.symbols = [symbol.strip() for symbol in self.config['Exchange']['symbols'].split(',')]
        self.limit = int(self.config['Exchange'].get('limit', 10))
        self.exchange_params = {
            k[6:]: v for k, v in self.config['Exchange'].items() if k.startswith('param_')
        }

    async def post_start(self):
        exchange_class = getattr(ccxtpro, self.exchange_id)
        self.exchange: ccxtpro.Exchange = exchange_class(self.exchange_params)
        task1 = asyncio.create_task(self.send_order_book())
        self.tasks.update({task1})

    async def pre_stop(self):
        await self.exchange.close()

    async def send_order_book(self):
        while not self.shutdown_event.is_set():
            for symbol in self.symbols:
                order_book = await self.exchange.watch_order_book(symbol, self.limit)
                self.logger.debug(f"{self.app_name} - Sending order book for {symbol}: {order_book}")
                message = {
                    'msg_type': MessageType.ORDER_BOOK.value,
                    'exchange': self.exchange_id,
                    'symbol': symbol,
                    'data': order_book
                }
                await self.send(message)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the MarketDataGateway app with specified configuration")
    parser.add_argument('--config', type=str, help='Path to the configuration file', required=True)
    args = parser.parse_args()

    asyncio.run(MarketDataGateway(args.config).run())
