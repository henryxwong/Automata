from core.base_app import BaseApp, MessageType

class Strategy(BaseApp):
    async def send_order(self, exchange, symbol, order_side, price, quantity, client_order_id, order_type='limit', post_only=True):
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
        await self.send(message)

    async def cancel_order(self, exchange, symbol, order_id, client_order_id):
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
        await self.send(message)

    async def cancel_all_orders(self, exchange, symbol):
        message = {
            'msg_type': MessageType.CANCEL_ALL_ORDER.value,
            'exchange': exchange,
            'symbol': symbol,
            'data': {
                'symbol': symbol
            }
        }
        await self.send(message)
