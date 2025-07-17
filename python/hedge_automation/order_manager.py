
from hedge_automation.data_handler import BrokerHandler
from hedge_automation.hedge_orders_sender import BitgetOrderSender

class OrderManager:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(OrderManager, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance

    def _initialize(self):
        params = {
            'exchange_trade': 'bitget',
            'account_trade': 'H1',
            'send_orders': 'bitget'
        }
        end_point = BrokerHandler.build_end_point('bitget', account='H1')
        self.bh = BrokerHandler(
            market_watch='bitget',
            strategy_param=params,
            end_point_trade=end_point,
            logger_name='bitget_order_sender'
        )
        self.order_sender = BitgetOrderSender(self.bh)

    async def close(self):
        await self.order_sender.close()

    def get_order_sender(self):
        return self.order_sender