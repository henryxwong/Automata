module.exports = {
  apps : [
    {
      name: 'Sequencer',
      script: 'core/sequencer_app.py',
      args: '--config conf/sequencer_config.ini',
      interpreter: 'python',
      watch: false
    },
    {
      name: 'Crypto.com ExecutionGateway',
      script: 'core/execution_gateway.py',
      args: '--config conf/cdc_execution_config.ini',
      interpreter: 'python',
      watch: false
    },
    {
      name: 'Crypto.com MarketDataGateway',
      script: 'core/market_data_gateway.py',
      args: '--config conf/cdc_market_data_config.ini',
      interpreter: 'python',
      watch: false
    },
    {
      name: 'MessageLogger',
      script: 'core/message_logger.py',
      args: '--config conf/message_logger_config.ini',
      interpreter: 'python',
      watch: false
    },
    {
      name: 'OptiTrade',
      script: 'strategy/opti_trade.py',
      args: '--config conf/opti_trade_config.ini',
      interpreter: 'python',
      watch: false
    }
  ]
};
