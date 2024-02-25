module.exports = {
  apps : [
    {
      name: 'Sequencer',
      script: 'sequencer_app.py',
      args: '--config sequencer_config.ini',
      interpreter: 'python',
      watch: false
    },
    {
      name: 'Crypto.com ExecutionGateway',
      script: 'execution_gateway.py',
      args: '--config cdc_execution_config.ini',
      interpreter: 'python',
      watch: false
    },
    {
      name: 'Crypto.com MarketDataGateway',
      script: 'market_data_gateway.py',
      args: '--config cdc_market_data_config.ini',
      interpreter: 'python',
      watch: false
    },
    {
      name: 'MessageLogger',
      script: 'message_logger.py',
      args: '--config message_logger_config.ini',
      interpreter: 'python',
      watch: false
    },
    {
      name: 'OptiTrade',
      script: 'opti_trade.py',
      args: '--config opti_trade_config.ini',
      interpreter: 'python',
      watch: false
    }
  ]
};
