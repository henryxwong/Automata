module.exports = {
  apps : [
    {
      name: 'Sequencer',
      script: 'core/sequencer.py',
      args: '--config conf/sequencer.ini',
      interpreter: 'python',
      watch: false
    },
    {
      name: 'CdcGateway',
      script: 'core/cdc_gateway.py',
      args: '--config conf/cdc_gateway.ini',
      interpreter: 'python',
      watch: false
    },
    {
      name: 'MessageLogger',
      script: 'core/message_logger.py',
      args: '--config conf/message_logger.ini',
      interpreter: 'python',
      watch: false
    },
    {
      name: 'OptiTrade',
      script: 'strategy/opti_trade.py',
      args: '--config conf/opti_trade.ini',
      interpreter: 'python',
      watch: false
    }
  ]
};
