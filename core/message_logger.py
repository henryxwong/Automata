import argparse
import asyncio
from daily_gzip_json_writer import DailyGzipJsonWriter
from proxy_app import ProxyApp


class MessageLogger(ProxyApp):

    def __init__(self, config_file):
        super().__init__(config_file)
        self.writer = None

    def _should_publish(self):
        return False

    async def post_start(self):
        await super().post_start()
        log_path = self.config['Logging']['path']
        log_filename = self.config['Logging']['filename']
        self.writer = DailyGzipJsonWriter(log_path, log_filename)
        task1 = asyncio.create_task(self.receive_and_log())
        self.tasks.update({task1})

    async def pre_stop(self):
        self.writer.close()
        await super().pre_stop()

    async def receive_and_log(self):
        try:
            while not self.shutdown_event.is_set():
                message = await self.receive()
                self.logger.debug(f"Received message: {message}")
                self.writer.write(message)
        except Exception as e:
            self.logger.error(f"Error writing message: {e}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the MessageLogger app with the specified configuration")
    parser.add_argument('--config', type=str, help='Path to the configuration file', required=True)
    args = parser.parse_args()

    asyncio.run(MessageLogger(args.config).run())
