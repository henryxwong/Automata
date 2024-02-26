import argparse
import asyncio
from base_app import BaseApp

class MessageLogger(BaseApp):

    def post_start(self):
        task1 = asyncio.create_task(self.receive_and_log())
        self.tasks.update({task1})

    async def pre_stop(self):
        pass

    async def receive_and_log(self):
        while not self.shutdown_event.is_set():
            message = await self.receive()
            self.logger.info(f"Received message: {message}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run the MessageLogger app with the specified configuration")
    parser.add_argument('--config', type=str, help='Path to the configuration file', required=True)
    args = parser.parse_args()

    asyncio.run(MessageLogger(args.config).run())
