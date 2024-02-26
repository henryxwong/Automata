import asyncio
import configparser
import datetime
import logging
import msgpack
import signal
import zmq.asyncio
from abc import ABC, abstractmethod
from enum import Enum
from typing import final

class IsoFormatter(logging.Formatter):
    def formatTime(self, record, datefmt=None):
        ct = datetime.datetime.now(datetime.timezone.utc)
        return ct.isoformat()

class MessageType(Enum):
    ORDER_BOOK = 'order_book'
    CREATE_ORDER = 'create_order'
    CREATE_ORDER_REJECT = 'create_order_reject'
    CANCEL_ORDER = 'cancel_order'
    CANCEL_ORDER_REJECT = 'cancel_order_reject'
    CANCEL_ALL_ORDER = 'cancel_all_order'
    ORDER_UPDATE = 'order_update'
    TRADE_EXECUTION = 'trade_execution'

class BaseApp(ABC):
    def __init__(self, config_file):
        self.config = self._read_config(config_file)
        self.app_name = self.config['General']['app_name']
        self.logger = self._setup_logging()
        self.virtual_time = None
        self.zmq_context = zmq.asyncio.Context()
        self.should_publish = self._should_publish()
        self.publisher_socket = None
        self.should_subscribe = self._should_subscribe()
        self.subscriber_socket = None
        self.shutdown_event = asyncio.Event()
        self.tasks = set()
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _should_publish(self):
        return True

    def _should_subscribe(self):
        return True

    @final
    def _read_config(self, config_file):
        config = configparser.ConfigParser()
        config.read(config_file)
        return config

    @final
    def _setup_logging(self):
        logging_level = self.config.get('Logging', 'level', fallback='INFO')
        logging_format = '%(asctime)s - %(levelname)s - %(message)s'

        # Create a custom formatter
        formatter = IsoFormatter(fmt=logging_format)

        # Set up basic configuration with custom formatter
        logging.basicConfig(level=logging_level, format=logging_format)

        # Get the root logger
        root_logger = logging.getLogger()

        # Set the custom formatter for all handlers of the root logger
        for handler in root_logger.handlers:
            handler.setFormatter(formatter)

        return logging.getLogger(self.__class__.__name__)

    @final
    def _signal_handler(self, signum, frame):
        self.logger.info(f"Received shutdown signal: {signum}")
        asyncio.get_event_loop().call_soon_threadsafe(self.shutdown_event.set)

    @final
    async def wait_for_shutdown(self):
        await self.shutdown_event.wait()

    @final
    def start(self):
        if self.should_publish:
            self.publisher_socket = self.zmq_context.socket(zmq.PUSH)
            self.publisher_socket.connect(self.config['ZeroMQ']['push_endpoint'])
        if self.should_subscribe:
            self.subscriber_socket = self.zmq_context.socket(zmq.SUB)
            self.subscriber_socket.connect(self.config['ZeroMQ']['sub_endpoint'])
            self.subscriber_socket.subscribe('')
        self.tasks.add(asyncio.create_task(self.wait_for_shutdown()))
        self.post_start()
        self.logger.info(f"{self.app_name} - Started successfully.")

    @abstractmethod
    def post_start(self):
        pass

    @final
    async def run(self):
        self.start()
        try:
            while not self.shutdown_event.is_set():
                done, pending = await asyncio.wait(self.tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if task.exception():
                        self.logger.error(f"{self.app_name} - Task encountered an exception: {task.exception()}")
        finally:
            await self.stop()

    @final
    async def send(self, message):
        if self.publisher_socket:
            await self.publisher_socket.send(msgpack.packb(message))

    @final
    async def receive(self):
        if self.subscriber_socket:
            message = await self.subscriber_socket.recv()
            unpacked_msg = msgpack.unpackb(message, raw=False)
            self.virtual_time = unpacked_msg.get('msg_time', self.virtual_time)
            return unpacked_msg
        return None

    @final
    async def stop(self):
        await self.pre_stop()

        for task in self.tasks:
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                self.logger.info(f"{self.app_name} - Task {task.get_name()} was cancelled.")
        self.tasks.clear()

        if self.publisher_socket:
            self.publisher_socket.close()
        if self.subscriber_socket:
            self.subscriber_socket.close()
        self.zmq_context.term()
        self.logger.info(f"{self.app_name} - Stopped successfully.")

    @abstractmethod
    async def pre_stop(self):
        pass
