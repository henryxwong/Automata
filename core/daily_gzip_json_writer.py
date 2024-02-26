import gzip
import json
from datetime import datetime


class DailyGzipJsonWriter:
    def __init__(self, base_path, base_filename):
        self.base_path = base_path.rstrip('/')  # Ensure no trailing slash
        self.base_filename = base_filename
        self.current_file_date = None
        self.file = None

    def _get_filename(self, dt):
        date_str = dt.strftime("%Y-%m-%d")
        return f"{self.base_path}/{self.base_filename}_{date_str}.json.gz"

    def _open_new_file(self, dt):
        if self.file is not None:
            self.file.close()
        filename = self._get_filename(dt)
        self.file = gzip.open(filename, 'at', encoding='utf-8')
        self.current_file_date = dt.date()

    def write(self, data_dict):
        msg_time_ns = data_dict.get("msg_time")
        if msg_time_ns is None:
            raise ValueError("msg_time field is missing in data_dict")

        msg_time = datetime.fromtimestamp(msg_time_ns / 1e9)
        if self.current_file_date != msg_time.date():
            self._open_new_file(msg_time)

        json_str = json.dumps(data_dict)
        self.file.write(json_str + '\n')
        self.file.flush()  # Flush data to file after each write

    def close(self):
        if self.file is not None:
            self.file.close()
