import gzip
import json
import os
from datetime import datetime, timedelta


class DailyGzipJsonReader:
    def __init__(self, base_path, base_filename):
        self.base_path = base_path.rstrip('/')  # Ensure no trailing slash
        self.base_filename = base_filename

    def _generate_file_dates(self, start_date, end_date):
        delta = end_date - start_date
        return [start_date + timedelta(days=i) for i in range(delta.days + 1)]

    def _get_filename(self, date):
        date_str = date.strftime("%Y-%m-%d")
        json_filename = f"{self.base_path}/{self.base_filename}_{date_str}.json"
        gzip_filename = f"{json_filename}.gz"
        if os.path.exists(gzip_filename):
            return gzip_filename
        elif os.path.exists(json_filename):
            return json_filename
        else:
            return None

    def read(self, start_ns, end_ns):
        start_dt = datetime.fromtimestamp(start_ns / 1e9)
        end_dt = datetime.fromtimestamp(end_ns / 1e9)

        for date in self._generate_file_dates(start_dt, end_dt):
            filename = self._get_filename(date)
            if filename:
                open_func = gzip.open if filename.endswith('.gz') else open
                with open_func(filename, 'rt', encoding='utf-8') as file:
                    for line in file:
                        data_dict = json.loads(line)
                        msg_time_ns = data_dict.get("msg_time")
                        if start_ns <= msg_time_ns <= end_ns:
                            yield data_dict
