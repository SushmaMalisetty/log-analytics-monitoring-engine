from datetime import datetime
import csv
import io

def parse_log_line(line):
    # Skip header row
    if line.startswith("timestamp"):
        return None
    try:
        row = next(csv.reader(io.StringIO(line.strip())))
        if len(row) != 4:
            return None
        timestamp_str, service, level, message = row
        return {
            "timestamp": datetime.fromisoformat(timestamp_str.split(".")[0]),
            "service": service,
            "level": level,
            "message": message,
        }
    except Exception:
        return None