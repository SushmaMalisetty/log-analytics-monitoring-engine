import random
import csv
import time
import os
from datetime import datetime

# Update this path to match your project structure
# This path is relative to your project root
PROJECT_ROOT = r"C:\Users\Harshavardhan\log-analytics\Log-Analytics-Monitoring-Engine"
LOG_FILE = os.path.join(PROJECT_ROOT, "backend", "log_generator", "realtime_logs.csv")

# Ensure the directory exists
os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)

# Log levels and weights
levels = ["INFO", "ERROR", "WARN", "DEBUG"]
log_weights = [0.7, 0.1, 0.15, 0.05]

services = ["auth", "payment", "inventory", "shipping", "user", "orders", "notifications", "analytics", "search", "recommendation"]

info_msgs = ["Request processed successfully", "User logged in", "Data retrieved", "Operation completed",
             "Cache hit", "Cache list", "Connection established", "Transaction completed", "Email sent", "Notification delivered"]
error_msgs = ["Database connection failed", "Timeout occurred", "Null pointer exception", "Out of memory",
              "Service unavailable", "Failed to process request", "Unauthorized access attempt",
              "Data validation error", "Disk full", "API rate limit exceeded"]
warn_msgs = ["High memory usage", "Slow response time", "Retry attempt", "Resource limit near",
             "Deprecated API used", "Configuration missing", "Connection pool near limit"]
debug_msgs = ["Debug info", "Variable value", "Function called", "Parameter received",
              "Processing step 1", "Processing step 2", "Cache miss"]

# Write header once
with open(LOG_FILE, "w", newline="") as f:
    writer = csv.writer(f)
    writer.writerow(["timestamp", "level", "message"])

print(f"Generating log data to: {LOG_FILE}")
print("Press Ctrl+C to stop...")

try:
    while True:
        # Choose level based on weights
        level = random.choices(levels, weights=log_weights)[0]
        
        # Pick message based on level
        if level == "ERROR":
            message = random.choice(error_msgs)
        elif level == "WARN":
            message = random.choice(warn_msgs)
        elif level == "DEBUG":
            message = random.choice(debug_msgs)
        else:
            message = random.choice(info_msgs)
        
        # Add service info to message for context (optional)
        service = random.choice(services)
        full_message = f"[{service}] {message}"
        
        row = [
            datetime.now().isoformat(),
            level,
            full_message
        ]
        
        with open(LOG_FILE, "a", newline="") as f:
            writer = csv.writer(f)
            writer.writerow(row)
        
        time.sleep(0.5)  # Generate a log every 0.5 seconds
        
except KeyboardInterrupt:
    print("\nLog generation stopped.")