from backend.config.dask_config import create_dask_client
from backend.pipeline.processing import process_pipeline
from backend.anamoly.detection import detect_anamoly
from backend.config.email_config import send_anomaly_email
import time
import os
import pandas as pd
import random
from datetime import datetime, timedelta

ADMIN_EMAIL = "admin@example.com" 

def create_sample_logs_if_needed(file_path):
    """Create sample log file if it doesn't exist"""
    if os.path.exists(file_path):
        print(f"✅ Log file found at: {file_path}")
        return True
    
    print(f"⚠️ Log file not found at: {file_path}")
    print("📝 Creating sample log file...")
    
    try:
        # Create directory if it doesn't exist
        os.makedirs(os.path.dirname(file_path), exist_ok=True)
        
        # Generate sample log data
        log_levels = ['INFO', 'ERROR', 'WARN', 'DEBUG']
        log_weights = [0.7, 0.1, 0.15, 0.05]
        
        info_msgs = [
            "Request processed successfully", "User logged in", "Data retrieved", 
            "Operation completed", "Cache hit", "Connection established", 
            "Transaction completed", "Email sent", "Notification delivered"
        ]
        error_msgs = [
            "Database connection failed", "Timeout occurred", "Null pointer exception", 
            "Out of memory", "Service unavailable", "Failed to process request", 
            "Unauthorized access attempt", "Data validation error", "Disk full"
        ]
        warn_msgs = [
            "High memory usage", "Slow response time", "Retry attempt", 
            "Resource limit near", "Deprecated API used", "Configuration missing"
        ]
        debug_msgs = [
            "Debug info", "Variable value", "Function called", "Parameter received",
            "Processing step", "Cache miss", "Query executed"
        ]
        
        services = ["auth", "payment", "inventory", "shipping", "user", "orders", "notifications"]
        
        # Generate 1000 sample logs
        data = []
        start_time = datetime.now() - timedelta(hours=1)
        
        for i in range(1000):
            timestamp = start_time + timedelta(seconds=random.randint(0, 3600))
            level = random.choices(log_levels, weights=log_weights)[0]
            
            if level == "ERROR":
                message = random.choice(error_msgs)
            elif level == "WARN":
                message = random.choice(warn_msgs)
            elif level == "DEBUG":
                message = random.choice(debug_msgs)
            else:
                message = random.choice(info_msgs)
            
            service = random.choice(services)
            full_message = f"[{service}] {message}"
            
            data.append({
                'timestamp': timestamp,
                'level': level,
                'message': full_message,
                'service': service
            })
        
        # Create DataFrame and save
        df = pd.DataFrame(data)
        df = df.sort_values('timestamp')
        df.to_csv(file_path, index=False)
        
        print(f"✅ Sample log file created at: {file_path}")
        print(f"Total logs generated: {len(df)}")
        return True
        
    except Exception as e:
        print(f"❌ Error creating sample log file: {e}")
        return False

def main():
    # Define log file path
    log_file_path = "backend/sample_data/log_data.log"
    
    # Create sample logs if needed
    if not create_sample_logs_if_needed(log_file_path):
        print("❌ Cannot proceed without log file.")
        return
    
    # Create Dask client
    print("\nCreating Dask cluster...")
    client = create_dask_client()
    print(f"Dask cluster created successfully!")
    print(client)
    print(f"Dashboard link: {client.dashboard_link}")
    print("\n" + "=" * 50)

    start = time.time()

    try:
        # Build log processing pipeline
        print("Processing logs...")
        log_df = process_pipeline(log_file_path)

        # Count total logs
        total_logs = log_df.count().compute()
        end = time.time()

        print("Total logs parsed:", total_logs)
        print("Time taken:", round(end - start, 2), "seconds")

        print("\nRunning anomaly detection...")

        # Detect anomalies
        anomalies_df = detect_anamoly(log_df)

        anomalies = anomalies_df

        if anomalies.empty:
            print("✅ No anomalies detected")
        else:
            print(f"🚨 {len(anomalies)} anomalies detected!")

            for _, row in anomalies.iterrows():
                anomaly_data = {
                    "timestamp": row["timestamp"],
                    "error_count": row["error_count"],
                    "z_score": row["z_score"]
                }

                try:
                    send_anomaly_email(
                        to_email=ADMIN_EMAIL,
                        anomaly=anomaly_data
                    )
                    print(f"📧 Alert sent | Time: {row['timestamp']} | Errors: {row['error_count']}")
                except Exception as e:
                    print(f"⚠️ Failed to send email: {e}")

    except FileNotFoundError as e:
        print(f"❌ File error: {e}")
        print("Please ensure the log file exists at the correct path.")
    except Exception as e:
        print(f"❌ Error during processing: {e}")
        import traceback
        traceback.print_exc()
    
    input("\nPress Enter to exit...")
    client.close()

if __name__ == "__main__":
    main()