import psutil
import time
import datetime
import csv
import os

# --- Configuration ---
LOG_INTERVAL_SECONDS = 5  # How often to log data (in seconds)
LOG_FILE_NAME = 'system_performance_log.csv'
LOG_DURATION_MINUTES = 60 # How long the script should run (in minutes)

def get_system_stats():
    """Retrieves current CPU and memory usage."""
    cpu_usage = psutil.cpu_percent(interval=None)  # interval=None gets instantaneous usage
    memory_info = psutil.virtual_memory()
    memory_usage_percent = memory_info.percent
    memory_used_gb = memory_info.used / (1024**3) # Convert bytes to GB
    memory_total_gb = memory_info.total / (1024**3) # Convert bytes to GB
    return cpu_usage, memory_usage_percent, memory_used_gb, memory_total_gb

def main():
    """Main function to monitor and log system performance."""
    log_file_exists = os.path.isfile(LOG_FILE_NAME)
    start_time = time.time()
    end_time = start_time + (LOG_DURATION_MINUTES * 60)

    print(f"Starting system performance monitoring for {LOG_DURATION_MINUTES} minutes.")
    print(f"Logging data every {LOG_INTERVAL_SECONDS} seconds to '{LOG_FILE_NAME}'")

    try:
        with open(LOG_FILE_NAME, 'a', newline='') as csvfile:
            log_writer = csv.writer(csvfile)

            # Write header row if the file is new
            if not log_file_exists or os.path.getsize(LOG_FILE_NAME) == 0:
                header = [
                    'Timestamp',
                    'CPU Usage (%)',
                    'Memory Usage (%)',
                    'Memory Used (GB)',
                    'Memory Total (GB)'
                ]
                log_writer.writerow(header)

            while time.time() < end_time:
                # Get current timestamp
                current_time = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

                # Get system stats
                cpu, mem_percent, mem_used, mem_total = get_system_stats()

                # Log the data
                row_data = [current_time, cpu, mem_percent, f"{mem_used:.2f}", f"{mem_total:.2f}"]
                print(row_data)
                log_writer.writerow(row_data)
                # Flush buffer to ensure data is written immediately
                csvfile.flush()

                # Wait for the next interval
                time.sleep(LOG_INTERVAL_SECONDS)

        print(f"Monitoring complete. Log saved to '{LOG_FILE_NAME}'")

    except FileNotFoundError:
        print(f"Error: Could not open or create the log file at '{LOG_FILE_NAME}'. Please check permissions.")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
    except KeyboardInterrupt:
        print("\nMonitoring stopped by user.")

if __name__ == "__main__":
    # --- Explanation of psutil.cpu_percent() behavior ---
    # When interval is > 0.0, psutil.cpu_percent() compares system CPU times
    # elapsed since the last call or module import, returning a float representing
    # the percentage of CPU time spent by the process in the last 'interval' seconds.
    # When interval is 0.0 or None, it returns a float representing the CPU utilization
    # percentage since the last call to cpu_percent() (this is a non-blocking call).
    # The first call with interval=None will return 0.0, but it initializes the baseline.
    # Subsequent calls will provide actual usage.
    # To get a more stable initial reading, we can call it once before the loop.
    psutil.cpu_percent(interval=None) # Initialize cpu_percent
    time.sleep(0.1) # Short delay to allow for a baseline to be established

    main()
