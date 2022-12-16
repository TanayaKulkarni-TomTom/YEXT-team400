import time
import os
from apscheduler.schedulers.background import BackgroundScheduler
from run_process import main as run_process
from definitions import SCHEDULER_DAY, SCHEDULER_HOUR, SCHEDULER_MINUTE

if __name__ == "__main__":
    scheduler = BackgroundScheduler()
    scheduler.add_job(
        func=run_process,
        trigger="cron",
        day=SCHEDULER_DAY,
        hour=SCHEDULER_HOUR,
        minute=SCHEDULER_MINUTE,
        timezone="utc",
    )
    scheduler.start()
    print("Press Ctrl+{0} to exit".format("Break" if os.name == "nt" else "C"))

    try:
        # This is here to simulate application activity
        # (which keeps the main thread alive).
        while True:
            time.sleep(2)
    except (KeyboardInterrupt, SystemExit):
        # Not strictly necessary if daemonic mode is enabled
        # but should be done if possible
        scheduler.shutdown()
