import schedule
import time

from script import run_stock_fetch

from datetime import datetime

schedule.every().minute.at(":00").do(run_stock_fetch)

while True:
    schedule.run_pending()
    time.sleep(1)