# pip install schedule==1.2.1
import time
import os
import datetime as dt

from schedule import every, repeat, run_pending


@repeat(every(1).hours.at("00:00"))
def update_kline():
    now = dt.datetime.now()
    print("update_kline called at " + now.strftime("%Y-%m-%d %H:%M:%S"))

    datefrom = dt.datetime.now() - dt.timedelta(days=1)
    datefrom = datefrom.strftime("%Y-%m-%d")
    symbol = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT", "ADAUSDT", "SHIBUSDT", "AVAXUSDT", "DOTUSDT"]  # fmt: skip
    freq = ["1d", "12h", "8h", "6h", "4h", "2h", "1h", "30m", "15m", "5m", "3m", "1m"]

    for frq in freq:
        print(f"Running job for freq={frq}, symbol={symbol}")
        cmd = f'python -m dbmaster update kline --vendor=binance --freq={frq} --datefrom="{datefrom}" --symbol="{symbol}" --if_row_exists=insert'
        if frq == "1m":
            cmd = "$ENV:DBMASTER_MAX_WORKERS=1; " + cmd
        os.system(cmd)


@repeat(every(1).hours.at("00:00"))
def compute_pmom():
    now = dt.datetime.now()
    print("compute_pmom called at " + now.strftime("%Y-%m-%d %H:%M:%S"))

    datefrom = dt.datetime.now() - dt.timedelta(days=2)
    datefrom = datefrom.strftime("%Y-%m-%d %H:%M:%S")
    step = "5m"
    symbol = ['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'SHIBUSDT', 'AVAXUSDT', 'DOTUSDT']  # fmt: skip
    period = ['-30d', '-14d', '-7d', '-3d', '-1d', '-12h', '-8h', '-4h', '-1h', '-30m', '-15m', '-5m', '+1d']  # fmt: skip
    print(f"Running job for {period=}, symbol={symbol}")

    cmd = f'python -m dbmaster compute pmom --vendor=binance --period="{period}" --{step=} --datefrom="{datefrom}" --symbol="{symbol}" --if_row_exists=insert'
    os.system(cmd)


# update_kline()
# compute_pmom()


while True:
    run_pending()
    time.sleep(1)
