from dbmaster.vendor import Binance


def test_get_kline_datefrom():
    df = Binance.get_kline("BTCUSDT", "1h", datefrom="2024-04-26")  # till now
    print(df)


def test_get_kline_dateto():
    df = Binance.get_kline("BTCUSDT", "1d", dateto="2024-01-01")  # will use limit, max to 1000
    print(df)


def test_get_kline_datefrom_dateto():
    df = Binance.get_kline("BTCUSDT", "1d", datefrom="2024-01-01", dateto="2024-01-10")
    print(df)
