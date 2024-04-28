from dbmaster.derived import PmomBinance, Pmom


def test_get():
    df = PmomBinance.get("BTC/USDT", period=["-1d", "-1h", "+1d"], datefrom="2024-04-01")
    print(df.head(5))
    #             Timestamp    Symbol Period      Pmom
    # 0 2024-04-01 00:00:00  BTC/USDT    +1d -0.022870
    # 1 2024-04-01 00:00:00  BTC/USDT    -1d  0.024400
    # 2 2024-04-01 00:00:00  BTC/USDT    -1h  0.004368
    # 3 2024-04-01 00:05:00  BTC/USDT    +1d -0.022804
    # 4 2024-04-01 00:05:00  BTC/USDT    -1d  0.022985


def test_get_all_period():
    df = PmomBinance.get("BTC/USDT", datefrom="2024-04-01", dateto="2024-04-02")
    print(df.head(5))
    #    Timestamp    Symbol Period      Pmom
    # 0 2024-04-01  BTC/USDT    +1d -0.022870
    # 1 2024-04-01  BTC/USDT   -12h  0.012932
    # 2 2024-04-01  BTC/USDT   -14d  0.042205
    # 3 2024-04-01  BTC/USDT   -15m -0.000018
    # 4 2024-04-01  BTC/USDT    -1d  0.024400


def test_pmom_get():
    df = Pmom.get("binance", symbol="BTC/USDT", period="1d", datefrom="2024-04-01", dateto="2024-04-10")
    print(df.head(5))
    #             Timestamp    Symbol Period      Pmom
    # 0 2024-04-01 00:00:00  BTC/USDT    +1d -0.022870
    # 1 2024-04-01 00:05:00  BTC/USDT    +1d -0.022804
    # 2 2024-04-01 00:10:00  BTC/USDT    +1d -0.023907
    # 3 2024-04-01 00:15:00  BTC/USDT    +1d -0.021970
    # 4 2024-04-01 00:20:00  BTC/USDT    +1d -0.025347
