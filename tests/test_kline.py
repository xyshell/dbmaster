from dbmaster.catalog import KlineBinance


def test_get():
    df = KlineBinance.get("BTCUSDT", "1d", datefrom="2024-01-01")
    print(df.head(5))
    #     Symbol   OpenTime           CloseTime  ...     Close   BaseVolume   QuoteVolume
    # 0  BTCUSDT 2024-01-01 2024-01-01 23:59:59  ...  44179.55  27174.29903  1.169996e+09
    # 1  BTCUSDT 2024-01-02 2024-01-02 23:59:59  ...  44946.91  65146.40661  2.944332e+09
    # 2  BTCUSDT 2024-01-03 2024-01-03 23:59:59  ...  42845.23  81194.55173  3.507105e+09
    # 3  BTCUSDT 2024-01-04 2024-01-04 23:59:59  ...  44151.10  48038.06334  2.095095e+09
    # 4  BTCUSDT 2024-01-05 2024-01-05 23:59:59  ...  44145.11  48075.25327  2.100954e+09
    # [5 rows x 9 columns]


def test_get_subset_column():
    df = KlineBinance.get("BTCUSDT", "1d", datefrom="2024-01-01", column=["Symbol", "OpenTime", "Close"])
    print(df.head(5))
    #           Symbol   OpenTime     Close
    # 0    BTCUSDT 2024-01-01  44179.55
    # 1    BTCUSDT 2024-01-02  44946.91
    # 2    BTCUSDT 2024-01-03  42845.23
    # 3    BTCUSDT 2024-01-04  44151.10
    # 4    BTCUSDT 2024-01-05  44145.11
    # [5 rows x 3 columns]
