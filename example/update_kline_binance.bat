ECHO OFF
CLS
ECHO.
ECHO ****************************************************************************************
ECHO                                         DBMaster
ECHO ****************************************************************************************
ECHO.

@REM symbol is optional, defaults to Binance.universe
set symbol="['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'SHIBUSDT', 'AVAXUSDT', 'DOTUSDT']"
set datefrom=2024-01-01
@REM set dateto=2023-09-02  
set freq=1w 3d 1d 12h 8h 6h 4h 2h 1h 30m 15m 5m 3m 1m

for %%f in (%freq%) do (
    echo -------------------------------------------------------------------------------------------------
    echo Running job for freq=%%f, symbol=%symbol%
    echo -------------------------------------------------------------------------------------------------
    @REM turn off concurrent fetching for large amount of data
    if %%f=1m (
        set DBMASTER_MAX_WORKERS=1  
    )
    python -m dbmaster update kline --vendor=binance --freq=%%f  --datefrom=%datefrom% --symbol=%symbol% --if_row_exists=insert
)

