ECHO OFF
CLS
ECHO.
ECHO ****************************************************************************************
ECHO                                         DBMaster
ECHO ****************************************************************************************
ECHO.

@REM symbol is optional, defaults to Binance.universe
set symbol="['BTCUSDT', 'ETHUSDT', 'BNBUSDT', 'SOLUSDT', 'XRPUSDT', 'DOGEUSDT', 'ADAUSDT', 'SHIBUSDT', 'AVAXUSDT', 'DOTUSDT']"
set datefrom=2024-04-01
@REM set dateto=2024-04-15  
set period="['-30d', '-14d', '-7d', '-3d', '-1d', '-12h', '-8h', '-4h', '-1h', '-30m', '-15m', '-5m', '+1d']"
set step=5m

echo -------------------------------------------------------------------------------------------------
echo Running job for period=%period%, symbol=%symbol%
echo -------------------------------------------------------------------------------------------------
python -m dbmaster compute pmom --vendor=binance --period=%period% --step=%step% --datefrom=%datefrom% --symbol=%symbol% --if_row_exists=insert
