ECHO OFF
CLS
ECHO.
ECHO ****************************************************************************************
ECHO                                         DBMaster
ECHO ****************************************************************************************
ECHO.

ECHO -------------------------------------- Git Status --------------------------------------
ECHO.

git status

if "%1"=="" GOTO DONE
if "%1"=="pkg" GOTO PKG

:PKG
ECHO.
ECHO -------------------------------------- Packaging --------------------------------------
ECHO.
rd /s/q build dbmaster.egg-info
del /F /Q "dist\*.whl"
python -m build -w
rd /s/q build dbmaster.egg-info
GOTO DONE

:DONE
ECHO DONE

