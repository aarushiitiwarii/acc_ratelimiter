@echo off
rem Open the RateGuard demo console at http://localhost:9000
rem A separate window keeps a port-forward to the in-cluster console running and
rem reconnects automatically if the console pod restarts. Close it to stop.
start "rateguard console port-forward" cmd /c "for /l %%i in () do (kubectl port-forward svc/console 9000:80 & timeout /t 2 /nobreak >nul)"
timeout /t 4 /nobreak >nul
start "" http://localhost:9000
