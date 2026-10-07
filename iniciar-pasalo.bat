@echo off
title Pasalo - servidor local
cd /d "%~dp0"

rem Buscar node aunque el PATH dentro del .bat venga incompleto.
set NODE=
node --version >nul 2>nul
if %errorlevel%==0 set NODE=node
if not defined NODE if exist "C:\Program Files\nodejs\node.exe" set NODE=C:\Program Files\nodejs\node.exe
if not defined NODE if exist "C:\Program Files (x86)\nodejs\node.exe" set NODE=C:\Program Files (x86)\nodejs\node.exe
if not defined NODE goto :nonode
goto :found

:nonode
echo.
echo  No se encontro Node.js en esta PC.
echo  Instalalo desde https://nodejs.org (boton LTS, instalador de Windows),
echo  luego cierra y vuelve a abrir este archivo.
echo.
pause
exit /b 1

:found
for %%I in ("%NODE%") do set "NODEDIR=%%~dpI"
set "PATH=%NODEDIR%;%PATH%"
echo  Node listo, arrancando el servidor...

if not exist node_modules (
  echo  Instalando dependencias por primera vez...
  call npm ci --omit=dev
  if errorlevel 1 (
    echo.
    echo  Fallo la instalacion de dependencias.
    echo  Revisa tu conexion a internet y vuelve a abrir este archivo.
    echo.
    pause
    exit /b 1
  )
  echo.
)

echo.
echo  La ventana del servidor te mostrara la direccion para el celular
echo  y el CODIGO de vinculacion. Dejala abierta mientras transfieres.
echo  Para detener el servidor, cierra esta ventana.
echo.
echo  Abriendo la pagina en tu navegador...
timeout /t 2 /nobreak >nul
start "" "http://localhost:3000"
"%NODE%" server.js
pause
