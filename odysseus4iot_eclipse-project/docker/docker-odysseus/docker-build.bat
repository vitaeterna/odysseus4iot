rd .\prediction.update /s /q
pause
mkdir .\prediction.update
pause
xcopy C:\Odysseus\_\eclipse-rcp-2021-09-R-win32-x86_64\workspace\prediction.update .\prediction.update /E
pause
docker build -t docker-odysseus .
pause