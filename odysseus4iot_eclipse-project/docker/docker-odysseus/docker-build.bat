rd .\de.uniol.inf.is.odysseus.activityclassify.feature.update /s /q
pause
mkdir .\de.uniol.inf.is.odysseus.activityclassify.feature.update
pause
xcopy C:\Odysseus\_\eclipse-rcp-2021-09-R-win32-x86_64\workspace\de.uniol.inf.is.odysseus.activityclassify.feature.update .\de.uniol.inf.is.odysseus.activityclassify.feature.update /E
pause
docker build -t percom2022-odysseus .
pause