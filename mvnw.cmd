@echo off
:: Maven Wrapper Script for Windows
:: Usage: mvnw.cmd <goals>

setlocal

set SCRIPT_DIR=%~dp0
set WRAPPER_JAR=%SCRIPT_DIR%.mvn\wrapper\maven-wrapper.jar
set WRAPPER_PROPS=%SCRIPT_DIR%.mvn\wrapper\maven-wrapper.properties

:: Prefer system Maven if available
where mvn >nul 2>&1
if %ERRORLEVEL% == 0 (
    mvn %*
    exit /b %ERRORLEVEL%
)

:: Fall back to wrapper jar
if exist "%WRAPPER_JAR%" (
    java -jar "%WRAPPER_JAR%" %*
    exit /b %ERRORLEVEL%
)

echo ERROR: mvn not found on PATH and maven-wrapper.jar is missing. 1>&2
echo Install Maven from https://maven.apache.org/install.html or run: 1>&2
echo   mvn wrapper:wrapper 1>&2
exit /b 1
