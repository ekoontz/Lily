@echo off
setlocal

rem Copyright (c) 1999, 2010 Tanuki Software, Ltd.
rem http://www.tanukisoftware.com
rem All rights reserved.
rem
rem This software is the proprietary information of Tanuki Software.
rem You shall use it only in accordance with the terms of the
rem license agreement you entered into with Tanuki Software.
rem http://wrapper.tanukisoftware.com/doc/english/licenseOverview.html
rem
rem Java Service Wrapper general startup script.

rem -----------------------------------------------------------------------------
rem These settings can be modified to fit the needs of your application
rem Optimized for use with version 3.5.4 of the Wrapper.

rem The base name for the Wrapper binary.
set _WRAPPER_BASE=wrapper

rem -----------------------------------------------------------------------------
rem Beginning Lily-specific section

if not "%JAVA_HOME%"=="" goto gotJavaHome
echo JAVA_HOME not set!
goto :eof
:gotJavaHome

if not "%LILY_HOME%"=="" goto gotLilyHome
rem %~dp0 is expanded pathname of the current script under NT
set LILY_HOME=%~dp0..
:gotLilyHome

rem
rem Also modified for Lily : _REALPATH further on in this file
rem
rem End of Lily-specific section
rem -----------------------------------------------------------------------------

rem The name and location of the Wrapper configuration file.   This will be used
rem  if the user does not specify a configuration file as the first argument to
rem  this script.
set _WRAPPER_CONF_DEFAULT=%LILY_HOME%/service/wrapper.conf

rem Do not modify anything beyond this point
rem -----------------------------------------------------------------------------

rem
rem Resolve the real path of the wrapper.exe
rem  For non NT systems, the _REALPATH and _WRAPPER_CONF values
rem  can be hard-coded below and the following test removed.
rem
if "%OS%"=="Windows_NT" goto nt
echo This script only works with NT-based versions of Windows.
goto :eof

:nt
rem
rem Find the application home.
rem
rem %~dp0 is location of current script under NT
set _REALPATH=%LILY_HOME%\service\wrapper-3.5.4\bin\

rem
rem Decide on the specific Wrapper binary to use (See delta-pack)
rem
if "%PROCESSOR_ARCHITECTURE%"=="AMD64" goto amd64
if "%PROCESSOR_ARCHITECTURE%"=="IA64" goto ia64
set _WRAPPER_L_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-x86-32.exe
goto search
:amd64
set _WRAPPER_L_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-x86-64.exe
goto search
:ia64
set _WRAPPER_L_EXE=%_REALPATH%%_WRAPPER_BASE%-windows-ia-64.exe
goto search
:search
set _WRAPPER_EXE=%_WRAPPER_L_EXE%
if exist "%_WRAPPER_EXE%" goto conf
set _WRAPPER_EXE=%_REALPATH%%_WRAPPER_BASE%.exe
if exist "%_WRAPPER_EXE%" goto conf
echo Unable to locate a Wrapper executable using any of the following names:
echo %_WRAPPER_L_EXE%
echo %_WRAPPER_EXE%
pause
goto :eof

rem
rem Find the wrapper.conf
rem
:conf
set _WRAPPER_CONF="%~f1"
if not %_WRAPPER_CONF%=="" goto startup
set _WRAPPER_CONF="%_WRAPPER_CONF_DEFAULT%"

rem
rem Start the Wrapper
rem
:startup
"%_WRAPPER_EXE%" -r %_WRAPPER_CONF%
if not errorlevel 1 goto :eof
pause
