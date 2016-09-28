@echo off
@setlocal enableextensions enabledelayedexpansion
:: Requiers pip and setuptools to already be installed on linux subsystem
Set "Pattern= "
Set "Replace=\ "
Set "cdrive=C:"
Set "linpath=/mnt/c"
:: Iterate over arguments, convert paths to linux format and concatinate

set argCount=0
for %%x in (%*) do (
    set /A argCount+=1
    set arg=%%x
    :: Backward slash to forward slash
    SET arg=!arg:\=/!
    :: C drive to /mnt/c/ - default linux subsystem mount point
    SET arg=!arg:%cdrive%=%linpath%!
    :: Space to escaped space
    SET arg=!arg:%Pattern%=%Replace%!
    :: Parethesis to escaped parenteses
    SET arg=!arg:^(=\^(!
    SET arg=!arg:^)=\^)%!
    :: Deqoute voodoo via http://ss64.com/nt/syntax-dequote.html
    SET arg=###!arg!###
    SET arg=!arg:"###=!
    SET arg=!arg:###"=!
    SET arg=!arg:###=!
    if "!args!"=="" (
        set args=!arg!
    ) else (
        set args=!args! !arg!
    )
)
:: Dump it to the interpreter
:: Output is piped inside the Linux subsys, as windows piping for bash seems broken
START "Terrible hack to avoid pipe error" /W /MIN bash -c "/mnt/c/Users/Public/Anaconda/anaconda3/bin/python !args! > ~/pychmxx.log 2>&1"
::C:\Windows\System32\bash.exe -c "/mnt/c/Users/Public/Anaconda/anaconda3/bin/python !args! > /mnt/c/Users/wlee/mytry.log"
:: Output resulr from piped file
type C:\Users\wlee\AppData\Local\lxss\home\wlee\pychmxx.log
bash -c "rm ~/pychmxx.log"
:: echo !args!
EXIT /B > NUL