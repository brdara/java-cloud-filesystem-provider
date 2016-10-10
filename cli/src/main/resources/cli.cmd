set CLASSWORLDS_LAUNCHER=org.codehaus.plexus.classworlds.launcher.Launcher

SET "JCFS_HOME=%~dp0"

:stripMHome
if not "_%JCFS_HOME:~-1%"=="_\" goto checkMCmd
set "JCFS_HOME=%JCFS_HOME:~0,-1%"
goto stripMHome

:checkMCmd
set CLASSWORLDS_JAR=%JCFS_HOME%\dependencies\plexus-classworlds-2.5.2.jar
set DEBUG=-Xdebug -Xrunjdwp:transport=dt_socket,server=y,suspend=n,address=8099

java %DEBUG% -classpath %CLASSWORLDS_JAR%;%JCFS_HOME% "-Dclassworlds.conf=%JCFS_HOME%\cli.conf" "-Djcfs.home=%JCFS_HOME%" %CLASSWORLDS_LAUNCHER%