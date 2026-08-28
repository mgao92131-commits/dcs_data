Option Explicit

Dim shell, files, scriptsDirectory, rootDirectory, executablePath, configPath
Dim commandLine

Set shell = CreateObject("WScript.Shell")
Set files = CreateObject("Scripting.FileSystemObject")

scriptsDirectory = files.GetParentFolderName(WScript.ScriptFullName)
rootDirectory = files.GetParentFolderName(scriptsDirectory)
executablePath = files.BuildPath(rootDirectory, "bin\HistorySync.exe")
configPath = files.BuildPath(rootDirectory, "config\config.ini")

If Not files.FileExists(executablePath) Then
    WScript.Echo "HistorySync.exe was not found: " & executablePath
    WScript.Quit 1
End If

If Not files.FileExists(configPath) Then
    WScript.Echo "config.ini was not found: " & configPath
    WScript.Quit 1
End If

shell.CurrentDirectory = files.GetParentFolderName(executablePath)
commandLine = Chr(34) & executablePath & Chr(34) & _
    " run --config " & Chr(34) & configPath & Chr(34)
shell.Run commandLine, 0, False
WScript.Quit 0
