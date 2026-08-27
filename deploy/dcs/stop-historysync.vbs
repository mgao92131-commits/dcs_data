Option Explicit

Dim shell
Dim fso
Dim baseDirectory
Dim executablePath
Dim commandLine

Set shell = CreateObject("WScript.Shell")
Set fso = CreateObject("Scripting.FileSystemObject")
baseDirectory = fso.GetParentFolderName(WScript.ScriptFullName)
executablePath = fso.BuildPath(baseDirectory, "HistorySync.exe")

If fso.FileExists(executablePath) Then
    shell.CurrentDirectory = baseDirectory
    commandLine = """" & executablePath & """ --stop"
    shell.Run commandLine, 0, True
End If
