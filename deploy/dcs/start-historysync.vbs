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

If Not fso.FileExists(executablePath) Then
    MsgBox "HistorySync.exe was not found: " & executablePath, 16, "HistorySync"
    WScript.Quit 1
End If

shell.CurrentDirectory = baseDirectory
commandLine = """" & executablePath & """ --console"
shell.Run commandLine, 0, False
