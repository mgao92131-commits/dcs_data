# DCS collector build and deployment

## Build

The DCS build requires the .NET Framework 3.5 compiler and always targets x86:

```bat
scripts\package-dcs.bat
```

The package is created at `artifacts\dcs_data` with this layout:

```text
bin\
config\
scripts\
state\
spool\
logs\
```

It contains no source, test, Probe, compatibility, Receiver, database, service,
scheduled-task, or startup files.

## DCS setup

1. Copy `artifacts\dcs_data` to a normal-user writable local directory.
2. Copy `config\config.example.ini` to `config\config.ini`.
3. Copy `config\tags.example.txt` to `config\tags.txt`.
4. Remove status/event and invalid tags from `tags.txt`.
5. Set Collector Id, Receiver URL, and API key.
6. Run `scripts\status.cmd` and then root-level `start-historysync.cmd`.
   Use root-level `stop-historysync.cmd` to stop it.

`start-historysync.cmd` launches a hidden normal-user host. It executes every
`[Sync] IntervalMinutes`; `stop-historysync.cmd` requests a graceful stop after
the current cycle. `run.cmd` provides the same host in the foreground for
diagnostics. None of these scripts installs a system component.

`MaxWindowMinutes` remains 30. All retained tags are read with
`InterpolatedValue` at `[Sampling] IntervalSeconds=10`.

## Upgrade

Preserve `config`, `state`, `spool`, and `logs`, then replace only `bin` and
`scripts`. Check pending batches before and after the replacement.

Because the processed-only collector intentionally has no Raw compatibility
mode, rollback requires restoring a complete earlier binary/config package.
