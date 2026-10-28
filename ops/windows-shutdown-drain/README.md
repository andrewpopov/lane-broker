# Windows shutdown drain (BRAIN-350)

A runner inside WSL on a Windows workstation (skybox) loses its in-flight remote jobs whenever Windows is shut down or
restarted: WSL is killed without a systemd stop, so nothing on the runner side runs. Each lost job used to leave its
submitter waiting for `runnerLostAfterMs` (2 h).

`lane-shutdown-drain.ps1` runs hidden in the user's session. When Windows asks to end the session
(`WM_QUERYENDSESSION`), it holds the shutdown with a visible reason and runs `lane drain --shutdown` in WSL, bounded by
`-TimeoutSeconds`. That drain takes no new remote work and ends each in-flight job with a `runner-shutdown` record, which
proves to the submitter that the job is dead, so the submitter re-dispatches it to another runner straight away. The
drain marker is tied to the WSL boot, so the runner takes work again after the next start with no `lane undrain`.

Windows Home has no Group Policy logoff scripts, which is why this is a window rather than a script policy. A forced
restart (Windows Update with no signed-in user, a crash, a power cut) does not run it; those losses still settle through
`runnerLostAfterMs`.

## Install (on the Windows host, as the signed-in user)

1. Copy `lane-shutdown-drain.ps1` to `%LOCALAPPDATA%\lane-broker\`.
2. Check the WSL distro name (`wsl -l -v`) and that `bash -lc "lane --version"` works in it; pass `-Distro <name>` if it is
   not `Ubuntu`.
3. Register a logon task:

   ```powershell
   $a = New-ScheduledTaskAction -Execute powershell.exe -Argument "-NoProfile -WindowStyle Hidden -ExecutionPolicy Bypass -File `"$env:LOCALAPPDATA\lane-broker\lane-shutdown-drain.ps1`""
   $t = New-ScheduledTaskTrigger -AtLogOn -User $env:USERNAME
   Register-ScheduledTask -TaskName 'lane-shutdown-drain' -Action $a -Trigger $t -Settings (New-ScheduledTaskSettingsSet -ExecutionTimeLimit 0)
   Start-ScheduledTask -TaskName 'lane-shutdown-drain'
   ```

4. Verify: restart Windows once, then read `%LOCALAPPDATA%\lane-shutdown-drain.log` (one `exit 0: ...` line per
   shutdown) and, on the submitter, `lane: remote-redispatch:` lines in the broker log for any job that was running.

Remove with `Unregister-ScheduledTask -TaskName lane-shutdown-drain`.
