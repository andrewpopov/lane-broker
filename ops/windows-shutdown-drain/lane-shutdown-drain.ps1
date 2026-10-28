# BRAIN-350: run `lane drain --shutdown` inside WSL when Windows is shut down or restarted, before WSL is killed.
#
# WSL gets no graceful systemd stop on a Windows shutdown, and Windows Home has no Group Policy logoff scripts, so this
# runs as a hidden window in the user's session (started at logon by a scheduled task, see README.md). On
# WM_QUERYENDSESSION it blocks the shutdown with a visible reason, runs the drain (bounded), then lets Windows continue.
# The drain stops this runner's in-flight remote jobs with a `runner-shutdown` record, which tells each submitter the job
# is dead so it re-dispatches it to another runner at once instead of waiting out runnerLostAfterMs.
param(
  [string]$Distro = 'Ubuntu',
  [string]$LaneCommand = 'bash -lc "lane drain --shutdown windows-shutdown --wait-ms 45000"',
  [int]$TimeoutSeconds = 60,
  [string]$LogPath = "$env:LOCALAPPDATA\lane-shutdown-drain.log"
)

$ErrorActionPreference = 'Stop'

Add-Type -ReferencedAssemblies System.Windows.Forms -TypeDefinition @'
using System;
using System.Diagnostics;
using System.IO;
using System.Runtime.InteropServices;
using System.Windows.Forms;

public class LaneShutdownDrainForm : Form {
  const int WM_QUERYENDSESSION = 0x0011;
  [DllImport("user32.dll", CharSet = CharSet.Unicode)] static extern bool ShutdownBlockReasonCreate(IntPtr hWnd, string reason);
  [DllImport("user32.dll")] static extern bool ShutdownBlockReasonDestroy(IntPtr hWnd);

  readonly string distro, laneCommand, logPath;
  readonly int timeoutMs;
  bool drained;

  public LaneShutdownDrainForm(string distro, string laneCommand, int timeoutSeconds, string logPath) {
    this.distro = distro; this.laneCommand = laneCommand; this.timeoutMs = timeoutSeconds * 1000; this.logPath = logPath;
    ShowInTaskbar = false; WindowState = FormWindowState.Minimized; FormBorderStyle = FormBorderStyle.FixedToolWindow; Opacity = 0;
  }

  void Log(string line) {
    try { File.AppendAllText(logPath, DateTime.Now.ToString("s") + " " + line + Environment.NewLine); } catch { }
  }

  protected override void WndProc(ref Message m) {
    if (m.Msg == WM_QUERYENDSESSION && !drained) {
      drained = true;
      ShutdownBlockReasonCreate(Handle, "Stopping lane runner work so it moves to another machine");
      try {
        var psi = new ProcessStartInfo("wsl.exe", "-d " + distro + " -- " + laneCommand) {
          UseShellExecute = false, CreateNoWindow = true, RedirectStandardOutput = true, RedirectStandardError = true,
        };
        var p = Process.Start(psi);
        if (!p.WaitForExit(timeoutMs)) { Log("lane drain --shutdown did not finish in " + timeoutMs + " ms; continuing shutdown"); }
        else { Log("exit " + p.ExitCode + ": " + p.StandardOutput.ReadToEnd().Trim() + " " + p.StandardError.ReadToEnd().Trim()); }
      } catch (Exception e) {
        Log("failed: " + e.Message);
      } finally {
        ShutdownBlockReasonDestroy(Handle);
      }
    }
    base.WndProc(ref m);
  }
}
'@

[System.Windows.Forms.Application]::Run((New-Object LaneShutdownDrainForm($Distro, $LaneCommand, $TimeoutSeconds, $LogPath)))
