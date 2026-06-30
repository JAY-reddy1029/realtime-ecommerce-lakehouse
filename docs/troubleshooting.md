# Troubleshooting Notes

## Git Bash mangles Docker exec paths on Windows

**Symptom:**
OCI runtime exec failed: exec failed: unable to start container process:
exec: "C:/Program Files/Git/opt/spark/bin/spark-submit": stat ...: no such file or directory


**Cause:**
Git Bash (MinGW) automatically converts any argument that looks like a Unix path
(starting with `/`) into a Windows path before passing it to the command. This breaks
`docker exec` commands targeting paths *inside* a Linux container.

**Fix:**
Add this to `~/.bashrc` (one-time, permanent fix):
```bash
export MSYS_NO_PATHCONV=1
```
Then reload with `source ~/.bashrc`.