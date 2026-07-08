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

## AWS Glue Crawler creates one table per file for Delta Lake tables

**Symptom:**
Running a Glue Crawler against a Delta Lake table folder creates dozens of
separate tables — one per Parquet file and even per `_delta_log` JSON file —
instead of one clean table per dataset.

**Cause:**
Glue Crawlers don't natively understand the Delta Lake transaction log format.
They treat every file in the S3 path as a potential independent table,
especially when file counts are low or irregular.

**Fix:**
Skip the crawler for Delta/Silver tables. Instead, manually define Glue tables
via "Add table manually" — specify the S3 path, format (Parquet), and exact
column schema matching what the Spark job writes. This gives full control and
avoids crawler heuristics entirely. Excluding `_delta_log**` via crawler
exclude patterns helps but doesn't fully solve the one-table-per-file issue
when there are few files.