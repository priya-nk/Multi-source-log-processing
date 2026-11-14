# Multi-Source Log Processing Engine

A C++17 TCP log-processing pipeline. Multiple clients send newline-delimited JSON, Syslog, or Apache access records. The receiver frames each connection, parser workers normalize records, sharded deduplication filters repeats, and one disk-writer thread appends normalized CSV rows. Malformed records and incomplete frames go to a quarantine file.

## Requirements

- A C++17 compiler: GCC or Clang (Linux/macOS), or MinGW-w64 GCC on Windows.
- Python 3.8+ for the included source simulator. The simulator uses only the Python standard library.
- Windows builds must link Winsock (`ws2_32`).

## Build

Run these commands from the repository root.

### Windows PowerShell with MinGW-w64

```powershell
g++ -std=c++17 -O2 -pthread -Iinclude `
  src/main.cpp src/TcpLogReceiver.cpp src/ParserPool.cpp `
  src/DedupStage.cpp src/DiskWriter.cpp `
  -lws2_32 -o log_processor.exe
```

### Linux

```bash
g++ -std=c++17 -O2 -pthread -Iinclude \
  src/main.cpp src/TcpLogReceiver.cpp src/ParserPool.cpp \
  src/DedupStage.cpp src/DiskWriter.cpp \
  -o log_processor
```

## Run the server

Windows:

```powershell
.\log_processor.exe --port 18080 --output normalized_output.csv --quarantine quarantine.log
```

Linux:

```bash
./log_processor --port 18080 --output normalized_output.csv --quarantine quarantine.log
```

The server listens on all local IPv4 interfaces. Press **Ctrl+C** to stop it. Shutdown closes the receiver, drains each queue through the parser and dedup workers, flushes the output file, and prints the final conservation check:

```text
received = written + duplicates_dropped + quarantined
```

The terminal prints periodic throughput and pipeline counters. The output CSV is appended to; a header is added only when the file is empty. The quarantine file contains a reason, source, and raw line for each rejected record.

Optional server settings:

| Option | Default | Meaning |
|---|---:|---|
| `--port` | `18080` | TCP listen port |
| `--output` | `normalized_output.csv` | Normalized CSV path |
| `--quarantine` | `quarantine.log` | Rejected and incomplete line path |
| `--queue-capacity` | `16384` | Capacity of each pipeline queue |
| `--parser-threads` | half hardware threads | Parser worker count |
| `--dedup-threads` | half hardware threads | Dedup worker count |
| `--window-ms` | `300000` | Duplicate retention window |
| `--telemetry-ms` | `1000` | Terminal telemetry interval |

## Generate concurrent live sources

The included Python simulator opens independent TCP connections concurrently. In `mixed` mode, its sources send JSON, Syslog, and Apache access lines with realistic API, authentication, service, and HTTP request messages. Severity levels vary across DEBUG, INFO, WARN, ERROR, and CRITICAL; Apache status codes map to INFO/WARN/ERROR. All generated clocks use UTC (RFC 3164 Syslog has no timezone field). By default it also repeats records to exercise deduplication and periodically sends malformed JSON to exercise quarantine. The exact lines sent are saved in `raw_logs/json_original.log`, `raw_logs/syslog_original.log`, and `raw_logs/apache_original.log` for comparison with the normalized output.

For a clear demo, use five terminals, all opened in the repository root:

1. **Terminal 1 — pipeline server:** start `log_processor` as shown above and leave it running. It prints pipeline telemetry.
2. **Terminal 2 — browser dashboard:**

   ```powershell
   python tools\live_dashboard.py --file .\normalized_output.csv --port 8765
   ```

   Open [http://127.0.0.1:8765](http://127.0.0.1:8765) in your browser. The page refreshes automatically and shows the latest parsed rows.

3. **Terminal 3 — JSON source:**

   ```powershell
   python tools\log_source_simulator.py --host 127.0.0.1 --port 18080 --sources 1 --rate 10 --duration 60 --format json
   ```

4. **Terminal 4 — Syslog source:**

   ```powershell
   python tools\log_source_simulator.py --host 127.0.0.1 --port 18080 --sources 1 --rate 10 --duration 60 --format syslog
   ```

5. **Terminal 5 — Apache access source:**

   ```powershell
   python tools\log_source_simulator.py --host 127.0.0.1 --port 18080 --sources 1 --rate 10 --duration 60 --format apache
   ```

Each generator opens its own TCP connection. The three sources together offer about 30 lines per second. The simulator repeats records to exercise deduplication and periodically sends malformed JSON to exercise quarantine; these options can be disabled with `--duplicates-every 0` and `--malformed-every 0`.

Raw copies are written to one file per format in `raw_logs/`. Each simulator process overwrites the file for its selected format at startup, so the three-terminal demo produces a clean matching set. Use `--raw-mode append` to preserve prior runs, `--raw-dir PATH` to choose another folder, or `--no-raw-files` to disable copies. The raw files include duplicates and malformed records because they preserve the lines sent to the receiver.

To compare a source with the normalized output in PowerShell:

```powershell
Get-Content .\raw_logs\json_original.log -Tail 5
Get-Content .\normalized_output.csv -Tail 5
```

You can also start all three connections from one terminal with `--sources 3 --format mixed`:

```powershell
python tools\log_source_simulator.py --host 127.0.0.1 --port 18080 --sources 3 --rate 10 --duration 60 --format mixed
```

Use `--duration 0` to keep sources running until interrupted. To watch quarantine activity, open another terminal and follow the quarantine file:

```powershell
while (-not (Test-Path .\quarantine.log)) { Start-Sleep -Milliseconds 200 }
Get-Content .\quarantine.log -Wait
```

On Linux, use:

```bash
tail -f normalized_output.csv
```

The writer uses double-buffered group commits and flushes after 256 records, 1 MiB, or 250 ms, whichever comes first, so dashboard rows appear in small batches while one I/O thread writes the file. The browser dashboard reads the CSV file and refreshes every 750 ms; terminal telemetry remains in Terminal 1. Start both the pipeline server and dashboard before starting the generators. If a generator reports a connection-aborted error, check Terminal 1 for a shutdown, fatal error, or writer error; the server must remain running while the source terminals are active.

## Which machines to use for a demo

Start with **one laptop and five terminals**: one for the pipeline, one for the dashboard server, and one generator process per format. Open the dashboard URL in a browser. This demonstrates three independent concurrent TCP clients without network or firewall setup. You can also run one simulator process with three source threads if you prefer fewer terminals.

Use virtual machines only when the demonstration needs visibly separate host identities or different operating systems. Use separate laptops only for a LAN demonstration; connect clients to the server laptop's LAN IP, allow the listen port through its firewall, and ensure both machines are on a network where clients can reach that port. Do not expose the receiver port to the public internet for this demo.

## Using public log data

Public logs are useful for replay and parser evaluation, but they are recorded datasets, not live sources. [LogHub](https://github.com/logpai/loghub) provides research log datasets, including Apache error logs and logs from systems such as Linux, HDFS, and OpenSSH. Check the dataset notes and usage terms before using or redistributing a dataset; the LogHub maintainers note that some data is not anonymized. The included simulator is the easiest way to produce continuous traffic in the formats this receiver currently accepts.

### Replay the text files in `Dataset`

The project parser now accepts the Apache **error** log format in `Apache.log` and the HDFS format in `HDFS_2k.log`. Rebuild using the current sources before replaying them. In one terminal, start the dataset-enabled server with fresh output files:

```powershell
.\log_processor_dataset.exe --port 18080 --output .\dataset_output.csv --quarantine .\dataset_quarantine.log
```

In another terminal, replay Apache lines:

```powershell
python tools\replay_dataset.py --file .\Dataset\Apache.log --host 127.0.0.1 --port 18080 --rate 1000
```

Open a third terminal to replay HDFS lines concurrently:

```powershell
python tools\replay_dataset.py --file .\Dataset\HDFS_2k.log --host 127.0.0.1 --port 18080 --rate 1000
```

Use `--rate 0` for unpaced replay or `--loop` to replay repeatedly until Ctrl+C. The `.pcap` file is a binary packet capture, not a text log, so `replay_dataset.py` deliberately rejects it. Extracting its TCP payloads requires a packet-capture tool and checking that the payloads are newline-delimited logs before replay.

