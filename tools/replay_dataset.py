#!/usr/bin/env python3
"""Replay a text log file to the TCP receiver, one source connection per file."""

import argparse
import socket
import sys
import time
from pathlib import Path


PCAP_MAGICS = {
    b"\xd4\xc3\xb2\xa1", b"\xa1\xb2\xc3\xd4",
    b"\x4d\x3c\xb2\xa1", b"\xa1\xb2\x3c\x4d",
    b"\x0a\x0d\x0d\x0a",
}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file", required=True, help="newline-delimited text log dataset")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=18080)
    parser.add_argument("--rate", type=float, default=1000,
                        help="maximum lines per second; use 0 for unpaced replay")
    parser.add_argument("--loop", action="store_true", help="replay the file repeatedly until Ctrl+C")
    args = parser.parse_args()
    if not 1 <= args.port <= 65535 or args.rate < 0:
        parser.error("port must be 1..65535 and rate must be nonnegative")

    path = Path(args.file)
    with path.open("rb") as probe:
        if probe.read(4) in PCAP_MAGICS:
            parser.error("this is a binary PCAP capture, not a text log; extract TCP payloads before replay")

    period = 0 if args.rate == 0 else 1.0 / args.rate
    sent = 0
    try:
        with path.open("r", encoding="utf-8", errors="replace", newline="") as input_file:
            with socket.create_connection((args.host, args.port), timeout=10) as sock:
                sock.settimeout(None)
                print(f"Replaying {path} -> {args.host}:{args.port} at "
                      f"{'maximum' if period == 0 else f'{args.rate:g} lines/sec'}")
                while True:
                    line = input_file.readline()
                    if line == "":
                        if not args.loop:
                            break
                        input_file.seek(0)
                        continue
                    line = line.rstrip("\r\n")
                    sock.sendall(line.encode("utf-8") + b"\n")
                    sent += 1
                    if period:
                        time.sleep(period)
    except KeyboardInterrupt:
        print("Replay interrupted")
    except OSError as error:
        print(f"Replay failed: {error}", file=sys.stderr)
        return 1
    print(f"Sent {sent} lines from {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
