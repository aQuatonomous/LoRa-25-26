#!/usr/bin/env python3
"""
lora_cmd.py - Direct command execution over LoRa via Dragino LA66 P2P
No SSH — commands sent as plaintext, compressed, executed directly on Jetson.

Jetson:  python3 lora_cmd.py jetson
Windows: python lora_cmd.py win
"""

import sys, time, socket, select, logging, argparse, zlib, threading, subprocess, os, re
import serial, serial.tools.list_ports

# ── config ─────────────────────────────────────────────────────

LORA_FREQ  = "915.000"
LORA_SF    = "7"
LORA_BW    = "0"
LORA_CR    = "1"
LORA_POWER = "20"
LORA_GROUP = "1"
BAUD       = 9600

FRAG_MAX    = 200
MAX_RETRIES = 3
RX_TIMEOUT  = 2.5

T_DATA = 0x10
T_ACK  = 0x20
T_POLL = 0x30
T_DONE = 0x40

M_CMD      = 0x01
M_STDOUT   = 0x02
M_STDERR   = 0x03
M_EXIT     = 0x04
M_PING     = 0x05
M_PONG     = 0x06

COMPRESS_MIN = 30

log = logging.getLogger("bridge")

# ── RSSI tracking ─────────────────────────────────────────────

last_rssi = None

def rssi_bar(rssi):
    """Return a signal strength bar like ▂▄▆█ from RSSI."""
    if rssi is None:
        return "?"
    # RSSI typically -30 (strong) to -120 (weak)
    if rssi > -50:
        return "████"
    elif rssi > -70:
        return "███░"
    elif rssi > -90:
        return "██░░"
    elif rssi > -110:
        return "█░░░"
    else:
        return "░░░░"

# ── compression ────────────────────────────────────────────────

def compress(data):
    if not data or len(data) < COMPRESS_MIN:
        return b'\x00' + data
    c = zlib.compress(data, 6)
    if len(c) < len(data):
        log.debug(f"  compressed {len(data)}B -> {len(c)}B ({100-len(c)*100//len(data)}% saved)")
        return b'\x01' + c
    return b'\x00' + data

def decompress(data):
    if not data:
        return b""
    if data[0] == 0x01:
        return zlib.decompress(data[1:])
    return data[1:]

# ── serial helpers ──────────────────────────────────────────────

def at_cmd(ser, cmd, timeout=2.0):
    ser.reset_input_buffer()
    ser.write(f"{cmd}\r\n".encode())
    resp = ""
    t = time.time() + timeout
    while time.time() < t:
        if ser.in_waiting:
            resp += ser.read(ser.in_waiting).decode(errors="ignore")
            if "OK" in resp or "ERROR" in resp:
                break
        time.sleep(0.01)
    return resp.strip()

def find_la66():
    for p in serial.tools.list_ports.comports():
        d = (p.description or "").lower()
        if "bluetooth" in d or "bt " in d:
            continue
        log.info(f"Probing {p.device}...")
        try:
            ser = serial.Serial(p.device, BAUD, timeout=0.1)
            time.sleep(0.5)
            ser.reset_input_buffer()
            r = at_cmd(ser, "AT", timeout=1.5)
            if "OK" in r or "AT" in r:
                log.info(f"LA66 on {p.device}")
                return ser
            ser.close()
        except (serial.SerialException, OSError):
            continue
    return None

def configure_la66(ser):
    log.info("Configuring LA66...")
    ser.write(b"ATZ\r\n")
    time.sleep(2.5)
    ser.reset_input_buffer()

    cmds = [
        f"AT+FRE={LORA_FREQ},{LORA_FREQ}",
        f"AT+SF={LORA_SF},{LORA_SF}",
        f"AT+BW={LORA_BW},{LORA_BW}",
        f"AT+CR={LORA_CR},{LORA_CR}",
        f"AT+POWER={LORA_POWER}",
        f"AT+GROUPMOD={LORA_GROUP},{LORA_GROUP}",
        "AT+CRC=1,1",
        "AT+HEADER=0,0",
        "AT+IQ=0,0",
        "AT+SYNCWORD=0",
        "AT+RXMOD=65535,0",
    ]
    for cmd in cmds:
        r = at_cmd(ser, cmd, timeout=2.0)
        log.debug(f"  {cmd} -> {r[:60]}")
        if "ERROR" in r:
            log.error(f"FAIL: {cmd}")
            return False
        time.sleep(0.15)

    log.debug("  ATZ (apply settings)...")
    ser.write(b"ATZ\r\n")
    time.sleep(2.5)
    ser.reset_input_buffer()

    r = at_cmd(ser, "AT", timeout=2.0)
    if "OK" not in r and "AT" not in r:
        log.error("LA66 not responding after config reset!")
        return False

    log.info(f"LA66 ready: {LORA_FREQ}MHz SF{LORA_SF} {LORA_POWER}dBm")
    return True

def find_and_configure():
    """Find LA66 and configure it. Returns ser or None."""
    ser = find_la66()
    if ser and configure_la66(ser):
        return ser
    if ser:
        ser.close()
    return None

# ── low-level TX/RX ────────────────────────────────────────────

def tx_raw(ser, payload):
    h = payload.hex().upper()
    ser.reset_input_buffer()
    ser.write(f"AT+SEND=0,{h},0,0\r\n".encode())
    t = time.time() + 5.0
    while time.time() < t:
        if ser.in_waiting:
            line = ser.readline().decode(errors="ignore")
            if "txDone" in line:
                return True
            if "ERROR" in line:
                log.error(f"TX err: {line.strip()}")
                return False
        time.sleep(0.01)
    log.warning("TX: no txDone")
    return False

def rx_packet(ser, timeout=2.5):
    global last_rssi
    buf = ""
    t = time.time() + timeout
    while time.time() < t:
        try:
            if ser.in_waiting:
                buf += ser.read(ser.in_waiting).decode(errors="ignore")
        except serial.SerialException:
            raise
        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            stripped = line.strip()

            # extract RSSI from lines like "Rssi= -29", "RSSI:-45", etc
            m = re.search(r'[Rr][Ss][Ss][Ii][=:]\s*(-?\d+)', stripped)
            if m:
                last_rssi = int(m.group(1))

            p = _parse_hex(stripped)
            if p is not None:
                return p
        time.sleep(0.005)
    return None

def _parse_hex(line):
    if "(HEX:)" not in line:
        return None
    i = line.find(")")
    if i < 0:
        return None
    raw = line[i+1:].strip().replace(" ", "")
    clean = ""
    for c in raw:
        if c in "0123456789abcdefABCDEF":
            clean += c
        else:
            break
    if len(clean) < 4 or len(clean) % 2:
        return None
    try:
        b = bytes.fromhex(clean)
        return b[1:] if len(b) > 1 else None
    except ValueError:
        return None

# ── reliable send/recv ─────────────────────────────────────────

tx_seq = 0

def reliable_send(ser, data):
    global tx_seq
    if not data:
        return True
    payload = compress(data)
    chunks = [payload[i:i+FRAG_MAX] for i in range(0, len(payload), FRAG_MAX)]
    total = len(chunks)
    seq = tx_seq & 0xFF
    tx_seq += 1
    log.debug(f"TX {len(data)}B (wire {len(payload)}B) in {total} frags seq={seq}")

    for idx, chunk in enumerate(chunks):
        pkt = bytes([T_DATA, seq, idx, total]) + chunk
        for attempt in range(MAX_RETRIES):
            tx_raw(ser, pkt)
            resp = rx_packet(ser, timeout=RX_TIMEOUT)
            if resp and len(resp) >= 3 and resp[0] == T_ACK and resp[1] == seq and resp[2] == idx:
                log.debug(f"  frag {idx+1}/{total} ACKed")
                break
            log.debug(f"  frag {idx+1}/{total} retry {attempt+1}")
        else:
            log.warning(f"  frag {idx+1}/{total} FAILED after {MAX_RETRIES} retries")
            return False
    return True

def reliable_recv(ser, timeout=15.0):
    frags = {}
    expected_total = None
    expected_seq = None
    deadline = time.time() + timeout

    while time.time() < deadline:
        remaining = deadline - time.time()
        if remaining <= 0:
            break
        pkt = rx_packet(ser, timeout=min(2.5, remaining))
        if pkt is None:
            continue

        if len(pkt) == 1 and pkt[0] in (T_POLL, T_DONE):
            assembled = None
            if frags and expected_total:
                raw = b"".join(frags[i] for i in range(expected_total) if i in frags)
                assembled = decompress(raw)
            return assembled, pkt[0]

        if len(pkt) >= 4 and pkt[0] == T_DATA:
            seq, idx, total = pkt[1], pkt[2], pkt[3]
            payload = pkt[4:]

            if expected_seq is None:
                expected_seq = seq
                expected_total = total

            if seq == expected_seq and idx < total:
                frags[idx] = payload
                log.debug(f"RX frag {idx+1}/{total} seq={seq} len={len(payload)}")
                tx_raw(ser, bytes([T_ACK, seq, idx]))
                deadline = time.time() + timeout

                if len(frags) >= total:
                    raw = b"".join(frags[i] for i in range(total))
                    assembled = decompress(raw)
                    log.debug(f"Reassembled {len(assembled)}B")
                    return assembled, None

    return None, None

# ── message protocol ───────────────────────────────────────────

def send_msg(ser, msg_type, payload=b""):
    return reliable_send(ser, bytes([msg_type]) + payload)

def recv_msg(ser, timeout=30.0):
    data, marker = reliable_recv(ser, timeout=timeout)
    if data and len(data) >= 1:
        return data[0], data[1:]
    return None, marker

# ── spinner ────────────────────────────────────────────────────

class Spinner:
    def __init__(self, msg=""):
        self._msg = msg
        self._stop = threading.Event()
        self._thread = None

    def start(self):
        self._stop.clear()
        self._thread = threading.Thread(target=self._spin, daemon=True)
        self._thread.start()

    def update(self, msg):
        self._msg = msg

    def _spin(self):
        chars = "⠋⠙⠹⠸⠼⠴⠦⠧⠇⠏"
        i = 0
        while not self._stop.is_set():
            sys.stdout.write(f"\r  {chars[i % len(chars)]} {self._msg}    ")
            sys.stdout.flush()
            i += 1
            time.sleep(0.1)

    def stop(self, clear=True):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=1)
        if clear:
            sys.stdout.write(f"\r{' ' * (len(self._msg) + 10)}\r")
            sys.stdout.flush()

# ── WINDOWS (master) ──────────────────────────────────────────

def run_win(ser):
    print()
    print("  ┌─────────────────────────────────┐")
    print("  │   aQuatonomous LoRa Terminal     │")
    print("  │   Direct Command Mode            │")
    print("  └─────────────────────────────────┘")
    print()

    while True:
        # reconnect loop for serial
        if ser is None:
            print("  Disconnected from LA66. Reconnecting...")
            while ser is None:
                time.sleep(3)
                ser = find_and_configure()
            print("  LA66 reconnected!")

        # ping jetson
        sp = Spinner("Pinging Jetson...")
        sp.start()

        hostname = "jetson"
        connected = False
        try:
            send_msg(ser, M_PING)
            tx_raw(ser, bytes([T_POLL]))
            msg_type, payload = recv_msg(ser, timeout=10.0)

            if msg_type == M_PONG:
                hostname = payload.decode(errors="replace").strip() if payload else "jetson"
                connected = True
                # drain DONE
                while True:
                    d, m = reliable_recv(ser, timeout=5.0)
                    if m == T_DONE or (d is None and m is None):
                        break
            elif msg_type == T_DONE:
                connected = True
        except serial.SerialException:
            ser = None
            sp.stop()
            continue

        sp.stop()

        if connected:
            rssi_str = f" {rssi_bar(last_rssi)} {last_rssi}dBm" if last_rssi else ""
            print(f"  Connected to {hostname} via LoRa!{rssi_str}")
        else:
            print("  No response from Jetson. Retrying in 5s...")
            time.sleep(5)
            continue

        print("  Type commands below. 'exit' to quit.\n")

        # command loop
        disconnected = False

        # heartbeat: background thread sends tiny ping every few seconds
        # to keep RSSI updated while user is idle at prompt
        hb_stop = threading.Event()
        def heartbeat_loop():
            while not hb_stop.is_set():
                if hb_stop.wait(HEARTBEAT_INTERVAL):
                    break
                try:
                    send_msg(ser, M_PING)
                    tx_raw(ser, bytes([T_POLL]))
                    # just drain the pong/done, we only care about RSSI side-effect
                    recv_msg(ser, timeout=5.0)
                    while True:
                        d, m = reliable_recv(ser, timeout=3.0)
                        if m == T_DONE or (d is None and m is None):
                            break
                except serial.SerialException:
                    break
                except Exception:
                    pass

        hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
        hb_thread.start()

        while not disconnected:
            rssi_str = f"[{rssi_bar(last_rssi)} {last_rssi}dBm] " if last_rssi else ""
            prompt = f"{rssi_str}{hostname}> "

            # stop heartbeat while we read input (input blocks, but
            # we stop hb so it doesn't TX while we're typing)
            hb_stop.set()

            try:
                cmd = input(prompt)
            except EOFError:
                return

            cmd = cmd.strip()
            if not cmd:
                # restart heartbeat for idle period
                hb_stop = threading.Event()
                hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
                hb_thread.start()
                continue
            if cmd.lower() in ("exit", "quit"):
                return

            sp = Spinner("Sending command...")
            sp.start()

            try:
                send_msg(ser, M_CMD, cmd.encode())
                tx_raw(ser, bytes([T_POLL]))
            except serial.SerialException:
                sp.stop()
                print("  Lost connection to LA66!")
                ser = None
                disconnected = True
                continue

            sp.update("Waiting for output...")

            stdout_buf = []
            stderr_buf = []
            exit_code = -1
            got_exit = False

            try:
                while not got_exit:
                    msg_type, payload = recv_msg(ser, timeout=60.0)

                    if msg_type == M_STDOUT and payload:
                        stdout_buf.append(payload.decode(errors="replace"))
                    elif msg_type == M_STDERR and payload:
                        stderr_buf.append(payload.decode(errors="replace"))
                    elif msg_type == M_EXIT:
                        exit_code = int.from_bytes(payload[:4], "big", signed=True) if payload and len(payload) >= 4 else 0
                        got_exit = True
                    elif msg_type == T_DONE:
                        if stdout_buf or stderr_buf:
                            got_exit = True
                        else:
                            tx_raw(ser, bytes([T_POLL]))
                    elif msg_type is None:
                        if payload is None:
                            # total timeout
                            print("\n  Timeout — Jetson may be disconnected.")
                            disconnected = True
                            got_exit = True
                        elif payload == T_DONE:
                            if stdout_buf or stderr_buf:
                                got_exit = True
                            else:
                                tx_raw(ser, bytes([T_POLL]))

            except serial.SerialException:
                sp.stop()
                print("  Lost connection to LA66!")
                ser = None
                disconnected = True
                continue

            sp.stop()

            out = "".join(stdout_buf)
            err = "".join(stderr_buf)
            if out:
                print(out, end="" if out.endswith("\n") else "\n")
            if err:
                sys.stderr.write(err)
                if not err.endswith("\n"):
                    sys.stderr.write("\n")
            if exit_code != 0 and got_exit and not disconnected:
                print(f"  (exit code: {exit_code})")

            # restart heartbeat for next idle period
            if not disconnected:
                hb_stop = threading.Event()
                hb_thread = threading.Thread(target=heartbeat_loop, daemon=True)
                hb_thread.start()

        # if we got here, we disconnected — loop back to reconnect
        hb_stop.set()
        print()

# ── JETSON (slave) ────────────────────────────────────────────

def run_jetson(ser):
    log.info("=== JETSON (SLAVE) === waiting for commands...")
    hostname = os.uname().nodename

    while True:
        # reconnect loop for serial
        if ser is None:
            log.error("Disconnected from LA66. Reconnecting...")
            while ser is None:
                time.sleep(5)
                ser = find_and_configure()
            log.info("LA66 reconnected! Waiting for commands...")

        # wait for message + POLL
        msg_type = None
        msg_payload = None

        while True:
            try:
                data, marker = reliable_recv(ser, timeout=30.0)
            except serial.SerialException:
                log.error("Serial error!")
                try:
                    ser.close()
                except Exception:
                    pass
                ser = None
                break

            if data and len(data) >= 1:
                msg_type = data[0]
                msg_payload = data[1:]

            if marker == T_POLL:
                break
            if marker is None and data is None:
                break

        if ser is None:
            continue

        # handle PING
        if msg_type == M_PING:
            log.info("PING received")
            try:
                send_msg(ser, M_PONG, hostname.encode())
                tx_raw(ser, bytes([T_DONE]))
            except serial.SerialException:
                try: ser.close()
                except: pass
                ser = None
                continue
            time.sleep(0.3)

        # handle CMD
        elif msg_type == M_CMD:
            cmd = msg_payload.decode(errors="replace") if msg_payload else ""
            log.info(f"CMD: {cmd}")

            try:
                result = subprocess.run(
                    cmd, shell=True,
                    capture_output=True,
                    timeout=55,
                    cwd=os.path.expanduser("~")
                )
                stdout = result.stdout
                stderr = result.stderr
                exit_code = result.returncode
            except subprocess.TimeoutExpired:
                stdout = b""
                stderr = b"Command timed out (55s limit)\n"
                exit_code = -1
            except Exception as e:
                stdout = b""
                stderr = f"Error: {e}\n".encode()
                exit_code = -1

            log.info(f"  stdout={len(stdout)}B stderr={len(stderr)}B exit={exit_code}")

            try:
                if stdout:
                    send_msg(ser, M_STDOUT, stdout)
                if stderr:
                    send_msg(ser, M_STDERR, stderr)
                send_msg(ser, M_EXIT, exit_code.to_bytes(4, "big", signed=True))
                tx_raw(ser, bytes([T_DONE]))
            except serial.SerialException:
                log.error("Serial error sending response!")
                try: ser.close()
                except: pass
                ser = None
                continue

            time.sleep(0.3)

        else:
            try:
                tx_raw(ser, bytes([T_DONE]))
            except serial.SerialException:
                try: ser.close()
                except: pass
                ser = None
                continue
            time.sleep(0.2)

# ── main ──────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="Direct command execution over LoRa")
    ap.add_argument("mode", choices=["jetson", "win"])
    ap.add_argument("--port", help="serial port override")
    ap.add_argument("-v", "--verbose", action="store_true", help="show debug logs")
    args = ap.parse_args()

    if args.verbose or args.mode == "jetson":
        level = logging.DEBUG
    else:
        level = logging.WARNING

    logging.basicConfig(level=level,
                        format="[%(asctime)s %(levelname)s] %(message)s",
                        datefmt="%H:%M:%S")

    ser = serial.Serial(args.port, BAUD, timeout=0.1) if args.port else find_la66()
    if not ser:
        if args.mode == "win":
            print("  LA66 not found! Will keep trying...")
            ser = None
        else:
            log.error("LA66 not found")
            ser = None

    if ser and not configure_la66(ser):
        ser.close()
        ser = None

    try:
        {"jetson": run_jetson, "win": run_win}[args.mode](ser)
    except KeyboardInterrupt:
        if args.mode == "win":
            print("\n  Bye.")
        else:
            log.info("Bye.")
    finally:
        if ser:
            ser.close()

if __name__ == "__main__":
    main()