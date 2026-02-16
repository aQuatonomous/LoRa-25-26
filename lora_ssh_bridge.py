#!/usr/bin/env python3
"""
lora_ssh_bridge.py - SSH over LoRa via Dragino LA66 P2P

Jetson:  python3 lora_ssh_bridge.py jetson
Windows: python lora_ssh_bridge.py win
         (opens a command prompt — type commands, get output over LoRa)
         python lora_ssh_bridge.py ssh
         (legacy mode — listens on port 2222 for external SSH client)
"""

import sys, time, socket, select, logging, argparse, zlib, threading
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

LISTEN_PORT = 2222
SSH_HOST    = "127.0.0.1"
SSH_PORT    = 22
SSH_USER    = "lorenzo"
SSH_PASS    = "Robotic2025!"

COMPRESS_MIN = 50

logging.basicConfig(level=logging.DEBUG,
                    format="[%(asctime)s %(levelname)s] %(message)s",
                    datefmt="%H:%M:%S")
log = logging.getLogger("bridge")

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
            p = _parse_hex(line.strip())
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

# ── TCP helpers ────────────────────────────────────────────────

def tcp_drain(sock):
    if not sock:
        return b""
    buf = b""
    try:
        while True:
            r, _, _ = select.select([sock], [], [], 0.02)
            if not r:
                break
            d = sock.recv(4096)
            if not d:
                return None
            buf += d
    except OSError:
        return None
    return buf

# ── WINDOWS: interactive terminal (paramiko) ──────────────────

def run_win(ser):
    """Combined bridge + terminal. No external SSH client needed."""
    try:
        import paramiko
    except ImportError:
        print("paramiko required: pip install paramiko")
        sys.exit(1)

    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", LISTEN_PORT))
    srv.listen(1)

    bridge_ready = threading.Event()

    def bridge_loop():
        try:
            cli, addr = srv.accept()
            cli.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            cli.setblocking(False)
            bridge_ready.set()
            log.debug("Bridge: internal SSH connection accepted")

            while True:
                time.sleep(0.15)
                tcp_data = tcp_drain(cli)
                if tcp_data is None:
                    log.debug("Bridge: SSH transport closed")
                    return

                if tcp_data:
                    if len(tcp_data) < 50:
                        time.sleep(0.1)
                        more = tcp_drain(cli)
                        if more is None:
                            return
                        if more:
                            tcp_data += more
                    log.debug(f"TCP->LoRa: {len(tcp_data)}B")
                    reliable_send(ser, tcp_data)

                tx_raw(ser, bytes([T_POLL]))

                rx_data = None
                while True:
                    data, marker = reliable_recv(ser, timeout=10.0)
                    if data:
                        rx_data = data
                        try:
                            cli.sendall(data)
                            log.debug(f"LoRa->TCP: {len(data)}B")
                        except OSError:
                            return
                    if marker == T_DONE:
                        break
                    if marker is None and data is None:
                        log.warning("Timeout waiting for DONE")
                        break

                if not tcp_data and not rx_data:
                    time.sleep(0.3)

        except Exception as e:
            log.error(f"Bridge error: {e}")
            bridge_ready.set()
        finally:
            srv.close()

    bt = threading.Thread(target=bridge_loop, daemon=True)
    bt.start()

    print(f"\n  aQuatonomous LoRa Terminal")
    print(f"  Connecting to {SSH_USER}@jetson via LoRa...")
    print(f"  (this takes ~30-60s for SSH handshake)\n")

    try:
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect("127.0.0.1", port=LISTEN_PORT,
                    username=SSH_USER, password=SSH_PASS,
                    timeout=120, banner_timeout=120, auth_timeout=120,
                    look_for_keys=False, allow_agent=False,
                    compress=True)
        print("  Connected!\n")
    except Exception as e:
        print(f"\n  SSH connection failed: {e}")
        return

    print("  Type commands to run on the Jetson. 'exit' to quit.")
    print("  Ctrl+C to abort.\n")

    try:
        while True:
            try:
                cmd = input("jetson> ")
            except EOFError:
                break

            cmd = cmd.strip()
            if not cmd:
                continue
            if cmd.lower() in ("exit", "quit"):
                break

            try:
                stdin, stdout, stderr = ssh.exec_command(cmd, timeout=120)
                out = stdout.read().decode(errors="replace")
                err = stderr.read().decode(errors="replace")
                if out:
                    print(out, end="" if out.endswith("\n") else "\n")
                if err:
                    print(err, end="" if err.endswith("\n") else "\n")
            except Exception as e:
                print(f"  Error: {e}")

    except KeyboardInterrupt:
        print("\n")
    finally:
        ssh.close()
        print("  Disconnected.")

# ── WINDOWS: legacy SSH passthrough mode ──────────────────────

def run_ssh(ser):
    """Legacy mode: listens on port 2222 for external SSH client."""
    log.info(f"=== SSH PASSTHROUGH === ssh -p {LISTEN_PORT} {SSH_USER}@127.0.0.1")
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", LISTEN_PORT))
    srv.listen(1)
    log.info("Waiting for SSH client...")
    cli, addr = srv.accept()
    cli.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    cli.setblocking(False)
    log.info(f"Connected: {addr}")

    try:
        while True:
            time.sleep(0.15)
            tcp_data = tcp_drain(cli)
            if tcp_data is None:
                log.info("SSH client disconnected"); return

            if tcp_data:
                if len(tcp_data) < 50:
                    time.sleep(0.1)
                    more = tcp_drain(cli)
                    if more is None:
                        log.info("SSH client disconnected"); return
                    if more:
                        tcp_data += more
                log.debug(f"TCP->LoRa: {len(tcp_data)}B")
                reliable_send(ser, tcp_data)

            tx_raw(ser, bytes([T_POLL]))

            rx_data = None
            while True:
                data, marker = reliable_recv(ser, timeout=10.0)
                if data:
                    rx_data = data
                    try:
                        cli.sendall(data)
                        log.debug(f"LoRa->TCP: {len(data)}B")
                    except OSError:
                        return
                if marker == T_DONE:
                    break
                if marker is None and data is None:
                    log.warning("Timeout waiting for DONE")
                    break

            if not tcp_data and not rx_data:
                time.sleep(0.3)

    finally:
        cli.close(); srv.close()

# ── JETSON (slave) ────────────────────────────────────────────

def recover_serial(ser):
    """Try to recover LA66 serial connection with retries."""
    try:
        ser.close()
    except Exception:
        pass
    for attempt in range(5):
        time.sleep(3)
        ser_new = find_la66()
        if ser_new:
            if configure_la66(ser_new):
                log.info("Recovered serial connection")
                return ser_new
            else:
                log.error(f"Reconfig failed, retry {attempt+1}/5...")
        else:
            log.info(f"LA66 not found, retry {attempt+1}/5...")
    return None

def run_jetson(ser):
    log.info("=== JETSON (SLAVE) === waiting for POLL...")
    ssh_sock = None

    while True:
        data_chunks = []
        while True:
            try:
                data, marker = reliable_recv(ser, timeout=30.0)
            except serial.SerialException:
                log.error("Serial error, attempting recovery...")
                ser = recover_serial(ser)
                if not ser:
                    log.error("LA66 lost after 5 attempts"); return
                data, marker = None, None
            if data:
                data_chunks.append(data)
            if marker == T_POLL:
                break
            if marker is None and data is None:
                break

        for chunk in data_chunks:
            if ssh_sock is None:
                log.info("Connecting to SSH...")
                try:
                    ssh_sock = socket.create_connection((SSH_HOST, SSH_PORT))
                    ssh_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    ssh_sock.setblocking(False)
                    log.info("SSH connected")
                except OSError as e:
                    log.error(f"SSH failed: {e}"); continue
            try:
                ssh_sock.sendall(chunk)
                log.debug(f"LoRa->SSH: {len(chunk)}B")
            except OSError:
                ssh_sock.close(); ssh_sock = None

        time.sleep(0.08)
        tcp_data = tcp_drain(ssh_sock) if ssh_sock else b""
        if tcp_data is None:
            log.info("SSH closed"); ssh_sock = None; tcp_data = b""

        if tcp_data:
            log.debug(f"SSH->LoRa: {len(tcp_data)}B")
            reliable_send(ser, tcp_data)

        try:
            tx_raw(ser, bytes([T_DONE]))
        except serial.SerialException:
            log.error("Serial error on DONE, attempting recovery...")
            ser = recover_serial(ser)
            if not ser:
                log.error("LA66 lost after 5 attempts"); return
            continue

        # let radio settle before switching back to receive
        time.sleep(0.3)

        if not data_chunks and not tcp_data:
            time.sleep(0.2)

# ── main ──────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser(
        description="SSH over LoRa bridge",
        epilog="Modes: jetson (run on Jetson), win (terminal), ssh (legacy passthrough)")
    ap.add_argument("mode", choices=["jetson", "win", "ssh"])
    ap.add_argument("--port", help="serial port override")
    args = ap.parse_args()

    ser = serial.Serial(args.port, BAUD, timeout=0.1) if args.port else find_la66()
    if not ser:
        log.error("LA66 not found"); sys.exit(1)
    if not configure_la66(ser):
        sys.exit(1)

    try:
        {"jetson": run_jetson, "win": run_win, "ssh": run_ssh}[args.mode](ser)
    except KeyboardInterrupt:
        log.info("Bye.")
    finally:
        ser.close()

if __name__ == "__main__":
    main()