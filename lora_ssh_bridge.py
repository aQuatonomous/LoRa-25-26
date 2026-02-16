#!/usr/bin/env python3
"""
lora_ssh_bridge.py - SSH over LoRa via Dragino LA66 P2P
Jetson:  python3 lora_ssh_bridge.py jetson
Windows: python lora_ssh_bridge.py win
Then:    ssh -p 2222 user@127.0.0.1
Requires: pip install pyserial

Windows is MASTER, Jetson is SLAVE. Strict turn-taking.
Each fragment is sent individually and ACKed before the next.
"""

import sys, time, socket, select, logging, argparse
import serial, serial.tools.list_ports

# radio
LORA_FREQ  = "915.000"
LORA_SF    = "7"
LORA_BW    = "0"
LORA_CR    = "1"
LORA_POWER = "20"
LORA_GROUP = "1"
BAUD       = 9600

# fragmentation
FRAG_MAX   = 220
FRAG_DELAY = 0.4  # seconds between fragments to let receiver process

# control markers
MARKER_POLL = 0xFE
MARKER_DONE = 0xFD

# network
LISTEN_PORT = 2222
SSH_HOST    = "127.0.0.1"
SSH_PORT    = 22

logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bridge")

# serial / AT

def at_cmd(ser, cmd, timeout=2.0):
    ser.reset_input_buffer()
    ser.write(f"{cmd}\r\n".encode())
    resp = ""
    deadline = time.time() + timeout
    while time.time() < deadline:
        if ser.in_waiting:
            resp += ser.read(ser.in_waiting).decode(errors="ignore")
            if "OK" in resp or "ERROR" in resp:
                break
        time.sleep(0.01)
    return resp.strip()

def find_la66():
    for p in serial.tools.list_ports.comports():
        desc = (p.description or "").lower()
        if "bluetooth" in desc or "bt" in desc:
            continue
        log.info(f"Probing {p.device}...")
        try:
            ser = serial.Serial(p.device, BAUD, timeout=0.1)
            time.sleep(0.3)
            resp = at_cmd(ser, "AT", timeout=1.0)
            if "OK" in resp or "AT" in resp:
                log.info(f"Found LA66 on {p.device}")
                return ser
            ser.close()
        except (serial.SerialException, OSError):
            continue
    return None

def configure_la66(ser):
    log.info("Configuring LA66...")
    at_cmd(ser, "ATZ")
    time.sleep(1.0)
    ser.reset_input_buffer()
    for cmd in [
        f"AT+FRE={LORA_FREQ},{LORA_FREQ}", f"AT+SF={LORA_SF},{LORA_SF}",
        f"AT+BW={LORA_BW},{LORA_BW}", f"AT+CR={LORA_CR},{LORA_CR}",
        f"AT+POWER={LORA_POWER}", f"AT+GROUPMOD={LORA_GROUP},{LORA_GROUP}",
        "AT+CRC=1,1", "AT+HEADER=0,0", "AT+IQ=0,0",
        "AT+SYNCWORD=0",
        "AT+RXMOD=65535,0",
    ]:
        resp = at_cmd(ser, cmd)
        if "ERROR" in resp:
            log.error(f"Config failed: {cmd} -> {resp}")
            return False
        time.sleep(0.05)
    log.info(f"LA66 ready: {LORA_FREQ}MHz SF{LORA_SF} {LORA_POWER}dBm")
    return True

# low level: send one LoRa packet, wait for txDone

def tx_packet(ser, payload):
    hexstr = payload.hex().upper()
    ser.reset_input_buffer()
    ser.write(f"AT+SEND=0,{hexstr},0,0\r\n".encode())
    deadline = time.time() + 5.0
    while time.time() < deadline:
        if ser.in_waiting:
            line = ser.readline().decode(errors="ignore").strip()
            if "txDone" in line:
                return True
            if "ERROR" in line:
                log.error(f"TX err: {line}")
                return False
        time.sleep(0.01)
    log.warning("TX no txDone")
    return False

# send data as fragments with delay, then a marker

tx_seq = 0

def send_burst(ser, data, marker):
    global tx_seq
    if data:
        chunks = [data[i:i + FRAG_MAX] for i in range(0, len(data), FRAG_MAX)]
        total = len(chunks)
        seq = tx_seq & 0xFF
        tx_seq += 1
        log.debug(f"TX burst: {len(data)}B, {total} frags, seq={seq}")
        for idx, chunk in enumerate(chunks):
            pkt = bytes([seq, idx, total]) + chunk
            tx_packet(ser, pkt)
            # delay between frags to let receiver process
            if idx < total - 1:
                time.sleep(FRAG_DELAY)
    tx_packet(ser, bytes([marker]))
    log.debug(f"TX marker: {marker:#x}")

# parse hex from LA66 serial output

def try_parse_hex(line):
    if "(HEX:)" not in line and "(HEX:)" not in line.upper():
        return None
    idx = line.find(")")
    if idx < 0:
        return None
    hexpart = line[idx+1:].strip().replace(" ", "")
    # strip trailing non-hex
    cleaned = ""
    for c in hexpart:
        if c in "0123456789abcdefABCDEF":
            cleaned += c
        else:
            break
    if len(cleaned) < 4 or len(cleaned) % 2 != 0:
        return None
    try:
        raw = bytes.fromhex(cleaned)
        return raw[1:] if len(raw) > 1 else None  # skip group byte
    except ValueError:
        return None

# receive until marker, with fragment reassembly

class Reassembler:
    def __init__(self):
        self.reset()

    def reset(self):
        self.seq = None
        self.total = 0
        self.frags = {}
        self.started = 0

    def feed(self, frame):
        if len(frame) < 3:
            return None
        seq, idx, total = frame[0], frame[1], frame[2]
        if total == 0 or idx >= total:
            return None
        if self.seq != seq or (time.time() - self.started > 30):
            self.reset()
            self.seq = seq
            self.total = total
            self.started = time.time()
        self.frags[idx] = frame[3:]
        log.debug(f"RX frag {idx+1}/{total} seq={seq} len={len(frame)-3}")
        if len(self.frags) >= self.total:
            result = b"".join(self.frags[i] for i in range(self.total) if i in self.frags)
            log.debug(f"Reassembled {len(result)}B")
            self.reset()
            return result
        return None

    def flush(self):
        """Return whatever we have, even if incomplete."""
        if not self.frags:
            return None
        result = b"".join(self.frags[i] for i in sorted(self.frags.keys()))
        log.warning(f"Flushing partial: {len(self.frags)}/{self.total} frags, {len(result)}B")
        self.reset()
        return result

def rx_until_marker(ser, marker, reasm, timeout=15.0):
    results = []
    buf = ""
    deadline = time.time() + timeout

    while time.time() < deadline:
        if ser.in_waiting:
            raw = ser.read(ser.in_waiting)
            buf += raw.decode(errors="ignore")

        while "\n" in buf:
            line, buf = buf.split("\n", 1)
            line = line.strip()
            if not line:
                continue

            payload = try_parse_hex(line)
            if payload is None:
                continue

            if len(payload) == 1 and payload[0] == marker:
                log.debug(f"RX marker: {marker:#x}")
                # flush any incomplete reassembly
                partial = reasm.flush()
                if partial:
                    results.append(partial)
                return results, True

            if len(payload) >= 4:
                assembled = reasm.feed(payload)
                if assembled:
                    results.append(assembled)

        time.sleep(0.005)

    # timeout - flush partial
    partial = reasm.flush()
    if partial:
        results.append(partial)
    log.warning("RX timeout")
    return results, False

# TCP helpers

def tcp_drain(sock):
    if sock is None:
        return b""
    buf = b""
    try:
        while True:
            r, _, _ = select.select([sock], [], [], 0.01)
            if not r:
                break
            d = sock.recv(4096)
            if not d:
                return None
            buf += d
    except OSError:
        return None
    return buf

# Windows (MASTER)

def run_win(ser):
    log.info(f"=== WINDOWS (MASTER) === ssh -p {LISTEN_PORT} user@127.0.0.1")
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(("127.0.0.1", LISTEN_PORT))
    srv.listen(1)
    log.info("Waiting for SSH client...")
    client, addr = srv.accept()
    client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    client.setblocking(False)
    log.info(f"Client connected: {addr}")
    reasm = Reassembler()

    try:
        while True:
            time.sleep(0.05)
            tcp_data = tcp_drain(client)
            if tcp_data is None:
                log.info("Client disconnected"); return

            if tcp_data:
                log.debug(f"TCP->LoRa: {len(tcp_data)}B")
            send_burst(ser, tcp_data, MARKER_POLL)

            rx, got_done = rx_until_marker(ser, MARKER_DONE, reasm, timeout=12.0)

            for data in rx:
                try:
                    client.sendall(data)
                    log.debug(f"LoRa->TCP: {len(data)}B")
                except OSError:
                    log.error("TCP write failed"); return

            if not got_done:
                log.warning("No DONE from Jetson, retrying...")
    finally:
        client.close(); srv.close()

# Jetson (SLAVE)

def run_jetson(ser):
    log.info("=== JETSON (SLAVE) === waiting...")
    reasm = Reassembler()
    ssh_sock = None

    while True:
        rx, got_poll = rx_until_marker(ser, MARKER_POLL, reasm, timeout=30.0)

        if not got_poll and not rx:
            continue

        for data in rx:
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
                ssh_sock.sendall(data)
                log.debug(f"LoRa->SSH: {len(data)}B")
            except OSError:
                ssh_sock.close(); ssh_sock = None

        time.sleep(0.1)
        tcp_data = tcp_drain(ssh_sock) if ssh_sock else b""
        if tcp_data is None:
            log.info("SSH closed"); ssh_sock = None; tcp_data = b""

        if tcp_data:
            log.debug(f"SSH->LoRa: {len(tcp_data)}B")
        send_burst(ser, tcp_data, MARKER_DONE)

def main():
    parser = argparse.ArgumentParser(description="SSH over LoRa (LA66 P2P)")
    parser.add_argument("mode", choices=["jetson", "win"])
    parser.add_argument("--port", help="serial port (skip auto-detect)")
    args = parser.parse_args()

    ser = serial.Serial(args.port, BAUD, timeout=0.1) if args.port else find_la66()
    if not ser:
        log.error("LA66 not found"); sys.exit(1)
    if not configure_la66(ser):
        sys.exit(1)

    try:
        {"jetson": run_jetson, "win": run_win}[args.mode](ser)
    except KeyboardInterrupt:
        log.info("Bye.")
    finally:
        ser.close()

if __name__ == "__main__":
    main()