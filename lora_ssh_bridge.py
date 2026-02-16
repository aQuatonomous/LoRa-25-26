#!/usr/bin/env python3
"""
lora_ssh_bridge.py - SSH over LoRa via Dragino LA66 P2P
Jetson:  python3 lora_ssh_bridge.py jetson
Windows: python lora_ssh_bridge.py win
Then:    ssh -p 2222 user@127.0.0.1
Requires: pip install pyserial
"""

import sys, time, socket, select, logging, argparse, threading
import serial, serial.tools.list_ports

# radio config
LORA_FREQ  = "915.000"
LORA_SF    = "7"
LORA_BW    = "0"       # 125kHz
LORA_CR    = "1"       # 4/5
LORA_POWER = "20"
LORA_GROUP = "1"
BAUD       = 9600

# fragmentation - SF7 max is 230, minus LA66 group byte, minus 3 byte header
FRAG_MAX   = 220
TX_DELAY   = 0.12

# network
LISTEN_PORT = 2222
SSH_HOST    = "127.0.0.1"
SSH_PORT    = 22

logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bridge")

# AT command stuff

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
        # skip bluetooth ports, they hang
        desc = (p.description or "").lower()
        if "bluetooth" in desc or "bt" in desc:
            log.debug(f"Skipping bluetooth port {p.device}")
            continue
        log.info(f"Probing {p.device}...")
        try:
            ser = serial.Serial(p.device, BAUD, timeout=0.5)
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
        "AT+SYNCWORD=0", "AT+RXMOD=65535,2",
    ]:
        resp = at_cmd(ser, cmd)
        if "ERROR" in resp:
            log.error(f"Config failed: {cmd} -> {resp}")
            return False
        time.sleep(0.05)
    log.info(f"LA66 ready: {LORA_FREQ}MHz SF{LORA_SF} {LORA_POWER}dBm")
    return True

# fragmented send

tx_seq = 0

def lora_send(ser, data):
    global tx_seq
    log.debug(f"lora_send: {len(data)} bytes to send")
    chunks = [data[i:i + FRAG_MAX] for i in range(0, len(data), FRAG_MAX)]
    total = len(chunks)
    seq = tx_seq & 0xFF
    tx_seq += 1
    for idx, chunk in enumerate(chunks):
        frame = bytes([seq, idx, total]) + chunk
        hexstr = frame.hex().upper()
        log.debug(f"TX frag {idx+1}/{total} seq={seq} len={len(chunk)} hexlen={len(hexstr)}")
        resp = at_cmd(ser, f"AT+SEND=0,{hexstr},0,0")
        log.debug(f"TX resp: {resp}")
        if idx < total - 1:
            time.sleep(TX_DELAY)
    log.debug(f"lora_send done: {total} fragments sent")

# reassembly

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
            log.debug(f"Reassembled {len(result)} bytes from {self.total} fragments")
            self.reset()
            return result
        return None

# parse LA66 serial output

def parse_rx_line(line):
    line = line.strip()
    if not line:
        return None
    for sep in [",", "="]:
        if sep in line:
            candidate = line.rsplit(sep, 1)[1].strip()
            try:
                return bytes.fromhex(candidate)
            except ValueError:
                pass
    return None

# background serial reader

def serial_reader(ser, reasm, rx_queue, rx_lock):
    buf = ""
    while True:
        try:
            chunk = ser.read(ser.in_waiting or 1)
            if chunk:
                buf += chunk.decode(errors="ignore")
                while "\n" in buf:
                    line, buf = buf.split("\n", 1)
                    line = line.strip()
                    if not line:
                        continue
                    log.debug(f"serial rx: {line}")
                    raw = parse_rx_line(line)
                    if raw:
                        assembled = reasm.feed(raw)
                        if assembled:
                            with rx_lock:
                                rx_queue.append(assembled)
        except (serial.SerialException, OSError):
            log.error("Serial port lost!")
            break
        time.sleep(0.005)

def start_reader(ser):
    rx_queue, rx_lock, ser_lock = [], threading.Lock(), threading.Lock()
    threading.Thread(
        target=serial_reader,
        args=(ser, Reassembler(), rx_queue, rx_lock),
        daemon=True,
    ).start()
    return rx_queue, rx_lock, ser_lock

# jetson mode

def run_jetson(ser):
    log.info("=== JETSON MODE === waiting for LoRa data...")
    rx_queue, rx_lock, ser_lock = start_reader(ser)
    ssh_sock = None

    while True:
        with rx_lock:
            pending = list(rx_queue)
            rx_queue.clear()

        for data in pending:
            if ssh_sock is None:
                log.info("Connecting to SSH...")
                try:
                    ssh_sock = socket.create_connection((SSH_HOST, SSH_PORT))
                    ssh_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    ssh_sock.setblocking(False)
                    log.info("SSH connected")
                except OSError as e:
                    log.error(f"SSH connect failed: {e}"); continue
            try:
                ssh_sock.sendall(data)
                log.debug(f"Sent {len(data)} bytes to SSH")
            except OSError:
                log.error("SSH write failed")
                ssh_sock.close(); ssh_sock = None

        if ssh_sock:
            try:
                if select.select([ssh_sock], [], [], 0.01)[0]:
                    data = ssh_sock.recv(4096)
                    if data:
                        log.debug(f"SSH -> LoRa: {len(data)} bytes")
                        with ser_lock: lora_send(ser, data)
                    else:
                        log.info("SSH closed"); ssh_sock.close(); ssh_sock = None
            except OSError:
                ssh_sock.close(); ssh_sock = None
        else:
            time.sleep(0.01)

# windows mode

def run_win(ser):
    log.info(f"=== WINDOWS MODE === ssh -p {LISTEN_PORT} user@127.0.0.1")
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", LISTEN_PORT))
    server.listen(1)
    log.info("Waiting for SSH client...")

    client, addr = server.accept()
    client.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    client.setblocking(False)
    log.info(f"Client connected: {addr}")

    rx_queue, rx_lock, ser_lock = start_reader(ser)

    try:
        while True:
            with rx_lock:
                pending = list(rx_queue)
                rx_queue.clear()
            for data in pending:
                try:
                    client.sendall(data)
                    log.debug(f"LoRa -> TCP: {len(data)} bytes")
                except OSError:
                    log.error("TCP send failed"); return

            try:
                if select.select([client], [], [], 0.01)[0]:
                    data = client.recv(4096)
                    if data:
                        log.debug(f"TCP -> LoRa: {len(data)} bytes")
                        with ser_lock: lora_send(ser, data)
                    else:
                        log.info("Client disconnected"); return
            except OSError:
                return
    finally:
        client.close(); server.close()

# main

def main():
    parser = argparse.ArgumentParser(description="SSH over LoRa (LA66 P2P)")
    parser.add_argument("mode", choices=["jetson", "win"])
    parser.add_argument("--port", help="serial port (skip auto-detect)")
    args = parser.parse_args()

    ser = serial.Serial(args.port, BAUD, timeout=0.1) if args.port else find_la66()
    if not ser:
        log.error("LA66 not found. Plug it in or use --port"); sys.exit(1)
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