#!/usr/bin/env python3
"""
lora_ssh_bridge.py - SSH over LoRa via Dragino LA66 P2P
Jetson:  python3 lora_ssh_bridge.py jetson
Windows: python lora_ssh_bridge.py win
Then:    ssh -p 2222 user@127.0.0.1
Requires: pip install pyserial
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
FRAG_MAX    = 220
QUIET_TIME  = 0.8   # seconds of radio silence before we're allowed to transmit
IDLE_POLL   = 0.01  # main loop sleep

# network
LISTEN_PORT = 2222
SSH_HOST    = "127.0.0.1"
SSH_PORT    = 22

logging.basicConfig(level=logging.DEBUG, format="[%(asctime)s %(levelname)s] %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("bridge")

# serial helpers

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
        "AT+RXMOD=65535,0",  # always listening, no auto-ACK
    ]:
        resp = at_cmd(ser, cmd)
        if "ERROR" in resp:
            log.error(f"Config failed: {cmd} -> {resp}")
            return False
        time.sleep(0.05)
    log.info(f"LA66 ready: {LORA_FREQ}MHz SF{LORA_SF} {LORA_POWER}dBm")
    return True

# send fragments, wait for txDone on each

tx_seq = 0

def lora_send(ser, data):
    global tx_seq
    chunks = [data[i:i + FRAG_MAX] for i in range(0, len(data), FRAG_MAX)]
    total = len(chunks)
    seq = tx_seq & 0xFF
    tx_seq += 1
    log.debug(f"TX: {len(data)} bytes in {total} frags, seq={seq}")

    for idx, chunk in enumerate(chunks):
        frame = bytes([seq, idx, total]) + chunk
        hexstr = frame.hex().upper()
        ser.reset_input_buffer()
        ser.write(f"AT+SEND=0,{hexstr},0,0\r\n".encode())

        deadline = time.time() + 5.0
        while time.time() < deadline:
            if ser.in_waiting:
                line = ser.readline().decode(errors="ignore").strip()
                if "txDone" in line:
                    break
                if "ERROR" in line:
                    log.error(f"TX error: {line}")
                    break
            time.sleep(0.01)

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
            log.debug(f"Reassembled {len(result)} bytes")
            self.reset()
            return result
        return None

def parse_rx_line(line):
    line = line.strip()
    if not line:
        return None
    if "HEX" in line.upper():
        after = line.split(")", 1)[-1].strip() if ")" in line else line.split(":", 2)[-1].strip()
        try:
            raw = bytes.fromhex(after.replace(" ", ""))
            if len(raw) > 1:
                return raw[1:]
        except ValueError:
            pass
    for sep in [",", "="]:
        if sep in line:
            candidate = line.rsplit(sep, 1)[1].strip()
            try:
                raw = bytes.fromhex(candidate)
                if len(raw) > 1:
                    return raw[1:]
            except ValueError:
                pass
    return None

# drain serial and return any reassembled payloads + whether we saw radio activity

def drain_serial(ser, reasm):
    results = []
    saw_activity = False
    while ser.in_waiting:
        line = ser.readline().decode(errors="ignore").strip()
        if not line:
            continue
        # any rxDone or Data line means the other side is transmitting
        if "rxDone" in line or "Data:" in line:
            saw_activity = True
        log.debug(f"RX serial: {line}")
        raw = parse_rx_line(line)
        if raw:
            assembled = reasm.feed(raw)
            if assembled:
                results.append(assembled)
    return results, saw_activity

# main bridge loop with turn-taking

def bridge_loop(ser, tcp_sock, is_server_side):
    reasm = Reassembler()
    ssh_sock = tcp_sock if not is_server_side else None
    tx_buf = b""
    last_rx_time = 0  # last time we saw radio activity from the other side

    while True:
        # 1) drain serial, deliver any complete messages to TCP
        rx_data, saw_activity = drain_serial(ser, reasm)
        if saw_activity:
            last_rx_time = time.time()

        for data in rx_data:
            if is_server_side and ssh_sock is None:
                log.info("Connecting to SSH...")
                try:
                    ssh_sock = socket.create_connection((SSH_HOST, SSH_PORT))
                    ssh_sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                    ssh_sock.setblocking(False)
                    log.info("SSH connected")
                except OSError as e:
                    log.error(f"SSH connect failed: {e}"); continue
            if ssh_sock:
                try:
                    ssh_sock.sendall(data)
                    log.debug(f"LoRa -> TCP: {len(data)} bytes")
                except OSError:
                    if is_server_side:
                        ssh_sock.close(); ssh_sock = None
                    else:
                        return

        # 2) read TCP into buffer
        if ssh_sock:
            try:
                ready, _, _ = select.select([ssh_sock], [], [], 0)
                if ready:
                    data = ssh_sock.recv(4096)
                    if data:
                        tx_buf += data
                    else:
                        log.info("TCP closed")
                        if is_server_side:
                            ssh_sock.close(); ssh_sock = None
                        else:
                            return
            except OSError:
                if is_server_side:
                    ssh_sock.close(); ssh_sock = None
                else:
                    return

        # 3) only transmit if we have data AND radio has been quiet
        #    this prevents us from transmitting while the other side is sending
        if tx_buf and (time.time() - last_rx_time) > QUIET_TIME:
            # grab any extra TCP data that arrived
            if ssh_sock:
                time.sleep(0.03)
                try:
                    while True:
                        ready, _, _ = select.select([ssh_sock], [], [], 0)
                        if not ready:
                            break
                        more = ssh_sock.recv(4096)
                        if more:
                            tx_buf += more
                        else:
                            break
                except OSError:
                    pass

            log.debug(f"TCP -> LoRa: {len(tx_buf)} bytes")
            lora_send(ser, tx_buf)
            tx_buf = b""
            last_rx_time = time.time()  # treat our own TX as activity too
        else:
            time.sleep(IDLE_POLL)

def run_jetson(ser):
    log.info("=== JETSON MODE === waiting for LoRa data...")
    bridge_loop(ser, None, is_server_side=True)

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
    try:
        bridge_loop(ser, client, is_server_side=False)
    finally:
        client.close(); server.close()

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