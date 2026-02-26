import socket
import threading
import json
import time
import logging
import random
import math
import sys
import signal

CONFIG_FILE = "./config.txt"
OUTPUT_FILE = "outputfile.txt"

logging.basicConfig(
    filename="peer.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - [%(peer_ip)s:%(peer_port)s] - %(message)s",
)


class PeerNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

        self.seed_nodes = self.read_seed_nodes()
        self.registered_seeds = set()

        self.peers = set()
        self.lock = threading.Lock()
        self.running = True

        # gossip
        self.message_list = set()
        self.message_count = 0

        # liveness consensus
        self.suspicions = {}       # peer -> suspicion count
        self.suspicion_votes = {}  # peer -> set(voters)

    # =================================================
    # LOGGING
    # =================================================
    def log_message(self, message):
        ts = time.strftime("[%Y-%m-%d %H:%M:%S]")
        log_entry = f"{ts} [{self.ip}:{self.port}] {message}"
        print(log_entry)

        with open(OUTPUT_FILE, "a") as f:
            f.write(log_entry + "\n")

        logging.info(log_entry, extra={"peer_ip": self.ip, "peer_port": self.port})

    # =================================================
    # CONFIG
    # =================================================
    def read_seed_nodes(self):
        seeds = []
        try:
            with open(CONFIG_FILE, "r") as f:
                for line in f:
                    if line.strip():
                        ip, port = line.strip().split(":")
                        seeds.append((ip, int(port)))
        except Exception as e:
            print("Error reading config:", e)
        return seeds

    def quorum_neighbors(self):
        with self.lock:
            n = len(self.peers)
        return math.floor(n / 2) + 1 if n > 0 else 1

    # =================================================
    # SEED REGISTRATION
    # =================================================
    def select_seed_nodes(self):
        n = len(self.seed_nodes)
        k = math.floor(n / 2) + 1
        return random.sample(self.seed_nodes, k)

    def register_with_seed(self):
        selected = self.select_seed_nodes()

        for ip, port in selected:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((ip, port))

                s.send(json.dumps({
                    "type": "register",
                    "ip": self.ip,
                    "port": self.port
                }).encode())

                s.close()
                self.registered_seeds.add((ip, port))
                self.log_message(f"Registration sent to seed {ip}:{port}")

            except Exception as e:
                self.log_message(f"Seed register failed {ip}:{port}: {e}")

    # =================================================
    # FETCH PEERS
    # =================================================
    def fetch_peers(self):
        all_peers = []

        for ip, port in self.registered_seeds:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((ip, port))
                s.send(json.dumps({"type": "get_peers"}).encode())

                resp = json.loads(s.recv(4096).decode())
                s.close()

                all_peers.extend(resp.get("peers", []))

            except Exception:
                pass

        unique = list({(p[0], p[1]) for p in all_peers})

        selected = self.select_peers_power_law(unique)
        self.connect_to_peers(selected)

        self.log_message(f"Peer list obtained: {selected}")

    # =================================================
    # POWER LAW SELECTION
    # =================================================
    def select_peers_power_law(self, peers, alpha=2.0):
        if not peers:
            return []

        degrees = [i + 1 for i in range(len(peers))]
        probs = [d ** -alpha for d in degrees]
        total = sum(probs)
        probs = [p / total for p in probs]

        k = min(3, len(peers))  # allow multiple neighbors
        idx = random.choices(range(len(peers)), weights=probs, k=k)
        return [peers[i] for i in idx]

    # =================================================
    # CONNECT TO PEERS
    # =================================================
    def connect_to_peers(self, peers):
        for ip, port in peers:
            if (ip, port) == (self.ip, self.port):
                continue

            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((ip, port))
                s.send(json.dumps({
                    "type": "peer_info",
                    "ip": self.ip,
                    "port": self.port
                }).encode())
                s.close()

                with self.lock:
                    self.peers.add((ip, port))

            except Exception:
                pass

    # =================================================
    # LISTENER
    # =================================================
    def start_listener(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.ip, self.port))
        server.listen(50)

        self.log_message("Peer listener started")

        while self.running:
            try:
                client, addr = server.accept()
                threading.Thread(
                    target=self.handle_peer,
                    args=(client, addr),
                    daemon=True
                ).start()
            except Exception:
                pass

    # =================================================
    # HANDLE PEER MSG
    # =================================================
    def handle_peer(self, sock, addr):
        try:
            data = json.loads(sock.recv(4096).decode())
            t = data.get("type")

            if t == "peer_info":
                with self.lock:
                    self.peers.add((data["ip"], data["port"]))

            elif t == "gossip":
                msg = data["message"]
                h = hash(msg)

                if h not in self.message_list:
                    self.message_list.add(h)
                    self.log_message(f"Gossip received: {msg}")
                    self.broadcast_message(msg, exclude=addr)

            elif t == "ping":
                sock.send(json.dumps({"type": "pong"}).encode())
                self.log_message(f"Ping received from {addr}")

            elif t == "suspicion_vote":
                peer = tuple(data["suspect"])
                voter = tuple(data["voter"])
                self.handle_suspicion_vote(peer, voter)

        except Exception:
            pass
        finally:
            sock.close()

    # =================================================
    # GOSSIP
    # =================================================
    def broadcast_message(self, message, exclude=None):
        with self.lock:
            peers_copy = list(self.peers)

        for ip, port in peers_copy:
            if exclude and (ip, port) == exclude:
                continue
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(5)
                s.connect((ip, port))
                s.send(json.dumps({
                    "type": "gossip",
                    "message": message
                }).encode())
                s.close()
            except Exception:
                pass

    # =================================================
    # LIVENESS CHECK
    # =================================================
    def ping_peers(self):
        while self.running:
            time.sleep(3)

            with self.lock:
                peers_copy = list(self.peers)

            for ip, port in peers_copy:
                try:
                    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                    s.settimeout(3)
                    s.connect((ip, port))
                    s.send(json.dumps({"type": "ping"}).encode())
                    s.recv(1024)
                    s.close()

                    self.suspicions[(ip, port)] = 0

                except Exception:
                    self.start_suspicion((ip, port))

    # =================================================
    # PEER CONSENSUS (CRITICAL)
    # =================================================
    def start_suspicion(self, peer):
        self.suspicions[peer] = self.suspicions.get(peer, 0) + 1

        if self.suspicions[peer] == 2:  # suspicion threshold
            self.broadcast_suspicion(peer)

    def broadcast_suspicion(self, peer):
        payload = {
            "type": "suspicion_vote",
            "suspect": peer,
            "voter": (self.ip, self.port)
        }

        with self.lock:
            peers_copy = list(self.peers)

        for ip, port in peers_copy:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((ip, port))
                s.send(json.dumps(payload).encode())
                s.close()
            except Exception:
                pass

    def handle_suspicion_vote(self, peer, voter):
        if peer not in self.suspicion_votes:
            self.suspicion_votes[peer] = set()

        self.suspicion_votes[peer].add(voter)

        if len(self.suspicion_votes[peer]) >= self.quorum_neighbors():
            self.report_dead_node(peer)

    # =================================================
    # REPORT TO SEEDS
    # =================================================
    def report_dead_node(self, peer):
        ip, port = peer
        self.log_message(f"Consensus reached: reporting dead {peer}")

        payload = {
            "type": "dead_node",
            "dead_ip": ip,
            "dead_port": port,
            "reporter_ip": self.ip,
            "reporter_port": self.port
        }

        for sip, sport in self.registered_seeds:
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.connect((sip, sport))
                s.send(json.dumps(payload).encode())
                s.close()
            except Exception:
                pass

    # =================================================
    # START
    # =================================================
    def start(self):
        threading.Thread(target=self.start_listener, daemon=True).start()

        time.sleep(1)
        self.register_with_seed()

        time.sleep(2)
        self.fetch_peers()

        threading.Thread(target=self.ping_peers, daemon=True).start()

        while self.running and self.message_count < 10:
            msg = f"{time.time()}:{self.ip}:{self.message_count}"
            self.broadcast_message(msg)
            self.message_count += 1
            time.sleep(5)

        while self.running:
            time.sleep(1)


# =================================================
# MAIN
# =================================================
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python peer.py <IP:PORT>")
        sys.exit(1)

    ip, port = sys.argv[1].split(":")
    port = int(port)

    peer = PeerNode(ip, port)

    signal.signal(signal.SIGINT, lambda *_: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))

    peer.start()