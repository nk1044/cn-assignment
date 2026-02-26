import socket
import threading
import json
import time
import sys
import math

CONFIG_FILE = "config.txt"  # seed list
OUTPUT_FILE = "outputfile.txt"


class SeedNode:
    def __init__(self, ip, port):
        self.ip = ip
        self.port = port

        self.peer_list = set()
        self.lock = threading.Lock()

        # consensus tracking
        self.register_votes = {}  # (ip,port) -> set(seed_ids)
        self.dead_votes = {}      # (ip,port) -> set(seed_ids)

        self.seed_nodes = self.read_seed_nodes()
        self.quorum = math.floor(len(self.seed_nodes) / 2) + 1

    # -------------------------------------------------
    # LOGGING
    # -------------------------------------------------
    def log_message(self, message):
        ts = time.strftime("[%Y-%m-%d %H:%M:%S]")
        log_entry = f"{ts} [SEED {self.ip}:{self.port}] {message}"
        print(log_entry)

        with open("seed.log", "a") as f:
            f.write(log_entry + "\n")

        with open(OUTPUT_FILE, "a") as f:
            f.write(log_entry + "\n")

    # -------------------------------------------------
    # CONFIG
    # -------------------------------------------------
    def read_seed_nodes(self):
        seeds = []
        try:
            with open(CONFIG_FILE, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        ip, port = line.split(":")
                        seeds.append((ip, int(port)))
        except Exception as e:
            print("Error reading config:", e)
        return seeds

    def other_seeds(self):
        return [(ip, p) for ip, p in self.seed_nodes if not (ip == self.ip and p == self.port)]

    # -------------------------------------------------
    # NETWORK
    # -------------------------------------------------
    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.bind((self.ip, self.port))
        server.listen(50)

        self.log_message(f"Seed started. quorum={self.quorum}")

        while True:
            client, addr = server.accept()
            threading.Thread(target=self.handle_client, args=(client,), daemon=True).start()

    # -------------------------------------------------
    # CONSENSUS BROADCAST
    # -------------------------------------------------
    def broadcast_to_seeds(self, payload):
        for ip, port in self.other_seeds():
            try:
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(2)
                s.connect((ip, port))
                s.send(json.dumps(payload).encode())
                s.close()
            except Exception:
                pass

    # -------------------------------------------------
    # HANDLE CLIENT
    # -------------------------------------------------
    def handle_client(self, sock):
        try:
            data = sock.recv(4096).decode()
            if not data:
                sock.close()
                return

            req = json.loads(data)
            rtype = req.get("type")

            # =============================
            # PEER REGISTRATION REQUEST
            # =============================
            if rtype == "register":
                peer = (req["ip"], req["port"])
                self.handle_register_request(peer)
                sock.send(json.dumps({"status": "pending"}).encode())

            # =============================
            # SEED VOTE (REGISTER)
            # =============================
            elif rtype == "register_vote":
                peer = tuple(req["peer"])
                voter = tuple(req["voter"])
                self.handle_register_vote(peer, voter)

            # =============================
            # GET PEERS
            # =============================
            elif rtype == "get_peers":
                with self.lock:
                    peers = list(self.peer_list)
                sock.send(json.dumps({"peers": peers}).encode())

            # =============================
            # DEAD NODE REPORT
            # =============================
            elif rtype == "dead_node":
                peer = (req["dead_ip"], req["dead_port"])
                self.handle_dead_report(peer)
                sock.send(json.dumps({"status": "received"}).encode())

            # =============================
            # DEAD VOTE FROM SEED
            # =============================
            elif rtype == "dead_vote":
                peer = tuple(req["peer"])
                voter = tuple(req["voter"])
                self.handle_dead_vote(peer, voter)

        except Exception as e:
            self.log_message(f"Error: {e}")
        finally:
            sock.close()

    # -------------------------------------------------
    # REGISTRATION CONSENSUS
    # -------------------------------------------------
    def handle_register_request(self, peer):
        self.log_message(f"Registration proposal for {peer}")

        with self.lock:
            if peer not in self.register_votes:
                self.register_votes[peer] = set()

            self.register_votes[peer].add((self.ip, self.port))

        # broadcast vote
        self.broadcast_to_seeds({
            "type": "register_vote",
            "peer": peer,
            "voter": (self.ip, self.port)
        })

        self.check_register_quorum(peer)

    def handle_register_vote(self, peer, voter):
        with self.lock:
            if peer not in self.register_votes:
                self.register_votes[peer] = set()
            self.register_votes[peer].add(voter)

        self.check_register_quorum(peer)

    def check_register_quorum(self, peer):
        with self.lock:
            votes = len(self.register_votes.get(peer, []))

            if votes >= self.quorum and peer not in self.peer_list:
                self.peer_list.add(peer)
                self.log_message(f"Peer REGISTERED via consensus: {peer}")

    # -------------------------------------------------
    # DEAD NODE CONSENSUS
    # -------------------------------------------------
    def handle_dead_report(self, peer):
        self.log_message(f"Dead node proposal for {peer}")

        with self.lock:
            if peer not in self.dead_votes:
                self.dead_votes[peer] = set()

            self.dead_votes[peer].add((self.ip, self.port))

        self.broadcast_to_seeds({
            "type": "dead_vote",
            "peer": peer,
            "voter": (self.ip, self.port)
        })

        self.check_dead_quorum(peer)

    def handle_dead_vote(self, peer, voter):
        with self.lock:
            if peer not in self.dead_votes:
                self.dead_votes[peer] = set()
            self.dead_votes[peer].add(voter)

        self.check_dead_quorum(peer)

    def check_dead_quorum(self, peer):
        with self.lock:
            votes = len(self.dead_votes.get(peer, []))

            if votes >= self.quorum and peer in self.peer_list:
                self.peer_list.remove(peer)
                self.log_message(f"Peer REMOVED via consensus: {peer}")


# -------------------------------------------------
# MAIN
# -------------------------------------------------
if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python seed.py <IP:PORT>")
        sys.exit(1)

    ip, port = sys.argv[1].split(":")
    port = int(port)

    # ensure seed is in config
    with open(CONFIG_FILE, "a+") as f:
        f.seek(0)
        lines = f.read()
        entry = f"{ip}:{port}"
        if entry not in lines:
            f.write(entry + "\n")

    SeedNode(ip, port).start()