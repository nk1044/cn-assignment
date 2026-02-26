import subprocess
import socket
import yaml
import time
import os

CONFIG_FILE = "config.yml"


class NodeManager:
    def __init__(self):
        with open(CONFIG_FILE, "r") as f:
            self.config = yaml.safe_load(f)

        self.seed_processes = []
        self.peer_processes = {}
        self.used_ports = set()

        self.peer_port_start = self.config["peer_port_range"]["start"]
        self.peer_port_end = self.config["peer_port_range"]["end"]

    # -----------------------------
    # PORT MANAGEMENT
    # -----------------------------
    def is_port_free(self, port):
        """Check if port is available"""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            return s.connect_ex(("127.0.0.1", port)) != 0

    def get_free_peer_port(self):
        """Find next free port in range"""
        for port in range(self.peer_port_start, self.peer_port_end + 1):
            if port in self.used_ports:
                continue
            if self.is_port_free(port):
                self.used_ports.add(port)
                return port
        raise RuntimeError("No free peer ports available")

    # -----------------------------
    # SEED MANAGEMENT
    # -----------------------------
    def spawn_seeds(self):
        for seed in self.config["seeds"]:
            ip = seed["ip"]
            port = seed["port"]

            proc = subprocess.Popen(
                ["python", "seed.py", f"{ip}:{port}"],
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )

            self.seed_processes.append(proc)
            time.sleep(0.5)

    # -----------------------------
    # PEER MANAGEMENT
    # -----------------------------
    def spawn_peer(self):
        port = self.get_free_peer_port()
        ip = "127.0.0.1"

        proc = subprocess.Popen(
            ["python", "peer.py", f"{ip}:{port}"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )

        self.peer_processes[port] = proc
        return port

    def spawn_initial_peers(self):
        count = self.config.get("initial_peers", 3)

        for _ in range(count):
            self.spawn_peer()
            time.sleep(1)

    def kill_peer(self, port):
        if port not in self.peer_processes:
            return

        proc = self.peer_processes[port]
        proc.terminate()
        proc.wait()

        del self.peer_processes[port]
        self.used_ports.discard(port)

    # -----------------------------
    # CLI LOOP
    # -----------------------------
    def interactive_loop(self):
        print(
            """
P2P NETWORK MANAGER
Commands:
  add
  kill <port>
  list
  exit
"""
        )

        while True:
            cmd = input("manager> ").strip()

            if cmd == "add":
                self.spawn_peer()

            elif cmd.startswith("kill"):
                try:
                    port = int(cmd.split()[1])
                    self.kill_peer(port)
                except Exception:
                    pass

            elif cmd == "list":
                print("Active peers:", list(self.peer_processes.keys()))

            elif cmd == "exit":
                self.shutdown()
                break
            elif cmd == "del":
                self.shutdown()
                os.remove("seed.log")
                os.remove("peer.log")
                os.remove("outputfile.txt")
                os.remove("config.txt")
                break


    # -----------------------------
    # CLEANUP
    # -----------------------------
    def shutdown(self):
        for p in self.peer_processes.values():
            p.terminate()

        for p in self.seed_processes:
            p.terminate()


# ==============================
# MAIN
# ==============================
if __name__ == "__main__":
    if os.path.exists("seed.log"):
        os.remove("seed.log")
    if os.path.exists("peer.log"):
        os.remove("peer.log")
    if os.path.exists("outputfile.txt"):
        os.remove("outputfile.txt")
    if os.path.exists("config.txt"):
        os.remove("config.txt")

    manager = NodeManager()

    manager.spawn_seeds()
    time.sleep(2)

    manager.spawn_initial_peers()
    time.sleep(2)

    manager.interactive_loop()