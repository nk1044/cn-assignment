import random
import matplotlib.pyplot as plt

class PeerNetwork:
    def __init__(self):
        self.peers = []  # List of peers
        self.degrees = {}  # Dictionary to track degrees of peers

    def add_peer(self):
        """Add a new peer to the network."""
        new_peer = f"Peer{len(self.peers) + 1}"  # Assign a unique ID to the new peer
        self.peers.append(new_peer)
        self.degrees[new_peer] = 0  # Initialize degree to 0

        if len(self.peers) > 1:
            # Select up to 2 peers to connect to using power-law distribution
            selected_peers = self.select_peers_power_law(self.peers[:-1], alpha=2.0)
            for peer in selected_peers:
                self.degrees[peer] += 1  # Increment degree of selected peers
                self.degrees[new_peer] += 1  # Increment degree of the new peer

    def select_peers_power_law(self, peers, alpha=2.0):
        """Select peers using power-law distribution."""
        if not peers:
            return []

        degrees = [self.degrees[peer] + 1 for peer in peers]  # Degrees of existing peers
        probabilities = [d ** -alpha for d in degrees]  # Power-law probabilities
        total = sum(probabilities)
        probabilities = [p / total for p in probabilities]  # Normalize probabilities

        num_connections = min(1, len(peers))  # Connect to at most 1 peer
        selected_indices = random.choices(range(len(peers)), weights=probabilities, k=num_connections)
        return [peers[i] for i in selected_indices]

    def simulate(self, num_peers):
        """Simulate the network growth."""
        for _ in range(num_peers):
            self.add_peer()

    def plot_degree_distribution(self):
        """Plot the degree distribution."""
        degree_counts = {}
        for degree in self.degrees.values():
            degree_counts[degree] = degree_counts.get(degree, 0) + 1

        degrees = sorted(degree_counts.keys())
        counts = [degree_counts[d] for d in degrees]

        plt.bar(degrees, counts, color='blue')
        plt.xlabel('Degree (Number of Connections)')
        plt.ylabel('Number of Peers')
        plt.title('Degree Distribution in P2P Network')
        plt.xticks(degrees)
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.show()

# Run the simulation
network = PeerNetwork()
network.simulate(num_peers=1000)
network.plot_degree_distribution()