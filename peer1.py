import socket
import threading
import random
import sys
import time

class PeerNode:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.seeds_file = "config.txt"
        self.listening_ready = threading.Event()
        self.peers_list = []
        self.seeds_list = []
        self.message_list = {}
        self.liveness_timer = None
        self.liveness_requests_sent = 0
        self.output_file = f"output_{self.port}.txt"
        self.dead_nodes = {}
        self.consecutive_failures = {}

    def start(self):
        threading.Thread(target=self.listen_for_connections).start()
        threading.Thread(target=self.update_peers_file).start()
        threading.Thread(target=self.gossip_message_generation).start()
        threading.Thread(target=self.check_liveness).start()

    def listen_for_connections(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen()

        print(f"Peer_host listening on {self.host}:{self.port}")

        # Signal that listening is ready
        self.listening_ready.set()

        while True:
            client_socket, addr = server_socket.accept()
            threading.Thread(
                target=self.handle_peer_connection, args=(client_socket,)
            ).start()

    def handle_peer_connection(self, client_socket):
        while True:
            message = client_socket.recv(1024).decode()
            if not message:
                break
            elif message.startswith("Gossip Message"):
                self.process_gossip_message(message)
            elif message.startswith("Liveness Request"):
                self.process_liveness_request(message)
            elif message.startswith("Liveness Reply"):
                self.process_liveness_reply(message)
        client_socket.close()

    def update_peers_file(self):
        self.listening_ready.wait()
        self.connect_to_seeds()

    def connect_to_seeds(self):
        with open(self.seeds_file, "r") as seeds_file:
            seed_ports = [line.strip().split(":") for line in seeds_file]
            self.seeds_list = [(ip, int(port)) for ip, port in seed_ports]

        for seed_ip, seed_port in self.seeds_list:
            threading.Thread(target=self.connect_to_seednode, args=(seed_ip, seed_port)).start()

    def connect_to_seednode(self, node_ip, node_port):
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            node_socket.connect((node_ip, node_port))
            message = f"REGISTER:{self.host}:{self.port}"
            node_socket.send(message.encode())
            while True:
                message = node_socket.recv(1024).decode()
                if not message:
                    break
                if message.startswith("PEERS"):
                    with open(self.output_file, "a") as file:
                        file.write(f"{message}\n")
                    peer_list = message.split()[1:]
                    self.peers_list.extend(peer_list)
                    self.peers_list = list(set(self.peers_list))
                    num_to_connect = min(len(self.peers_list), 4)
                    for peer_port in self.peers_list[:num_to_connect]:
                        threading.Thread(
                            target=self.connect_to_peernode, args=(peer_port,)
                        ).start()
        except ConnectionRefusedError:
            print(f"Failed to connect to seednode on {node_ip}:{node_port}")
        finally:
            node_socket.close()

    def connect_to_peernode(self, node_port):
        if node_port == self.port:
            return
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            node_socket.connect((self.host, node_port))
            self.consecutive_failures[node_port] = 0
            print(f"Connected to peernode on port: {node_port}")
        except ConnectionRefusedError:
            print(f"Failed to connect to peernode on port: {node_port}")
        finally:
            node_socket.close()

    def gossip_message_generation(self):
        for _ in range(10):  # Generate 10 messages
            timestamp = int(time.time())
            message = f"Gossip Message:{timestamp}:{self.host}:{self.port}"
            print(f"Generated message: {message}")
            for peer_port in self.peers_list:
                threading.Thread(
                    target=self.send_gossip_message, args=(peer_port, message)
                ).start()
            time.sleep(5)  # Sleep for 5 seconds between messages

    def send_gossip_message(self, peer_port, message):
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            node_socket.connect((self.host, int(peer_port)))
            node_socket.send(message.encode())
            print(f"Sent gossip message to peer on port {peer_port}: {message}")
        except ConnectionRefusedError:
            print(f"Failed to send message to peer on port: {peer_port}")
        finally:
            node_socket.close()

    def process_gossip_message(self, message):
        _, timestamp, _, _ = message.split(":")
        if timestamp not in self.message_list:
            self.message_list[timestamp] = True
            print(f"Received new gossip message: {message}")
            with open(self.output_file, "a") as file:
                file.write(f"{message}\n")
            for peer_port in self.peers_list:
                threading.Thread(
                    target=self.send_gossip_message, args=(peer_port, message)
                ).start()

    def check_liveness(self):
        while True:
            self.liveness_timer = threading.Timer(13.0, self.send_liveness_request)
            self.liveness_timer.start()
            time.sleep(15)  # Check liveness every 15 seconds

    def send_liveness_request(self):
        for peer_port in self.peers_list:
            if peer_port not in self.dead_nodes:
                message = f"Liveness Request:{int(time.time())}:{self.port}"
                threading.Thread(
                    target=self.send_liveness_message, args=(peer_port, message)
                ).start()

    def send_liveness_message(self, peer_port, message):
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            node_socket.connect((self.host, int(peer_port)))
            node_socket.send(message.encode())
            print(f"Sent liveness request to peer on port {peer_port}: {message}")
        except ConnectionRefusedError:
            print(f"Failed to send liveness request to peer on port: {peer_port}")
            self.consecutive_failures[peer_port] += 1
            if self.consecutive_failures[peer_port] >= 3:
                self.notify_seed_dead_node(peer_port)
                node_socket.close()
        finally:
            node_socket.close()

    def send_liveness_message_reply(self, peer_port, message):
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            node_socket.connect((self.host, int(peer_port)))
            node_socket.send(message.encode())
            print(f"Sent liveness reply to peer on port {peer_port}: {message}")
        except ConnectionRefusedError:
            print(f"Failed to send liveness reply to peer on port: {peer_port}")
        finally:
            node_socket.close()

    def process_liveness_reply(self, request):
        print(f"Received liveness request: {request}")
        _, sender_timestamp, sender_port = request.split(":")
        reply = f"Liveness Reply:{int(time.time())}:{self.port}"
        threading.Thread(
            target=self.send_liveness_message_reply, args=(int(sender_port), reply)
        ).start()

    def notify_seed_dead_node(self, peer_port):
        if peer_port not in self.dead_nodes:
            self.dead_nodes[peer_port] = True
            for seed_ip, seed_port in self.seeds_list:
                message = f"Dead Node:{self.host}:{peer_port}:{int(time.time())}:{self.host}"
                threading.Thread(
                    target=self.send_dead_node_message, args=(seed_ip, seed_port, message)
                ).start()
        else:
            return

    def send_dead_node_message(self, node_ip, node_port, message):
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            node_socket.connect((node_ip, node_port))
            node_socket.send(message.encode())
            with open(self.output_file, "a") as file:
                file.write(f"{message}\n")
            print(f"Sent dead node message to seed on {node_ip}:{node_port}: {message}")
        except ConnectionRefusedError:
            print(f"Failed to send dead node message to seed on {node_ip}:{node_port}")
        finally:
            node_socket.close()

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python PeerNode.py <peer_ip> <random_port>")
        sys.exit(1)

    peer_ip = sys.argv[1]
    try:
        random_port = int(sys.argv[2])
    except ValueError:
        print("Invalid port number. Please provide a valid integer.")
        sys.exit(1)

    peer = PeerNode(peer_ip, random_port)
    threading.Thread(target=peer.start).start()
