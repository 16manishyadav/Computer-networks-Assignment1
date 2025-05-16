import socket
import threading
import random
import sys


class SeedNode:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.peers = set()
        self.peers_file = f"peerslist_{self.port}.txt"
        self.output_file = f"output_{self.port}.txt"

    def start(self):
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen()
        # make a txt file to store the peers
        with open(self.peers_file, "w") as file:
            pass

        print(f"Seed node listening on {self.host}:{self.port}")

        while True:
            client_socket, addr = server_socket.accept()
            threading.Thread(
                target=self.handle_peer_connection, args=(client_socket, addr)
            ).start()

    def handle_peer_connection(self, client_socket, addr):
        # Receive message from peer
        message = client_socket.recv(1024).decode()
        print(f"Received message from peer {addr}: {message}")
        # write the message to output file
        # Extract port number from message
        try:
            if message.startswith("REGISTER"):
                peer_port = int(message.split()[1])
                with open(self.output_file, "a") as file:
                    file.write(f"{message}\n")

                # Add peer port to the peers set
                self.peers.add(peer_port)

                # send a message to the peer of list of peers of seed
                with open(self.peers_file, "r") as file:
                    # send the list of peers to the peer
                    peers = file.read().splitlines()
                    message = "PEERS " + " ".join(peers)
                    print(f"Sending list to peer {addr}: {message}")
                    client_socket.send(message.encode())
                    # Add peer port to the peers file
                with open(self.peers_file, "a") as file:
                    file.write(f"{peer_port}\n")
                print(f"Added peer port {peer_port} to peers list")
            else:
                # print(f"Invalid message format received from peer {addr}")
                _, seed_ip, dead_port, timestamp, sender_ip = message.split(":")
                dead_port = int(dead_port)
                # print(dead_port)
                if dead_port in self.peers:
                    self.peers.remove(dead_port)
                    print(f"Removed dead peer port {dead_port} from peers list")
                    with open(self.output_file, "a") as file:
                        file.write(f"{message}\n")

                    # Convert the set to a sorted list before writing it to the file
                    peer_list = sorted(self.peers)

                    with open(self.peers_file, "w") as file:
                        file.write("\n".join(str(port) for port in peer_list))
                # close the thread of the dead peer
                client_socket.close()

        except (IndexError, ValueError):
            print(f"Invalid message format received from peer {addr}")
            return
        finally:
            client_socket.close()


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script_name.py <seed_port>")
        sys.exit(1)

    try:
        seed_port = int(sys.argv[1])
    except ValueError:
        print("Invalid port number. Please provide a valid integer.")
        sys.exit(1)

    with open("config.txt", "a") as seeds_file:
        seeds_file.write(f"{seed_port}\n")

    seed = SeedNode("127.0.0.1", seed_port)
    threading.Thread(target=seed.start).start()
