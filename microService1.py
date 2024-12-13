
import socket
import json
import time
import uuid
from datetime import datetime

# Configuration for Fluentd
FLUENTD_HOST = '192.168.56.102'  # IP of SEED VM
FLUENTD_PORT = 24226             # Port for Microservice 1

# Function to generate a unique log ID
def generate_log_id():
    return str(uuid.uuid4())

# Function to send data to Fluentd
def send_data(data):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.connect((FLUENTD_HOST, FLUENTD_PORT))
        sock.sendall(json.dumps(data).encode('utf-8'))

# Function to send registration message
def send_registration(node_id, service_name):
    registration = {
        "message_type": "REGISTRATION",
        "node_id": node_id,
        "service_name": service_name,
        "status": "UP",
        "timestamp": datetime.now().isoformat()
    }
    send_data(registration)

# Function to send log data
def send_log(log_level, message, service_name, node_id):
    log = {
        "log_id": generate_log_id(),
        "node_id": node_id,
        "log_level": log_level,
        "message_type": "LOG",
        "message": message,
        "service_name": service_name,
        "timestamp": datetime.now().isoformat()
    }
    send_data(log)

# Heartbeat function to check node status
def send_heartbeat(node_id, status):
    heartbeat = {
        "node_id": node_id,
        "message_type": "HEARTBEAT",
        "status": status,
        "timestamp": datetime.now().isoformat()
    }
    send_data(heartbeat)

# Main function to simulate log generation, registration, and heartbeat
if __name__ == "__main__":
    node_id = "node1"
    service_name = "NodeService1"

    # Send registration messages at startup
    send_registration(node_id, service_name)

    # Simulate regular log generation and heartbeat messages
    while True:
        send_log("INFO", "Node is running smoothly.", service_name, node_id)
        send_log("WARN", "Potential issue detected in service.", service_name, node_id)
        send_log("ERROR", "Error occurred in service.", service_name, node_id)
        send_heartbeat(node_id, "UP")
        time.sleep(5)
