from kafka import KafkaConsumer
from elasticsearch import Elasticsearch
import json
import time
from datetime import datetime, timedelta

node_heartbeats = {}
consumer = KafkaConsumer(
    'logs',
    bootstrap_servers='192.168.56.101:9092',
    group_id='log_consumer_group-1',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=1000  
)

# Elasticsearch connection setup
client = Elasticsearch("http://localhost:9200")

if not client.ping():
    raise ValueError("Connection to Elasticsearch failed")
# Index names for logs in Elasticsearch
log_index = "log_data"
node_status_index = "nodes_status"

# Ensure indices exist or create if missing
for index in [log_index, node_status_index]:
    if not client.indices.exists(index=index):
        client.indices.create(index=index)

def store_log_in_elasticsearch(log_data, index_name):
    """Store log data in specified Elasticsearch index"""
    try:
        response = client.index(index=index_name, document=log_data)
        print(f"Status update stored in Elasticsearch: {log_data}")
        return response

    except Exception as e:
        print(f"Error storing in Elasticsearch: {e}")
        return None

def check_node_status():
    """Check for nodes that haven't sent heartbeats in last 10 seconds"""
    current_time = datetime.now()

    for node_id, last_heartbeat in list(node_heartbeats.items()):
        time_diff = current_time - last_heartbeat['timestamp']
        if time_diff.total_seconds() > 10 and last_heartbeat['status'] == 'UP':
            # Create down status log
            down_status = {
                "message_type": "STATUS_CHANGE",
                "node_id": node_id,
                "service_name": last_heartbeat['service_name'],
                "status": "DOWN",
                "timestamp": current_time.isoformat()
            }
            # Store the DOWN status in Elasticsearch
            store_result = store_log_in_elasticsearch(down_status, node_status_index)
            if store_result:
                # Update stored status only if successfully stored in Elasticsearch
                node_heartbeats[node_id]['status'] = 'DOWN'
                print(f"Node {node_id} marked as DOWN due to missing heartbeat")
last_check_time = time.time()

try:
    while True:
        try:
            # Poll for messages with timeout
            message = next(consumer, None)
            current_time = time.time()

            if message:
                log_data = message.value
                # Extract message details if wrapped in 'msg'
                if 'msg' in log_data:
                    log_data = log_data['msg']

                # Handle regular logs
                store_log_in_elasticsearch(log_data, log_index)
                print(f"Log stored: {log_data}")

                # Handle registration and heartbeat messages
                if log_data.get('message_type') == 'REGISTRATION':
                    node_id = log_data['node_id']
                    registration_status = {
                        "message_type": "STATUS_CHANGE",
                        "node_id": node_id,
                        "service_name": log_data['service_name'],
                        "status": "UP",
                        "timestamp": datetime.now().isoformat()
                    }
                    store_log_in_elasticsearch(registration_status, node_status_index)
                    
                    node_heartbeats[node_id] = {
                        'timestamp': datetime.now(),
                        'service_name': log_data['service_name'],
                        'status': 'UP'
                    }
                    print(f"Node {node_id} registered")
                    
                elif log_data.get('message_type') == 'HEARTBEAT':
                    node_id = log_data['node_id']
                    if node_id in node_heartbeats:
                        node_heartbeats[node_id]['timestamp'] = datetime.now()
                        # If node was DOWN, mark it as UP again
                        if node_heartbeats[node_id]['status'] == 'DOWN':
                            up_status = {
                                "message_type": "STATUS_CHANGE",
                                "node_id": node_id,
                                "service_name": node_heartbeats[node_id]['service_name'],
                                "status": "UP",
                                "timestamp": datetime.now().isoformat()
                            }
                            store_result = store_log_in_elasticsearch(up_status, node_status_index)
                            if store_result:
                                node_heartbeats[node_id]['status'] = 'UP'
                                print(f"Node {node_id} is back UP")

            # Check node status every second, regardless of message receipt
            if current_time - last_check_time >= 1:
                check_node_status()
                last_check_time = current_time
        except StopIteration:
            # Check node status when no messages are available
            if time.time() - last_check_time >= 1:
                check_node_status()
                last_check_time = time.time()
            time.sleep(0.1) 

except KeyboardInterrupt:
    print("Shutting down consumer...")
    # Final status check before shutting down
    check_node_status()
    consumer.close()
