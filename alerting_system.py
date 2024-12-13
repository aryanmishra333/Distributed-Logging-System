from kafka import KafkaConsumer
import json
from collections import defaultdict
from datetime import datetime, timedelta
import time
import threading

consumer = KafkaConsumer(
    'logs',
    bootstrap_servers='192.168.56.101:9092',
    group_id='alerting_consumer_group',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')),
    consumer_timeout_ms=1000 
)

heartbeat_status = defaultdict(lambda: {'last_heartbeat': datetime.now() - timedelta(minutes=5), 'status': 'UP'})
down_nodes = set()  
HEARTBEAT_THRESHOLD = 10


def check_alerts():
    current_time = datetime.now()
    no_active_nodes = True

    for node_id, status in list(heartbeat_status.items()):
        time_since_last_heartbeat = (current_time - status['last_heartbeat']).total_seconds()

        if time_since_last_heartbeat > HEARTBEAT_THRESHOLD:
            if status['status'] != 'DOWN':
                print(f"ALERT: Missed heartbeat for node '{node_id}'. Service may be down or unresponsive. Take action quickly!")
                status['status'] = 'DOWN'
                down_nodes.add(node_id)  

        else:
            if status['status'] == 'DOWN':
                print(f"INFO: Node '{node_id}' is back online.")
                status['status'] = 'UP'
                if node_id in down_nodes:
                    down_nodes.remove(node_id)
        if status['status'] == 'UP':
            no_active_nodes = False

    if no_active_nodes and heartbeat_status:
        print("ALERT: No registered nodes are sending heartbeats. All services may be down. Take action quickly!")

    for node_id in down_nodes:
        print(f"ALERT: Node '{node_id}' is still down. Service remains unresponsive. Take action quickly!")


def consumer_thread():
    check_time = time.time()

    while True:
        try:
            message = next(consumer, None)
            current_time = time.time()

            if message:
                log_data = message.value
                if 'msg' in log_data:
                    log_data = log_data['msg']

                message_type = log_data.get('message_type', None)
                node_id = log_data.get('node_id', None)
                log_level = log_data.get('log_level', None)
                message = log_data.get('message', None)
                service_name = log_data.get('service_name', None)

                if message_type == 'REGISTRATION':
                    heartbeat_status[node_id]['last_heartbeat'] = datetime.now()
                    heartbeat_status[node_id]['status'] = 'UP'
                    if node_id in down_nodes:
                        down_nodes.remove(node_id) 
                    print(f"Node {node_id} registered")

                elif message_type == 'HEARTBEAT':
                    heartbeat_status[node_id]['last_heartbeat'] = datetime.now()
                    heartbeat_status[node_id]['status'] = 'UP'
                    if node_id in down_nodes:
                        down_nodes.remove(node_id)  

                elif log_level in ['ERROR', 'WARN']:
                    print(f"ALERT: {log_level} message received from node '{node_id}' (service: {service_name}): {message}. Take action quickly!")
            if current_time - check_time >= 1:
                check_alerts()
                check_time = current_time

        except StopIteration:
            if time.time() - check_time >= 1:
                check_alerts()
                check_time = time.time()
            time.sleep(0.1)

        except KeyboardInterrupt:
            print("Shutting down consumer...")
            break


consumer_thread_instance = threading.Thread(target=consumer_thread)
consumer_thread_instance.start()

try:
    consumer_thread_instance.join()
except KeyboardInterrupt:
    print("Stopping alerting system...")

