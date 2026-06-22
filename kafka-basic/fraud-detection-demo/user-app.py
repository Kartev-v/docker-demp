from kafka import KafkaConsumer
import json
def user_login_and_listen():
    print("=== Fraud Alert System ===")
    user_id_input = input("Enter your userId to login (No password required): ")
    try:
        user_id = int(user_id_input)
    except ValueError:
        print("Invalid ID. Exiting.")
        return 
    print(f"Logged in successfully as User {user_id}. Listening for real-time alerts...")
 # Initialize Kafka Consumer listening to the notification topic
    consumer = KafkaConsumer(
        'fraud-notification',
        bootstrap_servers=['kafka:9092'], # Adjust to 'kafka:9092' if running inside docker
        auto_offset_reset='latest',
        value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    for message in consumer:
        alert_data = message.value
    # Only show alerts meant for the logged-in user
        if alert_data.get('userId') == user_id:
            print("\n[CRITICAL ALERT] ")
            print(f"User Name: {alert_data.get('name')}")
            print(f"Suspicious Transaction ID: {alert_data.get('tx_id')}")
            print(f"Amount: ${alert_data.get('amount'):.2f}")
            print("Please verify this transaction immediately.\n")
if __name__ == "__main__":
 user_login_and_listen()