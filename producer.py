import json
import requests
from confluent_kafka import Producer
import time

def send_data_to_kafka(rest_api_endpoint, bootstrap_servers):

    # Define the categories and their corresponding Kafka topics
    stock_categories = {
        'financial': ['HDFCBANK', 'ICICIBANK', 'AXISBANK', 'SBI', 'KOTAKBANK', 'SBIN', 'NIFTY 50', 'LTIM', 'BAJFINANCE', 'SBILIFE', 'HDFCLIFE'],
        'oil_and_gas': ['ONGC', 'BPCL', 'RELIANCE'],
        'healthcare': ['DRREDDY', 'APOLLOHOSP', 'SUNPHARMA', 'CIPLA'],
        
        'fmcg': ['BRITANNIA', 'HUL', 'NESTLEIND', 'ITC']
    }

    # Create a Kafka producer configuration
    producer_conf = {
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'python-producer'
    }

    # Create a Kafka producer instance
    producer = Producer(producer_conf)

    count = 0

    # Iterate through each category and send data to the corresponding topic
    for category, stocks in stock_categories.items():
        # Make a GET request to the REST API
        response = requests.get(rest_api_endpoint)

        # Check if the request was successful (HTTP status code 200)
        if response.status_code == 200:
            # Retrieve the data from the response
            api_data = response.json()['body']
            #print(api_data)

            print(f"\n{category.capitalize()}:")
            for entry in api_data:
                if isinstance(entry, dict) and "symbol" in entry and entry["symbol"] in stocks:
                    symbol = entry["symbol"]
                    print(f"Processing entry for symbol {entry['symbol']}")
                    entry_json = json.dumps(entry)
                    print(entry_json)
                    #key_bytes = symbol.encode('utf-8')
                    #value_bytes = entry_json.encode('utf-8')
                    topic = f"{category}"
                    producer.produce(topic, value=entry_json)

                    
                    #producer.flush()
                    print('\nData sent to Kafka topic {} successfully.'.format(category))
                    time.sleep(60)  # Adjust the delay as needed
                    count += 1
                    if count >= 22:
                        break

    # Close the Kafka producer
    producer.flush()
    #producer.close()

rest_api_endpoint = " "
bootstrap_servers = " "
send_data_to_kafka(rest_api_endpoint, bootstrap_servers)
