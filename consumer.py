from confluent_kafka import Consumer, KafkaException, KafkaError
import boto3
import json

access_key = " "
secret_access_key = " "

# Kafka configuration
kafka_config = {
    'bootstrap.servers': ' ',
    'group.id': 'my_consumer_group',
    'auto.offset.reset': 'earliest'
}

# DynamoDB configuration
dynamodb = boto3.client('dynamodb', region_name='ap-south-1', aws_access_key_id=access_key, aws_secret_access_key=secret_access_key)

# List of topics and corresponding DynamoDB tables
topics = ['financial', 'oil_and_gas', 'healthcare', 'fmcg']
table_mapping = {
    'financial': 'financial',
    'oil_and_gas': 'oil_and_gas',
    'healthcare': 'healthcare',
    'fmcg': 'fmcg'
}

def process_message(msg, table_name):
    try:
        # Decode the message value from Kafka
        message = json.loads(msg.value().decode('utf-8'))

        if 'symbol' not in message:
            raise ValueError("Missing 'symbol' in Kafka message")

        # Extract relevant data from the message
        symbol = message['symbol']
        identifier = message.get('identifier', '')
        open_val = message.get('open', '')
        day_high = message.get('dayHigh', '')
        day_low = message.get('dayLow', '')
        last_price = message.get('lastPrice', '')
        previous_close = message.get('previousClose', '')
        change = message.get('change', '')
        p_change = message.get('pChange', '')
        total_traded_volume = message.get('totalTradedVolume', '')
        total_traded_value = message.get('totalTradedValue', '')
        last_update_time = message.get('lastUpdateTime', '')
        year_high = message.get('yearHigh', '')
        year_low = message.get('yearLow', '')
        per_change_365d = message.get('perChange365d', '')
        per_change_30d = message.get('perChange30d','')
        

        item = {
            'symbol': {'S': symbol},
            'identifier': {'S': identifier},
            'open_val':{'N':str(open_val)},
            'day_high':{'N':str(day_high)},
            'day_low':{'N':str(day_low)},
            'last_price':{'N',str(last_price)},
            'previous_close':{'S',str(previous_close)},
            'change':{'S',str(change)},
            'p_change':{'s',str(p_change)},
            'total_traded_volume':{'N',str(total_traded_volume)},
            'total_traded_value':{'S',str(total_traded_value)},
            'last_update_time':{'S',last_update_time},
            'year_high':{'N',str(year_high)},
            'year_low':{'S',str(year_low)},
            'per_change_365d':{'S',str(per_change_365d)},
            'per_change_30d':{'S',str(per_change_30d)}
                
            # Add more attributes as needed
        }

        print(f"Received message for table {table_name}: {message}")
        # Put the item into DynamoDB
        dynamodb.put_item(
            TableName=table_name,
            Item=item
        )

        print(f"Item added to DynamoDB for table {table_name}: {item}")

    except Exception as e:
        print(f"Error processing message: {e}")

# Kafka consumer
consumer = Consumer(kafka_config)

try:
    for topic in topics:
        table_name = table_mapping.get(topic)
        if table_name:
            consumer.subscribe([topic])
            print(f"Subscribed to topic: {topic}")

            while True:
                msg = consumer.poll(1000)

                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        print(msg.error())
                        break

                process_message(msg, table_name)

except KeyboardInterrupt:
    pass

finally:
    consumer.close()
