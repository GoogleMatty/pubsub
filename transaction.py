import random
import json
import time

from google.cloud import pubsub


class SyntheticTransaction:

    def __init__(self):
        self.transaction_id = random.randint(1, 1000000)
        self.timestamp = "2023-04-24T12:00:00Z"
        self.store_id = random.randint(1, 1000)
        self.employee_id = random.randint(1, 1000)
        self.customer_id = random.randint(1, 1000)
        self.products = []
        self.total_price = 0.00
        self.payment_type = random.choice(["Credit Card", "Debit Card", "Cash"])
        self.card_number = "1234-5678-9012-3456"
        self.expiration_date = "01/23"
        self.cvv = "123"

    def add_product(self, product_id, product_name, product_price, quantity):
        self.products.append({
            "productId": product_id,
            "productName": product_name,
            "productPrice": product_price,
            "quantity": quantity
        })
        self.total_price += product_price * quantity

    def to_json(self):
        return json.dumps(self, indent=4)


def create_transaction(store_id, employee_id, customer_id):
    transaction = SyntheticTransaction()
    transaction.store_id = store_id
    transaction.employee_id = employee_id
    transaction.customer_id = customer_id
    return transaction


def main():
    # Create a Pub/Sub client.
    client = pubsub.Client()

    # Create a topic.
    topic = client.topic("my-topic")

    # Create a loop to stream data indefinitely.
    while True:
        # Create a transaction.
        transaction = create_transaction(random.randint(1, 1000), random.randint(1, 1000), random.randint(1, 1000))

        # Convert the transaction to JSON.
        transaction_json = transaction.to_json()

        # Send the transaction to the Pub/Sub topic.
        topic.publish(transaction_json)

        # Sleep for 1 second.
        time.sleep(1)


if __name__ == "__main__":
    main()
