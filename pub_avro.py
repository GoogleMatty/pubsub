import random
import time
import avro.schema as schema
import avro.io
from avro.io import BinaryEncoder, DatumWriter
import io
import json
import google.cloud.pubsub as pubsub
from google.cloud.pubsub import PublisherClient





# Generate some synthetic POS transactions
def generate_transactions(num_transactions):
  transactions = []
  for i in range(num_transactions):
    transaction = {}
    transaction["transactionId"] = random.randint(1, 1000)
    transaction["pos_timestamp"] = int(time.time())
    products = []
    for j in range(3):
      product = {}
      product["productID"] = random.randint(1, 100)
      product["productName"] = "Product " + str(j)
      products.append(product)
    transaction["products"] = products
    transactions.append(transaction)
   
  return transactions

# Main function
if __name__ == "__main__":
  # Generate some synthetic POS transactions
  #transactions = generate_transactions(1)
  
  # Define the Arvo schema for a POS transaction
  avsc_file = "pos3.avsc" 
  # Prepare to write Avro records to the binary output stream.
  schema = schema.parse(open(avsc_file, "rb").read())
 
  project_id = "point-of-sales-384803"
  topic_id = "pos-transaction-stream"
  publisher_client = PublisherClient()
  topic_path = publisher_client.topic_path(project_id, topic_id)
  
  while True: 
     
     transactions = generate_transactions(1000)
  
     for transaction in transactions:
   
        writer = DatumWriter(schema)
        bout   = io.BytesIO()
        encoder = BinaryEncoder(bout)
        writer.write(transaction, encoder)
        data = bout.getvalue()
     
        future = publisher_client.publish(topic_path, data)
        future.result()
    
 

 
    
 
