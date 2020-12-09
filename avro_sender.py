import avro.schema
import avro.io as avroio
from io import StringIO, BytesIO
 
#buffer_writer = StringIO()
buffer_writer = BytesIO()
buffer_encoder = avroio.BinaryEncoder(buffer_writer)
 
body = {"name": "bob b.", "age": 100}
print(body)
 
writers_schema = avro.schema.parse(open("avro_schemas/msg_servers.avsc").read())
datum_writer = avroio.DatumWriter(writers_schema)
datum_writer.write(body, buffer_encoder)
 
import pika
 
connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()
 
channel.queue_declare(queue='hello')
 
properties = pika.BasicProperties(content_type='application/octet-stream')
 
channel.basic_publish(exchange='', routing_key='hello', body=buffer_writer.getvalue())
print(" [x] Sent 'Hello World!'")
connection.close()
