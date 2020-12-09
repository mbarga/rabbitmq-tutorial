import pika
import avro.schema
from avro import io
from io import StringIO, BytesIO

schema = avro.schema.parse(open("avro_schemas/msg_servers.avsc").read())

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()

channel.queue_declare(queue='hello')
print(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    print(" [x] Received %r" % (body,))

    #buffer_reader = StringIO(body)
    buffer_reader = BytesIO(body)
    buffer_decoder = io.BinaryDecoder(buffer_reader)

    writers_schema = avro.schema.parse(open("avro_schemas/msg_servers.avsc").read())
    readers_schema = avro.schema.parse(open("avro_schemas/msg_servers.avsc").read())
    datum_reader = io.DatumReader(writers_schema, readers_schema)
    try:
        msg = datum_reader.read(buffer_decoder)
        print(msg)
    except:
        print('error')

channel.basic_consume(queue='hello', on_message_callback=callback, auto_ack=True)
channel.start_consuming()
