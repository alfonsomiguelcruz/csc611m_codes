import pika
import json, functools
from PIL import Image
from datetime import datetime
from io import BytesIO
import base64

def incrementCounter(ch, method, properties, body, body_dict, connection):
    n_enhanced_images = int(body.decode())
    n_enhanced_images += 1
    n_images = body_dict['n_images']
    if n_enhanced_images == n_images:    
        start_time = body_dict['start_time']
        n_machines = body_dict['n_machines']
        img = body_dict['img']
        fname = body_dict['filename']
        output_directory = body_dict['output_directory']
        execution_time = datetime.now() - datetime.fromtimestamp(start_time)  # TODO: not sure 

        f = open(output_directory + "/output.txt", "w")
        f.write("Number of machines: " + str(n_machines) + "\n")
        f.write("Number of input images: " + str(n_images) + "\n")
        f.write("Number of enhanced images: " + str(n_enhanced_images) + "\n")
        f.write("Execution time: " + str(execution_time) + " seconds\n")
        f.write("Output folder location: " + output_directory)
        f.close()
    else: 
        ch.basic_publish(exchange='', routing_key='counter', body=str(n_enhanced_images), properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
    ch.basic_ack(delivery_tag=method.delivery_tag)
    connection.close()


def callback(ch, method, properties, body):
    # Decode the json bytefile, convert it to a dictionary
    json_body = body.decode('utf-8')
    body_dict = json.loads(json_body)

    # Get Image and Filename Extraction
    start_time = body_dict['start_time']
    img = body_dict['img']
    fname = body_dict['filename']
    output_directory = body_dict['output_directory']
    print(f" [x] Received {fname}")

    # Get image
    #enhanced_image = Image.fromarray(np.uint8(img)).convert('RGB')
    encoded_image = body_dict['img']
    decoded_image = base64.b64decode(encoded_image)
    image = Image.open(BytesIO(decoded_image)).convert('RGB')

    # Save image and output file
    image.save(output_directory + "/" + fname)
    
    credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
    connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.254.158', 5672, '/', credentials))
    channel = connection.channel()
    channel.queue_declare(queue='counter', durable=True)
    channel.basic_qos(prefetch_count=1)
    on_message_callback = functools.partial(incrementCounter, body_dict=body_dict, connection=connection)
    channel.basic_consume(queue="counter", on_message_callback=on_message_callback)
    channel.start_consuming()
    
    print(f" [x] Done saving {fname}")
    ch.basic_ack(delivery_tag=method.delivery_tag) # TODO: check if this should be auto_ack=True instead
    

credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.254.158', 5672, '/', credentials))
channel = connection.channel()

channel.queue_declare(queue='enhanced_images', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

channel.basic_qos(prefetch_count=1) # quality of service
channel.basic_consume(queue='enhanced_images', on_message_callback=callback)

channel.start_consuming()
