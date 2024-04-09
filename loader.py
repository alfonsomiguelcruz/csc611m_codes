import pika, os, shutil, json, multiprocessing, time
from PIL import Image
import numpy as np
import base64

def get_image_format(file_name):
    # Extract the file extension
    _, extension = os.path.splitext(file_name)
    # Map extensions to image formats
    extension_to_format = {
        '.jpg': 'JPEG',
        '.jpeg': 'JPEG',
        '.gif': 'GIF',
        '.png': 'PNG'
    }
    # Return the corresponding image format or None if not found
    return extension_to_format.get(extension.lower())

def load_images(file_list, msg_dict):
    credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
    connection = pika.BlockingConnection(pika.ConnectionParameters('10.0.2.15', 5672, '/', credentials)) # Wait for connection to be established before continuing execution
    channel = connection.channel()

    channel.queue_declare(queue='loaded_images', durable=True) # Queue won't be lost when broker is down

    for file in file_list:
        if file.endswith('.jpg') or file.endswith('.png') or file.endswith('.gif'):
            with open(os.path.join(input_directory, file), 'rb') as f:
                image_data = f.read()
            encoded_image = base64.b64encode(image_data).decode('utf-8')
            #image = Image.open(os.path.join(input_directory, file)).convert('RGB')
            #arr_img = np.array(image).tolist()
            #msg_dict['img'] = arr_img
            msg_dict['img'] = encoded_image
            msg_dict['filename'] = file
            msg_dict['format'] = get_image_format(file)
            msg_json = json.dumps(msg_dict)

            channel.basic_publish(
                exchange='',
                routing_key='loaded_images',
                body=msg_json,
                properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent 
                )
            )

            print(f" [x] Sent {file} to enhancer")

    connection.close() # Clear network buffer, ensure message is sent to consumer 


if __name__ == "__main__":

    # Semaphore - start
    credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
    connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.254.158', 5672, '/', credentials)) # Wait for connection to be established before continuing execution 
    channel = connection.channel()
    channel.queue_delete(queue='counter')
    channel.queue_declare(queue='counter', durable=True) # Queue won't be lost when broker is down
    message = "0"
    channel.basic_publish(exchange='', routing_key='counter', body=message, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
    connection.close() # double check if we really should close the connection 
    # Semaphore - end

    input_directory = './input_images'
    output_directory = './output_images'
    #number_of_processes = multiprocessing.cpu_count()
    number_of_processes = 4 
    n_machines = 4
    brightness = 4.0
    sharpness = 4.0
    contrast = 4.0
    file_list = [file for file in os.listdir(input_directory) if not file.startswith('.')]
    n_images = len(file_list)
    inc = int(n_images / number_of_processes)
    processes = []
    start_time = time.time()

    msg_dict = {
        'start_time': start_time, 
        'n_machines': n_machines,
        'n_images': n_images,
        'img': None,
        'filename': None,
        'format': None,
        'output_directory': output_directory,
        'brightness': brightness,
        'sharpness': sharpness,
        'contrast': contrast 
    }


    for i in range(number_of_processes):
        if (i == 0):
            process = multiprocessing.Process(target=load_images, args=(file_list[0:inc], msg_dict))
        elif(i == number_of_processes - 1):
            process = multiprocessing.Process(target=load_images, args=(file_list[inc*i:n_images], msg_dict))
        else:
            process = multiprocessing.Process(target=load_images, args=(file_list[inc*i:inc*(i+1)], msg_dict))
        processes.append(process)
        process.start()

    for p in processes:
        p.join()

"""
import pika
import sys

credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.64.11', 5672, '/', credentials)) # Wait for connection to be established before continuing execution
channel = connection.channel()

channel.queue_declare(queue='task_queue', durable=True) # Queue won't be lost when broker is down
i = 0
for j in range(1000):
    message = "Hello World: " + str(i) 
    channel.basic_publish(
            exchange='',
            routing_key='task_queue',
            body=message,
            properties=pika.BasicProperties(
                    delivery_mode=pika.DeliveryMode.Persistent  # Save message when broker is down?
            ))

    print(f" [x] Sent {message}")
    i+=1
connection.close() # Clear network buffer, ensure message is sent to consumer 

"""

"""
            with open(os.path.join(input_directory, file), "rb") as f:
                image = f.read()
"""
