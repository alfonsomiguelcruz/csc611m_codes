import pika, os, json, multiprocessing, time
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

def load_images(file_list, msg_dict, os_input_dir):
    credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
    connection = pika.BlockingConnection(pika.ConnectionParameters('192.168.254.158', 5672, '/', credentials)) # Wait for connection to be established before continuing execution
    channel = connection.channel()

    channel.queue_declare(queue='loaded_images', durable=True) # Queue won't be lost when broker is down

    for file in file_list:
        if file.endswith('.jpg') or file.endswith('.png') or file.endswith('.gif'):
            with open(os.path.join(os_input_dir, file), 'rb') as f:
                image_data = f.read()
            encoded_image = base64.b64encode(image_data).decode('utf-8')

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
    connection = pika.BlockingConnection(pika.ConnectionParameters('10.151.24.15', 5672, '/', credentials)) # Wait for connection to be established before continuing execution 
    channel = connection.channel()
    channel.queue_delete(queue='counter')
    channel.queue_declare(queue='counter', durable=True) # Queue won't be lost when broker is down
    message = "0"
    channel.basic_publish(exchange='', routing_key='counter', body=message, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent))
    connection.close() # double check if we really should close the connection 
    # Semaphore - end

    input_directory = input("Input Directory: ")
    output_directory = input("Output Directory: ")
    number_of_processes = 5
    # n_machines = 4
    brightness = float(input("Brightness Factor: "))
    sharpness  = float(input("Sharpness Factor: "))
    contrast   = float(input("Contrast Factor: "))
    
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
            process = multiprocessing.Process(target=load_images, args=(file_list[0:inc], msg_dict, input_directory))
        elif(i == number_of_processes - 1):
            process = multiprocessing.Process(target=load_images, args=(file_list[inc*i:n_images], msg_dict, input_directory))
        else:
            process = multiprocessing.Process(target=load_images, args=(file_list[inc*i:inc*(i+1)], msg_dict, input_directory))
        processes.append(process)
        process.start()

    for p in processes:
        p.join()