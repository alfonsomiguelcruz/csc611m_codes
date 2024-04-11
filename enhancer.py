import pika
import base64
import json
from PIL import Image, ImageEnhance
from io import BytesIO

def adjust_brightness(image, factor):
    enhancer = ImageEnhance.Brightness(image)
    return enhancer.enhance(factor)

def adjust_sharpness(image, factor):
    enhancer = ImageEnhance.Sharpness(image)
    return enhancer.enhance(factor)

def adjust_contrast(image, factor):
    enhancer = ImageEnhance.Contrast(image)
    return enhancer.enhance(factor)


credentials = pika.PlainCredentials('rabbituser', 'rabbit1234')
connection = pika.BlockingConnection(pika.ConnectionParameters('10.151.24.15', 5672, '/', credentials))
channel = connection.channel()

channel.queue_declare(queue='loaded_images', durable=True)
channel.queue_declare(queue='enhanced_images', durable=True)

print(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    
    json_body = body.decode('utf-8')
    body_dict = json.loads(json_body)

    # Get Image and Filename Extraction
    fname = body_dict['filename']
    brightness = body_dict['brightness']
    sharpness = body_dict['sharpness']
    contrast = body_dict['contrast']
    format = body_dict['format']

    print(f" [x] Received {fname}")

    # Get image
    encoded_image = body_dict['img']
    decoded_image = base64.b64decode(encoded_image)
    image = Image.open(BytesIO(decoded_image)).convert('RGB')
    enhanced_image = adjust_brightness(image, brightness)
    enhanced_image = adjust_sharpness(enhanced_image, sharpness)
    enhanced_image = adjust_contrast(enhanced_image, contrast)

    edited_image_buffer = BytesIO()
    enhanced_image.save(edited_image_buffer, format=format)
    edited_image_data = edited_image_buffer.getvalue()
    encoded_edited_image = base64.b64encode(edited_image_data).decode('utf-8')

    body_dict['img'] = encoded_edited_image 

    msg_json = json.dumps(body_dict)

    channel.basic_publish(
            exchange='',
            routing_key='enhanced_images',
            body=msg_json,
            properties=pika.BasicProperties(
                delivery_mode=pika.DeliveryMode.Persistent 
            )
        )

    print(" [x] Done")
    

    ch.basic_ack(delivery_tag=method.delivery_tag)

channel.basic_qos(prefetch_count=1) # quality of service
channel.basic_consume(queue='loaded_images', on_message_callback=callback)

channel.start_consuming()