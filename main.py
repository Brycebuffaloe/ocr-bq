# Copyright 2018, Google, LLC.
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# [START functions_ocr_setup]
import base64
import json
import os

from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import translate
from google.cloud import vision
from google.cloud import bigquery


vision_client = vision.ImageAnnotatorClient()
translate_client = translate.Client()
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client()
bq_client = bigquery.Client()

project_id = os.environ['GCP_PROJECT']

with open('config.json') as f:
    data = f.read()
config = json.loads(data)
# [END functions_ocr_setup]


# [START functions_ocr_detect]
def detect_text(bucket, filename):
    print('Looking for text in image {}'.format(filename))

    futures = []

    text_detection_response = vision_client.document_text_detection(image = {
        'source': {'image_uri': 'gs://{}/{}'.format(bucket, filename)}
        }, image_context= {'language_hints':'en-t-i0-handwrit'})
    annotations = text_detection_response.text_annotations
    if len(annotations) > 0:
        text = annotations[0].description
    else:
        text = ''
    print('Extracted text {} from image ({} chars).'.format(text, len(text)))
    
    if config['TRANSLATE']:
        detect_language_response = translate_client.detect_language(text)
        src_lang = detect_language_response['language']
        print('Detected language {} for text {}.'.format(src_lang, text))

        # Submit a message to the bus for each target language
        for target_lang in config.get('TO_LANG', []):
            if src_lang == target_lang:
                topic_name = config['RESULT_TOPIC']
            else:
                topic_name = config['TRANSLATE_TOPIC']
    else: 
        topic_name = config['RESULT_TOPIC']
        src_lang = config['DEFAULT_LANG']
        target_lang = config['DEFAULT_LANG']
        
    message = {
            'text': text,
            'filename': filename,
            'lang': target_lang,
            'src_lang': src_lang
            }
    
    message_data = json.dumps(message).encode('utf-8')
    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, data=message_data)
    futures.append(future)
    for future in futures:
        future.result()
# [END functions_ocr_detect]


# [START message_validatation_helper]
def validate_message(message, param):
    var = message.get(param)
    if not var:
        raise ValueError('{} is not provided. Make sure you have \
                          property {} in the request'.format(param, param))
    return var
# [END message_validatation_helper]


# [START functions_ocr_process]
def process_image(file, context):
    """Cloud Function triggered by Cloud Storage when a file is changed.
    Args:
        file (dict): Metadata of the changed file, provided by the triggering
                                 Cloud Storage event.
        context (google.cloud.functions.Context): Metadata of triggering event.
    Returns:
        None; the output is written to stdout and Stackdriver Logging
    """
    bucket = validate_message(file, 'bucket')
    name = validate_message(file, 'name')

    detect_text(bucket, name)

    print('File {} processed.'.format(file['name']))
# [END functions_ocr_process]


# [START functions_ocr_translate]
def translate_text(event, context):
    if event.get('data'):
        message_data = base64.b64decode(event['data']).decode('utf-8')
        message = json.loads(message_data)
    else:
        raise ValueError('Data sector is missing in the Pub/Sub message.')

    text = validate_message(message, 'text')
    filename = validate_message(message, 'filename')
    target_lang = validate_message(message, 'lang')
    src_lang = validate_message(message, 'src_lang')

    print('Translating text into {}.'.format(target_lang))
    translated_text = translate_client.translate(text,
                                                 target_language=target_lang,
                                                 source_language=src_lang)
    topic_name = config['RESULT_TOPIC']
    message = {
        'text': translated_text['translatedText'],
        'filename': filename,
        'lang': target_lang,
    }
    message_data = json.dumps(message).encode('utf-8')
    topic_path = publisher.topic_path(project_id, topic_name)
    future = publisher.publish(topic_path, data=message_data)
    future.result()
# [END functions_ocr_translate]

# [START fucntions_ocr_stream]
def stream_result(event, context):
    if event.get('data'):
        message_date = base64.b64decode(event['data']).decode('utf-8')
        message = json.loads(message_data)
    else:
        raise ValueError('Data sector is missing in the Pub/Sub message.')
    text = validate_message(message, 'text')
    filename = validate_message(message, 'filename')
    bq_table_path = '{}.{}'.format(config['BQ_DATASET'], config['BQ_TABLE'])
    print('Recieved request to insert text from {} to {}'.format(filename, bq_table_path))
    bq_client.insert_row(table, (message_date, filename, text))
    print('Inserted text from {} into {} '.format(filename, bq_table_path))
    
# [END functions_oce_stream]

# [START functions_ocr_save]
def save_result(event, context):
    if event.get('data'):
        message_data = base64.b64decode(event['data']).decode('utf-8')
        message = json.loads(message_data)
    else:
        raise ValueError('Data sector is missing in the Pub/Sub message.')

    text = validate_message(message, 'text')
    filename = validate_message(message, 'filename')
    lang = validate_message(message, 'lang')

    print('Received request to save file {}.'.format(filename))

    bucket_name = config['RESULT_BUCKET']
    result_filename = '{}_{}.txt'.format(filename, lang)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(result_filename)

    print('Saving result to {} in bucket {}.'.format(result_filename,
                                                     bucket_name))

    blob.upload_from_string(text)

    print('File saved.')
# [END functions_ocr_save]