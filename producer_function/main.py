import json
import logging
import config
import traceback
from gobits import Gobits
from google.cloud import pubsub_v1, storage, pubsub


logging.basicConfig(level=logging.INFO)
client = storage.Client()

batch_settings = pubsub_v1.types.BatchSettings(**config.TOPIC_BATCH_SETTINGS)
publisher = pubsub.PublisherClient(batch_settings)


def json_from_bucket(bucket_name, blob_name):
    bucket = storage.Client().get_bucket(bucket_name)
    blob = storage.Blob(blob_name, bucket)
    content = blob.download_as_string()
    data = json.loads(content.decode('utf-8'))
    logging.info('Read file {} from {}'.format(blob_name, bucket_name))
    return data


def flatten_json(data):
    rows_json = []
    for key, value in data.items():
        processed = {}
        processed['project'] = key
        for k, v in value.items():
            processed[k] = v
        processed['id'] = processed['project'] + '-' + processed['weeknumber']
        for k, v in config.COLUMN_MAPPING.items():
            processed[v] = processed.pop(k)
        rows_json.append(processed)
    return rows_json


def remove_file_from_filestore(bucket_name, filename):
    bucket = client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.delete()
    logging.info('Deleted file {} from {}'.format(filename, bucket_name))


def publish_json(gobits, msg_data, rowcount, rowmax, topic_project_id, topic_name, subject=None):
    topic_path = publisher.topic_path(topic_project_id, topic_name)
    if subject:
        msg = {
            "gobits": [gobits.to_json()],
            subject: msg_data
        }
    else:
        msg = msg_data
    future = publisher.publish(
        topic_path, bytes(json.dumps(msg).encode('utf-8')))
    future.add_done_callback(
        lambda x: logging.debug(
            'Published msg with ID {} ({}/{} rows).'.format(
                future.result(), rowcount, rowmax))
    )


def fileprocessing(data, context):
    logging.info('Run started')
    bucket_name = data['bucket']
    filename = data['name']
    batch_message_size = config.BATCH_MESSAGE_SIZE if hasattr(config, 'BATCH_MESSAGE_SIZE') else None
    delete = config.DELETE if hasattr(config, 'DELETE') else True
    try:
        json_file = json_from_bucket(bucket_name, filename)
        rows_json = flatten_json(json_file)
        # In case of no new records: don't send any updates
        if len(rows_json) == 0:
            logging.info('No new rows found')
        else:
            logging.info('Found {} new rows.'.format(len(rows_json)))
            # Publish individual rows to topic
            i = 1
            message_batch = []
            gobits = Gobits.from_context(context=context)
            for publish_message in rows_json:
                if not batch_message_size:
                    publish_json(gobits, [publish_message], rowcount=i, rowmax=len(rows_json), **config.TOPIC_SETTINGS)
                else:
                    message_batch.append(publish_message)
                    if len(message_batch) == batch_message_size:
                        publish_json(gobits, message_batch, rowcount=i, rowmax=len(rows_json), **config.TOPIC_SETTINGS)
                        message_batch = []
                i += 1
        if message_batch:
            publish_json(gobits, message_batch, rowcount=i, rowmax=len(rows_json), **config.TOPIC_SETTINGS)
        if delete:
            remove_file_from_filestore(bucket_name, filename)
        logging.info('Publishing file {} successful'.format(filename))
    except Exception:
        logging.error('Publishing file {} failed!'.format(filename))
        traceback.print_exc()
