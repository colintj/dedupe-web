from multiprocessing.connection import Listener, Client
from multiprocessing import Pool
import dedupe
from dedupe.serializer import _to_json
from dedupe import AsciiDammit
from uuid import uuid4
import copy
import os
import re
import logging
from cStringIO import StringIO
import csv
from collections import defaultdict

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')
dedupers = {}

TRAINING_RECV = ('localhost', 6000)

def preProcess(column):
    column = AsciiDammit.asciiDammit(column)
    column = re.sub('  +', ' ', column)
    column = re.sub('\n', ' ', column)
    column = column.strip().strip('"').strip("'").lower().strip()
    return column

def readData(f):
    data = {}
    reader = csv.DictReader(f)
    for i, row in enumerate(reader):
        clean_row = [(k, preProcess(v)) for (k,v) in row.items()]
        row_id = i
        data[row_id] = dedupe.core.frozendict(clean_row)
    return data

def writeResults(inp, file_path, clustered_dupes):

    # Write our original data back out to a CSV with a new column called 
    # 'Cluster ID' which indicates which records refer to each other.

    logging.info('saving results to: %s' % file_path)

    cluster_membership = defaultdict(lambda : 'x')
    for cluster_id, cluster in enumerate(clustered_dupes):
        for record_id in cluster:
            cluster_membership[record_id] = cluster_id

    writer = csv.writer(open(file_path, 'wb'))

    reader = csv.reader(StringIO(inp))

    heading_row = reader.next()
    heading_row.insert(0, 'Cluster ID')
    writer.writerow(heading_row)

    for i, row in enumerate(reader):
        row_id = i
        cluster_id = cluster_membership[row_id]
        row.insert(0, cluster_id)
        writer.writerow(row)

# ## Writing results
def writeUniqueResults(inp, file_path, clustered_dupes):

    # Write our original data back out to a CSV with a new column called 
    # 'Cluster ID' which indicates which records refer to each other.

    logging.info('saving unique results to: %s' % file_path)

    cluster_membership = {}
    for (cluster_id, cluster) in enumerate(clustered_dupes):
        logging.info(cluster)
        for record_id in cluster:
            cluster_membership[record_id] = cluster_id

    writer = csv.writer(open(file_path, 'wb'))

    reader = csv.reader(StringIO(inp))

    heading_row = reader.next()
    writer.writerow(heading_row)

    seen_clusters = set()
    for i, row in enumerate(reader):
        row_id = i
        if row_id in cluster_membership: 
            cluster_id = cluster_membership[row_id]
            if cluster_id not in seen_clusters:
                writer.writerow(row)
                seen_clusters.add(cluster_id)
        else:
            writer.writerow(row)

def training_loop(msg, conn):
    deduper_id = msg.get('deduper_id')
    if msg.get('step') is 1:
        # Establish the key and save the CSV
        dedupers[deduper_id] = {
            'csv': msg['csv'],
            'filename': msg['filename']
        }
    elif msg.get('step') is 2:
        # Save field_defs and make sample
        field_defs = {}
        field_list = msg['field_list']
        for field in field_list:
            field_defs[field] = {'type': 'String'}
        dedupers[deduper_id]['field_defs'] = copy.deepcopy(field_defs)
        dedupers[deduper_id]['deduper'] = dedupe.Dedupe(field_defs)
        inp = StringIO(dedupers[deduper_id]['csv'])
        dedupers[deduper_id]['data_d'] = readData(inp)
        dedupers[deduper_id]['deduper'].sample(dedupers[deduper_id]['data_d'], 150000)
    elif msg.get('step') == 'get_pair':
        # Get uncertain pairs

        deduper = dedupers[deduper_id]['deduper']
        fields = deduper.data_model.comparison_fields
        record_pair = deduper.uncertainPairs()[0]
        dedupers[deduper_id]['current_pair'] = record_pair
        client = Client(('localhost', msg['port']), authkey=deduper_id)
        client.send((record_pair, fields))
        client.close()
    elif msg.get('step') == 'mark_pair':
        current_pair = dedupers[deduper_id]['current_pair']
        action = msg.get('action')
        if dedupers[deduper_id].get('counter'):
            counter = dedupers[deduper_id]['counter']
        else:
            counter = {'yes': 0, 'no': 0, 'unsure': 0}
        if dedupers[deduper_id].get('training_data'):
            labels = dedupers[deduper_id]['training_data']
        else:
            labels = {'distinct' : [], 'match' : []}
        if action == 'yes':
            labels['match'].append(current_pair)
            counter['yes'] += 1
            resp = {'counter': counter}
        elif action == 'no':
            labels['distinct'].append(current_pair)
            counter['no'] += 1
            resp = {'counter': counter}
        else:
            counter['unsure'] += 1
            dedupers[deduper_id]['counter'] = counter
            resp = {'counter': counter}
        deduper = dedupers[deduper_id]['deduper']
        deduper.markPairs(labels)
        dedupers[deduper_id]['training_data'] = labels
        dedupers[deduper_id]['counter'] = counter
    elif msg.get('step') == 'finish':
        deduper = dedupers[deduper_id]['deduper']
        deduper.train()
        filename = dedupers[deduper_id]['filename']
        settings_path = os.path.join(UPLOAD_FOLDER, '%s-settings.dedupe' % deduper_id)
        training_path = os.path.join(UPLOAD_FOLDER, '%s-training.json' % deduper_id)
        deduper.writeTraining(training_path)
        deduper.writeSettings(settings_path)
        data_d = dedupers[deduper_id]['data_d']
        threshold = deduper.threshold(data_d, recall_weight=2)
        clustered_dupes = deduper.match(data_d, threshold)
        logging.info('clustering done')
        deduped_unique_file_path = os.path.join(UPLOAD_FOLDER, '%s-deduped_unique.csv' % deduper_id)
        raw = dedupers[deduper_id]['csv']
        writeUniqueResults(raw, deduped_unique_file_path, clustered_dupes)
        deduped_file_path = os.path.join(UPLOAD_FOLDER, '%s-deduped.csv' % deduper_id)
        writeResults(raw, deduped_file_path, clustered_dupes)
        deduper.pool.terminate()
        del deduper
        del dedupers[deduper_id]
    return 'Step %s done: %s' % (msg['step'], msg['deduper_id'])

if __name__ == "__main__":
    listener = Listener(TRAINING_RECV)
    while True:
        conn = listener.accept()
        try:
            msg = conn.recv()
            training_loop(msg, conn)
        except EOFError:
            conn.close()
            continue

    # dedupe_loop(DEDUPE_RECV)

