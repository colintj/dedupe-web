from multiprocessing.connection import Listener
import dedupe
from dedupe.serializer import _to_json
from uuid import uuid4
import copy
import os

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')
dedupers = {}

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

def dedupe_loop(conn):
    while True:
        msg = conn.recv()
        deduper_id = msg['deduper_id']
        deduper = dedupers[deduper_id]
        deduper.train()
        filename = dedupers[deduper_id]['filename']
        settings_path = os.path.join(UPLOAD_FOLDER, '%s-settings.dedupe' % filename)
        training_path = os.path.join(UPLOAD_FOLDER, '%s-training.json' % filename)
        deduper.writeTraining(training_path)
        deduper.writeSettings(settings_path)

def training_loop(conn):
    while True:
        msg = conn.recv()
        deduper_id = msg.get('deduper_id')
        if msg.get('step') == 'done':
            dedupe_conn = Client(('localhost', 6002))
            dedupe_conn.send(deduper_id)
            dedupe_conn.close()
        elif msg.get('step') is 1:
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
            deduper.sample(data_d, 150000)
        elif msg.get('step') == 'get_pair':
            deduper = dedupers[deduper_id]['deduper']
            fields = deduper.data_model.comparison_fields
            record_pair = deduper.getUncertainPair()[0]
            client = Client(('localhost', 6001), authkey=deduper_id)
            client.send(record_pair)
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
            conn.send(resp)

if __name__ == "__main__":
    training_address = ('localhost', 6000)
    training_listener = Listener(training_address)
    training_conn = training_listener.accept()
    training_loop(training_conn)

    dedupe_address = ('localhost', 6002)
    dedupe_listener = Listener(dedupe_address)
    dedupe_conn = dedupe_listener.accept()
    dedupe_loop(dedupe_conn)

