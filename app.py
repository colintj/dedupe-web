from flask import Flask, request, make_response, render_template, \
    session as flask_session, redirect, url_for, send_from_directory, jsonify
from werkzeug import secure_filename
import time
from datetime import datetime, timedelta
import json
import requests
import re
import os
import copy
from dedupe import AsciiDammit
from dedupe.serializer import _to_json, dedupe_decoder
import dedupe
from dedupe_utils import dedupeit, static_dedupeit, DedupeFileIO,\
    DedupeFileError
from cStringIO import StringIO
import csv
from queue import DelayedResult
from uuid import uuid4
import collections
from redis import Redis

redis = Redis()

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')
ALLOWED_EXTENSIONS = set(['csv', 'xls', 'xlsx'])

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 5 * 1024 * 1024
app.config['REDIS_QUEUE_KEY'] = 'deduper'
app.secret_key = os.environ['FLASK_KEY']

dedupers = {}

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET', 'POST'])
def index():
    status_code = 200
    error = None
    if request.method == 'POST':
        f = request.files['input_file']
        if f and allowed_file(f.filename):
            deduper_id = str(uuid4())
            filename = secure_filename(str(time.time()) + "_" + f.filename)
            try:
                inp_file = DedupeFileIO(f, filename)
                dedupers[deduper_id] = {
                    'csv': inp_file,
                    'last_interaction': datetime.now()
                }
                for key, deduper in dedupers.items():
                    last_interaction = deduper['last_interaction']
                    old = datetime.now() - timedelta(seconds=60 * 30)
                    if last_interaction < old:
                        del dedupers[key]
                flask_session['session_id'] = deduper_id
                flask_session['filename'] = filename
                flask_session['row_count'] = inp_file.line_count
                return redirect(url_for('select_fields'))
            except DedupeFileError as e:
                error = e.message
                status_code = 500
        else:
            error = 'Error uploading file. Did you forget to select one?'
            status_code = 500
    return make_response(render_app_template('index.html', error=error), status_code)

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

@app.route('/select_fields/', methods=['GET', 'POST'])
def select_fields():
    status_code = 200
    error = None
    if not flask_session.get('session_id'):
        return redirect(url_for('index'))
    else:
        deduper_id = flask_session['session_id']
        inp = dedupers[deduper_id]['csv'].converted
        filename = flask_session['filename']
        dedupers[deduper_id]['last_interaction'] = datetime.now()
        reader = csv.reader(inp)
        fields = reader.next()
        inp.seek(0)
        if request.method == 'POST':
            field_list = [r for r in request.form]
            if field_list:
                training = True
                field_defs = {}
                for field in field_list:
                    field_defs[field] = {'type': 'String'}
                data_d = readData(inp)
                dedupers[deduper_id]['data_d'] = data_d
                dedupers[deduper_id]['field_defs'] = copy.deepcopy(field_defs)
                deduper = dedupe.Dedupe(field_defs)
                deduper.sample(data_d, 150000)
                dedupers[deduper_id]['deduper'] = deduper
                return redirect(url_for('training_run'))
            else:
                error = 'You must select at least one field to compare on.'
                status_code = 500
        return render_app_template('select_fields.html', error=error, fields=fields, filename=filename)

@app.route('/training_run/')
def training_run():
    if not flask_session.get('session_id'):
        return redirect(url_for('index'))
    else:
        filename = flask_session['filename']
        return render_app_template('training_run.html', filename=filename)

@app.route('/get-pair/')
def get_pair():
    if not flask_session.get('session_id'):
        return make_response(jsonify(status='error', message='need to start a session'), 400)
    else:
        deduper_id = flask_session['session_id']
        deduper = dedupers[deduper_id]['deduper']
        filename = flask_session['filename']
        dedupers[deduper_id]['last_interaction'] = datetime.now()
        fields = deduper.data_model.comparison_fields
        record_pair = deduper.uncertainPairs()[0]
        dedupers[deduper_id]['current_pair'] = record_pair
        data = []
        left, right = record_pair
        for field in fields:
            d = {
                'field': field,
                'left': left[field],
                'right': right[field],
            }
            data.append(d)
        resp = make_response(json.dumps(data))
        resp.headers['Content-Type'] = 'application/json'
        return resp

@app.route('/mark-pair/')
def mark_pair():
    if not flask_session.get('session_id'):
        return make_response(jsonify(status='error', message='need to start a session'), 400)
    else:
        action = request.args['action']
        deduper_id = flask_session['session_id']
        current_pair = dedupers[deduper_id]['current_pair']
        dedupers[deduper_id]['last_interaction'] = datetime.now()
        if dedupers[deduper_id].get('counter'):
            counter = dedupers[deduper_id]['counter']
        else:
            counter = {'yes': 0, 'no': 0, 'unsure': 0}
        if dedupers[deduper_id].get('training_data'):
            labels = dedupers[deduper_id]['training_data']
        else:
            labels = {'distinct' : [], 'match' : []}
        deduper = dedupers[deduper_id]['deduper']
        if action == 'yes':
            labels['match'].append(current_pair)
            counter['yes'] += 1
            resp = {'counter': counter}
        elif action == 'no':
            labels['distinct'].append(current_pair)
            counter['no'] += 1
            resp = {'counter': counter}
        elif action == 'finish':
            file_io = dedupers[deduper_id]['csv']
            training_file_path = os.path.join(UPLOAD_FOLDER, '%s-training.json' % file_io.file_path)
            training_data = dedupers[deduper_id]['training_data']
            with open(training_file_path, 'wb') as f:
                f.write(json.dumps(training_data, default=_to_json))
            field_defs = dedupers[deduper_id]['field_defs']
            sample = deduper.data_sample
            args = {
                'field_defs': field_defs,
                'training_data': training_file_path,
                'file_io': file_io,
                'data_sample': sample,
            }
            rv = dedupeit.delay(**args)
            flask_session['deduper_key'] = rv.key
            resp = {'finished': True}
        else:
            counter['unsure'] += 1
            dedupers[deduper_id]['counter'] = counter
            resp = {'counter': counter}
        deduper.markPairs(labels)
        dedupers[deduper_id]['training_data'] = labels
        dedupers[deduper_id]['counter'] = counter
        if resp.get('finished'):
            deduper.pool.terminate()
            del dedupers[deduper_id]
    resp = make_response(json.dumps(resp))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/dedupe_finished/')
def dedupe_finished():
    return render_app_template("dedupe_finished.html")

@app.route('/trained_dedupe/', methods=['POST'])
def trained_dedupe():
    deduper_id = flask_session['session_id']
    inp = StringIO(dedupers[deduper_id]['csv'])
    filename = flask_session['filename']
    dedupers[deduper_id]['last_interaction'] = datetime.now()
    field_defs = dedupers[deduper_id]['field_defs']
    training_data = request.files['training_data']
    dedupers[deduper_id]['training_data'] = json.load(training_data)
    return redirect(url_for('dedupe_finished'))

@app.route('/adjust_threshold/')
def adjust_threshold():
    filename = flask_session['filename']
    file_path = os.path.join(UPLOAD_FOLDER, filename)
    start = filename.split('_')[0]
    settings_path = None
    for f in os.listdir(UPLOAD_FOLDER):
        if f.startswith(start) and f.endswith('.dedupe'):
            settings_path = os.path.join(UPLOAD_FOLDER, f)
    recall_weight = request.args.get('recall_weight')
    args = {
        'settings_path': settings_path,
        'file_path': file_path,
        'recall_weight': recall_weight,
    }
    rv = static_dedupeit.delay(**args)
    flask_session['deduper_key'] = rv.key
    resp = make_response(json.dumps({'adjusted': True}))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/about/')
def about():
  return render_app_template("about.html")

@app.route('/working/')
def working():
    key = flask_session.get('deduper_key')
    if key is None:
        return jsonify(ready=False)
    rv = DelayedResult(key)
    if rv.return_value is None:
        return jsonify(ready=False)
    redis.delete(key)
    del flask_session['deduper_key']
    return jsonify(ready=True, result=rv.return_value)

@app.route('/upload_data/<path:filename>/')
def upload_data(filename):
    return send_from_directory(UPLOAD_FOLDER, filename)

# UTILITY
def render_app_template(template, **kwargs):
    '''Add some goodies to all templates.'''

    if 'config' not in kwargs:
        kwargs['config'] = app.config
    return render_template(template, **kwargs)

# INIT
if __name__ == "__main__":
    app.run(debug=True, port=9999)
