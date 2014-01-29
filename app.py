from flask import Flask, request, make_response, render_template, \
    session as flask_session, redirect, url_for, send_from_directory, jsonify
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from werkzeug import secure_filename
import time
from datetime import datetime
import json
import requests
import re
import os
import pickle
from dedupe import AsciiDammit
from dedupe.serializer import _to_json, dedupe_decoder
import dedupe
from deduper import dedupeit
from cStringIO import StringIO
import csv
from models import DedupeSession, Training
from queue import DelayedResult
from uuid import uuid4
import collections
from redis import Redis

redis = Redis()

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')
SETTINGS_FOLDER = os.path.join(os.path.dirname(__file__), 'settings_data')
ALLOWED_EXTENSIONS = set(['csv', 'json'])

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
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
            dedupers[deduper_id] = {
                'csv': f.read(),
                'filename': secure_filename(str(time.time()) + "_" + f.filename)
            }
            flask_session['session_id'] = deduper_id
            return redirect(url_for('training_start'))
        else:
            # probably need to make sure to handle the error in the template
            error = 'Error uploading file'
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

@app.route('/training-start/', methods=['GET', 'POST'])
def training_start():
    if not flask_session.get('session_id'):
        return redirect(url_for('index'))
    else:
        deduper_id = flask_session['session_id']
        inp = StringIO(dedupers[deduper_id]['csv'])
        filename = dedupers[deduper_id]['filename']
        reader = csv.reader(inp)
        fields = reader.next()
        inp = StringIO(dedupers[deduper_id]['csv'])
        if request.method == 'POST':
            field_list = [r for r in request.form]
            if field_list:
                training = True
                field_defs = {}
                for field in field_list:
                    field_defs[field] = {'type': 'String'}
                data_d = readData(inp)
                dedupers[deduper_id]['data_d'] = data_d
                dedupers[deduper_id]['field_defs'] = field_defs
                deduper = dedupe.Dedupe(field_defs)
                deduper.sample(data_d, 150000)
                dedupers[deduper_id]['deduper'] = deduper
                return redirect(url_for('training_run'))
        return render_app_template('training.html', fields=fields, filename=filename)

@app.route('/training-run/')
def training_run():
    if not flask_session.get('session_id'):
        return redirect(url_for('index'))
    else:
        deduper_id = flask_session['session_id']
        deduper = dedupers[deduper_id]['deduper']
        filename = dedupers[deduper_id]['filename']
        fields = deduper.data_model.comparison_fields
        record_pair = deduper.getUncertainPair()[0]
        dedupers[deduper_id]['current_pair'] = record_pair
        data = {
            'fields': fields,
            'left': {},
            'right': {},
        }
        left, right = record_pair
        for k,v in left.items():
            if k in fields:
                data['left'][k] = v
        for k,v in right.items():
            if k in fields:
                data['right'][k] = v
        return render_app_template('training_run.html', data=data, fields=fields, filename=filename)

@app.route('/get-pair/')
def get_pair():
    if not flask_session.get('session_id'):
        return make_response(jsonify(status='error', message='need to start a session'), 400)
    else:
        deduper_id = flask_session['session_id']
        deduper = dedupers[deduper_id]['deduper']
        filename = dedupers[deduper_id]['filename']
        fields = deduper.data_model.comparison_fields
        record_pair = deduper.getUncertainPair()[0]
        dedupers[deduper_id]['current_pair'] = record_pair
        data = {
            'fields': fields,
            'left': {},
            'right': {},
        }
        left, right = record_pair
        for k,v in left.items():
            if k in fields:
                data['left'][k] = v
        for k,v in right.items():
            if k in fields:
                data['right'][k] = v
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
        if dedupers[deduper_id].get('training_data'):
            labels = dedupers[deduper_id]['training_data']
        else:
            labels = {'distinct' : [], 'match' : []}
        deduper = dedupers[deduper_id]['deduper']
        if action == 'yes':
            labels['match'].append(current_pair)
            deduper.markPairs(labels)
            dedupers[deduper_id]['training_data'] = labels
            resp = make_response(json.dumps({'marked': True}))
            resp.headers['Content-Type'] = 'application/json'
        elif action == 'no':
            labels['distinct'].append(current_pair)
            deduper.markPairs(labels)
            dedupers[deduper_id]['training_data'] = labels
            resp = make_response(json.dumps({'marked': True}))
            resp.headers['Content-Type'] = 'application/json'
        elif action == 'finish':
            field_defs = dedupers[deduper_id]['field_defs']
            filename = dedupers[deduper_id]['filename']
            file_path = os.path.join(UPLOAD_FOLDER, filename)
            with open(file_path, 'wb') as f:
                f.write(dedupers[deduper_id]['csv'])
            training_file_path = os.path.join(UPLOAD_FOLDER, '%s-training.json' % filename)
            with open(training_file_path, 'wb') as f:
                f.write(json.dumps(dedupers[deduper_id]['training_data'], default=_to_json))
            args = {
                'field_defs': field_defs,
                'file_path': file_path,
                'training_file': training_file_path,
            }
            print field_defs
            rv = dedupeit.delay(**args)
            flask_session['deduper_key'] = rv.key
            resp = make_response(json.dumps({'finished': True}))
            resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/dedupe-start/', methods=['POST'])
def dedupe_start():
    if not flask_session.get('session_id'):
        return redirect(url_for('index'))
    else:
        s = sql_session.query(DedupeSession).get(flask_session['session_id'])
        f = request.files.get('training_file')
        if f and allowed_file(f.filename):
            error = None
            training_filename = secure_filename('%s_%s' % (str(time.time()), f.filename))
            training_path = os.path.join(
                app.config['UPLOAD_FOLDER'], training_filename)
            f.save(training_path)
            s.training_file_path = training_path
            # TODO Get the fields from user input if no training file
            # is uploaded in the else clause below
            fields = {
                'Site name': {'type': 'String'},
                'Address': {'type': 'String'},
                'Zip': {'type': 'String', 'Has Missing' : True},
                'Phone': {'type': 'String', 'Has Missing' : True},
            }
            s.field_definitions = json.dumps(fields)
            sql_session.add(s)
            sql_session.commit()
            rv = dedupeit.delay(s.id)
            flask_session['deduper_key'] = rv.key
        else:
            error = 'Need a training file for now'
        context = {
            'filename': s.human_filename, 
            'fields': fields,
            'error': error,
            'working': True,
        }
        return render_app_template('training.html', **context)

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

# UTILITY
def render_app_template(template, **kwargs):
    '''Add some goodies to all templates.'''

    if 'config' not in kwargs:
        kwargs['config'] = app.config
    return render_template(template, **kwargs)

# INIT
if __name__ == "__main__":
    app.run(debug=True, port=9999)
