from flask import Flask, request, make_response, render_template, \
    session as flask_session, redirect, url_for, send_from_directory, jsonify
from werkzeug import secure_filename
import time
from datetime import datetime
import json
import requests
import re
import os
import copy
from dedupe import AsciiDammit
from dedupe.serializer import _to_json, dedupe_decoder
import dedupe
from deduper import dedupeit, DedupeShell, object_echo
from cStringIO import StringIO
import csv
from queue import DelayedResult
import collections
from redis import Redis

redis = Redis()

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')
ALLOWED_EXTENSIONS = set(['csv', 'json'])

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['REDIS_QUEUE_KEY'] = 'deduper'
app.secret_key = os.environ['FLASK_KEY']

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
            fname = secure_filename(str(time.time()) + "_" + f.filename)
            d = DedupeShell(raw=f.read(), filename=fname)
            deduper_id = object_echo.delay(d)
            flask_session['deduper_id'] = deduper_id.key
            return redirect(url_for('select_fields'))
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
    if not flask_session.get('deduper_id'):
        return redirect(url_for('index'))
    else:
        deduper_id = flask_session['deduper_id']
        while True:
            dedupe_shell = DelayedResult(deduper_id).return_value
            if dedupe_shell:
                break
            else:
                continue
        inp = StringIO(dedupe_shell.raw)
        filename = dedupe_shell.filename
        reader = csv.reader(inp)
        fields = reader.next()
        inp = StringIO(dedupe_shell.raw)
        if request.method == 'POST':
            field_list = [r for r in request.form]
            if field_list:
                training = True
                field_defs = {}
                for field in field_list:
                    field_defs[field] = {'type': 'String'}
                data_d = readData(inp)
                kwargs = {
                    "field_defs": copy.deepcopy(field_defs),
                }
                deduper = dedupe.Dedupe(field_defs)
                deduper.sample(data_d, 150000)
                kwargs['sample'] = deduper.data_sample
                dedupe_shell.add_fields(**kwargs)
                flask_session['deduper_id'] = object_echo.delay(dedupe_shell).key
                return redirect(url_for('training_run'))
            else:
                error = 'You must select at least one field to compare on.'
                status_code = 500
        return render_app_template('select_fields.html', error=error, fields=fields, filename=filename)

@app.route('/training_run/')
def training_run():
    if not flask_session.get('deduper_id'):
        return redirect(url_for('index'))
    else:
        deduper_id = flask_session['deduper_id']
        while True:
            d = DelayedResult(deduper_id).return_value
            if d:
                break
            else:
                continue
        filename = d.filename
        flask_session['deduper_id'] = object_echo.delay(dedupe_shell).key
        return render_app_template('training_run.html', filename=filename)

@app.route('/get-pair/')
def get_pair():
    if not flask_session.get('deduper_id'):
        return make_response(jsonify(status='error', message='need to start a session'), 400)
    else:
        deduper_id = flask_session['deduper_id']
        while True:
            d = DelayedResult(deduper_id).return_value
            if d:
                break
            else:
                continue
        deduper = dedupe.Dedupe(d.field_defs, data_sample=d.sample)
        if d.training_file:
            deduper.readTraining(d.training_file)
        filename = d.filename
        fields = deduper.data_model.comparison_fields
        record_pair = deduper.getUncertainPair()[0]
        d.set_current_pair(record_pair)
        data = []
        left, right = record_pair
        for field in fields:
            d = {
                'field': field,
                'left': left[field],
                'right': right[field],
            }
            data.append(d)
        flask_session['deduper_id'] = object_echo.delay(dedupe_shell).key
        resp = make_response(json.dumps(data))
        resp.headers['Content-Type'] = 'application/json'
        return resp

@app.route('/mark-pair/')
def mark_pair():
    if not flask_session.get('session_id'):
        return make_response(jsonify(status='error', message='need to start a session'), 400)
    else:
        action = request.args['action']
        deduper_id = flask_session['deduper_id']
        while True:
            d = DelayedResult(deduper_id).return_value
            if d:
                break
            else:
                continue
        current_pair = d.current_pair
        if not d.counter:
            d.set_counter({'yes': 0, 'no': 0, 'unsure': 0})
        if not d.training_dict:
            d.set_training({'distinct' : [], 'match' : []})
        if action == 'yes':
            d.training_dict['match'].append(d.current_pair)
            d.counter['yes'] += 1
            resp = {'counter': d.counter}
        elif action == 'no':
            d.training_dict['distinct'].append(d.current_pair)
            d.counter['no'] += 1
            resp = {'counter': d.counter}
        elif action == 'finish':
            file_path = os.path.join(UPLOAD_FOLDER, d.filename)
            with open(file_path, 'wb') as f:
                f.write(d.raw)
            rv = dedupeit.delay(d)
            flask_session['deduper_key'] = rv.key
            resp = {'finished': True}
        else:
            d.counter['unsure'] += 1
            resp = {'counter': d.counter}
        deduper = dedupe.Dedupe(field_definitions=d.field_defs, data_sample=d.sample)
        if d.training_file:
            deduper.readTraining(d.training_file)
        deduper.markPairs(labels)
        flask_session['deduper_id'] = object_echo.delay(dedupe_shell).key
    resp = make_response(json.dumps(resp))
    resp.headers['Content-Type'] = 'application/json'
    return resp

@app.route('/dedupe_finished/')
def dedupe_finished():
  return render_app_template("dedupe_finished.html")

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

# UTILITY
def render_app_template(template, **kwargs):
    '''Add some goodies to all templates.'''

    if 'config' not in kwargs:
        kwargs['config'] = app.config
    return render_template(template, **kwargs)

# INIT
if __name__ == "__main__":
    app.run(debug=True, port=9999)
