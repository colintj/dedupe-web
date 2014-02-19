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
from cStringIO import StringIO
import csv
from uuid import uuid4
import collections
from multiprocessing.connection import Client, Listener
from run_queue import TRAINING_RECV

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')
ALLOWED_EXTENSIONS = set(['csv', 'json'])

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['REDIS_QUEUE_KEY'] = 'deduper'
app.secret_key = os.environ['FLASK_KEY']

import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def get_client():
    while True:
        try:
            client = Client(TRAINING_RECV)
            break
        except Exception as e:
            time.sleep(1)
            continue
    return client

def send_msg(client, msg):
    while True:
        try:
            client.send(msg)
            break
        except IOError as e:
            client = Client(TRAINING_RECV)
            time.sleep(1)
            continue

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
            d = {
                'step': 1,
                'deduper_id': str(uuid4()),
                'csv': f.read(),
                'filename': secure_filename(str(time.time()) + "_" + f.filename)
            }
            inp = StringIO(d['csv'])
            filename = d['filename']
            reader = csv.reader(inp)
            fields = reader.next()
            client = get_client()
            send_msg(client, d)
            flask_session['session_id'] = d['deduper_id']
            flask_session['filename'] = d['filename']
            flask_session['fields'] = fields
            return redirect(url_for('select_fields'))
        else:
            error = 'Error uploading file. Did you forget to select one?'
            status_code = 500
    return make_response(render_app_template('index.html', error=error), status_code)

@app.route('/select_fields/', methods=['GET', 'POST'])
def select_fields():
    status_code = 200
    error = None
    if not flask_session.get('session_id'):
        return redirect(url_for('index'))
    else:
        deduper_id = flask_session['session_id']
        fields = flask_session['fields']
        filename = flask_session['filename']
        if request.method == 'POST':
            field_list = [r for r in request.form]
            if field_list:
                d = {
                    'step': 2,
                    'deduper_id': deduper_id,
                    'field_list': field_list
                }
                client = get_client()
                send_msg(client, d)
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
        for port in range(6002, 6100):
            try:
                listener = Listener(('localhost', port), authkey=deduper_id)
                break
            except:
                continue
        client = get_client()
        send_msg(client, {'deduper_id': deduper_id, 'step': 'get_pair', 'port': port})
        try:
            conn = listener.accept()
            record_pair, fields = conn.recv()
        except EOFError:
            listener.close()
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
        resp = make_response(jsonify(status='error', message='need to start a session'), 400)
    else:
        action = request.args['action']
        deduper_id = flask_session['session_id']
        client = get_client()
        if flask_session.get('counter'):
            counter = flask_session['counter']
        else:
            counter = {'yes': 0, 'no': 0, 'unsure': 0}
        if action == 'yes':
            counter['yes'] += 1
            send_msg(client, {'deduper_id': deduper_id, 'action': 'yes', 'step': 'mark_pair'})
        elif action == 'no':
            counter['no'] += 1
            send_msg(client, {'deduper_id': deduper_id, 'action': 'no', 'step': 'mark_pair'})
        elif action == 'finish':
            send_msg(client, {'deduper_id': deduper_id, 'step': 'finish'})
        else:
            send_msg(client, {'deduper_id': deduper_id, 'action': 'unsure', 'step': 'mark_pair'})
            counter['unsure'] += 1
        flask_session['counter'] = counter
        resp = make_response(json.dumps({'counter': counter}))
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
    deduper_id = flask_session['session_id']
    files = [t for t in os.listdir(UPLOAD_FOLDER) if t.startswith(deduper_id)]
    if len(files) is 4:
        deduped = os.path.join(UPLOAD_FOLDER, 
            [f for f in files if f.endswith('deduped.csv')][0])
        deduped_unique = os.path.join(UPLOAD_FOLDER, 
            [f for f in files if f.endswith('deduped_unique.csv')][0])
        training = os.path.join(UPLOAD_FOLDER, 
            [f for f in files if f.endswith('training.json')][0])
        msg = {
            'deduped': os.path.relpath(deduped, __file__),
            'deduped_unique': os.path.relpath(deduped_unique, __file__),
            'training': os.path.relpath(training, __file__),
        }
        return jsonify(ready=True, result=msg)
    else:
        return jsonify(ready=False)

# UTILITY
def render_app_template(template, **kwargs):
    '''Add some goodies to all templates.'''

    if 'config' not in kwargs:
        kwargs['config'] = app.config
    return render_template(template, **kwargs)

# INIT
if __name__ == "__main__":
    app.run(debug=True, port=9999)
