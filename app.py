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
from cStringIO import StringIO
import csv
from deduper import dedupeit
from models import DedupeSession
from queue import DelayedResult
from redis import Redis

redis = Redis()

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')
ALLOWED_EXTENSIONS = set(['csv', 'json'])

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.config['REDIS_QUEUE_KEY'] = 'deduper'
app.secret_key = os.environ['FLASK_KEY']
engine = create_engine('sqlite:///deduper.db')
Session = sessionmaker(bind=engine)
sql_session = Session()

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
            filename = secure_filename(str(time.time()) + "_" + f.filename)
            inp = StringIO(f.read())
            reader = csv.reader(inp)
            fields = reader.next()
            f.seek(0)
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], filename)
            f.save(file_path)
            sess_data = {
                'file_path': file_path,
                'human_filename': f.filename,
                'csv_header': ','.join(fields),
                'uploaded_date': datetime.now(),
                'user_agent': request.headers.get('User-Agent'),
                'ip_address': request.remote_addr,
            }
            s = DedupeSession(**sess_data)
            sql_session.add(s)
            sql_session.commit()
            flask_session['session_id'] = s.id
            return redirect(url_for('training'))
        else:
            # probably need to make sure to handle the error in the template
            error = 'Error uploading file'
            status_code = 500
    return make_response(render_app_template('index.html', error=error), status_code)

@app.route('/training/', methods=['GET', 'POST'])
def training():
    if not flask_session.get('session_id'):
        return redirect(url_for('index'))
    else:
        s = sql_session.query(DedupeSession).get(flask_session['session_id'])
        error = None
        if request.method == 'POST':
            f = request.files.get('training_file')
            if f and allowed_file(f.filename):
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
                return redirect(url_for('working'))
            else:
                error = 'Need a training file for now'
        context = {
            'filename': s.human_filename, 
            'fields': None,
            'error': None,
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
