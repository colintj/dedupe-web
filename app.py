from flask import Flask, request, make_response, render_template, session, \
    redirect, url_for
from werkzeug import secure_filename
import time
import json
import requests
import re
import os
from cStringIO import StringIO
import csv

UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), 'upload_data')
ALLOWED_EXTENSIONS = set(['csv'])

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = 16 * 1024 * 1024
app.secret_key = os.environ['FLASK_KEY']

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@app.route('/', methods=['GET', 'POST'])
def index():
    status_code = 200
    if request.method == 'POST':
        f = request.files['input_file']
        if f and allowed_file(f.filename):
            error = None
            filename = secure_filename(str(time.time()) + "_" + f.filename)
            inp = StringIO(f.read())
            reader = csv.reader(inp)
            fields = reader.next()
            f.seek(0)
            f.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
            session['filename'] = {'human': f.filename, 'machine': filename}
            session['fields'] = fields
            return redirect(url_for('training'))
        else:
            error = 'Error uploading file'
            status_code = 500
    else:
        filename = None
        fields = None
        error = None
    context = {
        'filename': filename, 
        'fields': fields,
        'error': error,
    }
    return make_response(render_app_template('index.html', **context), status_code)

@app.route('/training/')
def training():
    if not session.get('filename'):
        return redirect(url_for('index'))
    else:
        filename = session.get('filename')
        fields = session.get('fields')
        return render_app_template('training.html', filename=filename, fields=fields)

# UTILITY
def render_app_template(template, **kwargs):
    '''Add some goodies to all templates.'''

    if 'config' not in kwargs:
        kwargs['config'] = app.config
    return render_template(template, **kwargs)

# INIT
if __name__ == "__main__":
    app.run(debug=True, port=9999)
