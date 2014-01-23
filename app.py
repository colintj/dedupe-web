from flask import Flask, request, make_response, render_template
from werkzeug import secure_filename
import time
import json
import requests
import re
import os

UPLOAD_FOLDER = 'upload_data'
ALLOWED_EXTENSIONS = set(['csv'])

app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER

def allowed_file(filename):
    return '.' in filename and \
           filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

# ROUTES
@app.route('/dedupe/', methods=['POST'])
def start_dedupe():
  f = request.files['input_file']
  if f and allowed_file(f.filename):
    filename = secure_filename(str(time.time()) + "_" + f.filename)
    f.save(os.path.join(app.config['UPLOAD_FOLDER'], filename))
    return make_response(json.dumps(request.form))
              
  else:
    return make_response('Error uploading file', 500)


@app.route('/')
def index():
  return render_app_template('index.html')

# UTILITY
def render_app_template(template, **kwargs):
    '''Add some goodies to all templates.'''

    if 'config' not in kwargs:
        kwargs['config'] = app.config
    return render_template(template, **kwargs)

# INIT
if __name__ == "__main__":
    app.run(debug=True, port=9999)
