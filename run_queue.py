from app import app
from queue import queue_daemon

queue_daemon(app)
