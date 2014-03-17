# Spreadsheet Deduper

Dedupe files via a web interface

### Setup

**Install OS level dependencies:** 

* Python 2.7
* Redis

**Install app requirements**

```bash
$ pip install "numpy>=1.6"
$ pip install -r requirements.txt
```

### Running the app

There are three components that should be running simultaneously for the app to
work: Redis, the Flask app, and the worker process that actually does the final
deduplication:

``` bash 
$ redis-server # This command may differ depending on your OS
$ nohup python run_queue.py &
$ python app.py
```

For debugging purposes, it is useful to run these three processes in separate
terminal sessions. 

## Community
* [Dedupe Google group](https://groups.google.com/forum/?fromgroups=#!forum/open-source-deduplication)
* IRC channel, #dedupe on irc.freenode.net
