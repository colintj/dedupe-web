# Spreadsheet Deduper

Dedupe files via a web interface

### Setup

*Install OS level dependencies:* 

* Python 2.7
* Redis

*``pip install`` app requirements*

```bash
$ pip install "numpy>=1.6"
$ pip install -r requirements.txt
```

### Running the app

To run this app locally, make sure you have your Redis Server running on the
default port (6379). You’ll then need to have ``app.py`` and ``run_queue.py``
running at the same time.

## Community
* [Dedupe Google group](https://groups.google.com/forum/?fromgroups=#!forum/open-source-deduplication)
* IRC channel, #dedupe on irc.freenode.net
