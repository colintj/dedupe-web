import csv
import re
import json
from dedupe import AsciiDammit
import dedupe
from cStringIO import StringIO
from collections import defaultdict
import logging
from queue import queuefunc
from numpy import nan
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WebDeduper(object):
    
    def __init__(self, field_defs=None, file_path=None, training_file=None):
        self.file_path = file_path
        self.training_file = training_file
        self.deduper = dedupe.Dedupe(field_defs)
        data_d = self.readData()
        self.deduper.sample(data_d, 150000)

    def dedupe(self):
        self.deduper.readTraining(self.training_file)
        self.deduper.train()
        threshold = self.deduper.threshold(data_d, recall_weight=2)
        clustered_dupes = self.deduper.match(data_d, threshold)
        deduped_file_path = '%s-deduped.csv' % os.path.dirname(self.file_path)
        outp = open(deduped_file_path, 'wb')
        membership = defaultdict(lambda: 'x')
        for (cluster_id, cluster) in enumerate(clustered_dupes):
            for record_id in cluster:
                membership[record_id] = cluster_id
        writer = csv.writer(outp)
        with open(self.session.file_path, 'rU') as f_input:
            reader = csv.reader(f_input)
            header = reader.next()
            header.insert(0,'Cluster ID')
            writer.writerow(header)
            for row in reader:
                row_id = int(row[0])
                cluster_id = membership[row_id]
                row.insert(0, cluster_id)
                writer.writerow(row)
        return deduped_file_path
    
    def preProcess(self, column):
        column = AsciiDammit.asciiDammit(column)
        column = re.sub('  +', ' ', column)
        column = re.sub('\n', ' ', column)
        column = column.strip().strip('"').strip("'").lower().strip()
        return column

    def readData(self):
        data = {}
        f = open(self.file_path, 'rU')
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            clean_row = [(k, self.preProcess(v)) for (k,v) in row.items()]
            row_id = i
            data[row_id] = dedupe.core.frozendict(clean_row)
        return data
    
    def sameOrNotComparator(field_1, field_2):
        if field_1 and field_2 :
            if field_1 == field_2 :
                return 1
            else:
                return 0
        else :
            return nan

@queuefunc
def dedupeit(**kwargs):
    deduper = WebDeduper(**kwargs)
    return deduper.dedupe()

if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultHelpFormatter
    )
    parser.add_argument('--session_id', type=int,
        help='Database row id for session')
    args = parser.parse_args()
    engine = create_engine('sqlite:///deduper.db')
    Session = sessionmaker(bind=engine)
    sql_session = Session()
    deduper = WebDeduper(args, sql_session)
    deduper.dedupe()
