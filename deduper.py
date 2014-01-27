import csv
import re
from dedupe import AsciiDammit
import dedupe
from cStringIO import StringIO
from collections import defaultdict
import logging
from models import DedupeSession, Field

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

class WebDeduper(object):
    
    def __init__(self, args, sql_session):
        self.session = sql_session.query(DedupeSession).get(args.session_id)
    
    def dedupe(self):
        data_d = self.readData()
        deduper = dedupe.Dedupe(self.fields)
        deduper.sample(data_d, 150000)
        if self.training_file:
            deduper.readTraining(self.training_file)
        deduper.train()
       #outp = StringIO()
       #deduper.writeTraining(outp)
       #self.training_data = outp.getvalue()
        threshold = deduper.threshold(data_d, recall_weight=2)
        clustered_dupes = deduper.match(data_d, threshold)
        outp = StringIO()
        membership = defaultdict(lambda: x)
        for (cluster_id, cluster) in enumerate(clustered_dupes):
            for record_id in cluster:
                cluster_membership[record_id] = cluster_id
        writer = csv.writer(outp)
        with open(self.filename, 'rU') as f_input:
            reader = csv.reader(f_input)
            header = reader.next()
            header.insert(0,'Cluster ID')
            writer.writerow(header)
            for row in reader:
                row_id = int(row[0])
                cluster_id = membership[row_id]
                row.insert(0, cluster_id)
                writer.writerow(row)
        return outp.getvalue()
    
    def preProcess(self, column):
        column = AsciiDammit.asciiDammit(column)
        column = re.sub('  +', ' ', column)
        column = re.sub('\n', ' ', column)
        column = column.strip().strip('"').strip("'").lower().strip()
        return column

    def readData(self):
        data = {}
        f = open(self.filename, 'rU')
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            clean_row = [(k, self.preProcess(v)) for (k,v) in row.items()]
            row_id = i
            data[row_id] = dedupe.core.frozendict(clean_row)
        return data

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
