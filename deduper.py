import csv
import re
import os
import json
from dedupe import AsciiDammit
import dedupe
from cStringIO import StringIO
from collections import defaultdict
import logging
from datetime import datetime
from queue import queuefunc

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WebDeduper(object):
    
    def __init__(self, deduper,
            file_path=None, 
            training_data=None, 
            destroy_dupes=True):
        self.destroy_dupes = destroy_dupes
        self.file_path = file_path
        self.data_d = self.readData()
        self.deduper = deduper
        self.deduper.readTraining(training_data)
        self.deduper.train()
        self.settings_path = '%s-settings.dedupe' % file_path
        self.training_data = training_data
        self.deduper.writeTraining(self.training_data)
        self.deduper.writeSettings(self.settings_path)

    def dedupe(self):
        threshold = self.deduper.threshold(self.data_d, recall_weight=2)
        clustered_dupes = self.deduper.match(self.data_d, threshold)
        logging.info('clustering done')
        self.deduped_file_path = '%s-deduped.csv' % self.file_path
        self.deduped_unique_file_path = '%s-deduped_unique.csv' % self.file_path
        self.writeUniqueResults(clustered_dupes)
        self.writeResults(clustered_dupes)
        files = {
            'original': os.path.relpath(self.file_path, __file__),
            'training': os.path.relpath(self.training_data, __file__),
            'settings': os.path.relpath(self.settings_path, __file__),
            'deduped': os.path.relpath(self.deduped_file_path, __file__),
            'deduped_unique': os.path.relpath(self.deduped_unique_file_path, __file__),
        }
        logging.info(files)
        return files
    
    # ## Writing results
    def writeResults(self, clustered_dupes):
 
        # Write our original data back out to a CSV with a new column called 
        # 'Cluster ID' which indicates which records refer to each other.
 
        cluster_membership = defaultdict(lambda : 'x')
        for cluster_id, cluster in enumerate(clustered_dupes):
            for record_id in cluster:
                cluster_membership[record_id] = cluster_id
 
        writer = csv.writer(open(self.deduped_file_path, 'wb'))

        reader = csv.reader(open(self.file_path, 'rb'))
 
        heading_row = reader.next()
        heading_row.insert(0, 'Cluster ID')
        writer.writerow(heading_row)
 
        for i, row in enumerate(reader):
            row_id = i
            cluster_id = cluster_membership[row_id]
            row.insert(0, cluster_id)
            writer.writerow(row)
 
    # ## Writing results
    def writeUniqueResults(self, clustered_dupes):
 
        # Write our original data back out to a CSV with a new column called 
        # 'Cluster ID' which indicates which records refer to each other.
 
        cluster_membership = {}
        for (cluster_id, cluster) in enumerate(clustered_dupes):
            logging.info(cluster)
            for record_id in cluster:
                cluster_membership[record_id] = cluster_id
 
        writer = csv.writer(open(self.deduped_unique_file_path, 'wb'))
 
        reader = csv.reader(open(self.file_path, 'rb'))
 
        heading_row = reader.next()
        writer.writerow(heading_row)
 
        seen_clusters = set()
        for i, row in enumerate(reader):
            row_id = i
            if row_id in cluster_membership: 
                cluster_id = cluster_membership[row_id]
                if cluster_id not in seen_clusters:
                    writer.writerow(row)
                    seen_clusters.add(cluster_id)
            else:
                writer.writerow(row)

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
    
@queuefunc
def dedupeit(**kwargs):
    d = dedupe.Dedupe(kwargs['field_defs'], kwargs['data_sample'])
    logger.info(d.pool)
    deduper = WebDeduper(d, 
        file_path=kwargs['file_path'],
        training_data=kwargs['training_data'])
    files = deduper.dedupe()
    d.pool.terminate()
    del d
    return files

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
