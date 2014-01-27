import csv
import AsciiDammit
import dedupe
from cStringIO import StringIO()

class WebDeduper(object):
    
    def __init__(self, filename, fields, training_file=None):
        self.fields = dict((field, {'type': 'String'}) for field in fields)
        self.filename = filename
        self.training_file = training_file
        self.outp = StringIO()
    
    def dedupe(self):
        data_d = self.readData()
        data_sample = dedupe.dataSample(data_d, 150000)
        deduper = dedupe.Dedupe(self.fields)
        deduper.train(data_sample, self.labeler)
        deduper.writeTraining(self.outp)
        return self.outp.getvalue()
    
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

