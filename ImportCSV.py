
import sys, argparse, csv, re
from datetime import datetime
from redisearch import Client

class CSVImporter: 
    def __init__(self, args):
        self.host = args.host
        self.port = args.port
        self.index = args.index
        self.file = open(args.file, 'r')
        self.delimiter = args.delimiter
        self.rows = args.rows
        self.hasHeader = args.header
        self.ignore = args.ignore
        self.docid = args.docid
        self.nosave = args.nosave
        self.date = args.date
        self.format = args.format
        self.client  = Client(self.index, self.host, self.port)
        self.fields = self.client.info()['fields']
    
    def dateToMillis(self, val):
        try:
            d = datetime.strptime(val, self.format)
        except ValueError:
            print("Invalid data format: " + val)
            return 0
        return str(int(d.strftime('%s')) * 1000)
    
    def adjustTagValue(self, val, sep):
        i = 0
        insinglequotes = False
        indoublequotes = False
        newTag = False
        newVal = ''
        while i < len(val):
            if val[i] == '\'' and not indoublequotes :
                insinglequotes = not insinglequotes
            elif val[i] == '"' and not insinglequotes:
               indoublequotes = not indoublequotes;
            else: 
                if val[i] == ',' and not insinglequotes and not indoublequotes:
                    newVal += sep
                else:
                    newVal += val[i]
            i += 1
        newVal = re.sub('[\t ]*' + sep + '[\t ]*', sep, newVal)
        return re.sub('[\[\]]', '', newVal)
                
        
    def addRow(self, row, num):
        args = {}
        idx = 0
        fieldnum = 0
        for val in row:
            idx += 1
            if self.ignore is not None and idx in self.ignore or idx == self.docid:
                continue
            if self.date is not None and idx in self.date:
                val = self.dateToMillis(val)
            if self.fields[fieldnum][2] == 'TAG':
                val = self.adjustTagValue(val, self.fields[fieldnum][4]) 
            args[self.fields[fieldnum][0]] = val
            fieldnum += 1
        
        doc = 'doc-' + str(num)
        if self.docid > 0:
            doc = row[self.docid - 1]
        self.client.add_document(doc, replace=True, nosave=self.nosave, **args)
         
    def loafFile(self):
        reader = csv.reader(self.file, delimiter=self.delimiter)
        if self.hasHeader == True:
            next(reader)
        n = 0
        for row in reader:
            if self.rows > 0 and n == self.rows:
                break
            self.addRow(row, n)
            n += 1
        print('Finished loading ' + str(n) + ' rows.')
        
def main():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
        description=__doc__)
    parser.add_argument(
        '-s, --host', dest='host', type=str, help='Server address',
        default='localhost')
    parser.add_argument(
        '-p, --port', dest='port', type=int, help='Server port',
        default=6379)
    parser.add_argument(
        '-i, --index', dest='index', type=str,  help='Index name', required=True)
    parser.add_argument(
        '-f, --file', dest='file', type=str,  help='CSV file path', required=True)
    parser.add_argument(
        '-d, --delimiter', dest='delimiter', type=str, help='Delimiter',
        default=',')
    parser.add_argument(
        '-n, --rows', dest='rows', type=int, help='Number of rows to import',
        default=0)
    parser.add_argument(
        '-r, --header', dest='header', type=bool, help='Contains a header row',
        default=False)
    parser.add_argument(
        '-g, --ignore', dest='ignore', type=int, nargs='+', help='Ignore columns') 
    parser.add_argument(
        '-c, --doc-id', dest='docid', type=int, help='Document id column number',
        default=0)
    parser.add_argument(
        '-t, --datetime', dest='date', type=int, nargs='+', help='Date time columns that need to be converted to milliseconds based on provided format') 
    parser.add_argument(
        '-o', '--date-format', dest='format', type=str, help='Date time format') 
    parser.add_argument(
        '-v, --no-save', dest='nosave', type=bool, help='Do not save the data',
        default=False)
    args= parser.parse_args()
    if args.date is not None and args.format is None:
        parser.print_usage()
        print("ERROR: Missing required date time format.")
        sys.exit(-1)
    csv = CSVImporter(args)
    csv.loafFile()

if __name__ == '__main__':
    main()

