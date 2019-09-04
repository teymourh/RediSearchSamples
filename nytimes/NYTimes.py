import redis, argparse
from StringIO import StringIO
from redisearch import Client, TextField, NumericField, TagField


class SearchDemo: 
    def __init__(self, args):
        self.index = args.index
        self.client = Client(self.index, host=args.host, port=args.port)
        
    def create(self):
        try:
            self.client.drop_index()
        except:
            pass

        self.client.create_index([NumericField('WORDCOUNT', sortable=True), TextField('BYLINE', no_stem=True, sortable=True), TextField('DOCUMENTTYPE', sortable=True), TextField('HEADLINE', sortable=True), TagField('KEYWORDS', separator=';'), NumericField('MULTIMEDIA', sortable=True), TextField('NEWDESK', sortable=True),  NumericField('PRINTPAGE', sortable=True), NumericField('PUBDATE', sortable=True), TextField('SECTIONNAME', sortable=True),  TextField('SNIPPET', sortable=True), TextField('TYPEOFMATERIAL', sortable=True), TextField('WEBURL')])


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
        '-d, --demo', dest='demo',  action='store_true', help='Run in demo mode. Otherwise, runs in interactive mode')
    parser.add_argument(
        '-c, --create', dest='create',  action='store_true', help='Create the index')
    args= parser.parse_args()
    searchDemo = SearchDemo(args)
    if args.create == True:
        searchDemo.create()

if __name__ == '__main__':
    main()

