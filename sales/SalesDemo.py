import redis, argparse
from StringIO import StringIO
from redisearch import Client, TextField, NumericField


class SearchDemo: 
    def __init__(self, args):
        self.index = args.index
        self.client = Client(self.index, host=args.host, port=args.port)
        
    def create(self):
        try:
            self.client.drop_index()
        except:
            pass

        self.client.create_index([NumericField('ORDERNUMBER'), NumericField('QUANTITYORDERED', sortable=True),  NumericField('PRICEEACH', sortable=True), NumericField('ORDERLINENUMBER'), NumericField('SALES', sortable=True), TextField('ORDERDATE'), TextField('STATUS', sortable=True), NumericField('QTR_ID', sortable=True),  NumericField('MONTH_ID', sortable=True), NumericField('YEAR_ID', sortable=True), TextField('PRODUCTLINE', sortable=True), NumericField('MSRP', sortable=True), TextField('PRODUCTCODE', sortable=True), TextField('CUSTOMERNAME', sortable=True), TextField('PHONE'), TextField('ADDRESSLINE1'), TextField('ADDRESSLINE2'), TextField('CITY', sortable=True), TextField('STATE', sortable=True), TextField('POSTALCODE', sortable=True), TextField('COUNTRY', sortable=True), TextField('TERRITORY', sortable=True), TextField('CONTACTLASTNAME'), TextField('CONTACTFIRSTNAME'), TextField('DEALSIZE', sortable=True)])


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

