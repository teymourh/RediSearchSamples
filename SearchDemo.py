import redis, csv, argparse, datetime
from StringIO import StringIO
from redisearch import Client, TextField, NumericField, TagField, Query, Result
from tabulate import tabulate
import readline

commands = ['FT.SEARCH', 'FT.AGGREGATE']
search_options = ['NOCONTENT', 'VERBATIM', 'NOSTOPWORDS', 'WITHSCORES', 'WITHPAYLOADS', 'WITHSORTKEYS', 'FILTER', 
            'GEOFILTER', 'INKEYS', 'INFIELDS', 'RETURN', 'SUMMARIZE', 'HIGHLIGHT', 'SLOP', 'LANGUAGE', 'EXPANDER',
            'SCORER', 'PAYLOAD', 'SORTBY', 'LIMIT']
aggregate_options = ['WITHSCHEMA', 'VERBATIM', 'LOAD', 'GROUPBY', 'REDUCE', 'SORTBY', 'APPLY', 'LIMIT', 'FILTER']
reduce_funcs =['COUNT', 'COUNT_DISTINCT', 'COUNT_DISTINCTISH', 'SUM', 'MIN', 'MAX', 'AVG', 'STDDEV', 'QUANTILE',
               'TOLIST', 'FIRST_VALUE', 'RANDOM_SAMPLE']
sortby_options = ['ASC', 'DESC']

def getfirst():
    idx = readline.get_begidx()
    full = readline.get_line_buffer()
    tokens = full[:idx].split()
    if len(tokens) > 0:
        return tokens[0]
    else:
        return ''

def getlast():
    idx = readline.get_begidx()
    full = readline.get_line_buffer()
    tokens = full[:idx].split()
    if len(tokens) > 0:
        return tokens[-1]
    else:
        return ''

def gettokens():
    idx = readline.get_begidx()
    full = readline.get_line_buffer()
    tokens = full[:idx].split()
    return tokens

def getparmnum():
    return len(gettokens())

def getparam(idx):
    return gettokens()[idx]

class SearchCompleter():
    def __init__(self, obj):
        self.obj = obj
        self.command = ''
        self.state = ''
        self.fields = 0
        self.tmp_fields = 0
    
    def getOptions(self, text, options, ignore=[]):
        options = [i for i in options if i not in ignore]
        if text:
            self.matches = [s for s in options if s and s.startswith(text.upper())]
        else:
            self.matches = options[:]
 
    def getState(self):
        tokens = gettokens()
        options = []
        if self.command == 'FT.SEARCH':
            options = search_options
        if  self.command == 'FT.AGGREGATE':
            options = aggregate_options 
        num = 0
        for t in reversed(tokens):
            if t.upper() in options:
                self.state = t.upper()
                return num
            num += 1
          
    
    def getCommandOptions(self, text):
         if self.command == 'FT.SEARCH':
             self.getOptions(text, search_options)
         elif  self.command == 'FT.AGGREGATE':
             self.getOptions(text, aggregate_options)

    def getFieldOptions(self, text, state, withat=False):
        fields = self.obj.fields
        if withat == True:
            fields = ['@' + f for f in fields]
        if getlast() == state:
            self.matches = []
        elif gettokens()[-2] == state:
            self.fields = int(getlast())
            self.tmp_fields = self.fields
            self.getOptions(text, fields)
        elif self.tmp_fields > 0:
            ignore = gettokens()[gettokens().index(state) + 2:]
            self.getOptions(text, fields, ignore=ignore)
            self.tmp_fields = self.fields - (getparmnum() - gettokens().index(state) - 2)
        else:
            self.matches = []
            return 0
        return 1

    def complete(self, text, state):
        response = None
        self.command = getfirst().upper()
        if getparmnum() == 0:
            self.getOptions(text, commands)
        elif getparmnum() == 1:
            self.getOptions(text, [self.obj.index])
        elif getparmnum() == 2:
            self.matches = [] 
        elif getparmnum() == 3:
            self.getCommandOptions(text)
        elif getparmnum() > 3:
            paramsnum = self.getState()
            ret = 1
            if self.state in ['RETURN', 'INKEYS', 'INFIELDS'] :
                ret = self.getFieldOptions(text, self.state)
            elif self.state == 'GROUPBY' or self.state == 'LOAD':
                ret = self.getFieldOptions(text, self.state, withat=True)
            elif self.state == 'REDUCE':
                if paramsnum == 0:
                    self.getOptions(text, reduce_funcs)
                elif paramsnum > 1:
                    func = gettokens()[-paramsnum]
                    ret = self.getFieldOptions(text, func, withat=True)
                    if ret == 0:
                        if getlast().upper() == 'AS':
                            self.matches = []
                            ret = 1
                        elif gettokens()[-2].upper() == 'AS':
                            ret = 0
                        else:
                            self.getOptions(text, ['AS'])
                            ret = 1
                else:
                    self.matches = []
            elif self.state == 'SORTBY':
                ret = self.getFieldOptions(text, self.state, withat=True)                   
                if ret > 0 and getlast() not in sortby_options and paramsnum > 1:
                    self.getOptions(text, self.matches + sortby_options)
            elif self.state == 'APPLY':
                if paramsnum == 1:
                    self.getOptions(text, ['AS'])
            else:
                self.matches = []
            if ret == 0:
                self.getCommandOptions(text)
        else:
            self.matches = []                
        try:
            response = self.matches[state]
        except IndexError:
            response = None
        return response 
    
class SearchDemo: 
    def __init__(self, args):
        self.index = args.index
        self.client = Client(self.index, host=args.host, port=args.port)
        self.redis = redis.Redis(args.host, args.port)
        self.fields = []
        info = self.client.info()['fields']
        for f in info:
            self.fields.append(f[0])

    def printSearchResults(self, total, docs, headers, duration, withid=False):
        print('Number of results: ' + str(total))
        if total == 0:
            return
        data = []
        for d in docs:
            row = []
            if withid == True:
                row.append(d.id)
            for f in headers:
                val = ''
                try:
                    val = getattr(d,f)
                except AttributeError:
                    pass
                if val is not None:
                    val = val.decode('utf-8')
                row.append(val)
            data.append(row)
        if withid == True:
            headers.insert(0, 'Document ID');
        print(tabulate(data, headers, tablefmt='grid', floatfmt=".2f"))
        print('Execution time : ' + str(duration) + ' ms')

    def getHeadersFromRow(self, row):
        headers = []
        i = 0
        for t in row:
            if i % 2 == 0:
                headers.append(t)
            i += 1
        return headers

    def getValuesFromRow(self, row, ignore):
        values =[]
        i = 0
        for t in row:
            if i % 2 == 1 and i not in ignore:
                if t is not None:
                    t = t.decode('utf-8')
                values.append(t)
            i += 1
        return values

    def getIgnoreList(self, headers):
        ignore = []
        ret = headers[:]
        i = 0
        for h in headers:
            if h.startswith('__generated_alias'):
                ignore.append(i + 1)
                ret.remove(h)
            i += 2
        return ret, ignore

    def printAggregateResult(self, res, duration, ignore_generated=True):
        num_results = res[0]
        print('Number of results: ' + str(res[0]))
        if num_results == 0:
            return 
        rows = res[1:]
        headers = self.getHeadersFromRow(rows[0])
        ignore = []
        if ignore_generated == True:
            headers, ignore = self.getIgnoreList(headers)    
        data = []
        for r in rows:
            data.append(self.getValuesFromRow(r, ignore))
        print(tabulate(data, headers, tablefmt='grid', floatfmt=".2f"))
        print('Execution time : ' + str(duration) + ' ms')

    def queryToArgs(self, query):
        query = " ".join(query.split())
        data = StringIO(query)
        reader = csv.reader(data, delimiter=' ') 
        return next(reader)

    def readQuery(self):
        query = raw_input('Enter query:')
        return query.strip()

    def executeQuery(self, query):
        res = None
        parts = self.queryToArgs(query)
        try:
            st = datetime.datetime.now()
            res = self.redis.execute_command(*parts)
            duration=(datetime.datetime.now() - st).total_seconds() * 1000
        except redis.exceptions.ResponseError, e:
            print('Error: ' + str(e))
            return
 
        nocontent = res[0] == 0 or not isinstance(res[2],(list,))
        if parts[0].upper().startswith('FT.SEARCH'):
            results = Result(res,
                  not nocontent,
                  duration=duration,
                  has_payload=False)
            if nocontent == True:
                self.printSearchResults(results.total, results.docs, [], duration, True)
            else:
                self.printSearchResults(results.total, results.docs, self.getHeadersFromRow(res[2]), duration, True)
        else:
            self.printAggregateResult(res, duration)

    def runDemo(self, queriesFile):
        f = open(queriesFile, 'r')
        for line in f.readlines():
            line=line.strip()
            if line == '':
                continue
            print(line)
            if line.startswith('#'):
                 continue
            raw_input("Press any key to execute the query...")
            print('\n')
            self.executeQuery(line)
            print('\n')
            raw_input("Press any key to go to the next query...")
            print('\n')

    def interactive(self):
        query = self.readQuery()
        while query.upper() != 'QUIT' and query.upper() != 'EXIT':
            if query != '':
                self.executeQuery(query)
            query = self.readQuery()

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
        '-d, --demo', dest='demo', type=str, help='Read queries from the specified file. Otherwise, runs in interactive mode')
    args= parser.parse_args()
   
    searchDemo = SearchDemo(args)
    readline.set_completer(SearchCompleter(searchDemo).complete) 
    readline.parse_and_bind('tab: complete')
    readline.parse_and_bind('set editing-mode vi')
    readline.set_completer_delims(' \t\n')

    if args.demo is not None:
        searchDemo.runDemo(args.demo)
    else:
        searchDemo.interactive()

if __name__ == '__main__':
    main()

