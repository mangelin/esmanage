#!/usr/bin/env python3
from docopt import docopt
import elasticsearch
from elasticsearch import Elasticsearch
import json
import sys
import os

doc = """ElasticSearch Index Management

Usage:
    esmanage.py alias new <index> <name> [--host=<host>] [--port=<port>] [--timeout=<timeout>]
    esmanage.py alias delete <index> <name> [--host=<host>] [--port=<port>] [--timeout=<timeout>]
    esmanage.py alias move <index1> <index2> <name> [--host=<host>] [--port=<port>] [--timeout=<timeout>]
    esmanage.py alias list [--host=<host>] [--port=<port>] [--timeout=<timeout>]
    esmanage.py index new <name> <mappings> [--host=<host>] [--port=<port>] [--timeout=<timeout>]
    esmanage.py index delete <name> [--host=<host>] [--port=<port>] [--timeout=<timeout>]
    esmanage.py index update <name> <mapping> [--host=<host>] [--port=<port>] [--timeout=<timeout>]
    esmanage.py index list [--host=<host>] [--port=<port>] [--timeout=<timeout>]
    

Options:
    -h --help           Show this screen.
    --version           Show version.
    --host=<host>       Elasticsearch IP address or hostname [default: localhost]
    --port=<port>       Elasticsearch port [default: 9200]
    --timeout=<timeout> Operation timeout [default: 10]
"""

def get_index_client(host,port):
    return elasticsearch.client.IndicesClient(Elasticsearch(['%s:%s'%(host,port)]))


def create_index(index_name, mappings,host,port,timeout):
    index_client = get_index_client(host, port)

    if index_client.exists(index=index_name):
        msg = 'WARNING: index %s already exists'%index_name
        return False, msg

    with open(mappings,'r') as f:
        jbody = json.load(f)
        index_client.create(index=index_name,body=jbody,timeout=timeout)
    
    msg = 'index %s created'%index_name
    return True, msg

    
def delete_index(index_name,host,port,timeout):
    index_client = get_index_client(host, port)

    if not index_client.exists(index=index_name):
        msg = 'WARNING: index %s not exists'%index_name
        return False,msg

    index_client.delete(index=index_name,timeout=timeout)
    msg = "index %s deleted"%index_name
    return True,msg

def add_alias(index_name,alias_name,host,port,timeout):
    index_client = get_index_client(host, port)
    if index_client.exists_alias(index=index_name,name=alias_name):
        msg = 'WARNING: alias %s for index %s already exists'%(alias_name,index_name)
        #return False, msg

    res = index_client.put_alias(index=index_name,name=alias_name)
    msg = 'alias %s for index %s created'%(alias_name,index_name)
    return True, msg

def delete_alias(index_name,alias_name,host,port,timeout):
    index_client = get_index_client(host, port)
    
    if not index_client.exists_alias(index=index_name,name=alias_name):
        msg = 'WARNING: alias %s for index %s not exists'%(alias_name,index_name)
        return False, msg

    index_client.delete_alias(index=index_name,name=alias_name,timeout=timeout)
    msg = 'alias %s for index %s deleted'%(alias_name,index_name)    
    return True, msg
    

def move_alias(index_name1, index_name2, alias_name, host, port, timeout):
    index_client = get_index_client(host, port)
    res, msg_del = delete_alias(index_name1, alias_name,host,port,timeout)
    if not res:
        return res, msg_del

    res, msg_add = add_alias(index_name2,alias_name,host,port,timeout)
    return res, msg_del+' and '+msg_add

def index_list(host,port,timeout):
    index_client = get_index_client(host, port)
    res = index_client.get(index='*',expand_wildcards='all',human=True)
    msg = '\n'.join(res.keys())
    return True,msg

def alias_list(host,port,timeout):
    def process_line(n):
        index_name = n['key']
        aliases = n['value']['aliases'].keys()

        return index_name + ' ==> '+ ' '.join(aliases)

    def make_node(k,v,l):
        return {
            'key' : k + ' '*(l-len(k)),
            'value' : v
        }

    def get_max_length(indices):
        return max(list(map(len,indices)))

    def make_header(maxl):
        str1 = 'Index'
        str2 = 'Alias(s)'
        h = ' '*(int((maxl - len(str1)) / 2))+str1+' '*(int((maxl - len(str2)) / 2))+'|'+' '*(int((maxl - len(str2)) / 2))+str2+' '*(int((maxl - len(str2)) / 2))+'\n'
        h += '-'*(maxl*2 + len(str1) + len(str2))+'\n'
        return h

    index_client = get_index_client(host, port)
    res = index_client.get_alias(index='*',name='*',expand_wildcards='all')
    if res == {}:
        print('No aliases defined')
        return False, None
    max_length = get_max_length(res.keys())
    header = make_header(max_length)
    msg = '\n'.join(map(process_line,[make_node(k,res.get(k),max_length) for k in res.keys()]))
    return True,header+msg

        

if __name__ == "__main__":
    args = docopt(doc, version="ES IM 0.1")
    
    es_host = os.environ.get('ES_HOST')
    if es_host:
        args['--host'] = es_host


    try:
        if args['index']:
            if args['new']:
                res, msg = create_index(args['<name>'],args['<mappings>'],args['--host'],args['--port'],args['--timeout'])
                print(msg)
            elif args['delete']:
                res, msg = delete_index(args['<name>'],args['--host'],args['--port'],args['--timeout'])
                print(msg)
            elif args['list']:
                res, msg = index_list(args['--host'],args['--port'],args['--timeout'])
                print(msg)
        elif args['alias']:
            if args['new']:
                res, msg = add_alias(args['<index>'],args['<name>'],args['--host'],args['--port'],args['--timeout'])
                print(msg)
            elif args['delete']:
                res, msg = delete_alias(args['<index>'],args['<name>'],args['--host'],args['--port'],args['--timeout'])
                print(msg)
            elif args['move']:
                res, msg= move_alias(args['<index1>'],args['<index2>'],args['<name>'],args['--host'],args['--port'],args['--timeout'])
                print(msg)
            elif args['list']:
                res, msg= alias_list(args['--host'],args['--port'],args['--timeout'])
                print(msg)

    except elasticsearch.TransportError as e:
        print(e)
    except Exception as ge:
        print(ge)