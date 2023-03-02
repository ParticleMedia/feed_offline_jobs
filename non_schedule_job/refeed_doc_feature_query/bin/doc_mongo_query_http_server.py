#!/usr/bin/env python
#-*- coding:UTF-8 -*-

import sys
import tornado.ioloop
import tornado.options
import tornado.web
import getopt
from configparser import ConfigParser
import logging
import os
import json
import requests
import traceback
import pickle
from pymongo import MongoClient
# import process
import time
import torch

class ProcessHandler(tornado.web.RequestHandler):

    def get_default_params(self, data_dict, field, default):
        result = data_dict.get(field, default)
        if result is None:
            result = default
        return result

    def post(self):
        response_dict = {'status': 'failed', 'reason': '', 'timeliness_type': '', 'expire_time': ''}
        _id = ''
        try:
            ##get parameters
            para_dict = json.loads(self.request.body)
            # logging.info('para_dic: ' + json.dumps(para_dict, ensure_ascii=False))
            _id = self.get_default_params(para_dict, '_id', None)
            if _id is not None:
                data_dict = staticfeature_col.find_one({'_id': _id})
                if data_dict is not None:
                    if 'timeliness_type' in data_dict and 'expire_time' in data_dict:
                        response_dict['timeliness_type'] = data_dict['timeliness_type']
                        response_dict['expire_time'] = data_dict['expire_time']
                        response_dict['status'] = 'success'
        except Exception as e:
            msg = str(traceback.format_exc())
            response_dict['reason'] = msg

        response_dict_string = json.dumps(response_dict, ensure_ascii=False)
        res = 'id={}\tresponse={}'.format(_id, response_dict_string)
        logging.info(res)
        self.finish(response_dict_string)

    def get(self):
        response_dict = {'status': 'failed', 'reason': '', 'timeliness_type': '', 'expire_time': ''}
        _id = ''
        try:
            ##get parameters
            _id = self.get_argument('_id', None)
            if _id is not None:
                data_dict = staticfeature_col.find_one({'_id': _id})
                if data_dict is not None:
                    if 'timeliness_type' in data_dict and 'expire_time' in data_dict:
                        response_dict['timeliness_type'] = data_dict['timeliness_type']
                        response_dict['expire_time'] = data_dict['expire_time']
                        response_dict['status'] = 'success'
        except Exception as e:
            msg = str(traceback.format_exc())
            response_dict['reason'] = msg

        response_dict_string = json.dumps(response_dict, ensure_ascii=False)
        res = 'id={}\tresponse={}'.format(_id, response_dict_string)
        logging.info(res)
        self.finish(response_dict_string)

def run():
    app = tornado.web.Application(
        [
         (r"/api/v0/timeliness", ProcessHandler),
         ])

    server = tornado.httpserver.HTTPServer(app)
    #logService.info('Instance {0} start successfully'.format(SERVER_PORT))
    print('Instance {0} start successfully'.format(SERVER_PORT))
    server.bind(SERVER_PORT)
    server.start(1)
    tornado.ioloop.IOLoop.instance().start()

def usage():
    print('usage: python server.py -p 9118')

if __name__ == "__main__":
    global SERVER_PORT
    global staticfeature_col

    staticfeature_col = MongoClient('172.31.29.170', 27017, unicode_decode_error_handler='ignore')['staticFeature']['document']

    logging.basicConfig(filename='../log/info.log', filemode="a", format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    try:
        if(len(sys.argv[1:]) == 0):
            usage()
        opts, args = getopt.getopt(sys.argv[1:], 'hf:p:')
        configfile = ''
        for opt, arg in opts:
            if opt == '-p':
                SERVER_PORT = int(arg)
            elif opt == '-h':
                usage()
            else:
                usage()
        run()
    except (IOError, getopt.GetoptError) as err:
        print(str(traceback.format_exc()))
        exit(1)
