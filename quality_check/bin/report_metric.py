#!/usr/bin/env python
# encoding: utf-8

import base64
import json
import time
import datetime
import sys
import urllib.request as urllib

def report(d, ts=0):
    TSDB_URL = 'http://opentsdb.ha.nb.com:4242/api/put'
    if ts == 0:
        ts = int(time.time())
    metrics_data = []
    for k, v in d.items():
        metrics_data.append(
            {
                'metric': k,
                'value': v,
                'timestamp': ts,
                'tags': {'client': 'airflow'},
            }
        )
    request = urllib.Request(
        TSDB_URL,
        data=json.dumps(metrics_data).encode('utf-8'),
        headers={'content-type': 'application/json'},
    )
    authentication_data = 'feed_quality_check_metrics:write_key'
    auth_header = base64.b64encode(bytes(authentication_data.encode('utf-8'))).decode('utf-8')
    request.add_header('Authorization', 'Basic {}'.format(auth_header))
    urllib.urlopen(request, timeout=10)
