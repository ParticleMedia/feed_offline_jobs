#!/bin/python
import sys
import os
import datetime
import logging
import time
from report_metric import report
import collections

report_status_set = {'ok', 'poor', 'detrimental'}

def ts_to_timestr(ts):
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))

def timestr_to_ts(time_str):
    return time.mktime(time.strptime(time_str, "%Y-%m-%d"))

def load_data(filename_list):
    succ_cnt = 0
    wrong_cnt = 0
    status_dict = {}
    status_reason_dict = {}
    succ_uniq_docid = set()
    uniq_status_dict = collections.defaultdict(int)

    for filename in filename_list:
        if not os.path.exists(filename):
            print('load file {}: file not found'.format(filename))
            continue
        with open(filename, 'r') as f:
            for line in f.readlines():
                line = line.strip()
                elems = line.split("\t")
                if len(elems) != 12:
                    logging.info("wrong:{}".format(line))
                    wrong_cnt += 1
                    continue
                docid = elems[1]
                status = elems[10]
                reason = elems[11]

                if status == 'not_reviewed':
                    logging.info("not_reviewed:{}".format(line))
                    wrong_cnt += 1
                    continue

                if status not in status_dict:
                    status_dict[status] = 0
                    status_reason_dict[status] = {}
                if reason not in status_reason_dict[status]:
                    status_reason_dict[status][reason] = 0

                status_dict[status] += 1
                status_reason_dict[status][reason] += 1
                if docid not in succ_uniq_docid:
                    succ_uniq_docid.add(docid)
                    uniq_status_dict[status] += 1
                succ_cnt += 1
        print('load file {}: succ={} wrong={} status={} status_reason={} uniq_docid={} uniq_status={}'.format(filename, succ_cnt, wrong_cnt, len(status_dict), len(status_reason_dict), len(succ_uniq_docid), len(uniq_status_dict)))
    print('finished load. file_size={}'.format(len(filename_list)))
    return succ_cnt, wrong_cnt, status_dict, status_reason_dict, succ_uniq_docid, uniq_status_dict

def process_day_range(filename, out_f, day_range):
    day = filename.split('/')[-2].strip()
    exp = filename.split('/')[-1].replace('read.data', '').strip().strip('.')
    if len(exp) == 0:
        exp = 'online'

    if day_range <= 1:
        METRIC_PREFIX = 'feed_quanlity_check_' + exp
    else:
        METRIC_PREFIX = 'feed_quanlity_check_{}d_{}'.format(day_range, exp)
    out_f.write('run day={}\n'.format(day))
    out_f.write('run exp={}\n'.format(exp))
    out_f.write('run metrix_prefix={}\n'.format(METRIC_PREFIX))

    filename_list = []
    if day_range <= 1:
        filename_list = [filename]
    else:
        day_dt = datetime.datetime.strptime(day, '%Y-%m-%d')
        for i in range(day_range):
            data_dt = day_dt - datetime.timedelta(i)
            cur_day = data_dt.strftime('%Y-%m-%d')
            cur_file = filename.replace(day, cur_day)
            filename_list.append(cur_file)

    succ_cnt, wrong_cnt, status_dict, status_reason_dict, succ_uniq_docid, uniq_status_dict = load_data(filename_list)

    if succ_cnt + wrong_cnt == 0:
        logging.info('empty file or file not found')
        return False

    out_f.write("\ntotal_cnt: {}\n".format(succ_cnt + wrong_cnt))
    out_f.write("succ_cnt: {}\n".format(succ_cnt))
    out_f.write("wrong_cnt: {}\n".format(wrong_cnt))
    report({'{}_count_total'.format(METRIC_PREFIX) : succ_cnt + wrong_cnt}, timestr_to_ts(day))
    report({'{}_count_succ'.format(METRIC_PREFIX) : succ_cnt}, timestr_to_ts(day))
    report({'{}_count_wrong'.format(METRIC_PREFIX) : wrong_cnt}, timestr_to_ts(day))
    report({'{}_count_succ_uniq_doc'.format(METRIC_PREFIX) : len(succ_uniq_docid)}, timestr_to_ts(day))
    report({'{}_rate_succ'.format(METRIC_PREFIX) : succ_cnt / float(succ_cnt + wrong_cnt)}, timestr_to_ts(day))

    if succ_cnt == 0:
        logging.info("no succ data")
        return False

    out_f.write("\ntotal_status_cnt:\n")
    for status, cnt in status_dict.items():
        rate = cnt / float(succ_cnt)
        out_f.write("{:20s}\t{}\t{:.6f}\n".format(status, cnt, rate))
        if status in report_status_set:
            report({'{}_count_status_{}'.format(METRIC_PREFIX, status) : cnt}, timestr_to_ts(day))
            report({'{}_rate_status_{}'.format(METRIC_PREFIX, status) : rate}, timestr_to_ts(day))

    for status, total_cnt in status_dict.items():
        reason_list = sorted(status_reason_dict[status].items(), key = lambda x : x[1], reverse = True)
        out_f.write("{}======\n".format(status))
        for reason, cnt in reason_list:
            out_f.write("{}\t{:30s}\t{}\t{:.6f}\n".format(status, reason, cnt, cnt / float(total_cnt)))

    out_f.write("\nuniq_doc_total_status_cnt:\n")
    for status, cnt in uniq_status_dict.items():
        rate = cnt / float(len(succ_uniq_docid))
        out_f.write("{:20s}\t{}\t{:.6f}\n".format(status, cnt, rate))
        if status in report_status_set:
            report({'{}_count_uniqdoc_status_{}'.format(METRIC_PREFIX, status) : cnt}, timestr_to_ts(day))
            report({'{}_rate_uniqdoc_status_{}'.format(METRIC_PREFIX, status) : rate}, timestr_to_ts(day))
    out_f.write('\n\n')

def process():
    if len(sys.argv) != 4:
        print("proc.py input_file output_file log_file")
        sys.exit(1)

    input_file = sys.argv[1]
    output_file = sys.argv[2]
    log_file = sys.argv[3]
    logging.basicConfig(filename=log_file, filemode="w", format='%(asctime)s : %(levelname)s : %(message)s', level=logging.INFO)
    out_f = open(output_file, 'w')
    out_f.write('**********  analysis current day  **********\n')
    process_day_range(input_file, out_f, 1)

    out_f.write('**********  analysis 7 day  **********\n')
    process_day_range(input_file, out_f, 7)

    out_f.close()

if __name__ == "__main__":
    process()
    