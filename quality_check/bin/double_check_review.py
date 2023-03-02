import os, sys
import collections

def double_check_by_editor(log_filename, output_filename):
    day = log_filename.split('/')[-2]
    editor_dict = collections.defaultdict(dict)
    uniq_editor_dict = collections.defaultdict(dict)
    if not os.path.exists(log_filename):
        print('file not exsts: ' + log_filename)
        return
    with open(log_filename, 'r') as f:
        for line in f:
            splits = line.strip().split('\t')
            if len(splits) != 7:
                continue
            flag, line_no, ret, docid, status, reason, editor = splits
            if flag.find('read_data') < 0:
                continue
            if ret != 'succ':
                continue

            if status not in editor_dict[editor]:
                editor_dict[editor][status] = 0
            if status not in uniq_editor_dict[editor]:
                uniq_editor_dict[editor][status] = set()

            editor_dict[editor][status] += 1
            uniq_editor_dict[editor][status].add(docid)

    status_list = ['ok', 'poor', 'detrimental']
    headers = ['day', 'type', 'editor', 'total', 'ok', 'poor', 'detrimental', 'ok_rate', 'poor_rate', 'detrimental_rate']
    with open(output_filename, 'w') as fw:
        fw.write('\t'.join(headers) + '\n')

        flag = 'weighted_doc'
        editor_list = list(editor_dict.keys())
        editor_list.sort()
        for editor in editor_list:
            data = editor_dict[editor]
            total = sum(data.values())
            if total != 0:
                cnt_list = []
                for status in status_list:
                    cur = data.get(status, 0)
                    cnt_list.append(cur)
                rate_list = [float(x)/total for x in cnt_list]
                res_list = [day, flag, editor, total] + cnt_list + rate_list
                s = '{}\t{}\t{:35s}\t{:4d}\t{:4d}\t{:4d}\t{:4d}\t{:.4f}\t{:.4f}\t{:.4f}\n'.format(*res_list)
                fw.write(s)
        fw.write("\n")

        flag = 'uniq_doc'
        res_list = []
        editor_list = list(editor_dict.keys())
        editor_list.sort()
        for editor in editor_list:
            data = uniq_editor_dict[editor]
            total = sum([len(x) for x in data.values()])
            if total != 0:
                cnt_list = []
                for status in status_list:
                    cur_set = data.get(status, set())
                    cur = len(cur_set)
                    cnt_list.append(cur)
                rate_list = [float(x)/total for x in cnt_list]
                res_list = [day, flag, editor, total] + cnt_list + rate_list
                s = '{}\t{}\t{:35s}\t{:4d}\t{:4d}\t{:4d}\t{:4d}\t{:.4f}\t{:.4f}\t{:.4f}\n'.format(*res_list)
                fw.write(s)
        fw.write("\n")

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('error parameter')
        exit(1)

    log_filename = sys.argv[1]
    output_filename = sys.argv[2]

    double_check_by_editor(log_filename, output_filename)
