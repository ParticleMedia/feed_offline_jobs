
import argparse
import collections
import logging

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : [WriteRedis] %(message)s',
    level=logging.INFO)


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--input', help='original merged file from hdfs')
    arg_parser.add_argument('--output', help='kv file')
    arg_parser.add_argument('--max_size_in_key', type=int, default=100, help='max results in one single key')
    args = arg_parser.parse_args()

    data = collections.defaultdict(list)
    with open(args.input, 'r') as fr, open(args.output, 'w') as fw:
        read_cnt = 0
        for line in fr:
            ws = line.strip().split("\t")
            read_cnt += 1
            if len(ws) != 5:
                continue
            pid = ws[0]
            docid = ws[1]
            data[pid].append(docid)
        write_cnt = len(data)
        logging.info(f"read={read_cnt} write={write_cnt}")

        if write_cnt > 0:
            tot_size, max_size, max_key = 0, 0, ''
            for key in data:
                if len(data[key]) > args.max_size_in_key:
                    data[key] = data[key][:args.max_size_in_key]

                cur_size = len(data[key])
                tot_size += cur_size
                if cur_size > max_size:
                    max_size = cur_size
                    max_key = key
                fw.write(f"{key}\t{cur_size}\n")
            avg_size = tot_size / float(write_cnt)
            logging.info(f"saved: write_key={write_cnt} total_value_size={tot_size} max_size={max_size} max_key={max_key} avg_size={avg_size:.4f}")


if __name__ == '__main__':
    main()
