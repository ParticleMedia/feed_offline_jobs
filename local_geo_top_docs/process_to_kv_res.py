
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
    args = arg_parser.parse_args()

    data = collections.defaultdict(list)
    with open(args.input, 'r') as fr, open(args.output, 'w') as fw:
        read_cnt = 0
        for line in fr:
            ws = line.strip().split("\t")
            if len(ws) != 5:
                continue
            pid = ws[0]
            docid = ws[1]
            data[pid].append(docid)
        write_cnt = len(data)
        logging.info(f"read={read_cnt} write={write_cnt}")

        for key in data:
            fw.write(f"{key}\t{pid}\n")


if __name__ == '__main__':
    main()
