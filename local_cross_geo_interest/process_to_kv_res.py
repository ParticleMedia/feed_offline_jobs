
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
            read_cnt += 1
            if len(ws) != 13:
                continue
            zip = ws[0]
            cate = ws[1]
            pid = ws[2]
            key = zip + '@' + cate
            data[key].append(pid)
        write_cnt = len(data)
        logging.info(f"read={read_cnt} write={write_cnt}")

        for key in data:
            fw.write(f"{key}\t{'#'.join(data[key])}\n")


if __name__ == '__main__':
    main()
