import collections

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : [WriteRedis] %(message)s',
    level=logging.INFO)


def main():
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('--input', help='original merged file from hdfs')
    arg_parser.add_argument('--output', help='kv file')
    args = arg_parser.parse_args()

    data = collections.defaultdict(list)
    with open(args.input, 'r') as fr:
        for line in fr:
            ws = line.strip().split("\t")
            if len(ws) != 13:
                continue
            zip = ws[0]
            cate = ws[1]
            pid = ws[2]
            ctr = float(ws[3])

            key = ge

    pass


if __name__ == '__main__':
    main()
