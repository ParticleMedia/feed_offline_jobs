import logging
import argparse
import redis
import json

logging.basicConfig(
    format='%(asctime)s : %(levelname)s : [WriteRedis] %(message)s',
    level=logging.INFO)


def main():
	arg_parser = argparse.ArgumentParser()
	arg_parser.add_argument('--input', help='To save key value file, each line format: key value ttl(second)')
	arg_parser.add_argument('--prefix', help='prefix')
	arg_parser.add_argument('--ttl', type=int, default=86400, help='expire ttl, default 1days')
	args = arg_parser.parse_args()

	prefix = args.prefix
	logging.info('Start.')

	# rc = redis.Redis(host="172.31.22.245", port=6379)
	rc = redis.Redis(host="proxy.cfb.redisc.nb.com", port=6379)
	pipe = rc.pipeline()

	idx = 0
	result = {}
	with open(args.input, 'r') as f:
            for idx, line in enumerate(f, start=1):
                    k, v = line.strip().split('\t')
                    rc.setex(prefix + '@' + k, args.ttl, v)
            pipe.execute()

	logging.info('Finish.')


if __name__ == '__main__':
	main()
