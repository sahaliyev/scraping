import argparse

parser = argparse.ArgumentParser(description='Description of your program')
parser.add_argument('-f', '--foo', help='Description for foo argument', required=True, type=str)
args = vars(parser.parse_args())

if args.get('foo') == 'Hello':
    print('foo there')
