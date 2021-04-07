import json
import argparse


def main():
    parser = argparse.ArgumentParser(
        description='Strips the PrecomputedPrimes section out of a json file'
    )
    parser.add_argument(
        '-i', '--infile', required=True,
        help='The input file'
    )
    parser.add_argument(
        '-o', '--outfile', required=True,
        help='The output file'
    )
    args = parser.parse_args()

    strip_precomputed_primes(args.infile, args.outfile)


def strip_precomputed_primes(infile: str, outfile: str) -> None:
    with open(infile, 'r') as f_in:
        val = json.load(f_in)

    del val['PrecomputedPrimes']

    with open(outfile, 'w') as f_out:
        json.dump(val, f_out)



if __name__ == '__main__':
    main()
