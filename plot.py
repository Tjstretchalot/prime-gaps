"""Plots info.json similarly to what was done by Matt Parker"""
import matplotlib.pyplot as plt
import numpy as np
import json


def main():
    with open('info.json', 'r') as f_in:
        info = json.load(f_in)

    num_primes = info['PrimesSoFar']
    ys = np.array(info['GapCounter'])

    last_nonzero_y = np.max(np.nonzero(ys))
    ys = ys[:last_nonzero_y + 1]
    xs = np.arange(len(ys)) * 2

    # drop the first index from each (0)
    ys = ys[1:]
    xs = xs[1:]

    # take the log of the ys
    with np.errstate(divide='ignore'):
        ys = np.log(ys)

    fig, ax = plt.subplots()
    ax.set_title(f'Log Prime Gaps (First {num_primes} Primes)')
    ax.set_xlabel('Gap Size')
    ax.set_ylabel('log(n)')

    ax.scatter(xs, ys)
    plt.savefig(f'plot_{num_primes}.png')




if __name__ == '__main__':
    main()
