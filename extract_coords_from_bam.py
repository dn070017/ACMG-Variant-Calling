import argparse
import os
import sys
import time

from collections import defaultdict
from subprocess import Popen

def read_bam(in_bam):
    bam_list = list()
    with open(in_bam, 'r') as in_bam_f:
        for data in in_bam_f:
            data = data.rstrip()
            bam_list.append(data)
    return bam_list

def read_coords(in_coords):
    acmg_coords = defaultdict(list)
    with open(in_coords, 'r') as in_coords_f:
        next(in_coords_f)
        for data in in_coords_f:
            data = data.rstrip().split('\t')
            acmg_coords[data[0]].append([data[1], int(data[3]), int(data[4])])
    return acmg_coords

def set_command(acmg_coords, bam_list):
    command_list = list()
    stdout_list = list()
    stderr_list = list()
    for _in_bam in bam_list:
        in_bam = os.path.basename(_in_bam)
        os.makedirs(outdir + '/' + in_bam[:-4], exist_ok=True)
        out_subdir = outdir + '/' + in_bam[:-4] + '/'
        for acmg_gene in acmg_coords:
            for _chr, _start, _end in acmg_coords[acmg_gene]:
                start = max(0, _start - 10001)
                end = _end + 10000
                out_header = '{}{}-{}-{}:{}-{}'.format(out_subdir, in_bam[:-4], acmg_gene, _chr, start, end)
                command_list.append('samtools view -Sb {} {}:{}-{}'.format(_in_bam, _chr, start, end))
                stdout_list.append('{}.bam'.format(out_header))
                stderr_list.append('{}.err'.format(out_header))
    
    return command_list, stdout_list, stderr_list

def run_command(command_list, stdout_list, stderr_list, threads=1):
    for i in range(0, len(command_list) // threads + 1):
        start = i * threads
        end = min(start + threads, len(command_list))
        queue = [Popen(command.split(), stdout=open(out, 'w'), stderr=open(err, 'w')) for command, out, err in zip(command_list[start:end], stdout_list[start:end], stderr_list[start:end])]
        while True:
            if len(queue) == 0:
                break
            for process in queue:
                retcode = process.poll()
                if retcode is not None:
                    queue.remove(process)
                else: 
                    time.sleep(.1)
                    continue

        #for out, err in zip(stdout_list[start:end], stderr_list[start:end]):
        #    out.close()
        #    err.close()

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--outdir', type=str, help='output directory (default: ./)', default='./', required=False)
    parser.add_argument('--thread', type=int, help='how many threads to use (default: 1)', default=1, required=False)
    
    required = parser.add_argument_group('arguments')
    required.add_argument('--bam', type=str, help='input bam list', required=True)
    required.add_argument('--coord', type=str, help='input coordinates', required=True)
    
    args = parser.parse_args()
    in_bam = args.bam
    in_coords = args.coord
    threads = args.thread
    outdir = args.outdir

    bam_list = read_bam(in_bam)
    acmg_coords = read_coords(in_coords)
    command_list, stdout_list, stderr_list = set_command(acmg_coords, bam_list)
    run_command(command_list, stdout_list, stderr_list, threads)

