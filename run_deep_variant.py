import argparse
import os
import sys
import time

from collections import defaultdict
from subprocess import Popen

from extract_coords_from_bam import read_bam

def set_command_samtools(bam_list):
    command_list = list()
    stdout_list = list()
    stderr_list = list()
    for _in_bam in bam_list:
        command_list.append('samtools index {}'.format(_in_bam))
        stdout_list.append('{}.index.out'.format(_in_bam[:-4]))
        stderr_list.append('{}.index.err'.format(_in_bam[:-4]))
    
    return command_list, stdout_list, stderr_list

def set_command_make_examples(bam_list):
    command_list = list()
    stdout_list = list()
    stderr_list = list()
    for _in_bam in bam_list:
        coord_array = _in_bam[:-4].split('-')
        coord_subarray = coord_array[-2].split(':')
        coord = coord_subarray[0] + ':' + '{:,}'.format(int(coord_subarray[1])) + '-' + '{:,}'.format(int(coord_array[-1]))
        command_list.append('docker run -it -v /home/dn070017/projects/ACMG-Variant-Calling:/input -v /home/dn070017/bin/Deep-Variant-0.4.1/models:/models gcr.io/deepvariant-docker/deepvariant ./opt/deepvariant/bin/make_examples --mode calling --ref /input/Deep-Variant/ucsc.hg19.fasta --reads /input/{} --examples /input/{} --regions {}'.format(_in_bam, _in_bam[:-4] + '.example.tfrecord', coord))
        stdout_list.append('{}.make_example.out'.format(_in_bam[:-4]))
        stderr_list.append('{}.make_example.err'.format(_in_bam[:-4]))

    return command_list, stdout_list, stderr_list

def set_command_call_variants(bam_list):
    command_list = list()
    stdout_list = list()
    stderr_list = list()
    for _in_bam in bam_list:
        coord = _in_bam[:-4].split('-')
        coord = coord[-2] + '-' + coord[-1]
        command_list.append('docker run -it -v /home/dn070017/projects/ACMG-Variant-Calling:/input -v /home/dn070017/bin/Deep-Variant-0.4.1/models:/models gcr.io/deepvariant-docker/deepvariant ./opt/deepvariant/bin/call_variants --outfile /input/{} --examples /input/{} --checkpoints /models/model.ckpt'.format(_in_bam[:-4] + '.example.tfrecord', _in_bam[:-4] + '.variants.tfrecord'))
        stdout_list.append('{}.call_variant.out'.format(_in_bam[:-4]))
        stderr_list.append('{}.call_variant.err'.format(_in_bam[:-4]))

    return command_list, stdout_list, stderr_list

def set_command_postprocessing_variants(bam_list):
    command_list = list()
    stdout_list = list()
    stderr_list = list()
    for _in_bam in bam_list:
        coord = _in_bam[:-4].split('-')
        coord = coord[-2] + '-' + coord[-1]
        command_list.append('docker run -it -v /home/dn070017/projects/ACMG-Variant-Calling:/input -v /home/dn070017/bin/Deep-Variant-0.4.1/models:/models gcr.io/deepvariant-docker/deepvariant ./opt/deepvariant/bin/postprocess_variants --ref /input/Deep-Variant/ucsc.hg19.fasta --infile /input/{} --outfile /input/{}'.format(_in_bam[:-4] + '.variants.tfrecord', _in_bam[:-4] + '.vcf'))
        stdout_list.append('{}.postprocessing_variant.out'.format(_in_bam[:-4]))
        stderr_list.append('{}.postprocessing_variant.err'.format(_in_bam[:-4]))

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
        os.system("docker ps -a | awk '{ print $1, $2 }' | grep gcr.io/deepvariant-docker/deepvariant | awk '{print $1 }' | xargs -I {} docker rm {}")


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument('--thread', type=int, help='how many threads to use (default: 1)', default=1, required=False)
   
    required = parser.add_argument_group('arguments')
    required.add_argument('--bam', type=str, help='input bam list', required=True)
    
    args = parser.parse_args()
    in_bam = args.bam
    threads = args.thread

    bam_list = read_bam(in_bam)
    #command_list, stdout_list, stderr_list = set_command_samtools(bam_list)
    #run_command(command_list, stdout_list, stderr_list, threads)
    command_list, stdout_list, stderr_list = set_command_make_examples(bam_list)
    run_command(command_list, stdout_list, stderr_list, threads)
    command_list, stdout_list, stderr_list = set_command_call_variants(bam_list)
    run_command(command_list, stdout_list, stderr_list, threads)
    command_list, stdout_list, stderr_list = set_command_postprocessing_variants(bam_list)
    run_command(command_list, stdout_list, stderr_list, threads)
