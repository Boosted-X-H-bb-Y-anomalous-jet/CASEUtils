import subprocess
import glob
import os
from collections import OrderedDict
import sys

if __name__=='__main__':
    from argparse import ArgumentParser
    parser = ArgumentParser()
    parser.add_argument('-i', type=str, dest='iFile',
                        action='store', required=True,
                        help='input ROOT file')
    parser.add_argument('-o', type=str, dest='oFile',
                        action='store', required=True,
                        help='output H5 file')
    parser.add_argument('-y', type=str, dest='year',
                        action='store', required=True,
                        help='2016, 2016APV, 2017, 2018')
    parser.add_argument('-f', type=str, dest='f',
			action='store', required=True,
			help='sets the truth label of the output. Usually signal is 1, QCD is 0, single top -1, ttbar -2, V+jets -3')
    parser.add_argument('--fTree', type=str, dest='friend_tree',
                    action='store', required=False,
                    help='Friend tree with extra branches for systematics')


    args = parser.parse_args()
    oFileName = args.oFile.split("/")[-1]

    if "TTToHadronic" in args.oFile or "MX" in args.oFile:
        sys_opt = " --sys"
    else:
        sys_opt = ""

    print("Sys: ", sys_opt)


    subprocess.call('python make_h5_local.py -i {} -o {} -y {} -f {} --fTree {} {}'.format(args.iFile, oFileName, args.year, args.f, args.friend_tree, sys_opt),shell=True)
    subprocess.call([f'xrdcp {oFileName} {args.oFile}'],shell=True)
