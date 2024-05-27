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
    parser.add_argument('-g', type=str, dest='gen_label',
                        action='store', required=False, default="",
                        help='YToWW (for signal only so far)')
    parser.add_argument('-f', type=str, dest='f',
			action='store', required=True,
			help='sets the truth label of the output. Usually signal is 1, QCD is 0, single top -1, ttbar -2, V+jets -3')
    parser.add_argument('--fTree', type=str, dest='friend_tree',
                    action='store', required=False,
                    help='Friend tree with extra branches for systematics')
    parser.add_argument('--sample_type', type=str, dest='sample_type',
        action='store', required=True,
        help='MC or data')


    args = parser.parse_args()
    oFileName = args.oFile.split("/")[-1]

    if "TTToHadronic" in args.oFile or "MX" in args.oFile:
        sys_opt = " --sys"
    else:
        sys_opt = ""

    if args.gen_label:
        gen_opt = f" --gen {args.gen_label}"
    else:
        gen_opt = ""

    print("Sys: ", sys_opt)


    print('python3 make_h5_local.py -i {} -o {} -y {} -f {} --fTree {} {} {} --sample_type {}'.format(args.iFile, oFileName, args.year, args.f, args.friend_tree, gen_opt,sys_opt,args.sample_type))
    subprocess.call('python3 make_h5_local.py -i {} -o {} -y {} -f {} --fTree {} {} {} --sample_type {}'.format(args.iFile, oFileName, args.year, args.f, args.friend_tree, gen_opt,sys_opt,args.sample_type),shell=True)
    print('python3 make_jet_images.py -i {} -o with_jet_images.h5'.format(oFileName))
    subprocess.call('python3 make_jet_images.py -i {} -o with_jet_images.h5'.format(oFileName),shell=True)
    print('python3 add_VAE_loss.py')
    subprocess.call('python3 add_VAE_loss.py',shell=True)
    print(f'xrdcp {oFileName} {args.oFile}')
    subprocess.call([f'xrdcp with_jet_images.h5 {args.oFile}'],shell=True)
    