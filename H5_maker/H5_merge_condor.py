from H5_merge import merge
import subprocess
from os.path import isfile
from os.path import getsize

eosls       = 'eos root://cmseos.fnal.gov ls'
eosmkdir       = 'eos root://cmseos.fnal.gov mkdir'

def run_merge(year,process):
    print(f"Doing {process} {year}")
    h5_dir_output  = f"/store/user/roguljic/H5_output/{year}/{process}/"
    h5_dir_input = f"/store/user/roguljic/H5_output/{year}/{process}/"

    fNames  = subprocess.check_output(['{} {}'.format(eosls,h5_dir_input)],shell=True,text=True).split('\n')
    fNames.remove('')
    try:
        fNames_output = subprocess.check_output(['{} {}'.format(eosls,h5_dir_output)],shell=True,text=True).split('\n')
        fNames_output.remove('')
    except:
        print(f"Creating directory: {h5_dir_output}")
        subprocess.check_output(['{} {}'.format(eosmkdir,h5_dir_output)],shell=True,text=True).split('\n')
        fNames_output=[]  

    if "merged.h5" in fNames_output:
        print(f"{process} in {year} merged, exiting")
        exit()
    for i,fName in enumerate(fNames):
        print("{}/{}".format(i,len(fNames)))
        xrdcp_cmd = f"xrdcp root://cmseos.fnal.gov/{h5_dir_input}/{fName} ."
        subprocess.call(xrdcp_cmd,shell=True)
        if getsize(fName) < 1000:
            print("Skipping over empty file")
            subprocess.call(f"rm {fName}",shell=True)
            continue
        if not isfile("merged.h5"):
            subprocess.call(f"mv {fName} merged.h5",shell=True)
        else:
            merge(fName,"merged.h5")
            subprocess.call(f"rm {fName}",shell=True)
    
    xrdcp_cmd = f"xrdcp merged.h5 root://cmseos.fnal.gov/{h5_dir_output}/merged.h5"
    subprocess.call(xrdcp_cmd,shell=True)
    subprocess.call("rm merged.h5",shell=True)

from argparse import ArgumentParser

parser = ArgumentParser()
parser.add_argument('-y', type=str, dest='year',
                    action='store', required=True,
                    help='Year')
parser.add_argument('-p', type=str, dest='process',
                    action='store', required=True,
                    help='process')

args = parser.parse_args()
run_merge(args.year,args.process)