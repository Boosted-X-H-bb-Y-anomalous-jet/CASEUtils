import subprocess
import glob
import os
from collections import OrderedDict
import sys
import h5py

redirector = 'root://cmseos.fnal.gov/'
eosls = 'eos root://cmseos.fnal.gov ls'
output_dir ='/store/user/roguljic/H5_output/'
years=["2016APV","2016","2017","2018"]



badfiles = []
for year in years:
    processes = subprocess.check_output([f'{eosls} {output_dir}/{year}'],shell=True,text=True).split('\n')
    processes.remove('')       
    for process in processes:
        print(process, year)
        store_path = f"{output_dir}/{year}/{process}"
        fNames = subprocess.check_output([f'{eosls} {store_path}'],shell=True,text=True).split('\n')
        fNames.remove('')
        for fName in fNames:
            if "merged" in fName:
                continue
            file_path = f"{redirector}{store_path}/{fName}"
            xrdcp_cmd = f"xrdcp {file_path} del.h5"
            subprocess.call([xrdcp_cmd],shell=True, stderr=subprocess.DEVNULL)
            with h5py.File('del.h5', 'r') as f:
                if 'Y_vae_loss' in f:
                    good_file_flag = True
                else:
                    good_file_flag = False
                    print(f"{file_path} bad")
                    badfiles.append(file_path)
            subprocess.call(["rm -f del.h5"],shell=True)


with open("badfiles", "w") as file:
    for badfile in badfiles:
        file.write(badfile + "\n")
