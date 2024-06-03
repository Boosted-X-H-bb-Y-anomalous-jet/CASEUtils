from H5_merge import merge
import subprocess
from os.path import isfile

eosls       = 'eos root://cmseos.fnal.gov ls'

processes = ["MX2400_MY100"]
years     = ["2017"]
for year in years:
    for process in processes:
        print(f"Doing {process} {year}")
        h5_dir  = f"/store/user/roguljic/H5_output/{year}/{process}/"
        fNames  = subprocess.check_output(['{} {}'.format(eosls,h5_dir)],shell=True,text=True).split('\n')
        fNames.remove('')
        if "merged.h5" in fNames:
            print(f"{process} in {year} merged, continuing")
            continue
        for i,fName in enumerate(fNames):
            # if(i>10):#For testing
            #     continue
            print("{}/{}".format(i,len(fNames)))
            xrdcp_cmd = f"xrdcp root://cmseos.fnal.gov/{h5_dir}/{fName} ."
            subprocess.call(xrdcp_cmd,shell=True)
            if not isfile("merged.h5"):
                subprocess.call(f"mv {fName} merged.h5",shell=True)
            else:
                merge(fName,"merged.h5")
                subprocess.call(f"rm {fName}",shell=True)
        
        xrdcp_cmd = f"xrdcp merged.h5 root://cmseos.fnal.gov/{h5_dir}/merged.h5"
        subprocess.call(xrdcp_cmd,shell=True)
        subprocess.call("rm merged.h5",shell=True)
