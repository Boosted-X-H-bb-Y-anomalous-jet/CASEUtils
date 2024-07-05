from H5_merge import merge
import subprocess
from os.path import isfile
from os.path import getsize

eosls       = 'eos root://cmseos.fnal.gov ls'

processes = ["TTToHadronic","MX1200_MY90","MX1400_MY90","MX1600_MY90","MX1800_MY90","MX2000_MY90","MX2200_MY90","MX2400_MY90","MX2500_MY90","MX2600_MY90","MX2800_MY90","MX3000_MY90","MX3500_MY90","MX4000_MY90"]
years     = ["2016"]
#years     = ["2016","2016APV","2017","2018"]
for year in years:
    for process in processes:
        print(f"Doing {process} {year}")
        h5_dir_output  = f"/store/user/roguljic/H5_output/{year}/{process}/"
        if(year=="2017" or year=="2016"):
            h5_dir_input = f"/store/user/shanning/H5_output/{year}/{process}/"
        else:
            h5_dir_input = f"/store/user/roguljic/H5_output/{year}/{process}/"
        fNames  = subprocess.check_output(['{} {}'.format(eosls,h5_dir_input)],shell=True,text=True).split('\n')
        fNames_output = subprocess.check_output(['{} {}'.format(eosls,h5_dir_output)],shell=True,text=True).split('\n')
        fNames.remove('')
        fNames_output.remove('')
        if "merged.h5" in fNames_output:
            print(f"{process} in {year} merged, continuing")
            continue
        for i,fName in enumerate(fNames):
            # if(i>10):#For testing
            #     continue
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
