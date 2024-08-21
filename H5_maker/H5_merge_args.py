from H5_merge import merge
import subprocess
from os.path import isfile
from os.path import getsize

eosls       = 'eos root://cmseos.fnal.gov ls'
eosmkdir       = 'eos root://cmseos.fnal.gov mkdir'

jdl_tpl='''
universe = vanilla
Executable = run_h5_condor.sh
Should_Transfer_Files = YES
request_cpus = 1
request_memory = 2000
Output = logs/output_$(Cluster)_$(Process).stdout
Error = logs/output_$(Cluster)_$(Process).stderr
Log = logs/output_$(Cluster)_$(Process).log
transfer_input_files = tarball_merge.tgz
Arguments = "$(args)"
transfer_output_files = ""
Queue args from args_merge.txt
'''

MX = ["1400","1600","1800","2200","2600","3000"]
MY = ["90","125","190","250","300","400"]
years = ["2016APV","2016","2017","2018"]
args=""
for mx in MX:
    for my in MY:
        for year in years:
            process = f"MX{mx}_MY{my}"
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
                print(f"{process} in {year} merged, continuing")
                continue
            args+=f"-p {process} -y {year}\n"
            
f = open("args_merge.txt","w")
f.write(args)


f = open("jdl_merge.txt","w")
f.write(jdl_tpl)
f.close()

subprocess.call(["tar czf tarball_merge.tgz run_merge_condor.sh H5_merge.py args_merge.txt H5_merge_condor.py"],shell=True)