import os
jdl_tpl = '''
universe = vanilla
Executable = exe.sh
Should_Transfer_Files = YES
request_cpus = 1
request_memory = 2000
Output = logs/output_$(Cluster)_$(Process).stdout
Error = logs/output_$(Cluster)_$(Process).stderr
Log = logs/output_$(Cluster)_$(Process).log
Arguments = "$(args)"
transfer_input_files = tarball.tgz
Queue args from ARGFILE.txt
'''

datasets = {    
    "2016": {
        "TTToHadronic": "/eos/uscms/store/group/lpcpfnano/cmantill/v2_3/2016/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220808_181812/0000/",
    },
    "2016APV": {
        "TTToHadronic": "/eos/uscms/store/group/lpcpfnano/cmantill/v2_3/2016APV/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220808_173601/0000/",
    },
    "2017": {
        "TTToHadronic": "/eos/uscms/store/group/lpcpfnano/rkansal/v2_3/2017/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220705_160139/0000/",
        "MX2400_MY100": "/eos/uscms/store/group/lpcpfnano/ammitra/v2_3/2017/XHYPrivate/NMSSM_XToYH_MX2400_MY100_HTo2bYTo2W_hadronicDecay/NMSSM_XToYH_MX2400_MY100_HTo2bYTo2W_hadronicDecay/221013_153330/0000/",
    },
    "2018": {
        "TTToHadronic": "/eos/uscms/store/group/lpcpfnano/cmantill/v2_3/2018/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220808_151154/0000/",
    }
}
def write_arg_files(process,year,outdir,filelist,n=50):
    counter = 0
    for i in range(0,len(filelist), n):  
        files_chunk = filelist[i:i + n]
        counter+=1

        f = open(f"args_{process}_{year}_{counter}.txt","w")
        for file in files_chunk:
            f.write(f"-i {file} -o {outdir} -y {year} \n") 
        f.close()
    return counter

if(__name__ == "__main__"):
    for year in ["2016APV","2016","2017","2018"]:
        for process, inputDir in datasets[year].items():
            input_dir_root = inputDir.replace("/eos/uscms/","root://cmseos.fnal.gov//")

            root_dir = f"root://cmseos.fnal.gov//store/user/roguljic/XHYAnomalous/PFNanoExtensions/{year}/{process}/"
            eos_dir  = root_dir.replace("root://cmseos.fnal.gov/","/eos/uscms/")
            
            if not os.path.exists(eos_dir):
                print(f"Making dir {eos_dir}")
                os.makedirs(eos_dir)

            file_list = os.listdir(inputDir)
            to_do     = []
            for name in file_list:
                if not ".root" in name:
                    continue
                output_name = eos_dir+name
                if os.path.exists(output_name):
                    continue
                else:
                    to_do.append(f"{input_dir_root}/{name}")
            
            if to_do:
                n_jobs = write_arg_files(process,year,root_dir,to_do)
                f      = open(f"args_{process}_{year}.txt","w")
                for job_id in range(1,n_jobs+1):
                    f.write(f"args_{process}_{year}_{job_id}.txt\n")

                condor_tpl = jdl_tpl.replace("ARGFILE",f"args_{process}_{year}")
                f = open(f"jdl_{process}_{year}.txt","w")
                f.write(condor_tpl)
                f.close()

                print(f"condor_submit jdl_{process}_{year}.txt")
            else:
                print("Directory processed")

    os.system("tar cf tarball.tgz addSys.py THmodules.cc args*txt")


#/eos/uscms/store/user/lpcpfnano/ammitra/v2_3/2017/XHYPrivate/NMSSM_XToYH_MX2400_MY100_HTo2bYTo2W_hadronicDecay/NMSSM_XToYH_MX2400_MY100_HTo2bYTo2W_hadronicDecay/221013_153330/0000/