import subprocess
import glob
import os
from collections import OrderedDict
import sys

redirector = 'root://cmsxrootd.fnal.gov/'
eosls = 'eos root://cmseos.fnal.gov ls'
output_dir ='root://cmseos.fnal.gov//store/user/roguljic/H5_output/'

jdl_tpl='''
universe = vanilla
Executable = run_h5_condor.sh
Should_Transfer_Files = YES
when_to_transfer_output = ON_EXIT_OR_EVICT
request_cpus = 1
request_memory = 2000
Output = logs/output_$(Cluster)_$(Process).stdout
Error = logs/output_$(Cluster)_$(Process).stderr
Log = logs/output_$(Cluster)_$(Process).log
transfer_input_files = tarball.tgz
Arguments = "$(args)"
transfer_output_files = logs/output_$(Cluster)_$(Process).stderr,logs/output_$(Cluster)_$(Process).stdout
Queue args from ARGFILE.txt
'''

jetht_datasets = {
    "2016APV": {
        "JetHT_Run2016B_ver2_HIPM": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016B_ver2_HIPM/220701_193532/0000/",
        "JetHT_Run2016C_HIPM": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016C_HIPM/220701_193745/0000/",
        "JetHT_Run2016D_HIPM": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016D_HIPM/220701_193811/0000/",
        "JetHT_Run2016E_HIPM": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016E_HIPM/220701_193836/0000/",
        "JetHT_Run2016F": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016F/220701_193506/0000/"
     },
    "2016": {
        "JetHT_Run2016F_HIPM": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016F_HIPM/220701_193559/0000/",
        "JetHT_Run2016G": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016G/220701_193626/0000/",
        "JetHT_Run2016G1": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016G/220701_193626/0001/",
        "JetHT_Run2016H": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016H/220701_193717/0000/",
        "JetHT_Run2016H": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016H/220801_140806/0000/"
     },
    "2017": {
        "JetHT_Run2017B": "/store/group/lpcpfnano/cmantill/v2_3/2017/JetHT2017/JetHT/JetHT_Run2017B/220701_194050/0000/",
        "JetHT_Run2017C": "/store/group/lpcpfnano/cmantill/v2_3/2017/JetHT2017/JetHT/JetHT_Run2017C/220701_194023/0000/",
        "JetHT_Run2017D": "/store/group/lpcpfnano/cmantill/v2_3/2017/JetHT2017/JetHT/JetHT_Run2017D/220701_193930/0000/",
        "JetHT_Run2017E": "/store/group/lpcpfnano/cmantill/v2_3/2017/JetHT2017/JetHT/JetHT_Run2017E/220701_193905/0000/",
        "JetHT_Run2017F": "/store/group/lpcpfnano/cmantill/v2_3/2017/JetHT2017/JetHT/JetHT_Run2017F/220701_193956/0000/",
        "JetHT_Run2017F1": "/store/group/lpcpfnano/cmantill/v2_3/2017/JetHT2017/JetHT/JetHT_Run2017F/220701_193956/0001/",
     },
    "2018": {
        "JetHT_Run2018D": "/store/group/lpcpfnano/cmantill/v2_3/2018/JetHT2018/JetHT/JetHT_Run2018D/220801_141548/0000/",
        "JetHT_Run2018D1": "/store/group/lpcpfnano/cmantill/v2_3/2018/JetHT2018/JetHT/JetHT_Run2018D/220801_141548/0001/",
        "JetHT_Run2018D2": "/store/group/lpcpfnano/cmantill/v2_3/2018/JetHT2018/JetHT/JetHT_Run2018D/220801_141548/0002/",
        "JetHT_Run2018A": "/store/group/lpcpfnano/cmantill/v2_3/2018/JetHT2018/JetHT/JetHT_Run2018A/220701_194145/0000/",
        "JetHT_Run2018B": "/store/group/lpcpfnano/cmantill/v2_3/2018/JetHT2018/JetHT/JetHT_Run2018B/220701_194212/0000/",
        "JetHT_Run2018C": "/store/group/lpcpfnano/cmantill/v2_3/2018/JetHT2018/JetHT/JetHT_Run2018C/220701_194237/0000/"
     }
}

mc_datasets = {#These contain trees with systematics
    "2016": {
        "TTToHadronic": "/store/group/lpcpfnano/cmantill/v2_3/2016/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220808_181812/0000/"
    },
    "2016APV": {
        "TTToHadronic": "/store/group/lpcpfnano/cmantill/v2_3/2016APV/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220808_173601/0000/"
    },
    "2017": {
        "TTToHadronic": "/store/group/lpcpfnano/rkansal/v2_3/2017/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220705_160139/0000/",
        "MX2400_MY100": "/store/group/lpcpfnano/ammitra/v2_3/2017/XHYPrivate/NMSSM_XToYH_MX2400_MY100_HTo2bYTo2W_hadronicDecay/NMSSM_XToYH_MX2400_MY100_HTo2bYTo2W_hadronicDecay/221013_153330/0000/",
    },
    "2018": {
       'TTToHadronic':'/store/group/lpcpfnano/cmantill/v2_3/2018/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220808_151154/0000/'
    }
}

friend_datasets= {
    "2016": {
        "TTToHadronic": "/store/user/roguljic/XHYAnomalous/PFNanoExtensions/2016/TTToHadronic/"
    },
    "2016APV": {
        "TTToHadronic": "/store/user/roguljic/XHYAnomalous/PFNanoExtensions/2016APV/TTToHadronic/"
    },
    "2017": {
        "TTToHadronic": "/store/user/roguljic/XHYAnomalous/PFNanoExtensions/2017/TTToHadronic/",
        "MX2400_MY100": "/store/user/roguljic/XHYAnomalous/PFNanoExtensions/2017/MX2400_MY100/"
    },
    "2018": {
       'TTToHadronic':'/store/user/roguljic/XHYAnomalous/PFNanoExtensions/2018/TTToHadronic/'
    }
}

def args_data():
    # loop over dataset dictionary
    for year, dataset in jetht_datasets.items():
    # create arg file
        argFile = open('JetHT_Args_{}.txt'.format(year),'w')
        for process, path in dataset.items():
            f=0
            # get the file names
            fNames = subprocess.check_output(['{} {}'.format(eosls,path)],shell=True,text=True).split('\n')
            fNames.remove('')
            fNames.remove('log')
            for fName in fNames:
                # create arguments
                iFile = '{}{}{}'.format(redirector, path, fName)
                oFile = '{}/{}_{}.h5'.format(output_dir,process, fName.split('.')[0])
                iYear = 2016 if 'APV' in year else year
                # write to file
                argFile.write(' -i {} -o {} -y {} -f {}\n'.format(iFile, oFile, iYear, f))
        # close file
        argFile.close()

def args_mc(datasets_dict,data_flag):
    for year, dataset in datasets_dict.items():
        if data_flag:
            arguments_file = f'JetHT_args_{year}.txt'
        else:
            arguments_file = f'mc_args_{year}.txt'
        arguments = []
        for process, path in dataset.items():
            friend_dataset_path = friend_datasets[year][process]
            if("MX" in process):
                f = 1
            else:
                f = -2


            # get the file names
            fNames   = subprocess.check_output(['{} {}'.format(eosls,path)],shell=True,text=True).split('\n')
            fNames.remove('')
            fNames.remove('log')

            if not data_flag:
                fFriends = subprocess.check_output(['{} {}'.format(eosls,friend_dataset_path)],shell=True,text=True).split('\n')
                fFriends.remove('')

            for fName in fNames:
                # create arguments
                if (fName not in fFriends) and not data_flag:
                    print(f"{fName} not in {friend_dataset_path}")
                    exit()

                if data_flag:
                    friend_tree_path = ''
                else:
                    friend_tree_path = f"{redirector}{friend_dataset_path}/{fName}"


                iFile = '{}{}{}'.format(redirector, path, fName)
                proc_dir = f'{output_dir}/{year}/{process}/'
                proc_dir_eos = proc_dir.replace("root://cmseos.fnal.gov/","/eos/uscms/")
                if not os.path.exists(proc_dir_eos):
                    print(f"Making dir {proc_dir_eos}")
                    os.makedirs(proc_dir_eos)

                if os.path.exists(f'{proc_dir_eos}{fName.split(".")[0]}.h5'):
                    continue

                oFile = f'{proc_dir}{fName.split(".")[0]}.h5'
                iYear = 2016 if 'APV' in year else year
                arguments.append(' -i {} -o {} -y {} -f {} --fTree {}\n'.format(iFile, oFile, iYear, f, friend_tree_path))

        if arguments==[]:
            print("Processed all")
            exit()
        else:
            n_files_to_process = len(arguments)
            print(f"{n_files_to_process} file to process in {year}")

        n_jobs = write_arguments(arguments_file,arguments)            

        f = open(arguments_file,"w")
        for job_id in range(1,n_jobs+1):
            f.write(arguments_file.replace(".txt",f"_{job_id}.txt\n"))

        condor_tpl = jdl_tpl.replace("ARGFILE",f"mc_args_{year}")
        f = open(f"jdl_{year}.txt","w")
        f.write(condor_tpl)
        f.close()

        print(f"condor_submit jdl_{year}.txt")


def write_arguments(filename,arguments,N=50):    
    counter = 0
    for i in range(0,len(arguments), N):  
        arg_chunk = arguments[i:i + N]
        counter+=1
        
        f = open(filename.replace(".txt",f"_{counter}.txt"),"w")
        for line in arg_chunk:
            f.write(line) 
        f.close()
    return counter

if __name__=='__main__':

    #args_data()
    print("rm -f *args*txt jdl*txt")
    subprocess.call(["rm -f *args*txt jdl*txt"],shell=True)
    args_mc(mc_datasets,False)
    subprocess.call(["tar czf tarball.tgz run_h5_condor.sh run_h5_condor.py make_h5_local.py H5_maker.py *args*txt"],shell=True)