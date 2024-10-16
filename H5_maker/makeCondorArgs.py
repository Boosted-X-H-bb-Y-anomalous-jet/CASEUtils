import subprocess
import glob
import os
from collections import OrderedDict
import sys

N_per_job_data = 10 #usually 20
N_per_job_mc = 1 #usually 20
#redirector = 'root://cmsxrootd.fnal.gov/'
redirector = 'root://cmseos.fnal.gov/'
eosls = 'eos root://cmseos.fnal.gov ls'
output_dir ='root://cmseos.fnal.gov//store/user/roguljic/H5_output/'

jdl_tpl='''
universe = vanilla
Executable = run_h5_condor.sh
Should_Transfer_Files = YES
when_to_transfer_output = ON_EXIT_OR_EVICT
request_cpus = 1
request_memory = 4000
Output = logs/output_$(Cluster)_$(Process).stdout
Error = logs/output_$(Cluster)_$(Process).stderr
Log = logs/output_$(Cluster)_$(Process).log
transfer_input_files = tarball.tgz
Arguments = "$(args)"
transfer_output_files = ""
Queue args from ARGFILE
'''

def fill_signal_datasets(datasets,MX, MY):
    base_path_template = "/eos/uscms/store/group/lpcpfnano/{user}/v2_3"
    users = {
        "2016APV": "rkansal",
        "2016": "rkansal",
        "2017": "ammitra",
        "2018": "cmantill"
    }

    signal_datasets = {}

    for mx in MX:
        for my in MY:
            signal_datasets[(mx, my)] = []

            for era, user in users.items():
                base_path = f"{base_path_template.format(user=user)}/{era}/XHY/NMSSM_XToYHTo2W2BTo4Q2B_MX-{mx}_MY-{my}_TuneCP5_13TeV-madgraph-pythia8/NMSSM_XToYHTo2W2BTo4Q2B_MX-{mx}_MY-{my}"

                try:
                    date_dirs = os.listdir(base_path)
                    for date_dir in date_dirs:
                        full_path = os.path.join(base_path, date_dir, "0000/")
                        if os.path.exists(full_path):
                            signal_datasets[(mx, my)].append(full_path)
                            datasets[era][f"MX{mx}_MY{my}"] = full_path
                            break
                except FileNotFoundError:
                    print("Did not find: ", base_path)
                    continue


jetht_datasets = {
    "2016APV": {
        "JetHT_Run2016B_ver2_HIPM": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016B_ver2_HIPM/220701_193532/0000/",
        "JetHT_Run2016C_HIPM": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016C_HIPM/220701_193745/0000/",
        "JetHT_Run2016D_HIPM": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016D_HIPM/220701_193811/0000/",
        "JetHT_Run2016E_HIPM": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016E_HIPM/220701_193836/0000/",
        "JetHT_Run2016F_HIPM": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016F_HIPM/220701_193559/0000/"
     },
    "2016": {
        "JetHT_Run2016F": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016F/220701_193506/0000/",
        "JetHT_Run2016G": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016G/220701_193626/0000/",
        "JetHT_Run2016G1": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016G/220701_193626/0001/",
        "JetHT_Run2016H": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016H/220701_193717/0000/",
        "JetHT_Run2016H1": "/store/group/lpcpfnano/cmantill/v2_3/2016/JetHT2016/JetHT/JetHT_Run2016H/220801_140806/0000/"
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

mc_datasets = {
    "2016": {
        "TTToHadronic": "/store/group/lpcpfnano/cmantill/v2_3/2016/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220808_181812/0000/",
        "QCD_HT1000to1500":"/store/group/lpcpfnano/cmantill/v2_3/2016/QCD/QCD_HT1000to1500_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT1000to1500/220808_181242/0000/",
        "QCD_HT1500to2000":"/store/group/lpcpfnano/cmantill/v2_3/2016/QCD/QCD_HT1500to2000_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT1500to2000/220808_181307/0000/",
        "QCD_HT2000toInf":"/store/group/lpcpfnano/cmantill/v2_3/2016/QCD/QCD_HT2000toInf_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT2000toInf/220808_181358/0000/",
        "QCD_HT700to1000":"/store/group/lpcpfnano/cmantill/v2_3/2016/QCD/QCD_HT700to1000_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT700to1000/220808_181216/0000/",
        "TTToSemiLeptonic":"/store/group/lpcpfnano/cmantill/v2_3/2016/TTbar/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/TTToSemiLeptonic/220808_181840/0000/",
    },
    "2016APV": {
        "TTToHadronic": "/store/group/lpcpfnano/cmantill/v2_3/2016APV/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220808_173601/0000/",
        "QCD_HT1000to1500":"/store/group/lpcpfnano/cmantill/v2_3/2016APV/QCD/QCD_HT1000to1500_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT1000to1500/220808_173028/0000/",
        "QCD_HT1500to2000":"/store/group/lpcpfnano/cmantill/v2_3/2016APV/QCD/QCD_HT1500to2000_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT1500to2000/220808_173053/0000/",
        "QCD_HT2000toInf":"/store/group/lpcpfnano/cmantill/v2_3/2016APV/QCD/QCD_HT2000toInf_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT2000toInf/220808_173144/0000/",
        "QCD_HT700to1000":"/store/group/lpcpfnano/cmantill/v2_3/2016APV/QCD/QCD_HT700to1000_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT700to1000/220808_173001/0000/",
        "TTToSemiLeptonic":"/store/group/lpcpfnano/cmantill/v2_3/2016APV/TTbar/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/TTToSemiLeptonic/220808_173625/0000/",
    },
    "2017": {
        "TTToHadronic": "/store/group/lpcpfnano/rkansal/v2_3/2017/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220705_160139/0000/",
        "QCD_HT1000to1500":"/store/group/lpcpfnano/cmantill/v2_3/2017/QCD/QCD_HT1000to1500_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT1000to1500/220808_164439/0000/",
        "QCD_HT1500to2000":"/store/group/lpcpfnano/cmantill/v2_3/2017/QCD/QCD_HT1500to2000_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT1500to2000/220808_164504/0000/",
        "QCD_HT2000toInf":"/store/group/lpcpfnano/cmantill/v2_3/2017/QCD/QCD_HT2000toInf_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT2000toInf/220808_164556/0000/",
        "QCD_HT700to1000":"/store/group/lpcpfnano/cmantill/v2_3/2017/QCD/QCD_HT700to1000_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT700to1000/220808_164413/0000/",
        "TTToSemiLeptonic":"/store/group/lpcpfnano/rkansal/v2_3/2017/TTbar/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/TTToSemiLeptonic/220705_160227/0000/",
    },
    "2018": {
        'TTToHadronic':'/store/group/lpcpfnano/cmantill/v2_3/2018/TTbar/TTToHadronic_TuneCP5_13TeV-powheg-pythia8/TTToHadronic/220808_151154/0000/',
        "QCD_HT1000to1500":"/store/group/lpcpfnano/cmantill/v2_3/2018/QCD/QCD_HT1000to1500_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT1000to1500_PSWeights_madgraph/220808_163033/0000/",
        "QCD_HT1500to2000":"/store/group/lpcpfnano/cmantill/v2_3/2018/QCD/QCD_HT1500to2000_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT1500to2000_PSWeights_madgraph/220808_163124/0000/",
        "QCD_HT2000toInf":"/store/group/lpcpfnano/cmantill/v2_3/2018/QCD/QCD_HT2000toInf_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT2000toInf_PSWeights_madgraph/220808_163214/0000/",
        "QCD_HT700to1000":"/store/group/lpcpfnano/cmantill/v2_3/2018/QCD/QCD_HT700to1000_TuneCP5_PSWeights_13TeV-madgraph-pythia8/QCD_HT700to1000_PSWeights_madgraph/220808_162918/0000/",
        "TTToSemiLeptonic":"/store/group/lpcpfnano/cmantill/v2_3/2018/TTbar/TTToSemiLeptonic_TuneCP5_13TeV-powheg-pythia8/TTToSemiLeptonic/220808_151244/0000/",
    }
}

MX = ["1400","1600","1800","2200","2600","3000"]
MY = ["90","125","190","250","300","400"]
fill_signal_datasets(mc_datasets,MX,MY)

def args_for_submission(datasets_dict,data_flag):
    for year, dataset in datasets_dict.items():
        if data_flag:
            N_per_job = N_per_job_data
            sample_type = 'data'
            arguments_file = f'JetHT_args_{year}.txt'
        else:
            sample_type = 'MC'
            N_per_job = N_per_job_mc
            arguments_file = f'mc_args_{year}.txt'
        arguments = []
        for process, path in dataset.items():
            print(process)
            if("MX" in process):
                f = 1
                gen_opt = "-g YtoWW"
            elif("TTToHadronic" in process or "TTToSemiLeptonic" in process):
                f = -2
                gen_opt = "-g ttobqq"
            else:
                f = 0
                gen_opt = ""
            
            


            # get the file names
            fNames   = subprocess.check_output(['{} {}'.format(eosls,path)],shell=True,text=True).split('\n')
            fNames.remove('')
            fNames.remove('log')

            fFriends=[]
            if not data_flag:
                #if(year=="2016" or year=="2017" or "QCD" in process):
                if("QCD" in process):
                    friend_dataset_path = f"/store/user/shanning/XHYAnomalous/PFNanoExtensions/{year}/{process}/"#Edit this when you want to pick up Sabrina's trees
                else:
                    friend_dataset_path = f"/store/user/roguljic/XHYAnomalous/PFNanoExtensions_v2/{year}/{process}/"
                fFriends = subprocess.check_output(['{} {}'.format(eosls,friend_dataset_path)],shell=True,text=True).split('\n')
                fFriends.remove('')

            for fName in fNames:
                # create arguments

                if (fName not in fFriends) and not data_flag:
                    choice = input(f"{fName} not in {friend_dataset_path}, continue? (y/n): ").strip().lower()
                    if choice != 'y':
                        exit()
                    continue

                if data_flag:
                    friend_tree_path = 'null'
                else:
                    friend_tree_path = f"{redirector}{friend_dataset_path}/{fName}"

                
                iFile = '{}{}{}'.format(redirector, path, fName)
                proc_dir = f'{output_dir}/{year}/{process}/'
                proc_dir_eos = proc_dir.replace("root://cmseos.fnal.gov/","/eos/uscms/")
                #print(proc_dir_eos)
                if not os.path.exists(proc_dir_eos):
                    print(f"Making dir {proc_dir_eos}")
                    os.makedirs(proc_dir_eos)

                if os.path.exists(f'{proc_dir_eos}{fName.split(".")[0]}.h5'):
                    if os.path.getsize(f'{proc_dir_eos}{fName.split(".")[0]}.h5') < 1000:
                        subprocess.call([f'rm -f {proc_dir_eos}{fName.split(".")[0]}.h5'],shell=True)
                    else:
                        continue

                # #H5s now might be stored in Sabrina's store
                # proc_dir_eos_sabrina = proc_dir_eos.replace("roguljic","shanning")
                # if os.path.exists(f'{proc_dir_eos_sabrina}{fName.split(".")[0]}.h5'):
                #     if os.path.getsize(f'{proc_dir_eos_sabrina}{fName.split(".")[0]}.h5') < 1000:
                #         subprocess.call([f'rm -f {proc_dir_eos_sabrina}{fName.split(".")[0]}.h5'],shell=True)
                #     else:
                #         continue

                oFile = f'{proc_dir}{fName.split(".")[0]}.h5'
                iYear = 2016 if 'APV' in year else year
                
                arguments.append(' -i {} -o {} -y {} -f {} --fTree {} {} --sample_type {}\n'.format(iFile, oFile, iYear, f, friend_tree_path, gen_opt, sample_type))

        if arguments==[]:
            if data_flag:
                print(f"Processed all data in {year}")
            else:
                print(f"Processed all MC in {year}")
            continue
        else:
            n_files_to_process = len(arguments)
            print(f"{n_files_to_process} file to process in {year}")

        n_jobs = write_arguments(arguments_file,arguments,N=N_per_job)            

        f = open(arguments_file,"w")
        for job_id in range(1,n_jobs+1):
            f.write(arguments_file.replace(".txt",f"_{job_id}.txt\n"))

        if data_flag:
            jdl_name = f"jdl_{year}_data.txt"
        else:
            jdl_name = f"jdl_{year}_mc.txt"
        condor_tpl = jdl_tpl.replace("ARGFILE",arguments_file)
        f = open(jdl_name,"w")
        f.write(condor_tpl)
        f.close()

        print(f"condor_submit {jdl_name}")

def write_arguments(filename,arguments,N=20):    
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
    data_flag = False
    args_for_submission(mc_datasets,data_flag)
    data_flag = True
    args_for_submission(jetht_datasets,data_flag)
    subprocess.call(["tar czf tarball.tgz run_h5_condor.sh ImageUtils.py make_jet_images.py run_h5_condor.py make_h5_local.py H5_maker.py *args*txt gen_utils.py add_VAE_loss.py jrand_autoencoder_m2500.h5"],shell=True)