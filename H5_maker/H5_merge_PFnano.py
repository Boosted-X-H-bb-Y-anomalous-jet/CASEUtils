from H5_merge import *
import subprocess

redirector = 'root://cmsxrootd.fnal.gov/'
eosls = 'eos root://cmseos.fnal.gov ls'
output_dir ='root://cmseos.fnal.gov//store/user/roguljic/H5_output/'



def prepare_arguments(store_dir):
    fNames = subprocess.check_output(['{} {}'.format(eosls,store_dir)],shell=True,text=True).split('\n')
    fNames.remove('')
    store_full_path = store_dir.replace("/store","/eos/uscms/store/")
    inputs = []
    for fName in fNames:
        full_name = f"{store_full_path}{fName}"
        inputs.append(full_name)
    output_file = f"{store_full_path}merged.h5"
    return output_file, inputs



if __name__ == "__main__":
    force_merge = False

    years = ["20216APV","2016","2017","2018"]
    years = ["2017"]
    for year in years:
        print(year)
        h5_dir    = f"/store/user/roguljic/H5_output/{year}/"
        processes = subprocess.check_output(['{} {}'.format(eosls,h5_dir)],shell=True,text=True).split('\n')
        processes.remove("")

        for process in processes:
            proc_dir = f"{h5_dir}/{process}/"
            fNames = subprocess.check_output(['{} {}'.format(eosls,proc_dir)],shell=True,text=True).split('\n')
            if "merged.h5" in fNames and not force_merge:
                print(f"{process} in {year} is already merged, skipping")
                continue
            output, inputs = prepare_arguments(proc_dir)
            merge_multiple(output, inputs)

    #pt_bin = sys.argv[3]
    #input_dir = sys.argv[2]
    
    #onlyfiles = [join(input_dir, f) for f in listdir(input_dir) if (isfile(join(input_dir, f)) & (pt_bin in f))]
    
    print("Done!")

