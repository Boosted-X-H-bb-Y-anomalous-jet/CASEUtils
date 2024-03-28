#!/bin/bash
echo "H5 conversion script starting"
source /cvmfs/cms.cern.ch/cmsset_default.sh
xrdcp root://cmseos.fnal.gov//store/user/roguljic/H5_env.tgz ./
export SCRAM_ARCH=slc7_amd64_gcc900
scramv1 project CMSSW CMSSW_11_3_4
tar -xzf H5_env.tgz
rm -f H5_env.tgz

mkdir tardir; cp tarball.tgz tardir/; cd tardir/
tar -xzf tarball.tgz; rm -f tarball.tgz
cp -r * ../CMSSW_11_3_4/src/CASEUtils/H5_maker/; cd ../CMSSW_11_3_4/src/CASEUtils/H5_maker/
echo "IN RELEASE"
pwd
ls
eval `scramv1 runtime -sh`

#python3 -m virtualenv py3-env
#source py3-env/bin/activate

python3 -m venv --without-pip myenv
source myenv/bin/activate
curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py
python get-pip.py
pip install numpy==1.21.0
#TF version in CMSSW_11_3_4 works with this numpy, otherwise some error during execution is reported

echo $1
while read -r line
do
    echo python3 run_h5_condor.py ${line}
    python3 run_h5_condor.py ${line}
done < $1