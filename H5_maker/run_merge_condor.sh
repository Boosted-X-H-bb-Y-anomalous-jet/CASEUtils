#!/bin/bash
echo "H5 merge script starting"
source /cvmfs/cms.cern.ch/cmsset_default.sh
xrdcp root://cmseos.fnal.gov//store/user/roguljic/el8_H5_env.tgz ./
export SCRAM_ARCH=el8_amd64_gcc10
scramv1 project CMSSW CMSSW_12_3_5
tar -xzf el8_H5_env.tgz
rm -f el8_H5_env.tgz

mkdir tardir; cp tarball_merge.tgz tardir/; cd tardir/
tar -xzf tarball_merge.tgz; rm -f tarball_merge.tgz
cp -r * ../CMSSW_12_3_5/src/CASEUtils/H5_maker/; cd ../CMSSW_12_3_5/src/CASEUtils/H5_maker/
echo "IN RELEASE"
pwd
ls
eval `scramv1 runtime -sh`

echo $1
while read -r line
do
    echo python3 H5_merge_condor.py ${line}
    python3 H5_merge_condor.py ${line}
done < $1