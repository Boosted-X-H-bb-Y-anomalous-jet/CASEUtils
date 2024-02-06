#!/bin/bash
echo "Run script starting"
export SCRAM_ARCH=el8_amd64_gcc10
echo $SCRAM_ARCH
source /cvmfs/cms.cern.ch/cmsset_default.sh
xrdcp root://cmseos.fnal.gov//store/user/roguljic/timber_tar.tgz ./
scramv1 project CMSSW CMSSW_12_3_5
tar -xf timber_tar.tgz
rm timber_tar.tgz
ls

#mkdir tardir; cp tarball.tgz tardir/; cd tardir/
#tar -xzf tarball.tgz; rm tarball.tgz
cd CMSSW_12_3_5
echo 'IN RELEASE'
eval `scramv1 runtime -sh`
cd ..
tar -xf tarball.tgz; rm tarball.tgz
pwd
ls

python3 -m virtualenv timber-env
source timber-env/bin/activate
cd TIMBER
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/cvmfs/cms.cern.ch/el8_amd64_gcc10/external/boost/1.78.0-0d68c45b1e2660f9d21f29f6d0dbe0a0/lib
echo $LD_LIBRARY_PATH
echo "STARTING TIMBER setup"
source setup.sh
echo "ENDING TIMBER SETUP"
cd ..

echo $1
while read -r line
do
    echo python addSys.py ${line}
    python addSys.py ${line}
done < $1

#xrdcp -f rootfiles/THselection_*.root root://cmseos.fnal.gov//store/user/roguljic/2017/selection/
