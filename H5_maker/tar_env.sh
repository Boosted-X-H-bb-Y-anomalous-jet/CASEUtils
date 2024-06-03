# capture the current directory in tarball
WD=$(pwd)
cd $CMSSW_BASE/../
tar --exclude-caches-all --exclude-vcs --exclude-caches-all --exclude-vcs --exclude="tmp" --exclude=".scram" --exclude=".SCRAM" --exclude="CMSSW_12_3_5/src/CASEUtils/H5_maker/logs" --exclude="CMSSW_12_3_5/src/CASEUtils/H5_maker/*.h5" -cvzf el8_H5_env.tgz CMSSW_12_3_5 
xrdcp -f el8_H5_env.tgz root://cmseos.fnal.gov//store/user/$USER/el8_H5_env.tgz
cd ${WD}
