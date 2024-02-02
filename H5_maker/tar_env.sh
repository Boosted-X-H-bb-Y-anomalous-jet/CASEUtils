# capture the current directory in tarball
WD=$(pwd)
cd $CMSSW_BASE/../
tar --exclude-caches-all --exclude-vcs --exclude-caches-all --exclude-vcs --exclude="tmp" --exclude=".scram" --exclude=".SCRAM" --exclude="CMSSW_12_3_5/src/CASEUtils/H5_maker/logs" --exclude="CMSSW_12_3_5/src/CASEUtils/H5_maker/*.h5" --exclude="CMSSW_12_3_5/src/CASEUtils/jet_images" --exclude="CMSSW_12_3_5/src/TagNTrain" --exclude="CMSSW_12_3_5/src/H5_Storage" --exclude="CMSSW_12_3_5/src/2DAlphabet" --exclude="CMSSW_12_3_5/src/CombineHarvester" --exclude="CMSSW_12_3_5/src/HiggsAnalysis" --exclude="CMSSW_12_3_5/src/twoD-env" -cvzf H5_env.tgz CMSSW_12_3_5 
xrdcp -f H5_env.tgz root://cmseos.fnal.gov//store/user/$USER/H5_env.tgz
cd ${WD}
