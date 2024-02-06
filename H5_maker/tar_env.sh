# capture the current directory in tarball
WD=$(pwd)
cd $CMSSW_BASE/../
tar --exclude-caches-all --exclude-vcs --exclude-caches-all --exclude-vcs --exclude="tmp" --exclude=".scram" --exclude=".SCRAM" --exclude="CMSSW_11_3_4/src/CASEUtils/H5_maker/logs" --exclude="CMSSW_11_3_4/src/CASEUtils/H5_maker/*.h5" --exclude="CMSSW_11_3_4/src/CASEUtils/jet_images" --exclude="CMSSW_11_3_4/src/TagNTrain" --exclude="CMSSW_11_3_4/src/H5_Storage" --exclude="CMSSW_11_3_4/src/2DAlphabet" --exclude="CMSSW_11_3_4/src/CombineHarvester" --exclude="CMSSW_11_3_4/src/HiggsAnalysis" --exclude="CMSSW_11_3_4/src/twoD-env" -cvzf H5_env.tgz CMSSW_11_3_4 
xrdcp -f H5_env.tgz root://cmseos.fnal.gov//store/user/$USER/H5_env.tgz
cd ${WD}
