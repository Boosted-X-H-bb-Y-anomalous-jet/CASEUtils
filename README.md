#CASEUtils

Various things for CMS Anomaly SEarch

NanoAODTools needed for h5 maker: 
```
git clone https://github.com/cms-nanoAOD/nanoAOD-tools.git PhysicsTools/NanoAODTools
```

Installation tested on lpc el8
```
cmsrel CMSSW_12_3_5
cd CMSSW_12_3_5/src
cmsenv
git clone https://github.com/cms-nanoAOD/nanoAOD-tools.git PhysicsTools/NanoAODTools
cd PhysicsTools/NanoAODTools
cmsenv
scram b
```
