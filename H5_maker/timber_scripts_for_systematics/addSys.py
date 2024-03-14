from TIMBER.Analyzer import *
from TIMBER.Tools import AutoJME as JM
from TIMBER.Tools import AutoPU as p
from TIMBER.Tools.Common import CompileCpp
import argparse
import ROOT,sys
import os

sys.path.append('../../')
sys.path.append('../')

def get_opts():

    parser = argparse.ArgumentParser()
    parser.add_argument("-i", "--inputFile", default = "in.root", help = "Inputfile")
    parser.add_argument("-y", "--year",  default = "2017", help = "Year of sample (2016, 2016APV,2017,2018)")
    parser.add_argument("-o", "--oDir",  default = "2017", help = "Output eos directory")
    parser.add_argument("--no_pdf", default = False, action = 'store_true', help = "Don't include pdf weight (some signals don't have it?)")
    return parser


def JMEvariationStr(variation):
    base_calibs = ['FatJet_JES_nom','FatJet_JER_nom', 'FatJet_JMS_nom', 'FatJet_JMR_nom']
    variationType = variation.split('_')[0]
    pt_calib_vect = '{'
    mass_calib_vect = '{'
    for c in base_calibs:
        if 'JM' in c:
            mass_calib_vect+='%s,'%('FatJet_'+variation if variationType in c else c)
        elif 'JE' in c:
            pt_calib_vect+='%s,'%('FatJet_'+variation if variationType in c else c)
            mass_calib_vect+='%s,'%('FatJet_'+variation if variationType in c else c)
    pt_calib_vect = pt_calib_vect[:-1]+'}'
    mass_calib_vect = mass_calib_vect[:-1]+'}'
    return pt_calib_vect, mass_calib_vect



def addSys(inputFile,oDir,year):
    pancakes = False
    print(inputFile)

    columns_to_save = []

    if("MX" in inputFile):
        eff = calcEff(inputFile,year)

    a = analyzer(inputFile)

    file_name = inputFile.split("/")[-1]

    PnetSFFlag = False
    if("MX" in inputFile):
        CompileCpp('TIMBER/Framework/XHYanomalous_modules/HF_tagging_SF.cc')
        if(year=="2016APV"):
            CompileCpp(f"HF_tagging_SF hf_tagging({eff},2015);")
        else:
            CompileCpp(f"HF_tagging_SF hf_tagging({eff},{year});")
        a.Define(f"PnetWeights", "hf_tagging.evtWeight(nFatJet,FatJet_pt,FatJet_particleNetMD_Xbb,FatJet_particleNetMD_QCD)")
        a.Define(f"PnetNom", "PnetWeights[0]")
        a.Define(f"PnetUp", "PnetWeights[1]")
        a.Define(f"PnetDown", "PnetWeights[2]")
        pnetCorr    = Correction('PNetSF',"TIMBER/Framework/src/BranchCorrection.cc",corrtype='weight',mainFunc='evalWeight')
        
        a.AddCorrection(pnetCorr, evalArgs={'val':'PnetNom','valUp':'PnetUp','valDown':'PnetDown'})
        PnetSFFlag = True
    CompileCpp('THmodules.cc')

    year_int = int(options.year[2:4])

    if(not options.no_pdf):
        a.AddCorrection( Correction('Pdfweight','TIMBER/Framework/include/PDFweight_uncert.h',[a.lhaid],corrtype='uncert'))
    else:
        a.Define('Pdfweight__nom', '1.')
        a.Define('Pdfweight__down', '1.')
        a.Define('Pdfweight__up', '1.')
        a.Define('nLHEScaleWeight', '0')
    if year_int == 16 or year_int == 17:
        a.AddCorrection( Correction("Prefire","TIMBER/Framework/include/Prefire_weight.h",[year_int],corrtype='weight'))
        columns_to_save.extend(['Prefire__nom','Prefire__up','Prefire__down'])

    #One of the two calls is needed if we want to save TLorentzVectors
    #print("Calling interpreter")
    #ROOT.gInterpreter.ProcessLine('.L def.h+')
    #ROOT.gInterpreter.ProcessLine('.L def_h.so')

    a.Define('FatJet_vect','hardware::TLvector(FatJet_pt, FatJet_eta, FatJet_phi, FatJet_msoftdrop)')
    a.Define('DijetIdxs','PickDijets(FatJet_pt, FatJet_eta, FatJet_phi, FatJet_msoftdrop, FatJet_jetId)')
    a.Define('DijetIdx1','DijetIdxs[0]')
    a.Define('DijetIdx2','DijetIdxs[1]')
    columns_to_save.extend(["DijetIdx1", "DijetIdx2"])

    #This is just to keep it compatible with downstream framework, we do not include subjet systematics in the analysis
    a.Define("lead_sjbtag_corr__nom","1.0")
    a.Define("lead_sjbtag_corr__up","1.0")
    a.Define("lead_sjbtag_corr__down","1.0")
    a.Define("sublead_sjbtag_corr__nom","1.0")
    a.Define("sublead_sjbtag_corr__up","1.0")
    a.Define("sublead_sjbtag_corr__down","1.0")

    if "ttbar" in inputFile.lower():
        ttbar_flag = True
    else:
        ttbar_flag = False

    if ttbar_flag:
        a.Define('GenParticle_vect','hardware::TLvector(GenPart_pt, GenPart_eta, GenPart_phi, GenPart_mass)')
        a.AddCorrection(
            Correction('TptReweight','TIMBER/Framework/include/TopPt_weight.h',corrtype='weight'),
            evalArgs={
                "jet0_idx":"DijetIdxs[0]",
                "jet1_idx":"DijetIdxs[1]",
                "FatJet_vect": "FatJet_vect",
                'GenPart_vect':'GenParticle_vect',
                'scale': 0.5,
            }
       )

    #JME Corrections
    a = JM.AutoJME(a, 'FatJet', year) #, 'D')
    mass_base_calibs = "{FatJet_JES_nom, FatJet_JER_nom, FatJet_JMS_nom, FatJet_JMR_nom}"
    pt_base_calibs = "{FatJet_JES_nom, FatJet_JER_nom}"

    variations = ['JES_up', 'JER_up', 'JMS_up', 'JMR_up',
                  'JES_down', 'JER_down', 'JMS_down', 'JMR_down']



    a.Define('FatJet_pt_corr', 'hardware::MultiHadamardProduct(FatJet_pt,%s)'% pt_base_calibs) 
    a.Define('FatJet_msoftdrop_corr', 'hardware::MultiHadamardProduct(FatJet_msoftdrop,%s)'% mass_base_calibs) 
    #save as individual columns b/c issues reading vectors...
    var = "corr"
    a.Define('FatJet1_pt_corr', '(DijetIdx1 >= 0) ? FatJet_pt_%s [DijetIdx1] : 1' % var) 
    a.Define('FatJet1_msoftdrop_corr', '(DijetIdx1 >= 0) ? FatJet_msoftdrop_%s [DijetIdx1] : 1' % var) 
    a.Define('FatJet2_pt_corr', '(DijetIdx2 >= 0) ? FatJet_pt_%s [DijetIdx2] : 1' % var) 
    a.Define('FatJet2_msoftdrop_corr', '(DijetIdx2 >= 0) ? FatJet_msoftdrop_%s [DijetIdx2] : 1' % var) 

    columns_to_save.extend(['FatJet1_pt_corr', 'FatJet1_msoftdrop_corr',  'FatJet2_pt_corr', 'FatJet2_msoftdrop_corr'])

    for var in variations:
        pt_calibs, m_calibs = JMEvariationStr(var)
        a.Define('FatJet_pt_'+ var, 'hardware::MultiHadamardProduct(FatJet_pt,%s)'% pt_calibs) 
        a.Define('FatJet_msoftdrop_'+ var, 'hardware::MultiHadamardProduct(FatJet_msoftdrop,%s)'% m_calibs) 

        #save as individual columns b/c issues reading vectors...
        a.Define('FatJet1_pt_'+ var, '(DijetIdx1 >= 0) ? FatJet_pt_%s [DijetIdx1] : 1' % var) 
        a.Define('FatJet1_msoftdrop_'+ var, '(DijetIdx1 >= 0) ? FatJet_msoftdrop_%s [DijetIdx1] : 1' % var) 
        a.Define('FatJet2_pt_'+ var, '(DijetIdx2 >= 0) ? FatJet_pt_%s [DijetIdx2] : 1' % var) 
        a.Define('FatJet2_msoftdrop_'+ var, '(DijetIdx2 >= 0) ? FatJet_msoftdrop_%s [DijetIdx2] : 1' % var) 

        columns_to_save.extend(['FatJet1_pt_'+ var, 'FatJet1_msoftdrop_'+ var, 'FatJet2_pt_'+ var, 'FatJet2_msoftdrop_'+ var ])
            
    a = p.AutoPU(a, options.year)

    columns_to_save = ['Prefire__nom', 'Prefire__up', 'Prefire__down', 'DijetIdx1', 'DijetIdx2', 'FatJet1_pt_corr', 'FatJet1_msoftdrop_corr', 'FatJet2_pt_corr', 'FatJet2_msoftdrop_corr', 'FatJet1_pt_JES_up', 'FatJet1_msoftdrop_JES_up', 'FatJet2_pt_JES_up', 'FatJet2_msoftdrop_JES_up', 'FatJet1_pt_JER_up', 'FatJet1_msoftdrop_JER_up', 'FatJet2_pt_JER_up', 'FatJet2_msoftdrop_JER_up', 'FatJet1_pt_JMS_up', 'FatJet1_msoftdrop_JMS_up', 'FatJet2_pt_JMS_up', 'FatJet2_msoftdrop_JMS_up', 'FatJet1_pt_JMR_up', 'FatJet1_msoftdrop_JMR_up', 'FatJet2_pt_JMR_up', 'FatJet2_msoftdrop_JMR_up', 'FatJet1_pt_JES_down', 'FatJet1_msoftdrop_JES_down', 'FatJet2_pt_JES_down', 'FatJet2_msoftdrop_JES_down', 'FatJet1_pt_JER_down', 'FatJet1_msoftdrop_JER_down', 'FatJet2_pt_JER_down', 'FatJet2_msoftdrop_JER_down', 'FatJet1_pt_JMS_down', 'FatJet1_msoftdrop_JMS_down', 'FatJet2_pt_JMS_down', 'FatJet2_msoftdrop_JMS_down', 'FatJet1_pt_JMR_down', 'FatJet1_msoftdrop_JMR_down', 'FatJet2_pt_JMR_down', 'FatJet2_msoftdrop_JMR_down', 'DijetIdxs', 'DijetIdx1', 'DijetIdx2', 'Pileup__nom', 'Pileup__up', 'Pileup__down', 'Pdfweight__nom', 'Pdfweight__up', 'Pdfweight__down','lead_sjbtag_corr__nom', 'lead_sjbtag_corr__up',  'lead_sjbtag_corr__down','sublead_sjbtag_corr__nom', 'sublead_sjbtag_corr__up',  'sublead_sjbtag_corr__down']

    if ttbar_flag:
        columns_to_save.extend(['TptReweight__nom','TptReweight__up', 'TptReweight__down',])

    if PnetSFFlag:
        columns_to_save.extend(['PNetSF__nom','PNetSF__up', 'PNetSF__down',])


    a.Snapshot(columns_to_save, file_name, 'Events', openOption = 'RECREATE')
    a.Close()
    xrdcp_command(file_name,f"{oDir}/{file_name}")

def calcEff(inputFile,year):
    pnet_tight  = {"2016APV":0.9883,"2016":0.9883,"2017":0.9870,"2018":0.9880}
    a = analyzer(inputFile)
    a.Cut("nFatJet cut for efficiency","nFatJet>1")
    a.Cut("FatJet pt cut for efficiency","FatJet_pt[0]>300 && FatJet_pt[1]>300")
    a.Define("pt0","FatJet_pt[0]")
    a.Define("pt1","FatJet_pt[1]")
    a.Define("PnetXbb0","FatJet_particleNetMD_Xbb[0]/(FatJet_particleNetMD_Xbb[0] + FatJet_particleNetMD_QCD[0])")
    a.Define("PnetXbb1","FatJet_particleNetMD_Xbb[1]/(FatJet_particleNetMD_Xbb[1] + FatJet_particleNetMD_QCD[1])")
    a.Define("PnetHiggs","(PnetXbb0>PnetXbb1) ? PnetXbb0 : PnetXbb1")
    a.Define("ptHiggs","(PnetXbb0>PnetXbb1) ? pt0 : pt1")
    nTotal = a.DataFrame.Count().GetValue()
    pnet_cut = pnet_tight[year]
    a.Cut("Pnet cut for efficiency",f"PnetHiggs>{pnet_cut}")
    nPass = a.DataFrame.Count().GetValue()
    efficiency = nPass/nTotal
    a.Close()
    print(f"Efficiency for PNet in {inputFile} = {efficiency:.3f}")
    return efficiency

def xrdcp_command(input_file, output_file):
    cmd = f"xrdcp {input_file} {output_file}"
    print(cmd)
    os.system(cmd)
    os.system(f"rm -f {input_file}")

if(__name__ == "__main__"):
    parser  = get_opts()
    options = parser.parse_args()
    oDir    = options.oDir
    year    = options.year
    inputFile = options.inputFile
    addSys(inputFile,oDir,year)
