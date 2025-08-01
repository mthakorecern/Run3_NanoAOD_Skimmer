# Run3 NanoAOD Skimmer

**Purpose:**  
A tool to skim CMS Run‑3 NanoAOD files - filtering events based on physics selection criteria and producing reduced-size ROOT files.

---

## Installation and Setup
<pre>source /cvmfs/cms.cern.ch/cmsset_default.sh
cmsrel CMSSW_15_0_2
cd CMSSW_15_0_2/src/
cmsenv

git clone https://github.com/mthakorecern/Run3_NanoAOD_Skimmer.git

cd Run3_NanoAOD_Skimmer
voms-proxy-init -voms cms -out /tmp/x509up_u10196 -valid 192:00<pre>

Now to run skimming jobs for  a particular datset:

<pre>
python3 skimNtuples.py --skimFileConfiguration WJets_2024.json   \
--datasetKey /WtoENu-4Jets_TuneCP5_13p6TeV_madgraphMLM-pythia8/RunIII2024Summer24NanoAODv15-150X_mcRun3_2024_realistic_v2-v2/NANOAODSIM    \
--skimCutConfiguration 2024_looseGeneralSkimCuttingConfiguration.json  \
--skimBranchCancelations Null_Cancelation.json \
--destination /hdfs/store/user/mithakor/2024_skimmed   \
--prepareCondorSubmission  \
--submitDirPath /nfs_scratch/mithakor/condor-jobs
<pre>

