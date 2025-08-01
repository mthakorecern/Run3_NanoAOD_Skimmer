#!/usr/bin/env python3

import ROOT 
ROOT.PyConfig.IgnoreCommandLineOptions = True
import argparse
import glob
import re
import json
import math
import datetime
import os
from tqdm import tqdm
from skimManager import skimManager 
import sys
sys.path.insert(0, os.getcwd())


def main(args):
    print(f"[DEBUG] Using Python executable: {sys.executable}")
    print('Setting up the skim...')
    with open(args.skimFileConfiguration) as jsonInputFile:
        jsonFileDict = json.load(jsonInputFile)


    if args.datasetKey not in jsonFileDict:
        print(f'Dataset key "{args.datasetKey}" not found in skim configuration.')
        print(f'Available keys: {list(jsonFileDict.keys())}')
        exit(1)

    json_keys = list(jsonFileDict.keys())
    job_index = json_keys.index(args.datasetKey) if args.datasetKey in json_keys else 0
    job_index_str = f"{job_index:03d}"

    metadata = jsonFileDict[args.datasetKey]
    short_name = metadata.get("short_name", "Unknown")
    listOfFiles = metadata.get("files", [])
    

    if not listOfFiles:
        print(f'No files found for dataset "{args.datasetKey}".')
        exit(1)

    print(f'Selected dataset: {args.datasetKey}')
    print(f'Short name: {short_name}')
    print(f'Number of files: {len(listOfFiles)}')

    if args.prepareCondorSubmission:
        timestamp = datetime.datetime.now().strftime('%d%b%y_%H%M')
        job_name = f"{short_name}_{timestamp}_{job_index_str}" + ('' if args.skimSuffix == '' else f"_{args.skimSuffix}")
        overallSubmitDir = os.path.join(args.submitDirPath, job_name)
        dagLocation = os.path.join(overallSubmitDir, 'dags')
        os.makedirs(os.path.join(dagLocation, 'daginputs'), exist_ok=True)

        inputFileTextName = os.path.join(dagLocation, 'daginputs', job_name + '_input.txt')
        with open(inputFileTextName, 'w') as f:
            f.write('\n'.join(listOfFiles))

        cutFileName = os.path.basename(args.skimCutConfiguration)
        branchCancelationFileName = os.path.basename(args.skimBranchCancelations) if args.skimBranchCancelations else ''
        
        commandList = [
            'farmoutAnalysisJobs',
            '--fwklite',
            '--infer-cmssw-path',
            '--input-files-per-job=1',
            '--use-singularity=rhel9',
            f'--input-file-list={inputFileTextName}',
            '--assume-input-files-exist',
            '--max-usercode-size=350',
            f'--submit-dir={overallSubmitDir}/submit',
            f'--output-dag-file={dagLocation}/dag',
            f'--output-dir={args.destination}/{job_name}',
            '--opsys=rhel9',
            '--memory-requirements=10000',
            '--disk-requirements=10000',
            '--input-dir=/',
            '--extra-inputs=' + ','.join(filter(None, [
                args.skimCutConfiguration,
                args.skimBranchCancelations,
                '/afs/hep.wisc.edu/home/mithakor/HH_bb_tautau_Analysis/CMSSW_15_0_10/src/forv15/singleFileSkimForSubmission.py',
                '/afs/hep.wisc.edu/home/mithakor/HH_bb_tautau_Analysis/CMSSW_15_0_10/src/forv15/skimManager.py',
                '/afs/hep.wisc.edu/home/mithakor/HH_bb_tautau_Analysis/CMSSW_15_0_10/src/forv15/cutManager.py'
            ])),
            job_name,
            os.environ['CMSSW_BASE']+'/src/forv15/singleFileSkimForSubmission.py',
            '--',
            '\'--inputFile=$inputFileNames\'',
            ''+('"--branchCancelationFile='+branchCancelationFileName+'"' if args.skimBranchCancelations != None else ''),
            '"--theCutFile='+cutFileName+'"',
            '\'--outputFileName=$outputFileName\'',
        ]

        theCommand = ' '.join([c for c in commandList if c.strip()])
        print(f'\n Submitting job with command:\n{theCommand}\n')
        #os.system(theCommand)
        retcode = os.system(theCommand)
        print(f"[DEBUG] farmoutAnalysisJobs exited with code {retcode}")

    else:
        print(f'Running local skim on {len(listOfFiles)} files...')
        for i, inputFile in enumerate(tqdm(listOfFiles, desc='Skimming')):
            digits = int(math.floor(math.log10(len(listOfFiles)))) + 1
            file_index = f"{i:0{digits}}"
            outputFileName = os.path.join(args.destination, f"{short_name}_{file_index}.root")

            theSkimManager = skimManager()
            theSkimManager.skimAFile(
                fileName=inputFile,
                branchCancelationFileName=args.skimBranchCancelations,
                theCutFile=args.skimCutConfiguration,
                outputFileName=outputFileName
            )

            
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description = 'Skim HDFS nanoAOD ntuples down to size in a configurable way')
    ##Added by GP
    #parser.add_argument('--type',nargs='?',required=True,help='Where these are data or mc files, accordingly we will store the gen weights on cutflow',choices=['Data','MC'],type=str)
    ####
    parser.add_argument('--skimFileConfiguration',nargs='?',required=True,help='JSON file describing the paths/files to be skimmed',type=str)
    parser.add_argument('--datasetKey', required=True, help='Exact dataset key from JSON to skim', type=str)
    parser.add_argument('--skimCutConfiguration',nargs='?',required=True,help='JSON file describing the cuts to be implemented in the files')
    parser.add_argument('--skimBranchCancelations',nargs='?',help='JSON file describing the branches that do not need to be ported around with the skimmed nanoAOD file')
    parser.add_argument('--destination',nargs='?',type=str,required=True,help='destination path for resut files')
    parser.add_argument('--skimSuffix',nargs='?',type=str,default='',help='String to identify the set of skims with')
    parser.add_argument('--prepareCondorSubmission',action='store_true',help='Instead of attempting the overall skimming on a local CPU, prepare a "Combine-style" submission to condor')
    parser.add_argument('--submitDirPath',nargs='?',default='/nfs_scratch/'+os.environ['USER']+'/',help='usually a place in nfs_scratch where the submit files wil be stored. Please do not end it with /')
    args = parser.parse_args()
    main(args)


'''
commandList = [
            'farmoutAnalysisJobs',
            '--fwklite',
            '--infer-cmssw-path',
            '--input-files-per-job=1',
            '--use-singularity=rhel9',
            f'--input-file-list={inputFileTextName}',
            '--assume-input-files-exist',
            '--max-usercode-size=350',
            f'--submit-dir={overallSubmitDir}/submit',
            f'--output-dag-file={dagLocation}/dag',
            f'--output-dir={args.destination}/{job_name}',
            '--opsys=rhel9',
            '--job-count=1',
            '--memory-requirements=5000',
            '--disk-requirements=8000',
            '--input-dir=/',
            '--extra-inputs=' + ','.join(filter(None, [
                args.skimCutConfiguration,
                args.skimBranchCancelations,
                '/afs/hep.wisc.edu/home/mithakor/HH_bb_tautau_Analysis/CMSSW_15_0_10/src/forv15/runSkimmer.sh',
                '/afs/hep.wisc.edu/home/mithakor/HH_bb_tautau_Analysis/CMSSW_15_0_10/src/forv15/singleFileSkimForSubmission.py',
                '/afs/hep.wisc.edu/home/mithakor/HH_bb_tautau_Analysis/CMSSW_15_0_10/src/forv15/skimManager.py',
                '/afs/hep.wisc.edu/home/mithakor/HH_bb_tautau_Analysis/CMSSW_15_0_10/src/forv15/cutManager.py'
            ])),
            job_name,
            os.environ['CMSSW_BASE']+'/src/forv15/runSkimmer.sh',
            '--',
            '$inputFileNames',
            '$outputFileName',
            '--theCutFile=' + cutFileName,
        ] + ([f'--branchCancelationFile={branchCancelationFileName}'] if args.skimBranchCancelations else [])
        '''