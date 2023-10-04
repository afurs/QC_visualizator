import os
import subprocess
import json
import time
from ROOT import TFile
from ROOT import TH1
from ROOT import TH2
from datetime import datetime
import urllib.parse
import posixpath

class QC_processor(object):
    severity='error'
    path_qc_url='http://ali-qcdb-gpn.cern.ch:8083/'
    path_metadata_base='browse/qc'
    base_command_curl='curl -s \'{}\' --header \"Accept: application/json\"'
    def __init__(self,path_task,hists,keep_hist_files=False,nchannels=None,do_init=True):
        self.path_task = path_task
        self.keep_hist_files=keep_hist_files
        self.hists = hists
        self.dict_data = {}
        self.dict_metadata={}
        self.path_task=self.path_task
        self.nchannels=nchannels
        print('Getting metadata from:',self.path_task)

        subfolders=QC_processor.get_subfolders(self.path_task)
        list_hists=[posixpath.basename(subfolder) for subfolder in subfolders]
        for hist_name in hists:
            if not hist_name in list_hists:
                print('Error! \"{}\" hist wasn\'t produced by task \"{}\"'.format(hist_name,path_task))
                continue
            print('Preparing metadata for hist \"{}\"'.format(hist_name))
            entry_hist=self.dict_metadata.setdefault(hist_name,{})
            entry_hist['path']=posixpath.join(self.path_task,hist_name)
            entry_hist['metadata']=QC_processor.get_json_header(entry_hist['path'])
            #filename_json = '{}.json'.format(entry_hist['path'].replace('/','_'))
            #with open(filename_json, "w") as fp:
                #json.dump(entry_hist['metadata'] ,fp,skipkeys=True,default=lambda o: '')  
        if do_init==True:
            self.init_hist_metadata()

    @staticmethod
    def get_hist_metadata(hist_name,dict_metadata,dict_metadata_entry,field_ts='createdAt'):
        #runnum = dict_metadata_entry['RunNumber']
        #print(dict_metadata_entry)
        runnum = dict_metadata_entry.get('RunNumber','None')
        if runnum == 'None' and QC_processor.severity=='warning':
            print('No runnum in:\n',dict_metadata_entry)
        timestamp = dict_metadata_entry[field_ts]
        date_time = str(datetime.fromtimestamp(timestamp/1000.))
        #date_time=timestamp
        filename = dict_metadata_entry['fileName']
        filepath = urllib.parse.urljoin(QC_processor.path_qc_url,dict_metadata_entry['replicas'][0])
        hist_name_ts='{}_{}'.format(hist_name,timestamp)
        
        entry_new = {
            'filename': filename,
            'filepath': filepath,
            'hist_name': hist_name_ts
        }
        entry=dict_metadata.setdefault(runnum,{}).setdefault(date_time,entry_new)
        return dict_metadata
        
        
    def init_hist_metadata(self,field_ts='Created'):
        for hist_name,entry_metadata in self.dict_metadata.items():
            hist_entry = self.dict_data.setdefault(hist_name,{})
            for entry in entry_metadata['metadata']['objects']:
                QC_processor.get_hist_metadata(hist_name,hist_entry,entry,field_ts)
        

        
    @staticmethod
    def get_json_header(path_local):
        path_local = posixpath.join(QC_processor.path_metadata_base,path_local)
        #print(QC_processor.path_qc_url,path_local)
        path_full=urllib.parse.urljoin(QC_processor.path_qc_url,path_local)
        
        command_curl=QC_processor.base_command_curl.format(path_full)
        #print('Command:',command_curl)
        output_stream = os.popen(command_curl)
        json_text=output_stream.read()
        output_stream.close()
        #print(json_text)
        return json.loads(json_text)
    
    @staticmethod
    def get_subfolders(path_local):
        return QC_processor.get_json_header(path_local)['subfolders']
    
    def get_hist(self,hist_name,runnum,date_time):
        entry = self.init_hist(hist_name,runnum,date_time)
        return entry.get('hist',None)
    
    def init_hist(self,hist_name,runnum,date_time):
        entry = self.dict_data.get(hist_name,{}).get(runnum,{}).get(date_time,None)
        if entry is None:
            print('Cannot fetch entry')
            return {}
        is_ready = entry.setdefault('is_ready',False)
        if is_ready==False:
            command_curl='curl -s {} --output {} > /dev/null 2>&1'.format(entry['filepath'], entry['filename'])
            p = subprocess.Popen(command_curl,stdin=None,stdout=None, shell=True).wait()
            path_root_obj='ccdb_object'
            tfile = TFile.Open(entry['filename'])
            hist=tfile.Get(path_root_obj).Clone(entry['hist_name'])
            hist.SetDirectory(0)
            time.sleep(0.1)
            tfile.Close()
            entry['hist']=hist
        if is_ready==False and self.keep_hist_files==False:
            time.sleep(0.1)
            command_rm='rm {}'.format(entry['filename'])
            os.system(command_rm)
        return entry
        
    def get_hist_proj(self,hist_name,runnum,date_time,bin_pos,axis='y'):
        bin_pos=int(bin_pos)
        hist=self.get_hist(hist_name,runnum,date_time)
        entry_data = self.dict_data.get(hist_name,{}).get(runnum,{}).get(date_time,None)
        if hist is None or entry_data is None:
            print('Cannot fetch hist')
            return None
        if not (axis=='x' or axis=='y'):
            return None
        entry = entry_data.setdefault('proj',{}).setdefault(axis,{})
        hist_proj=entry.get(bin_pos,None)
        if not hist_proj is None:
            return hist_proj
        if axis=='x':
            hist_proj= hist.ProjectionX('{}_projX_chID{}'.format(hist.GetName(),bin_pos),bin_pos+1,bin_pos+1)
            
        elif axis=='y':
            hist_proj= hist.ProjectionY('{}_projY_chID{}'.format(hist.GetName(),bin_pos),bin_pos+1,bin_pos+1)
        hist_proj.SetTitle('{} channel {}'.format(hist_proj.GetTitle(),bin_pos))
        entry[bin_pos]=hist_proj
        return hist_proj
       
    @classmethod
    def from_json(cls,det='FT0',task='DigitQcTask',keep_files=False,path_cfg='config.json',do_init=True):
        with open(path_cfg) as json_file:
            dict_cfg = json.load(json_file)
            dict_det=dict_cfg['qc_processors'][det]
            dict_entry=dict_det['tasks'][task]
            path_qc = posixpath.join(det,'MO',task)
            return cls(path_qc,dict_entry['hists'],keep_files,dict_det['nchannels'],do_init)
                    
                
        