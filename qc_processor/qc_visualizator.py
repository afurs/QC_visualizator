import ipywidgets as wd
from IPython.display import display, clear_output

from ROOT import TCanvas
class visualizator(object):
    def __init__(self,qc):
        self.qc=qc
        self.hist_widget = wd.Dropdown(options = list(self.qc.dict_data.keys()), description = 'Hist')
        
        self.run_widget = wd.Dropdown(description = 'Run')
        self.update_run(None)
        self.datetime_widget = wd.Dropdown(description = 'Date-time')
        self.update_datetime(None)
        self.hist_widget.observe(self.update_run, names='value')
        self.run_widget.observe(self.update_datetime, names='value')
        box_layout = wd.Layout(display='flex',
                    flex_flow='row',
                    align_items='stretch')
        self.chid_widget = wd.BoundedIntText(value=0,
                                        min=0,
                                        max=self.qc.nchannels-1,
                                        step=1,
                                        description='ChannelID:',
                                        disabled=False)
        

        self.bt_upload_hist=wd.Button(description="Upload hist")
        self.bt_upload_hist.on_click(self.bt_upload_hist_click)
        self.bt_proj_hist=wd.Button(description="Make proj")
        self.bt_proj_hist.on_click(self.bt_proj_click)
        
        self.box1=wd.Box(children=[self.hist_widget,
                                   self.run_widget,
                                   self.datetime_widget,
                                   self.bt_upload_hist
                                 ], layout=box_layout)
        self.box2=wd.Box(children=[self.chid_widget,
                                  self.bt_proj_hist
                                 ], layout=box_layout)        

    def update_run(self,change):
        self.run_widget.options = list(self.qc.dict_data[self.hist_widget.value].keys())
    def update_datetime(self,change):
        self.datetime_widget.options = list(self.qc.dict_data[self.hist_widget.value][self.run_widget.value].keys())

    def bt_upload_hist_click(self,bt):
        clear_output()
        canv=TCanvas()
        run=self.run_widget.value
        date_time=self.datetime_widget.value
        hist_name=self.hist_widget.value
        hist=self.qc.get_hist(hist_name=hist_name,
                         runnum=run,
                         date_time=date_time)
        if hist is None:
            print('None')
            return
        canv.cd()
        canv.Draw()
        display(self.box1)
        display(self.box2)
        hist.Print()
        hist.Draw()
    def bt_proj_click(self,bt):
        clear_output()
        canv=TCanvas()
        run=self.run_widget.value
        date_time=self.datetime_widget.value
        bin_pos=self.chid_widget.value
        hist_name=self.hist_widget.value
        hist=self.qc.get_hist_proj(hist_name=hist_name,runnum=run,date_time=date_time,bin_pos=bin_pos)
        if hist is None:
            print('None')
            return
        canv.cd()
        canv.Draw()
        display(self.box1)
        display(self.box2)
        hist.Print()
        hist.Draw()
    def show(self):
        display(self.box1)
        display(self.box2)
        
        