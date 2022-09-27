#!/usr/bin/env python
# coding: utf-8

# In[ ]:


import os
import requests
import pandas as pd
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
from time import sleep
from energy_settings import (
    backdate,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers
)


# In[ ]:


pd.set_option('display.max_columns', None)


# **Daten-Abfrage**

# In[ ]:


base_url = 'https://transparency.entsog.eu/api/v1/operationalData.csv?forceDownload=true'


# In[ ]:


urls = {
    'import': {
        'Oltingue': '&pointDirection=fr-tso-0003itp-00039exit,ch-tso-0002itp-00039entry,ch-tso-0001itp-00039entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Wallbach': '&pointDirection=de-tso-0007itp-00294exit,de-tso-0009itp-00294exit&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Basel': '&pointDirection=de-tso-0014itp-00228exit,ch-dso-0001itp-00228entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Thayngen': '&pointDirection=de-tso-0014itp-00229exit,ch-dso-0002itp-00229entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Bizzarone': '&pointDirection=it-tso-0001itp-00278exit&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Griespass': '&pointDirection=it-tso-0001itp-00136exit,ch-tso-0001itp-00131entry,ch-tso-0002itp-00125entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true'
        #'Jura': '&pointDirection=fr-tso-0003itp-00281exit,ch-dso-0003itp-00281entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true'
    },
    'export': {
        'Oltingue': '&pointDirection=fr-tso-0003itp-00039entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Wallbach': '&pointDirection=ch-tso-0002itp-00294exit,de-tso-0019itp-00294entry,de-tso-0007itp-00294entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Basel': '&pointDirection=ch-dso-0001itp-00228exit,de-tso-0014itp-00228entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Thayngen': '&pointDirection=ch-dso-0002itp-00229exit,de-tso-0014itp-00229entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Griespass': '&pointDirection=ch-tso-0002itp-00125exit,ch-tso-0001itp-00131exit,it-tso-0001itp-00136entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true'
        #'Jura': '&pointDirection=ch-dso-0003itp-00281exit,fr-tso-0003itp-00281entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true'
    } 
}


# In[ ]:


year_range = range(2015,2023)


# Import

# In[ ]:


df_import = pd.DataFrame()


# In[ ]:


for year in year_range:
    for point, url in urls['import'].items():
        u = (base_url + url).format(f'{year}-01-01', f'{year}-12-31')

        df_i_temp = pd.read_csv(u)

        if len(df_i_temp['unit'].unique()) > 1 or df_i_temp['unit'].unique()[0] != 'kWh/d':
            print('alert_unit')
        elif len(df_i_temp['directionKey'].unique()) > 1 or df_i_temp['directionKey'].unique()[0] != 'exit':
            print('alert_direction')

        if point == 'Wallbach':
            df_i_temp = df_i_temp[df_i_temp['operatorLabel'] == 'Fluxys TENP'].copy()

        df_import = pd.concat([df_import, df_i_temp])

        sleep(3)


# In[ ]:


df_import.to_csv('/root/energiemonitor/data/gas/import_data_ch.csv', index=False)


# Export

# In[ ]:


df_export = pd.DataFrame()


# In[ ]:


for year in year_range:
    for point, url in urls['export'].items():
        u = (base_url + url).format(f'{year}-01-01', f'{year}-12-31')
        df_e_temp = pd.read_csv(u)

        if len(df_e_temp['unit'].unique()) > 1 or df_e_temp['unit'].unique()[0] != 'kWh/d':
            print('alert_unit')
        elif len(df_e_temp['directionKey'].unique()) > 1 or df_e_temp['directionKey'].unique()[0] != 'entry':
            print('alert_direction')

        df_export = pd.concat([df_export, df_e_temp])

        sleep(3)


# In[ ]:


df_export.to_csv('/root/energiemonitor/data/gas/export_data_ch.csv', index=False)

