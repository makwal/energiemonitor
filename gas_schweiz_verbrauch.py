#!/usr/bin/env python
# coding: utf-8

# In[7]:


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
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')


# **Abzufragende urls**

# In[8]:


base_url = 'https://transparency.entsog.eu/api/v1/operationalData.csv?forceDownload=true'


# In[9]:


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
        'Oltingue': '&pointDirection=ch-tso-0002itp-00039exit,ch-tso-0001itp-00039exit,fr-tso-0003itp-00039entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Wallbach': '&pointDirection=ch-tso-0002itp-00294exit,de-tso-0019itp-00294entry,de-tso-0007itp-00294entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Basel': '&pointDirection=ch-dso-0001itp-00228exit,de-tso-0014itp-00228entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Thayngen': '&pointDirection=ch-dso-0002itp-00229exit,de-tso-0014itp-00229entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true',
        'Griespass': '&pointDirection=ch-tso-0002itp-00125exit,ch-tso-0001itp-00131exit,it-tso-0001itp-00136entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true'
        #'Jura': '&pointDirection=ch-dso-0003itp-00281exit,fr-tso-0003itp-00281entry&from={}&to={}&indicator=Physical%20Flow&periodType=day&timezone=CET&limit=-1&dataset=1&directDownload=true'
    } 
}


# **Import**

# In[11]:


df_import_curr = pd.DataFrame()

for point, url in urls['import'].items():
    u = (base_url + url).format('2022-01-01', '2022-12-31')

    df_i_temp = pd.read_csv(u)

    if len(df_i_temp['unit'].unique()) > 1 or df_i_temp['unit'].unique()[0] != 'kWh/d':
        print('alert_unit')
        break
    elif len(df_i_temp['directionKey'].unique()) > 1 or df_i_temp['directionKey'].unique()[0] != 'exit':
        print('alert_direction')
        break

    if point == 'Wallbach':
        df_i_temp = df_i_temp[df_i_temp['operatorLabel'] == 'Fluxys TENP'].copy()

    df_import_curr = pd.concat([df_import_curr, df_i_temp])

    sleep(3)


# In[12]:


import_list = os.listdir('root/energiemonitor/gasverbrauch_daten/import')

for file in import_list:
    dfi = pd.read_csv(f'root/energiemonitor/gasverbrauch_daten/import/{file}')

    df_import_curr = pd.concat([df_import_curr, dfi])


# **Export**

# In[16]:


df_export_curr = pd.DataFrame()

for point, url in urls['export'].items():
    u = (base_url + url).format('2022-01-01', '2022-12-31')

    df_e_temp = pd.read_csv(u)

    if len(df_e_temp['unit'].unique()) > 1 or df_e_temp['unit'].unique()[0] != 'kWh/d':
        print('alert_unit')
        break
    elif len(df_e_temp['directionKey'].unique()) > 1 or df_e_temp['directionKey'].unique()[0] != 'entry':
        print('alert_direction')
        break

    df_export_curr = pd.concat([df_export_curr, df_e_temp])
    sleep(3)

export_list = os.listdir('root/energiemonitor/gasverbrauch_daten/export')

for file in export_list:
    dfe = pd.read_csv(f'root/energiemonitor/gasverbrauch_daten/export/{file}')

    df_export_curr = pd.concat([df_export_curr, dfe])


# **Daten für Wrangling vorbereiten**

# In[85]:


def data_preparator(df_func):

    df_func = df_func[['pointLabel', 'operatorLabel', 'periodFrom', 'periodTo', 'value']].copy()

    df_func['periodFrom'] = pd.to_datetime(df_func['periodFrom'])
    df_func['date'] = df_func['periodFrom'].dt.date

    df_func.set_index('date', inplace=True)

    df_func = df_func.groupby('date')['value'].sum().to_frame()

    return df_func


# In[86]:


df_import = data_preparator(df_import_curr)
df_export = data_preparator(df_export_curr)


# **Merge Import/Export**

# In[87]:


df_final = df_import.merge(df_export, left_index=True, right_index=True, how='left')
df_final.rename(columns={'value_x': 'import', 'value_y': 'export'}, inplace=True)


# Den letzten Tag lassen wir draussen

# In[88]:


df_final = df_final.iloc[:-1].copy()


# Die Import-Export-Differenz berechnen (das, was in der Schweiz bleibt)

# In[89]:


df_final['diff'] = df_final['import'] - df_final['export']


# Formatieren

# In[90]:


df_final.reset_index(inplace=True)
df_final['date'] = pd.to_datetime(df_final['date'])
df_final['year'] = df_final['date'].dt.year
df_final['date_show'] = '2022-' + df_final['date'].dt.strftime('%m-%d')


# **Endberechnung** anhand des gleitenden 7-Tages-Medians (Median, weil es Ausreisser in den Daten hat)

# In[91]:


df_final['diff_rolling_median'] = df_final['diff'].rolling(7).median()


# Mehrjährigen Durchschnitt berechnen und mit den Daten des aktuellen Jahrs mergen (2016 bis 2021, weil es 2015 grobe Ausreisser drin hat)

# In[92]:


df_mean = df_final[(df_final['year'] > 2015) & (df_final['year'] < 2022)].groupby('date_show')['diff_rolling_median'].mean().to_frame().rename(columns={'diff_rolling_median': 'Durchschnitt 2016-2021'})
df22 = df_final[df_final['year'] == 2022].set_index('date_show')[['diff_rolling_median']].rename(columns={'diff_rolling_median': '2022'})


# Merge

# In[93]:


df_end = df_mean.merge(df22, left_index=True, right_index=True, how='left')


# Die Daten von Kilo- zu Gigawattstunden umformen

# In[94]:


df_end = df_end / 10**6


# Formatieren (u.a. den 29. Februar weglassen, da nur alle vier Jahre)

# In[95]:


df_end.reset_index(inplace=True)
df_end = df_end[df_end['date_show'] != '2022-02-29'].copy()
df_end = df_end[['date_show', '2022', 'Durchschnitt 2016-2021']].copy()


# In[97]:


df_end.to_csv('root/energiemonitor/data/gas/gas_verbrauch_schweiz.csv', index=False)


# **Datawrapper-Update**

# In[112]:


chart_id = 'fOPdl'

last_updated = datetime.today().strftime('%-d. %B %Y')


# In[113]:


def chart_updater(chart_id, last_updated):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {
                'annotate': {'notes': f'Die letzten zwei Tage werden wegen unvollständiger Daten nicht berücksichtigt. Wird täglich aktualisiert, zuletzt am {last_updated}.'}}

    }

    res_update = requests.patch(url_update, json=payload, headers=datawrapper_headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=datawrapper_headers)


# In[114]:


chart_updater(chart_id, last_updated)

