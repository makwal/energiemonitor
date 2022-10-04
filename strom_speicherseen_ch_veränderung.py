#!/usr/bin/env python
# coding: utf-8

# # Speichersee-Daten Schweiz: Veränderung

# In[1]:


import requests
import pandas as pd
from datetime import datetime
from time import sleep
from energy_settings import (
    backdate,
    curr_year,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers
)
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')


# **Daten-Import**

# In[2]:


url = 'https://www.uvek-gis.admin.ch/BFE/ogd/17/ogd17_fuellungsgrad_speicherseen.csv'


# In[3]:


df = pd.read_csv(url)


# In[4]:


df['Datum'] = pd.to_datetime(df['Datum'])


# **Datenverarbeitung**

# Absolute Veränderung errechnen

# In[5]:


df['Füllstand Veränderung'] = df['TotalCH_speicherinhalt_gwh'].diff()


# Kalenderwochen-Angaben eruieren

# In[6]:


df['Kalenderwoche'] = df['Datum'].dt.isocalendar().week
df['Kalenderwoche_show'] = df['Datum'].dt.strftime('%YW%U')


# Den Durchschnitt pro Kalenderwoche eruieren

# In[7]:


df_mean = df[df['Datum'] <= '2022-01-01'].groupby('Kalenderwoche')['Füllstand Veränderung'].mean().to_frame()


# df des aktuellen Jahrs erstellen, formatieren und am Schuss alle dfs zusammenfügen.

# In[8]:


df22 = df[df['Datum'] >= '2022-01-01'][['Kalenderwoche', 'Füllstand Veränderung']].set_index('Kalenderwoche').copy()


# In[9]:


df_mean.rename(columns={'Füllstand Veränderung': 'Mittelwert'}, inplace=True)
df22.rename(columns={'Füllstand Veränderung': '2022'}, inplace=True)


# In[10]:


df_final = df_mean.join(df22)
df_final.reset_index(inplace=True)


# Kalenderwochenformat anpassen für Datawrapper

# In[11]:


df_final['Kalenderwoche_show'] = '2022W' + df_final['Kalenderwoche'].astype(str)
df_final = df_final[['Kalenderwoche_show', '2022', 'Mittelwert']].copy()


# Balkenfarbe für jede Woche bestimmen

# In[12]:


df_color = df_final.copy()


# In[13]:


df_color.loc[df_color['2022'] >= 0, 'farbe'] = '#e4d044'
df_color.loc[df_color['2022'] < 0, 'farbe'] = '#686000'
df_color = df_color[df_color['2022'].notna()].copy()

color_assign = df_color[['Kalenderwoche_show', 'farbe']].set_index('Kalenderwoche_show').to_dict()['farbe']


# **Export**

# In[ ]:


#Backups
df_final.to_csv(f'/root/energiemonitor/backups/strom/speicherseen_ch_veränderung_{backdate(0)}.csv', index=False)

#Data
df_final.to_csv('/root/energiemonitor/data/strom/speicherseen_ch_veränderung.csv', index=False)


# **Datawrapper-Update**

# In[ ]:


last_updated = datetime.today().strftime('%-d. %B %Y')


# In[ ]:


chart_id = 'F26cs'


# In[ ]:


def chart_updater(chart_id, color_assign, last_updated):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

        'metadata': {
                'visualize': {'custom-colors': color_assign},
                'annotate': {'notes': f'Wird wöchentlich aktualisiert, zuletzt am {last_updated}.'}
        }

    }

    res_update = requests.patch(url_update, json=payload, headers=datawrapper_headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=datawrapper_headers)


# In[ ]:


chart_updater(chart_id, color_assign, last_updated)

