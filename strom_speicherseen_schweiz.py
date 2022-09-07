#!/usr/bin/env python
# coding: utf-8

# # Speichersee-Daten Schweiz

# In[42]:


import requests
import pandas as pd
from datetime import datetime
from time import sleep
from energy_settings import backdate, datawrapper_api_key, datawrapper_url, datawrapper_headers


# **Daten-Import**

# In[43]:


url = 'https://www.uvek-gis.admin.ch/BFE/ogd/17/ogd17_fuellungsgrad_speicherseen.csv'


# In[44]:


df = pd.read_csv(url)


# In[45]:


df['Datum'] = pd.to_datetime(df['Datum'])


# **Datenvearbeitung**

# Füllstand in Prozent errechnen

# In[46]:


df['Füllstand total'] = (df['TotalCH_speicherinhalt_gwh'] / df['TotalCH_max_speicherinhalt_gwh']) * 100


# Kalenderwochen-Angaben eruieren

# In[47]:


df['Kalenderwoche'] = df['Datum'].dt.isocalendar().week


# Für jede Kalenderwoche den Minimal-, Maximal- und mittleren Wert berechnen.

# In[50]:


df_mean = df[df['Datum'] <= '2022-01-01'].groupby('Kalenderwoche')['Füllstand total'].mean().to_frame()
df_max = df[df['Datum'] <= '2022-01-01'].groupby('Kalenderwoche')['Füllstand total'].max().to_frame()
df_min = df[df['Datum'] <= '2022-01-01'].groupby('Kalenderwoche')['Füllstand total'].min().to_frame()


# Separates df für 2022

# In[51]:


df22 = df[df['Datum'] >= '2022-01-01'][['Kalenderwoche', 'Füllstand total']].set_index('Kalenderwoche')


# Vorbereitung für nachfolgenden Join

# In[52]:


df_mean.rename(columns={'Füllstand total': 'Mittelwert'}, inplace=True)
df_max.rename(columns={'Füllstand total': 'Maximum'}, inplace=True)
df_min.rename(columns={'Füllstand total': 'Minimum'}, inplace=True)
df22.rename(columns={'Füllstand total': '2022'}, inplace=True)


# Alle Daten zusammenfügen. Wichtig: Standard-Join, nicht how=outer, damit alle Kalenderwochen genommen werden (nicht jene des angebrochenen Jahres).

# In[53]:


df_final = df_mean.join([df_max, df_min, df22])


# In[54]:


df_final = df_final[['2022', 'Mittelwert', 'Maximum', 'Minimum']].copy()


# **Export**

# In[55]:


#Backups
df_final.to_csv(f'/root/energiemonitor/backups/strom/speicherseen_ch_füllstand_{backdate(0)}.csv')

#Data
df_final.to_csv('/root/energiemonitor/data/strom/speicherseen_ch_füllstand.csv')

