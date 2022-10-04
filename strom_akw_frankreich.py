#!/usr/bin/env python
# coding: utf-8

# # Produktion der AKW in Frankreich

# **API-Dokumentation Entso-E** 
# 
# https://transparency.entsoe.eu/content/static_content/Static%20content/web%20api/Guide.html
# 
# 4.4.8. Aggregated Generation per Type [16.1.B&C]

# **Erklärung der url**
# 
# - base_url = 'https://web-api.tp.entsoe.eu/api'
# - documentType=A75 heisst Actual generation per type (A.9. DocumentType)
# - &processType=A16 heisst realised (A.7. ProcessType)
# - psrType=B14 heisst nuclear (A.5. PsrType)
# - in_Domain=10YFR-RTE------C heisst France (A.10. Areas)
# - periodStart und periodEnd sind selbsterklärend

# In[1]:


import requests
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
import xmltodict
import json
from energy_settings import (
    api_key_entsoe,
    backdate,
    curr_year,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers
)
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')


# In[2]:


api_key = api_key_entsoe


# **Generelle Variablen, die später gebraucht werden**

# In[3]:


start_period = '01010000'
end_period = '12312300'
today = datetime.now().date()
today = today.strftime('%Y-%m-%d')


# **Daten-Bezug und -Verarbeitung**

# Funktion, die die Daten herunterlädt

# In[4]:


def requester(start_date, end_date):
    url = f'https://web-api.tp.entsoe.eu/api?documentType=A75&processType=A16&psrType=B14&in_Domain=10YFR-RTE------C&periodStart={start_date}&periodEnd={end_date}&securityToken={api_key}'
    
    r = requests.get(url)
    
    decoded_response = r.content.decode('utf-8')
    response_json = json.loads(json.dumps(xmltodict.parse(r.content)))
    raw_data = response_json['GL_MarketDocument']['TimeSeries']
    
    return raw_data


# In[8]:


r = requester('202209120000', '202209160000')


# In[9]:


r


# Funktion, die gebraucht wird, um für jeden Wert das richtige Datum zu errechnen
# 
# Formel:
# 
# Period.timeInterval.start + ((Point.position – 1) * Period.resolution(in minutes))
# 
# Quelle: Seite 11 https://transparency.entsoe.eu/content/static_content/download?path=/Static%20content/knowledge%20base/entso-e-transparency-xml-schema-use-1-0.pdf&loggedUserIsPrivileged=false

# In[5]:


def minute_maker(x, start_time):
    minutes = (x - 1) * 60
    new_minutes = start_time + timedelta(minutes=minutes)
    return new_minutes


# Funktion, die für jedes Jahr im Datensatz ein df erstellt. Die Daten werden pro Jahr abgefragt, siehe unten. Pro Jahr sind sie aber nochmals portioniert. Darum werden die einzelnen Portionen in einem For-Loop in ein df_temp gespeichert und dann dem df_year hinzugefügt.

# In[6]:


def data_wrangler(res):
    df_year = pd.DataFrame()
    
    for i in range(len(res)):
        start_time = res[i]['Period']['timeInterval']['start']
        start_time = pd.to_datetime(start_time)

        data = res[i]['Period']['Point']
        
        try:
            df_temp = pd.DataFrame(data)
        except ValueError:
            df_temp = pd.DataFrame([data])
        
        df_temp['position'] = df_temp['position'].astype(int)
        df_temp['date'] = df_temp['position'].apply(lambda x: minute_maker(x, start_time))
        df_year = pd.concat([df_year, df_temp])

    return df_year


# In[7]:


def main_function(start_date, end_date):
    
    res = requester(start_date, end_date)
    
    sleep(3)
    
    df = data_wrangler(res)
    return df


# **Funktion ausführen**

# In[8]:


df_all = pd.DataFrame()


# Hier kommen die df_year zurück und werden zum grossen ganzen df_all zusammengefügt.

# In[9]:


for i in range(2015, curr_year + 1):
    start_date = str(i) + start_period
    end_date = str(i) + end_period
        
    df = main_function(start_date, end_date)
    
    df_all = pd.concat([df_all, df])


# Bearbeitung von df_all

# In[10]:


df_all.reset_index(drop=True, inplace=True)
df_all['quantity'] = df_all['quantity'].astype(int)
df_all = df_all[['date', 'quantity']].copy()

df_all['date_only'] = df_all['date'].dt.date
df_all['date_only'] = df_all['date_only'].astype(str)
df_all['date_only'] = pd.to_datetime(df_all['date_only'])

df_all.set_index('date_only', inplace=True)


# Daten zu wöchentlichem Intervall resamplen und gleichzeitig von Mega- zu Gigawatt umformen

# In[11]:


df_final = df_all[['quantity']].resample('W').sum() / 1000

df_final.reset_index(inplace=True)
df_final['week_num'] = df_final['date_only'].dt.isocalendar().week


# Für jede Kalenderwoche den Minimal-, Maximal- und mittleren Wert berechnen.

# In[12]:


df_mean = df_final[df_final['date_only'] <= '2021-12-31'].groupby('week_num')['quantity'].mean().to_frame()
df_max = df_final[df_final['date_only'] <= '2021-12-31'].groupby('week_num')['quantity'].max().to_frame()
df_min = df_final[df_final['date_only'] <= '2021-12-31'].groupby('week_num')['quantity'].min().to_frame()


# Für das Jahr 2022 machen wir ein eigenes df, das nur die Werte des aktuellen Jahres enthält und bis gestern geht.

# In[13]:


date_cond1 = df_final['date_only'] >= f'{curr_year}-01-01'
date_cond2 = df_final['date_only'] != '2022-01-02'
date_cond3 = df_final['date_only'] <= today

df22 = df_final[(date_cond1) & (date_cond2) & (date_cond3)].copy()
df22.set_index('week_num', inplace=True)


# Wir benennen die Spalten um und fügen dann alle dfs mit join zusammen (outer, damit das finale df nicht beim aktuellen Stand 2022 abgeschnitten wird)

# In[14]:


df_mean.rename(columns={'quantity': 'Mittelwert'}, inplace=True)
df_max.rename(columns={'quantity': 'Maximum'}, inplace=True)
df_min.rename(columns={'quantity': 'Minimum'}, inplace=True)
df22.rename(columns={'quantity': '2022'}, inplace=True)


# In[15]:


df_end = df22[['2022']].join([df_mean, df_max, df_min], how='outer')


# In[16]:


df_end = df_end[:52].copy()


# Export

# In[ ]:


#Backup
df_end.to_csv(f'/root/energiemonitor/backups/strom/akw_frankreich_{backdate(0)}.csv')

#Data
df_end.to_csv('/root/energiemonitor/data/strom/akw_frankreich.csv')


# **Datawrapper-Update**

# In[17]:


last_updated = datetime.today()

last_week = df_end[df_end['2022'].notna()].index[-1]

year_week = str(datetime.today().year) + f'-W{last_week}'
monday_of_last_week = datetime.strptime(year_week + '-1', "%Y-W%W-%w")


# In[18]:


chart_id = 'cYF47'


# In[19]:


def chart_updater(chart_id, note):

    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    payload = {

    'metadata': {'annotate': {'notes': note}}

    }

    res_update = requests.patch(url_update, json=payload, headers=datawrapper_headers)

    sleep(3)

    res_publish = requests.post(url_publish, headers=datawrapper_headers)


# In[20]:


if last_updated - timedelta(days=16) <= monday_of_last_week:

    last_updated_str = last_updated.strftime('%-d. %B %Y')

    note = f'Minimum, Maximum und Mittelwert der Jahre 2015 bis 2021. Wird wöchentlich aktualisiert, zuletzt am {last_updated_str}.'

    chart_updater(chart_id, note)

