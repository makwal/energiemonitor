#!/usr/bin/env python
# coding: utf-8

# - Verbrauch: https://opendata.swiss/de/dataset/energiedashboard-ch-stromverbrauch-swissgrid/resource/271a6814-75ad-400b-824e-27043a60b294
# - Produktion: https://opendata.swiss/de/dataset/energiedashboard-ch-stromproduktion-swissgrid
# - Wetterdaten: https://opendata.swiss/de/dataset/klimamessnetz-tageswerte

# In[1]:


import requests
import pandas as pd
from datetime import datetime, timedelta
from time import sleep
import numpy as np
from energy_settings import (
    backdate,
    datawrapper_api_key,
    datawrapper_url,
    datawrapper_headers
)
import locale
locale.setlocale(locale.LC_TIME, 'de_CH.UTF-8')


# Wir beziehen immer die Daten von gestern.

# In[2]:


yesterday_raw = datetime.today() - timedelta(days=1)
yesterday = yesterday_raw.strftime('%Y-%m-%d')
yesterday_str = yesterday_raw.strftime('%d. %B %Y')

day_before_yesterday = (yesterday_raw - timedelta(days=1)).strftime('%Y-%m-%d')


# **Stromverbrauch**

# In[3]:


data_url = 'https://bfe-energy-dashboard-ogd.s3.amazonaws.com/ogd103_stromverbrauch_geschaetzt_swissgrid.csv'

df = pd.read_csv(data_url)


# In[4]:


df['Datum'] = pd.to_datetime(df['Datum'])


# Wir nehmen den gestrigen Verbrauchswert und das 10-Tages-Mittel.

# In[5]:


df['mean_10d'] = df['Landesverbrauch_geschaetzt_SG_GWh'].rolling(10).mean()

usage = df[df['Datum'] == yesterday]['Landesverbrauch_geschaetzt_SG_GWh'].values[0]

mean_10d = df[df['Datum'] == day_before_yesterday]['mean_10d'].values[0]


# Anhand dieser Funktion wird die textliche Gegenüberstellung des aktuellen und des 10-Tage-Mittel-Verbrauchs gemacht.

# In[6]:


def texter_strom(usage, mean_10d):
    diff_pct = (int(round(usage)) - int(round(mean_10d))) / int(round(mean_10d)) * 100
    
    if diff_pct >= 15:
        return 'deutlich mehr als'
    elif diff_pct >= 5:
        return 'mehr als'
    elif diff_pct >= 2:
        return 'leicht mehr als'
    elif diff_pct == 0:
        return 'gleich viel wie'
    elif diff_pct < 2 and diff_pct > -2:
        return 'etwa gleich viel wie'
    elif diff_pct <= -15:
        return 'deutlich weniger als'
    elif diff_pct <= -5:
        return 'weniger als'
    elif diff_pct <= -2:
        return 'leicht weniger als'
    
est_strom = texter_strom(usage, mean_10d)


# **Stromproduktion**

# In[7]:


data_url_prod = 'https://bfe-energy-dashboard-ogd.s3.amazonaws.com/ogd104_stromproduktion_swissgrid.csv'

df_prod = pd.read_csv(data_url_prod)


# Für jeden Tag rechnen wir alle Produktionssektoren zusammen und berechnen die gleichen Kennzahlen wie oben.

# In[8]:


df_prod['Datum'] = pd.to_datetime(df_prod['Datum'])

df_prod = df_prod.groupby('Datum')['Produktion_GWh'].sum().to_frame().reset_index()


# In[9]:


df_prod['mean_10d'] = df_prod['Produktion_GWh'].rolling(10).mean()

prod_date = df_prod.tail(1)['Datum'].dt.strftime('%d.%m.').values[0]
production = df_prod.tail(1)['Produktion_GWh'].values[0]

mean_10d_prod = df_prod.tail(2).head(1)['mean_10d'].values[0]


# In[10]:


def texter_strom_prod(production, mean_10d_prod):
    diff_pct = (int(round(production)) - int(round(mean_10d_prod))) / int(round(mean_10d_prod)) * 100
    
    if diff_pct >= 15:
        return 'deutlich mehr als'
    elif diff_pct >= 5:
        return 'mehr als'
    elif diff_pct >= 2:
        return 'leicht mehr als'
    elif diff_pct == 0:
        return 'gleich viel wie'
    elif diff_pct < 2 and diff_pct > -2:
        return 'etwa gleich viel wie'
    elif diff_pct <= -15:
        return 'deutlich weniger als'
    elif diff_pct <= -5:
        return 'weniger als'
    elif diff_pct <= -2:
        return 'leicht weniger als'
    
est_strom_prod = texter_strom_prod(production, mean_10d_prod)


# Hier importieren wir die Ampelinformationen, Gefahrenstufe und dazugehöriger Text.

# In[11]:


ampel_url = 'https://bfe-energy-dashboard-ogd.s3.amazonaws.com/ogd108_stufen_energiemangellage.json'
r = requests.get(ampel_url)
r = r.json()

res = r['ampel_status_strom'][0]

level = res['level']
titel = res['titel@de']


# **Wetter**

# Wir lesen die Tageswerte von Meteo Schweiz ein. Als Referenzmessstation dient Zürich-Fluntern (SMA). Wir greifen das Tagesmittel ab, das sich in der Spalte tre200d0 befindet. Als Vergleichswert bilden wir das Mittel der vorherigen 10 Tage.

# In[12]:


weather_url = 'https://data.geo.admin.ch/ch.meteoschweiz.klima/nbcn-tageswerte/nbcn-daily_SMA_current.csv'

df_weather = pd.read_csv(weather_url, delimiter=';')

df_weather['date'] = pd.to_datetime(df_weather['date'], format='%Y%m%d')

df_weather['mean_10d'] = df_weather['tre200d0'].rolling(10).mean()


# Weil die Wetterdaten von gestern erst im Verlauf des Tages kommen, wir die Grafik aber schon am Morgen updaten, fügen wir die Wetterdaten dynamisch dazu.

# In[13]:


try:
    temperature = df_weather[df_weather['date'] == yesterday]['tre200d0'].values[0]
    mean_temp_10d = df_weather[df_weather['date'] == day_before_yesterday]['mean_10d'].values[0]
except IndexError as e:
    temperature = 'nicht verfügbar'
    
try:
    mean_temp_10d = df_weather[df_weather['date'] == day_before_yesterday]['mean_10d'].values[0]
except IndexError as e:
    mean_temp_10d = 'nicht verfügbar'


# Anhand dieser Funktion wird der textliche Vergleich der aktuellsten Temperatur mit dem 10-Tage-Mittel gemacht.

# In[14]:


def texter_temp(temperature, mean_temp_10d):
    diff = temperature - mean_temp_10d

    if diff >= 5:
        return 'deutlich höheren'
    elif diff >= 2:
        return 'höheren'
    elif diff >= 1:
        return 'leicht höheren'
    elif diff < 1 and diff > -1:
        return 'ungefähr gleichen'
    elif diff <= -5:
        return 'deutlich tieferen'
    elif diff <= -2:
        return 'tieferen'
    elif diff <= -1:
        return 'leicht tieferen'


# Hier werden die Texte zusammengesetzt und in ein DataFrame verpackt.

# In[15]:


icon_url = 'https://chm-editorial-data-static.s3.eu-west-1.amazonaws.com/red_mantel/energiedashboard/icons/blitz.png'

text_usage = f'<strong style="font-size:32px">{int(round(usage))} GWh</strong>&nbsp <span style="letter-spacing:0.75px">geschätzter Wert</span>'

text_comparison = f'''So viel <b>Strom</b> verbrauchte die Schweiz am {yesterday_str} (inkl. Speicherpumpen).<br><br> Das ist <b>{est_strom}</b> im Durchschnitt der vorherigen zehn Tage mit {int(round(mean_10d))} Gigawattstunden (GWh). '''

text_production = f'''Die Schweiz <b>produzierte</b> {int(round(production))} GWh Strom ({prod_date}, Schätzwert). Das ist <b>{est_strom_prod}</b> im Zehn-Tages-Durchschnitt mit {int(round(mean_10d_prod))} GWh.'''

text_bundesrat = f'Beurteilung Bundesrat: <strong>{titel}</strong> (Gefahrenstufe {level} von 5)'


# Den Temperaturteil fügen wir nur dazu, wenn die Angaben vorhanden sind.

# In[16]:


if temperature != 'nicht verfügbar' and mean_temp_10d != 'nicht verfügbar':
    est_temp = texter_temp(temperature, mean_temp_10d)
    
    text_comparison += f' Dies bei <strong>{est_temp}</strong> Temperaturen (Zürich).'


# In[17]:


data = [
    {'icon': f'![]({icon_url})', 'text': text_usage},
    {'icon': np.nan, 'text': text_comparison},
    {'icon': np.nan, 'text': text_production},
    {'icon': np.nan, 'text': text_bundesrat}
]


# In[18]:


df_final = pd.DataFrame(data)


# **Datawrapper-Update**

# In[19]:


chart_id = 'YNAIQ'


# Daten in die Grafik laden.

# In[20]:


def data_uploader(chart_id, df_func):
    dw_upload_url = datawrapper_url + chart_id +'/data'

    headers = {
        'Accept': '*/*',
        'Content-Type': 'text/csv',
        'Authorization': datawrapper_headers['Authorization']
    }
    
    #data is being transformed to a csv
    data = df_func.to_csv(encoding='utf-8', index=False)

    response = requests.put(dw_upload_url, data=data.encode('utf-8'), headers=datawrapper_headers)

    status_code = response.status_code
    if status_code > 204:
        print(chart_id + ': ' + str(status_code))
    
    sleep(3)
    
    url_update = datawrapper_url + chart_id
    url_publish = url_update + '/publish'

    res_publish = requests.post(url_publish, headers=datawrapper_headers)
    
    status_code2 = res_publish.status_code
    
    if status_code2 > 204:
        print(chart_id + ': ' + str(status_code2))


# In[21]:


data_uploader(chart_id, df_final)

