#!/usr/bin/env python
# coding: utf-8

# # Stromdaten von Swissgrid, monatlich aktualisiert

# In[1]:


from urllib.error import HTTPError
import pandas as pd
from datetime import datetime
from time import sleep
import numpy as np
from energy_settings import curr_year


# **Daten-Import**

# In[2]:


def data_wrangler(year):
    
    try:
        df_year = pd.read_excel(f'https://www.swissgrid.ch/dam/dataimport/energy-statistic/EnergieUebersichtCH-{year}.xlsx', sheet_name='Zeitreihen0h15')
    except HTTPError:
        df_year = pd.read_excel(f'https://www.swissgrid.ch/dam/dataimport/energy-statistic/EnergieUebersichtCH-{year}.xls', sheet_name='Zeitreihen0h15')
    
    df_year = df_year.loc[1:].copy()
    df_year.rename(columns={'Unnamed: 0': 'date'}, inplace=True)

    df_year['date'] = pd.to_datetime(df_year['date'], format='%d.%m.%Y %H:%M')
    df_year['date_normal'] = df_year['date'].dt.date

    df_year.columns = [
        'date',
        'endverbrauchte_energie',
        'produzierte_energie',
        'verbrauchte_energie',
        'netto_ausspeisung',
        'vertikale_einspeisung',
        'positive_sekundär_regelenergie',
        'negative_sekundär_regelenergie',
        'positive_tertiär_regelenergie',
        'negative_tertiär_regelenergie',
        'austausch_ch_zu_at',
        'austausch_at_zu_ch',
        'austausch_ch_zu_de',
        'austausch_de_zu_ch',
        'austausch_ch_zu_fr',
        'austausch_fr_zu_ch',
        'austausch_ch_zu_it',
        'austausch_it_zu_ch',
        'transit',
        'import',
        'export',
        'durchschnitt_preise_pos_sekundär_regelenergie',
        'durchschnitt_preise_neg_sekundär_regelenergie',
        'durchschnitt_preise_pos_tertiär_regelenergie',
        'durchschnitt_preise_neg_tertiär_regelenergie',
        'produktion_ag',
        'verbrauch_ag',
        'produktion_fr',
        'verbrauch_fr',
        'produktion_gl',
        'verbrauch_gl',
        'produktion_gr',
        'verbrauch_gr',
        'produktion_lu',
        'verbrauch_lu',
        'produktion_ne',
        'verbrauch_ne',
        'produktion_so',
        'verbrauch_so',
        'produktion_sg',
        'verbrauch_sg',
        'produktion_ti',
        'verbrauch_ti',
        'produktion_tg',
        'verbrauch_tg',
        'produktion_vs',
        'verbrauch_vs',
        'produktion_ai_ar',
        'verbrauch_ai_ar',
        'produktion_bl_bs',
        'verbrauch_bl_bs',
        'produktion_be_ju',
        'verbrauch_be_ju',
        'produktion_sz_zg',
        'verbrauch_sz_zg',
        'produktion_ow_nw',
        'verbrauch_ow_nw',
        'produktion_ge_vd',
        'verbrauch_ge_vd',
        'produktion_sh_zh',
        'verbrauch_sh_zh',
        'produktion_kantonsübergreifend',
        'verbrauch_kantonsübergreifend',
        'produktion_regelzone_ch_ausland',
        'verbrauch_regelzone_ch_ausland',
        'date_normal'
    ]
    
    return df_year


# In[3]:


df_import = pd.DataFrame()

for i in range(2015, curr_year + 1):
    df_temp = data_wrangler(str(i))
    df_import = pd.concat([df_import, df_temp])
    
    sleep(3)


# **Exkurs: Monatsverbrauch**

# In[34]:


df_mon = df_import[['date', 'date_normal', 'endverbrauchte_energie']].copy()


# In[35]:


df_mon['date'] = pd.to_datetime(df_mon['date'])
df_mon['month'] = df_mon['date'].dt.month
df_mon['year'] = df_mon['date'].dt.year


# Endverbrauchte Energie des vergangenen Monats in GWh berechnen

# In[36]:


#Manuell anpassen
month_num = 9
date_start = '2022-09-01'
date_end = '2022-09-30' + ' 23:45:00'


# In[37]:


endusage_last_mon = df_mon[(df_mon['date'] >= date_start) & (df_mon['date'] <= date_end)]['endverbrauchte_energie'].sum() / 10**6


# Mehrjahresdurchschnitt für jeden Monat errechnen

# In[38]:


df_mon_final = df_mon[(df_mon['date'] < '2022')].groupby(['year', 'month'])['endverbrauchte_energie'].sum().to_frame().reset_index()
df_mon_final = (df_mon_final.groupby('month')['endverbrauchte_energie'].mean() / 10**6).to_frame()


# In[39]:


endusage_mon_average = df_mon_final.loc[month_num].values[0]


# In[40]:


diff_mon = (endusage_last_mon-endusage_mon_average)
diff_mon_pct = diff_mon / endusage_mon_average * 100


# In[41]:


print(f'Verbrauch vom {date_start} bis {date_end}: {str(round(endusage_last_mon,1))} Gigawattstunden')
print(f'Durchschnitts-Verbrauch im Monat {str(month_num)}: {str(round(endusage_mon_average,1))} Gigawattstunden')
print(f'Differenz zum Durchschnittsverbrauch: {str(round(diff_mon,1))} GWh oder {str(round(diff_mon_pct,1))} Prozent')


# **Daten-Verarbeitung**

# Weiter nur mit Datumsangaben und Endverbrauchte Energie

# In[60]:


df = df_import[['date', 'date_normal', 'endverbrauchte_energie']].copy()
df['date_normal'] = pd.to_datetime(df['date_normal'])


# Für jeden Tag die endverbrauchte Energie berechnen.

# In[61]:


df_final = df.groupby('date_normal')['endverbrauchte_energie'].sum().to_frame()


# Daten in Kalenderwochenzyklen umformen.

# In[62]:


df_final = df_final.resample('W')['endverbrauchte_energie'].sum().to_frame().reset_index()
df_final['week_num'] = df_final['date_normal'].dt.isocalendar().week


# Mittelwert, Minimum und Maximum eruieren (ohne aktuelles Jahr)

# In[63]:


df_mean = df_final[df_final['date_normal'] <= '2021-12-31'].groupby('week_num')['endverbrauchte_energie'].mean().to_frame()
df_max = df_final[df_final['date_normal'] <= '2021-12-31'].groupby('week_num')['endverbrauchte_energie'].max().to_frame()
df_min = df_final[df_final['date_normal'] <= '2021-12-31'].groupby('week_num')['endverbrauchte_energie'].min().to_frame()


# Werte des aktuellen Jahres eruieren

# In[64]:


df22 = df_final[(df_final['date_normal'] >= f'{curr_year}-01-01') & (df_final['date_normal'] != '2022-01-02')].copy()
df22.set_index('week_num', inplace=True)


# Formatieren und alles zusammenfügen

# In[65]:


df_mean.rename(columns={'endverbrauchte_energie': 'Mittelwert'}, inplace=True)
df_max.rename(columns={'endverbrauchte_energie': 'Maximum'}, inplace=True)
df_min.rename(columns={'endverbrauchte_energie': 'Minimum'}, inplace=True)
df22.rename(columns={'endverbrauchte_energie': '2022'}, inplace=True)


# In[66]:


df_end = df22[['2022']].join([df_mean, df_max, df_min], how='outer')


# Werte in Gigawattstunden umformen, da sie in kWh angegeben sind

# In[67]:


df_end = df_end / 10**6


# # Vor Export: Angefangene Woche rausnehmen!!!

# In[68]:


df_end.at[39, '2022'] = np.nan


# **Export**

# In[69]:


today = datetime.today().strftime('%Y-%m-%d')


# In[70]:


#Backup
df_end.to_csv(f'Results/Backups/{today}_stromverbrauch_schweiz.csv')

#Data
df_end.to_csv(f'Results/stromverbrauch_schweiz.csv')

