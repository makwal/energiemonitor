#!/usr/bin/env python
# coding: utf-8

# # Stromdaten von Swissgrid, monatlich aktualisiert

# In[5]:


from urllib.error import HTTPError
import pandas as pd
from datetime import datetime
from time import sleep
from energy_settings import curr_year


# In[6]:


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


# In[7]:


df = pd.DataFrame()

for i in range(2015, curr_year + 1):
    df_temp = data_wrangler(str(i))
    df = pd.concat([df, df_temp])
    
    sleep(3)


# In[8]:


df = df[['date', 'date_normal', 'endverbrauchte_energie']].copy()


# In[9]:


df['date_normal'] = pd.to_datetime(df['date_normal'])


# In[10]:


df_final = df.groupby('date_normal')['endverbrauchte_energie'].sum().to_frame()


# In[11]:


df_final = df_final.resample('W')['endverbrauchte_energie'].sum().to_frame().reset_index()


# In[12]:


df_final['week_num'] = df_final['date_normal'].dt.isocalendar().week


# In[13]:


df_mean = df_final[df_final['date_normal'] <= '2021-12-31'].groupby('week_num')['endverbrauchte_energie'].mean().to_frame()
df_max = df_final[df_final['date_normal'] <= '2021-12-31'].groupby('week_num')['endverbrauchte_energie'].max().to_frame()
df_min = df_final[df_final['date_normal'] <= '2021-12-31'].groupby('week_num')['endverbrauchte_energie'].min().to_frame()


# In[14]:


df22 = df_final[(df_final['date_normal'] >= f'{curr_year}-01-01') & (df_final['date_normal'] != '2022-01-02')].copy()
df22.set_index('week_num', inplace=True)


# In[15]:


df_mean.rename(columns={'endverbrauchte_energie': 'Mittelwert'}, inplace=True)
df_max.rename(columns={'endverbrauchte_energie': 'Maximum'}, inplace=True)
df_min.rename(columns={'endverbrauchte_energie': 'Minimum'}, inplace=True)
df22.rename(columns={'endverbrauchte_energie': '2022'}, inplace=True)


# In[16]:


df_end = df22[['2022']].join([df_mean, df_max, df_min], how='outer')


# In[17]:


df_end = df_end / 10**6


# In[69]:


df_end.to_csv('Results/stromverbrauch_schweiz.csv')

