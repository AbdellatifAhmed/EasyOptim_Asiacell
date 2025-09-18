import streamlit as st
import pandas as pd
import time
import os
import io
import numpy as np
import datetime
import base64

# Define Major Variables
output_dir = os.path.join(os.getcwd(), 'OutputFiles')
Study_Output = os.path.join(output_dir, 'output_ARBStudy.xlsx')
SpectrumEff_DF = pd.DataFrame({
        'SINR [dB]':[-6.7 , -4.7 , -2.3 , 0.2 , 2.4 , 4.3 , 5.9 , 8.1 , 10.3 , 11.7 , 14.1 , 16.3 , 18.7 , 21 , 22.7] ,
        'CQI code':[1 , 2 , 3 , 4 , 5 , 6 , 7 , 8 , 9 , 10 , 11 , 12 , 13 , 14 , 15] ,
        'Modulation':['QPSK' , 'QPSK' , 'QPSK' , 'QPSK' , 'QPSK' , 'QPSK' , '16QAM' , '16QAM' , '16QAM' , '64QAM' , '64QAM' , '64QAM' , '64QAM' , '64QAM' , '64QAM'] ,
        'Code Rate':[0.076 , 0.12 , 0.19 , 0.3 , 0.44 , 0.59 , 0.37 , 0.48 , 0.6 , 0.45 , 0.55 , 0.65 , 0.75 , 0.85 , 0.93] ,
        'Spectral efficiency':[0.15 , 0.23 , 0.38 , 0.6 , 0.88 , 1.18 , 1.48 , 1.91 , 2.41 , 2.73 , 3.32 , 3.9 , 4.52 , 5.12 , 5.55]
        })
lte_nameMapping_df = pd.DataFrame({'SecCode': [1,2,3,4,5,6,7,8], 'ID': ['1' , '2' , '3' , '4' , '1 - S' , '2 - S' , '3 - S' , '4 - S' ]})
lte_nameMapping_df2 = pd.DataFrame({'SecCode': [1,2,3,4,5,6,7,8], 'ID': ['1' , '2' , '3' , '4' , '1' , '2' , '3' , '4' ]})
Mimo_df = pd.DataFrame({'Mimo Num': [ 1 , 2 , 4], 'MIMO': ['1Tx1Rx' , '2Tx2Rx' , '4Tx4Rx']})

#  Event capacity assessment function itself
def arb_Study(arb_form):
    start_time = time.time()
    print("ARB Study initiated")
    normal_start_date = pd.to_datetime(arb_form['normalStartDate'])
    normal_end_date = pd.to_datetime(arb_form['normalEndDate'])
    event_start_date = pd.to_datetime(arb_form['eventStartDate'])
    event_end_date = pd.to_datetime(arb_form['eventEndDate'])
    
    fileArbRadioCounters = arb_form['fileArbRadioCounters']
    forecast_DF =   pd.read_excel(arb_form['fileArbForecast'],sheet_name='Forecast')
    steps_DF =   pd.read_excel(arb_form['fileArbForecast'],sheet_name='Steps')
    if arb_form['filesiteScope']:
        scope_DF = pd.read_excel(arb_form['filesiteScope'])
    # start_time = time.time()
    file_stream = io.TextIOWrapper(fileArbRadioCounters, encoding='utf-8')
    # thrpt_cells = pd.read_csv(file_stream, skiprows=6, skipfooter=1, engine='python', encoding='utf-8')
    # Initialize an empty list to hold DataFrame chunks
    chunks = []
    chunk_size = 100000  # Adjust this value based on your memory capacity
    # Read the CSV file in chunks
    chunk_j=0
    for chunk in pd.read_csv(file_stream, skiprows=6, engine='python', encoding='utf-8', chunksize=chunk_size):
        chunks.append(chunk)
        print("Radio Counters  1Mil chunk appended - chunk number:", str(chunk_j))
        chunk_j=chunk_j+1
        # if i == 1:
        #     break
    
    end_time =time.time()
    duration = str(round((end_time - start_time),0))+" Seconds"
    print("Loading the big file Chunks done. Total elapsed duration is:", duration)
    arb_Radio = pd.concat(chunks, ignore_index=True)
    arb_Radio = arb_Radio[:-1]

    arb_Radio['Code'] = arb_Radio.apply(lambda row: str(row['eNodeB Name'])[-7:], axis=1)
    arb_Radio['City'] = arb_Radio.apply(lambda row: row['Code'][:3], axis=1)
    arb_Radio = arb_Radio[arb_Radio['City'].isin(forecast_DF['City'])].copy()
    if arb_form['filesiteScope']:
        arb_Radio = arb_Radio[arb_Radio['Code'].isin(scope_DF['Code'])].copy()
    # print(arb_Radio['City'].unique())
    # print(arb_Radio)
    end_time =time.time()
    duration = str(round((end_time - start_time),0))+" Seconds"
    print("filtering to only the Needed Cities/Clusters done. Total elapsed duration is:", duration)
    arb_Radio.rename(columns={'L.ChMeas.PRB.DL.Avail': 'BW(PRBs)'}, inplace=True)
    arb_Radio.rename(columns={'Used Rank1 _Asiacell': 'RANK1%'}, inplace=True)
    arb_Radio.rename(columns={'Used Rank2 _Asiacell': 'RANK2%'}, inplace=True)
    arb_Radio.rename(columns={'Used Rank3 _Asiacell': 'RANK3%'}, inplace=True)
    arb_Radio.rename(columns={'Used Rank4 _Asiacell': 'RANK4%'}, inplace=True)        
    arb_Radio['Date'] = pd.to_datetime(arb_Radio['Date'],format='%d-%m-%Y', errors='coerce')
    arb_Radio['Site_Time_LKUP'] = (arb_Radio['Code'] + "_" + arb_Radio['Date'].dt.strftime('%Y-%m-%d') + "_" + arb_Radio['Time'])
    arb_Radio['Downlink EARFCN'] = arb_Radio['Downlink EARFCN'].apply(pd.to_numeric , errors='coerce')  
    arb_Radio['Frequency Band'] = arb_Radio.apply(lambda row: get_band_by_frequency(row['Downlink EARFCN']) , axis=1)      
    arb_Radio['DL User Throughput_Asiacell'] = arb_Radio['DL User Throughput_Asiacell'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['UL Interference_Asiacell'] = arb_Radio['UL Interference_Asiacell'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['RANK1%'] = arb_Radio['RANK1%'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['RANK2%'] = arb_Radio['RANK2%'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['RANK3%'] = arb_Radio['RANK3%'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['RANK4%'] = arb_Radio['RANK4%'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['MIMO_Effect'] = ((arb_Radio['RANK1%']) + 2 * (arb_Radio['RANK2%']) + 3 * (arb_Radio['RANK3%']) + 4 * (arb_Radio['RANK4%'])) / ((arb_Radio['RANK1%']) + (arb_Radio['RANK2%']) + (arb_Radio['RANK3%']) + (arb_Radio['RANK4%']))
    arb_Radio['Average CQI'] = arb_Radio['Average CQI'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['CQI code'] = round((arb_Radio['Average CQI']) , 0)
    arb_Radio['Spectral efficiency'] = arb_Radio['CQI code'].map(dict(zip(SpectrumEff_DF['CQI code'], SpectrumEff_DF['Spectral efficiency'])))
    arb_Radio['BW(PRBs)'] = pd.to_numeric(arb_Radio['BW(PRBs)'], errors='coerce')
    arb_Radio['MIMO_Effect'] = pd.to_numeric(arb_Radio['MIMO_Effect'], errors='coerce')
    arb_Radio['Spectral efficiency'] = pd.to_numeric(arb_Radio['Spectral efficiency'], errors='coerce')
    arb_Radio['DL IBLER_Asiacell'] = pd.to_numeric(arb_Radio['DL IBLER_Asiacell'], errors='coerce')
    arb_Radio.fillna(0, inplace=True)

    arb_Radio['Calculated Cell THRPT'] = 13.2 * ((arb_Radio['BW(PRBs)']) / 100) * (arb_Radio['MIMO_Effect']) * (arb_Radio['Spectral efficiency']) * (1- ((arb_Radio['DL IBLER_Asiacell'] )/100))

    arb_Radio['BW(PRBs)'] = arb_Radio['BW(PRBs)'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['UL IBLER_Asiacell'] = arb_Radio['UL IBLER_Asiacell'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['4G DL Traffic Volume (GB)_Asiacell'] = arb_Radio['4G DL Traffic Volume (GB)_Asiacell'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['4G UL Traffic Volume (GB)_Asiacell'] = arb_Radio['4G UL Traffic Volume (GB)_Asiacell'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['LTE Data Volume (TB)'] = (arb_Radio['4G DL Traffic Volume (GB)_Asiacell'] + arb_Radio['4G UL Traffic Volume (GB)_Asiacell'])/1024
    arb_Radio['HW_DL PRB Avg Utilization(%)'] = arb_Radio['HW_DL PRB Avg Utilization(%)'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['L.Traffic.ActiveUser.Avg'] = arb_Radio['L.Traffic.ActiveUser.Avg'].apply(pd.to_numeric , errors='coerce')
    arb_Radio['Max MIMO Config'] = arb_Radio['LTECell Tx and Rx Mode'].str[:1].astype(int)


    arb_Radio['SecCode'] = (arb_Radio['LocalCell Id'] % 10)
    arb_Radio['Sector No.'] = arb_Radio['SecCode'].map(dict(zip(lte_nameMapping_df2['SecCode'],lte_nameMapping_df2['ID'])))
    arb_Radio['Sector-ID'] = arb_Radio['SecCode'].map(dict(zip(lte_nameMapping_df['SecCode'],lte_nameMapping_df['ID'])))
    arb_Radio['Sector ID'] =arb_Radio.apply(lambda row: str(row['Code']+ "-" + str(row['Sector-ID'])), axis=1)
    arb_Radio['Sector Code'] =arb_Radio.apply(lambda row: str(row['Code']+ "-" + str(row['Sector No.'])), axis=1)
    arb_Radio['Date'] =pd.to_datetime(arb_Radio['Date'], format='%d-%m-%Y')
    
    end_time =time.time()
    duration = str(round((end_time - start_time),0))+" Seconds"
    print("Foramting the Big Table Columns done. Total elapsed duration is:", duration)

    Sites_Forecast_Cols = ['City','Code','eNodeB Name']
    Sites_Forecast = arb_Radio[Sites_Forecast_Cols].drop_duplicates()

    cities_Normal_Traffic = arb_Radio[
    (arb_Radio['Date'] >= normal_start_date) & 
    (arb_Radio['Date'] <= normal_end_date)
    ].pivot_table(
    values=['LTE Data Volume (TB)'], 
    index=['City'], 
    columns=['Date'], 
    aggfunc='sum', 
    fill_value=0)
    cities_Normal_Traffic['Normal Traffic'] = cities_Normal_Traffic.max(axis=1)
    
    cities_Normal_Traffic = cities_Normal_Traffic.reset_index()
    cities_Normal_Traffic.columns = ['_'.join([str(c) if pd.notna(c) else '' for c in col]).strip() 
                                if isinstance(col, tuple) else col for col in cities_Normal_Traffic.columns]
    code_col = [value for value in cities_Normal_Traffic.columns if 'City' in value][0]
    traffic_col = [value for value in cities_Normal_Traffic.columns if 'Normal Traffic' in value][0]
    forecast_DF['Normal Traffic'] = forecast_DF['City'].map(dict(zip(cities_Normal_Traffic[code_col], cities_Normal_Traffic[traffic_col])))
    Sites_Forecast['City Normal Traffic']=Sites_Forecast['City'].map(dict(zip(cities_Normal_Traffic[code_col], cities_Normal_Traffic[traffic_col])))
    del cities_Normal_Traffic

    cities_Event_Traffic = arb_Radio[
    (arb_Radio['Date'] >= event_start_date) & 
    (arb_Radio['Date'] <= event_end_date)
    ].pivot_table(
    values=['LTE Data Volume (TB)'], 
    index=['City'], 
    columns=['Date'], 
    aggfunc='sum', 
    fill_value=0)
    cities_Event_Traffic['Event Traffic'] = cities_Event_Traffic.max(axis=1)
    cities_Event_Traffic = cities_Event_Traffic.reset_index()
    cities_Event_Traffic.columns = ['_'.join([str(c) if pd.notna(c) else '' for c in col]).strip() 
                                if isinstance(col, tuple) else col for col in cities_Event_Traffic.columns]
    code_col = [value for value in cities_Event_Traffic.columns if 'City' in value][0]
    traffic_col = [value for value in cities_Event_Traffic.columns if 'Event Traffic' in value][0]
    forecast_DF['Event Traffic'] = forecast_DF['City'].map(dict(zip(cities_Event_Traffic[code_col], cities_Event_Traffic[traffic_col])))
    Sites_Forecast['City Event Traffic']=Sites_Forecast['City'].map(dict(zip(cities_Event_Traffic[code_col], cities_Event_Traffic[traffic_col])))
    # del cities_Event_Traffic
    

    # lookup_index = steps_DF.set_index(['City', 'Step'])['Step ID']
    max_steps = steps_DF['Step'].max()
    print("Maximum Number of Steps in a city is:",max_steps)

    Sites_Forecast['City Forecast Traffic'] = Sites_Forecast['City'].map(dict(zip(forecast_DF['City'], forecast_DF['Forecast'])))
    Sites_Forecast['City_Event_Delta'] = Sites_Forecast['City Event Traffic'] - Sites_Forecast['City Normal Traffic']
    Sites_Forecast['City_Forecast_Delta'] = Sites_Forecast['City Forecast Traffic'] - Sites_Forecast['City Normal Traffic']
    # del forecast_DF


    
    sites_Normal_Traffic = arb_Radio[
    (arb_Radio['Date'] >= normal_start_date) & 
    (arb_Radio['Date'] <= normal_end_date)
    ].pivot_table(
    values=['LTE Data Volume (TB)'], 
    index=['Code'], 
    columns=['Date'], 
    aggfunc='sum', 
    fill_value=0)
    sites_Normal_Traffic['Normal Traffic'] = sites_Normal_Traffic.max(axis=1)
    sites_Normal_Traffic = sites_Normal_Traffic.reset_index()
    sites_Normal_Traffic.columns = ['_'.join([str(c) if pd.notna(c) else '' for c in col]).strip() 
                                if isinstance(col, tuple) else col for col in sites_Normal_Traffic.columns]
    code_col = [value for value in sites_Normal_Traffic.columns if 'Code' in value][0]
    traffic_col = [value for value in sites_Normal_Traffic.columns if 'Normal Traffic' in value][0]
    Sites_Forecast['Site Normal Traffic']=Sites_Forecast['Code'].map(dict(zip(sites_Normal_Traffic[code_col], sites_Normal_Traffic[traffic_col])))
    del sites_Normal_Traffic

    sites_Event_Traffic = arb_Radio[
    (arb_Radio['Date'] >= event_start_date) & 
    (arb_Radio['Date'] <= event_end_date)
    ].pivot_table(
    values=['LTE Data Volume (TB)'], 
    index=['Code'], 
    columns=['Date'], 
    aggfunc='sum', 
    fill_value=0)
    sites_Event_Traffic['Event Traffic'] = sites_Event_Traffic.max(axis=1)
    sites_Event_Traffic = sites_Event_Traffic.reset_index()
    sites_Event_Traffic.columns =['_'.join([str(c) if pd.notna(c) else '' for c in col]).strip() 
                                if isinstance(col, tuple) else col for col in sites_Event_Traffic.columns]
    code_col = [value for value in sites_Event_Traffic.columns if 'Code' in value][0]
    traffic_col = [value for value in sites_Event_Traffic.columns if 'Event Traffic' in value][0]
    Sites_Forecast['Site Event Traffic']=Sites_Forecast['Code'].map(dict(zip(sites_Event_Traffic[code_col], sites_Event_Traffic[traffic_col])))
    Sites_Forecast['Site Event Delta'] = Sites_Forecast['Site Event Traffic'] - Sites_Forecast['Site Normal Traffic']
    Sites_Forecast['Site Forecast Delta'] = Sites_Forecast['Site Event Delta']*Sites_Forecast['City_Forecast_Delta']/Sites_Forecast['City_Event_Delta']
    Sites_Forecast['Site Forecast Delta'] = pd.to_numeric(Sites_Forecast['Site Forecast Delta'], errors='coerce')
    Sites_Forecast['Site Event Delta'] = pd.to_numeric(Sites_Forecast['Site Event Delta'], errors='coerce')
    Sites_Forecast['Site Event Traffic'] = pd.to_numeric(Sites_Forecast['Site Event Traffic'], errors='coerce')
    Sites_Forecast['Site Forecast Traffic'] = Sites_Forecast['Site Normal Traffic'] + Sites_Forecast['Site Forecast Delta']
    Sites_Forecast['Site Forecast %'] = (Sites_Forecast['Site Forecast Delta'] - Sites_Forecast['Site Event Delta']) / Sites_Forecast['Site Event Traffic']
    # del sites_Event_Traffic
    
    arb_Radio = arb_Radio[arb_Radio['LTE Data Volume (TB)']>0]
    config_Columns = ['Sector Code','Sector ID','City','Code','eNodeB Name','Cell FDD TDD Indication','Cell Name','LocalCell Id','Frequency Band','Downlink EARFCN','LTECell Tx and Rx Mode','BW(PRBs)','Max MIMO Config']
    config_DF = arb_Radio[config_Columns].drop_duplicates()
    config_DF['Sectorization'] = config_DF['Sector ID'].apply(lambda x: 1 if x[-1].lower() == 's' else 0) 
    config_DF['Sectorization'] = config_DF['Sectorization'].apply(pd.to_numeric , errors='coerce')

    sectors_Band_config_DF = config_DF.pivot_table(values=['Sector ID'], index=['Sector Code'], columns=['Frequency Band'], aggfunc='count', fill_value=0)
    sectors_Band_config_DF['Sector Code'] = sectors_Band_config_DF.index

    sites_Mimo_config_DF = config_DF.pivot_table(values=['Max MIMO Config'], index=['Code'], columns=[], aggfunc='max', fill_value=0)
    sites_Mimo_config_DF['MIMO Configuarion'] = sites_Mimo_config_DF['Max MIMO Config'].map(dict(zip(Mimo_df['Mimo Num'], Mimo_df['MIMO'])))
    sites_Mimo_config_DF['Code'] = sites_Mimo_config_DF.index 

    Sectors_bibeams_DF = config_DF.pivot_table(values=['Sectorization'], index=['Sector Code'], columns=[], aggfunc='sum', fill_value=0)
    Sectors_bibeams_DF['HOS'] = Sectors_bibeams_DF['Sectorization'].apply(lambda x: "Yes" if x > 0 else "No")
    Sectors_bibeams_DF['Sector Code'] = Sectors_bibeams_DF.index
    

    sectors_Config_Columns = ['City','Code','eNodeB Name','Sector Code']
    sectors_Config_DF = config_DF[sectors_Config_Columns].drop_duplicates()
    sectors_Config_DF['Site Forecast %'] = sectors_Config_DF['Code'].map(dict(zip(Sites_Forecast['Code'],Sites_Forecast['Site Forecast %'])))

    try:
        Band_3_Col = sectors_Band_config_DF.columns[sectors_Band_config_DF.columns.get_level_values(1)=='Band3']
        sectors_Band_config_DF['L1800'] = sectors_Band_config_DF[Band_3_Col]
        sectors_Band_config_DF['L1800'] = sectors_Band_config_DF['L1800'].apply(pd.to_numeric , errors='coerce')
        sectors_Band_config_DF['L1800_exist'] = sectors_Band_config_DF['L1800'].apply(lambda x: "Yes" if x > 0 else "No")
        sectors_Config_DF['L1800'] = sectors_Config_DF['Sector Code'].map(dict(zip(sectors_Band_config_DF['Sector Code'], sectors_Band_config_DF['L1800_exist'])))  
    except:
        print("L1800 is not configured in any on the given Radio Sites")
        sectors_Config_DF['L1800'] = "No"
    
    try:
        Band_1_Col = sectors_Band_config_DF.columns[sectors_Band_config_DF.columns.get_level_values(1)=='Band1']
        sectors_Band_config_DF['L2100'] = sectors_Band_config_DF[Band_1_Col]
        sectors_Band_config_DF['L2100'] = sectors_Band_config_DF['L2100'].apply(pd.to_numeric , errors='coerce')
        sectors_Band_config_DF['L2100_exist'] = sectors_Band_config_DF['L2100'].apply(lambda x: "Yes" if x > 0 else "No")
        sectors_Config_DF['L2100'] = sectors_Config_DF['Sector Code'].map(dict(zip(sectors_Band_config_DF['Sector Code'], sectors_Band_config_DF['L2100_exist'])))  
    except:
        print("L2100 is not configured in any on the given Radio Sites")
        sectors_Config_DF['L2100'] = "No"
    
    try:
        Band_8_Col = sectors_Band_config_DF.columns[sectors_Band_config_DF.columns.get_level_values(1)=='Band8']
        sectors_Band_config_DF['L900'] = sectors_Band_config_DF[Band_8_Col]
        sectors_Band_config_DF['L900'] = sectors_Band_config_DF['L900'].apply(pd.to_numeric , errors='coerce')
        sectors_Band_config_DF['L900_exist'] = sectors_Band_config_DF['L900'].apply(lambda x: "Yes" if x > 0 else "No")
        sectors_Config_DF['L900'] = sectors_Config_DF['Sector Code'].map(dict(zip(sectors_Band_config_DF['Sector Code'], sectors_Band_config_DF['L900_exist'])))  
    except:
        print("L900 is not configured in any on the given Radio Sites")
        sectors_Config_DF['L900'] = "No"
    
    try:
        Band_38_Col = sectors_Band_config_DF.columns[sectors_Band_config_DF.columns.get_level_values(1)=='Band38']
        sectors_Band_config_DF['L2600[40MHz]'] = sectors_Band_config_DF[Band_38_Col]
        sectors_Band_config_DF['L2600[40MHz]'] = sectors_Band_config_DF['L2600[40MHz]'].apply(pd.to_numeric , errors='coerce')
        sectors_Band_config_DF['L2600[40MHz]_exist'] = sectors_Band_config_DF['L2600[40MHz]'].apply(lambda x: "Yes" if x > 0 else "No")
        sectors_Config_DF['L2600[40MHz]'] = sectors_Config_DF['Sector Code'].map(dict(zip(sectors_Band_config_DF['Sector Code'], sectors_Band_config_DF['L2600[40MHz]_exist'])))  
    except:
        print("L2600 is not configured in any on the given Radio Sites")
        sectors_Config_DF['L2600[40MHz]'] = "No"

    
    try:
        Band_41_Col = sectors_Band_config_DF.columns[sectors_Band_config_DF.columns.get_level_values(1)=='Band41']
        sectors_Band_config_DF['L2600_F3'] = sectors_Band_config_DF[Band_41_Col]
        sectors_Band_config_DF['L2600_F3'] = sectors_Band_config_DF['L2600_F3'].apply(pd.to_numeric , errors='coerce')
        sectors_Band_config_DF['L2600_F3_Exist'] = sectors_Band_config_DF['L2600_F3'].apply(lambda x: "Yes" if x > 0 else "No")
        sectors_Config_DF['L2600_F3'] = sectors_Config_DF['Sector Code'].map(dict(zip(sectors_Band_config_DF['Sector Code'], sectors_Band_config_DF['L2600_F3_Exist'])))  
    except Exception as e:
        print("L2600_F3 is not configured in any on the given Radio Sites")
        sectors_Config_DF['L2600_F3'] = "No"
        print (str(e))
    
    sectors_Config_DF['HOS'] = sectors_Config_DF['Sector Code'].map(dict(zip(Sectors_bibeams_DF['Sector Code'], Sectors_bibeams_DF['HOS'])))
    sectors_Config_DF['MIMO Configuarion'] = sectors_Config_DF['Code'].map(dict(zip(sites_Mimo_config_DF['Code'], sites_Mimo_config_DF['MIMO Configuarion'])))

    end_time =time.time()
    duration = str(round((end_time - start_time),0))+" Seconds"
    print("Preparing the Sectors Current Configuartion done. Total elapsed duration is:", duration)
    # Add the Allowed upgrade Steps per Cluster/City

    arb_Radio['Site Forecast %'] = arb_Radio['Sector Code'].map(dict(zip(sectors_Config_DF['Sector Code'], sectors_Config_DF['Site Forecast %'])))
    Sectors_Load = arb_Radio[
    (arb_Radio['Date'] >= event_start_date) & 
    (arb_Radio['Date'] <= event_end_date)].pivot_table(
    values=['LTE Data Volume (TB)', 'Calculated Cell THRPT', 'L.Traffic.ActiveUser.Avg', 'BW(PRBs)', 'Site Forecast %'], 
    index=['Sector Code'], 
    columns=['Date', 'Time'], 
    aggfunc={
        'LTE Data Volume (TB)': 'sum',
        'Calculated Cell THRPT': 'sum',
        'L.Traffic.ActiveUser.Avg': 'sum',
        'BW(PRBs)': 'sum',
        'Site Forecast %': 'max'}, 
    fill_value=0)
    utiThshld = int(arb_form['utiThshld'])
    qoshshld = int(arb_form['qoshshld'])
    hrs_threshld = int(arb_form['hrshshld'])
    # print(Sectors_Load)
    # print(Sectors_Load.index)
    # Add recent Event Load to the sectors
    calculated_Load = get_Event_Util(Sectors_Load,utiThshld,qoshshld,False,False)
    # print(calculated_Load)
    sectors_Config_DF['Max PRBs'] = sectors_Config_DF['Sector Code'].map(dict(zip(calculated_Load['Sector Code'], calculated_Load['Max PRBs'])))
    sectors_Config_DF['Event Load Times'] = sectors_Config_DF['Sector Code'].map(dict(zip(calculated_Load['Sector Code'], calculated_Load['High Load Times'])))
    print("Done Adding Event Load.")
 
    # Add Next Event Load to the sectors
    calculated_Load = get_Event_Util(Sectors_Load,utiThshld,qoshshld,True,False)
    sectors_Config_DF['Forecast Load Times'] = sectors_Config_DF['Sector Code'].map(dict(zip(calculated_Load['Sector Code'], calculated_Load['High Load Times'])))
    print("Done Adding Forecast Load.")
    
    del Sectors_Load
    del calculated_Load

    # Add Upgrade Steps & Utilization Per Step :
    for i in range(max_steps):
        current_Step = "Step"+ str(i+1)
        prev_Step = "Step" + str(i)
        impact = "Impact_"+ current_Step
        load_Column = "Load Times After " + current_Step
        assessing_col = "Load Times After " + prev_Step
        current_acc_Impact = "Accumulated Impact After" + current_Step
        prev_acc_Impact = "Accumulated Impact After" + prev_Step
        if i ==0:
            assess_col = "Forecast Load Times"
        else:
            assess_col = assessing_col
        sectors_Config_DF[current_Step] =  sectors_Config_DF.apply(lambda row: get_upgrade_Step(row,steps_DF,prev_Step,hrs_threshld,assess_col), axis=1)
        sectors_Config_DF[impact] = sectors_Config_DF[current_Step].apply(lambda x: str(x).split(',')[2][:-1] if str(x)!="" else '')
        sectors_Config_DF[impact] = sectors_Config_DF.apply(lambda x: get_step_impact(x,impact),axis=1)
        if i==0:
            sectors_Config_DF[current_acc_Impact] = sectors_Config_DF[impact]
        else:
            sectors_Config_DF[current_acc_Impact] = sectors_Config_DF.apply(lambda x: (float(x[impact]) + float(x[prev_acc_Impact]) if str(x[impact])!="" else x[prev_acc_Impact] ),axis=1)
        
        # Getting the Load Times after the identifying the impact of the upgrade step
        arb_Radio['Upgrades Impact'] = arb_Radio['Sector Code'].map(dict(zip(sectors_Config_DF['Sector Code'], sectors_Config_DF[current_acc_Impact])))
        Sectors_Load = arb_Radio[
            (arb_Radio['Date'] >= event_start_date) & 
            (arb_Radio['Date'] <= event_end_date)].pivot_table(values=['LTE Data Volume (TB)', 'Calculated Cell THRPT', 'L.Traffic.ActiveUser.Avg', 'BW(PRBs)', 'Site Forecast %','Upgrades Impact'], 
                                                                         index=['Sector Code'], 
                                                                         columns=['Date', 'Time'], 
                                                                         aggfunc={
                                                                             'LTE Data Volume (TB)': 'sum',
                                                                             'Calculated Cell THRPT': 'sum',
                                                                             'L.Traffic.ActiveUser.Avg': 'sum',
                                                                             'BW(PRBs)': 'sum',
                                                                             'Site Forecast %': 'max',
                                                                             'Upgrades Impact': 'max'}, 
                                                                             fill_value=0)
        calculated_Load = get_Event_Util(Sectors_Load,utiThshld,qoshshld,False,True)
        sectors_Config_DF[load_Column] = sectors_Config_DF['Sector Code'].map(dict(zip(calculated_Load['Sector Code'], calculated_Load['High Load Times'])))
        del Sectors_Load
        del calculated_Load
        print("Done Adding Forecast Load after ",current_Step,".")
    
    print ("Now Starting the clean up.")
    for i in range(max_steps):
        current_Step = "Step"+ str(i+1)
        impact = "Impact_"+ current_Step
        current_acc_Impact = "Accumulated Impact After" + current_Step
        sectors_Config_DF.drop(columns=[impact,current_acc_Impact], inplace=True)
        sectors_Config_DF[current_Step] = sectors_Config_DF[current_Step].apply(lambda x: str(x).split(',')[1].replace("'", "").strip() if x != "" else "")

    study_inputs = {
        'Normal Days': arb_form['normalStartDate'].strftime('%Y-%m-%d') + " : " + arb_form['normalEndDate'].strftime('%Y-%m-%d'),
        'Event Days': arb_form['eventStartDate'].strftime('%Y-%m-%d') + " : " + arb_form['eventEndDate'].strftime('%Y-%m-%d'),
        'QoS THRPT [Mbps]': arb_form['qoshshld'] ,
        'High OG Load Threshold': arb_form['utiThshld'] ,
        'Considered High Load Times for Upgrade': arb_form['hrshshld'], 
        'Max Site Thrpt Repitations Identify Clipping': arb_form['hrsTxEthershld'], 
        'High Flow Control Drop Rate Threshold': arb_form['flwCtrlPercentage'], 
        'High Flow Control Drop number Threshold': arb_form['flwCtrlNumber'], 
    }
    conditions_DF = pd.DataFrame(study_inputs, index=[0])    
    conditions_DF = conditions_DF.T.reset_index()

    steps_Columns = [value for value in sectors_Config_DF.columns if ('Step' in value) and ('Load' not in value)]
    unique_steps = steps_DF['Step ID'].unique()
    req_Cols = []
    for stepID in unique_steps:
        Upgrade_Col_name = stepID + " Needed?"
        sectors_Config_DF[Upgrade_Col_name] = sectors_Config_DF.apply(lambda x: 1 if stepID in str(x[steps_Columns]) else 0,axis=1)
        req_Cols.append(Upgrade_Col_name)

    Sites_Summary = sectors_Config_DF.pivot_table(values=req_Cols,index=['City','Code'],aggfunc='sum',fill_value=0)
    Sites_Summary.reset_index(inplace=True)
    for stepID in unique_steps:
        Upgrade_Col_name = stepID + " Needed?"
        upgrade_Scale = steps_DF.loc[steps_DF['Step ID'] == stepID, 'Site/Sector Upgrade'].iloc[0]
        Sites_Summary[Upgrade_Col_name] =  Sites_Summary[Upgrade_Col_name].apply(lambda x: 1 if upgrade_Scale == "Site" and x>0 else x)
    
    Citeis_Summary = Sites_Summary.pivot_table(values=req_Cols,index=['City'],aggfunc='sum',fill_value=0)
    Citeis_Summary.reset_index(inplace=True)
    # Prepare Sector Max Needed BW:
    sectors_Config_DF['Sector_Config Tx BW [Mbps]'] =  sectors_Config_DF.apply(lambda row: get_Sec_Tx_BW(row), axis=1)
    sectors_Config_DF['Upgraded Sector_Config Tx BW [Mbps]'] =  sectors_Config_DF.apply(lambda row: get_upgraded_Sec_Tx_BW(row,req_Cols,steps_DF), axis=1)
    Tx_Config = sectors_Config_DF.pivot_table(values=['Sector_Config Tx BW [Mbps]','Upgraded Sector_Config Tx BW [Mbps]'], index=['City','Code'],aggfunc='max',fill_value=0)
    Tx_Config.reset_index(inplace=True)
    sectors_Config_DF= sectors_Config_DF.drop(columns=['Sector_Config Tx BW [Mbps]','Upgraded Sector_Config Tx BW [Mbps]'])
    # Preparing the Tx Consumed Port THRPT
    if arb_form['fileArbEthernetCounters']:
        print("Ethernet Port counters Exist")
        fileArbEthernetCounters = arb_form['fileArbEthernetCounters']
        file_stream1 = io.TextIOWrapper(fileArbEthernetCounters, encoding='utf-8')
        chunks_EtherPort = []
        chunks_EtherPort_size = 100000  # Adjust this value based on your memory capacity
        chunk_k=0
        for chunk in pd.read_csv(file_stream1, skiprows=6, engine='python', encoding='utf-8', chunksize=chunks_EtherPort_size):
            chunks_EtherPort.append(chunk)
            print("EtherPort Counters  1Mil chunk appended - chunk number:", str(chunk_k))
            chunk_k=chunk_k+1
        
        arb_Ether = pd.concat(chunks_EtherPort, ignore_index=True)
        arb_Ether = arb_Ether[:-1]
        
        arb_Ether = arb_Ether[arb_Ether['FEGE.RxMaxSpeed (Mbps)(Mbit/s)']>0]
        arb_Ether['Code'] = arb_Ether.apply(lambda row: str(row['eNodeB Name'])[-7:], axis=1)
        arb_Ether['City'] = arb_Ether.apply(lambda row: row['Code'][:3], axis=1)
        arb_Ether['Date'] = pd.to_datetime(arb_Ether['Date'], format='%d-%m-%Y')
        arb_Ether = arb_Ether[(arb_Ether['Date'] >= event_start_date) & (arb_Ether['Date'] <= event_end_date)]
        arb_Ether = arb_Ether[arb_Ether['Code'].isin(sectors_Config_DF['Code'])].copy()
        arb_Ether['FEGE.RxMaxSpeed (Mbps)(Mbit/s)']= arb_Ether['FEGE.RxMaxSpeed (Mbps)(Mbit/s)'].apply(pd.to_numeric , errors='coerce')
        arb_Ether['Tx BW [Mbps]'] = arb_Ether['FEGE.RxMaxSpeed (Mbps)(Mbit/s)'].apply(lambda x: 5*round(x/5,0))
        end_time =time.time()
        duration = str(round((end_time - start_time),0))+" Seconds"
        print("Done Processing the EtherPort Speed and Tx BW consumed prepared, consumed time is :", duration)
        
        site_etherPort = arb_Ether.pivot_table(values='Tx BW [Mbps]',index=['City','Code'],aggfunc='max',fill_value=0)
        max_count = arb_Ether.groupby(['City', 'Code'])['Tx BW [Mbps]'].apply(lambda x: (x == x.max()).sum()).reset_index(name='Max Count')
        site_etherPort = pd.merge(site_etherPort, max_count, on=['City', 'Code'], how='left')

        site_etherPort.reset_index(inplace=True)
        site_etherPort.columns = ['No.', 'City','Code','Tx BW [Mbps]','Max Count']
        site_etherPort['Site Forecast %'] = site_etherPort['Code'].map(dict(zip(Sites_Forecast['Code'], Sites_Forecast['Site Forecast %'])))
        site_etherPort['Forecast BW'] =( 1 + site_etherPort['Site Forecast %'] ) * site_etherPort['Tx BW [Mbps]']
        site_etherPort['Forecast BW'] = 5 * round( site_etherPort['Forecast BW']/5 , 0)
        site_etherPort['Sector_Config Tx BW [Mbps]'] = site_etherPort['Code'].map(dict(zip(Tx_Config['Code'], Tx_Config['Sector_Config Tx BW [Mbps]'])))
        site_etherPort['Upgraded Sector_Config Tx BW [Mbps]'] = site_etherPort['Code'].map(dict(zip(Tx_Config['Code'], Tx_Config['Upgraded Sector_Config Tx BW [Mbps]'])))
        site_etherPort['BW Comment'] = site_etherPort['Max Count'].apply(lambda x: "BW Expansion Needed - Sites was clipping" if x >= int(arb_form['hrsTxEthershld']) else "")
        del Tx_Config
    else:
        print("Ethernet Port Counters Not Exist")
    if arb_form['fileArbFlowTxCounters']:
        print("Flow Control counters Exist")
        fileArbFlowTxCounters = arb_form['fileArbFlowTxCounters']
        file_stream2 = io.TextIOWrapper(fileArbFlowTxCounters, encoding='utf-8')
        chunks_flowCtrl = []
        chunks_flowCtrl_size = 100000  # Adjust this value based on your memory capacity
        chunk_L=0
        for chunk in pd.read_csv(file_stream2, skiprows=6, engine='python', encoding='utf-8', chunksize=chunks_flowCtrl_size):
            chunks_flowCtrl.append(chunk)
            print("EtherPort Counters  1Mil chunk appended - chunk number:", str(chunk_L))
            chunk_L=chunk_L+1
        arb_FlowCtrl = pd.concat(chunks_flowCtrl, ignore_index=True)
        arb_FlowCtrl = arb_FlowCtrl[:-1]
        arb_FlowCtrl['4G DL Traffic Volume (GB)_Asiacell']= arb_FlowCtrl['4G DL Traffic Volume (GB)_Asiacell'].apply(pd.to_numeric , errors='coerce')
        arb_FlowCtrl = arb_FlowCtrl[arb_FlowCtrl['4G DL Traffic Volume (GB)_Asiacell']>0]
        arb_FlowCtrl['Code'] = arb_FlowCtrl.apply(lambda row: str(row['eNodeB Name'])[-7:], axis=1)
        arb_FlowCtrl['City'] = arb_FlowCtrl.apply(lambda row: row['Code'][:3], axis=1)
        arb_FlowCtrl['Date'] = pd.to_datetime(arb_FlowCtrl['Date'], format='%d-%m-%Y')
        arb_FlowCtrl = arb_FlowCtrl[(arb_FlowCtrl['Date'] >= event_start_date) & (arb_FlowCtrl['Date'] <= event_end_date)]
        arb_FlowCtrl = arb_FlowCtrl[arb_FlowCtrl['Code'].isin(sectors_Config_DF['Code'])].copy()
        arb_FlowCtrl['VS.RscGroup.FlowCtrol.DL.DropNum']= arb_FlowCtrl['VS.RscGroup.FlowCtrol.DL.DropNum'].apply(pd.to_numeric , errors='coerce')
        arb_FlowCtrl['VS.RscGroup.FlowCtrol.DL.ReceiveNum']= arb_FlowCtrl['VS.RscGroup.FlowCtrol.DL.ReceiveNum'].apply(pd.to_numeric , errors='coerce')
        arb_FlowCtrl['Flow Control Drop %'] = 100 * arb_FlowCtrl['VS.RscGroup.FlowCtrol.DL.DropNum']/ (arb_FlowCtrl['VS.RscGroup.FlowCtrol.DL.DropNum'] + arb_FlowCtrl['VS.RscGroup.FlowCtrol.DL.ReceiveNum'])
        arb_FlowCtrl['Flow Control Drop %'] = arb_FlowCtrl['Flow Control Drop %'].replace([np.inf, -np.inf, np.nan], 0)
        arb_FlowCtrl['High Flow Control'] = ((arb_FlowCtrl['Flow Control Drop %'] >= int(arb_form['flwCtrlPercentage'])) | (arb_FlowCtrl['VS.RscGroup.FlowCtrol.DL.DropNum'] >= int(arb_form['flwCtrlNumber']))).astype(int)
        Site_FlowCtrl =  arb_FlowCtrl.pivot_table(values='High Flow Control',index=['City','Code'],aggfunc='sum',fill_value=0)
        Site_FlowCtrl.reset_index(inplace=True)
        site_etherPort['High Flow Control Drop Times'] = site_etherPort['Code'].map(dict(zip(Site_FlowCtrl['Code'], Site_FlowCtrl['High Flow Control'])))
    else:
        print("Flow Control Counters Not Exist")

    #  Exporting the Output
    with pd.ExcelWriter(Study_Output, engine='openpyxl') as writer:
        conditions_DF.to_excel(writer, sheet_name='Inputs', index=False,header=False)
        forecast_DF.to_excel(writer, sheet_name='Considered Forecast', index=False)
        sectors_Config_DF.to_excel(writer, sheet_name='Capacity Study', index=False)
        Sites_Summary.to_excel(writer, sheet_name='Sites Requirements Summary', index=False)
        Citeis_Summary.to_excel(writer, sheet_name='Cities Requirements Summary', index=False)
        if arb_form['fileArbEthernetCounters']:
            site_etherPort.to_excel(writer, sheet_name='Access Tx BW', index=False)
    end_time =time.time()
    duration = str(round((end_time - start_time),0))+" Seconds"
    print("Preparing Massive Event Capacity Assesment consumed a duration of:", duration)
    return duration

def get_band_by_frequency(frequency):
    bands = [
        {"BAND_Name": "Band1", "start": 0, "end": 599},
        {"BAND_Name": "Band2", "start": 600, "end": 1199},
        {"BAND_Name": "Band3", "start": 1200, "end": 1949},
        {"BAND_Name": "Band4", "start": 1950, "end": 2399},
        {"BAND_Name": "Band5", "start": 2400, "end": 2649},
        {"BAND_Name": "Band7", "start": 2750, "end": 3449},
        {"BAND_Name": "Band8", "start": 3450, "end": 3799},
        {"BAND_Name": "Band9", "start": 3800, "end": 4149},
        {"BAND_Name": "Band10", "start": 4150, "end": 4749},
        {"BAND_Name": "Band11", "start": 4750, "end": 4949},
        {"BAND_Name": "Band12", "start": 5010, "end": 5179},
        {"BAND_Name": "Band13", "start": 5180, "end": 5279},
        {"BAND_Name": "Band14", "start": 5280, "end": 5379},
        {"BAND_Name": "Band17", "start": 5730, "end": 5849},
        {"BAND_Name": "Band18", "start": 5850, "end": 5999},
        {"BAND_Name": "Band19", "start": 6000, "end": 6149},
        {"BAND_Name": "Band20", "start": 6150, "end": 6449},
        {"BAND_Name": "Band21", "start": 6450, "end": 6599},
        {"BAND_Name": "Band22", "start": 6600, "end": 7399},
        {"BAND_Name": "Band24", "start": 7700, "end": 8039},
        {"BAND_Name": "Band25", "start": 8040, "end": 8689},
        {"BAND_Name": "Band26", "start": 8690, "end": 9039},
        {"BAND_Name": "Band27", "start": 9040, "end": 9209},
        {"BAND_Name": "Band28", "start": 9210, "end": 9659},
        {"BAND_Name": "Band29", "start": 9660, "end": 9769},
        {"BAND_Name": "Band30", "start": 9770, "end": 9869},
        {"BAND_Name": "Band31", "start": 9870, "end": 9919},
        {"BAND_Name": "Band32", "start": 9920, "end": 10359},
        {"BAND_Name": "Band33", "start": 36000, "end": 36199},
        {"BAND_Name": "Band34", "start": 36200, "end": 36349},
        {"BAND_Name": "Band35", "start": 36350, "end": 36949},
        {"BAND_Name": "Band36", "start": 36950, "end": 37549},
        {"BAND_Name": "Band37", "start": 37550, "end": 37749},
        {"BAND_Name": "Band38", "start": 37750, "end": 38249},
        {"BAND_Name": "Band39", "start": 38250, "end": 38649},
        {"BAND_Name": "Band40", "start": 38650, "end": 39649},
        {"BAND_Name": "Band41", "start": 39650, "end": 41589},
        {"BAND_Name": "Band42", "start": 41590, "end": 43589},
        {"BAND_Name": "Band43", "start": 43590, "end": 45589},
        {"BAND_Name": "Band44", "start": 45590, "end": 46589},
        {"BAND_Name": "Band45", "start": 46590, "end": 46789},
        {"BAND_Name": "Band46", "start": 46790, "end": 54539},
        {"BAND_Name": "Band47", "start": 54540, "end": 55239},
        {"BAND_Name": "Band48", "start": 55240, "end": 56739},
        {"BAND_Name": "Band49", "start": 56740, "end": 58239},
        {"BAND_Name": "Band50", "start": 58240, "end": 59089},
        {"BAND_Name": "Band51", "start": 59090, "end": 59139},
        {"BAND_Name": "Band52", "start": 59140, "end": 60139},
        {"BAND_Name": "Band53", "start": 60140, "end": 60254},
        {"BAND_Name": "Band65", "start": 65536, "end": 66435},
        {"BAND_Name": "Band66", "start": 66436, "end": 67335},
        {"BAND_Name": "Band67", "start": 67336, "end": 67535},
        {"BAND_Name": "Band68", "start": 67536, "end": 67835},
        {"BAND_Name": "Band69", "start": 67836, "end": 68335},
        {"BAND_Name": "Band70", "start": 68336, "end": 68585},
        {"BAND_Name": "Band71", "start": 68586, "end": 68935},
        {"BAND_Name": "Band72", "start": 68936, "end": 68985},
        {"BAND_Name": "Band73", "start": 68986, "end": 69035},
        {"BAND_Name": "Band74", "start": 69036, "end": 69465},
        {"BAND_Name": "Band75", "start": 69466, "end": 70315},
        {"BAND_Name": "Band76", "start": 70316, "end": 70365},
        {"BAND_Name": "Band85", "start": 70366, "end": 70545},
        {"BAND_Name": "Band87", "start": 70546, "end": 70595},
        {"BAND_Name": "Band88", "start": 70596, "end": 70645},
    ]
    for band in bands:
        if band["start"] <= frequency <= band["end"]:
            return band["BAND_Name"]    
    return None

def get_Event_Util(Sectors_Load,utiThshld,qoshshld,is_Forecast,is_Upgrade):
    utilization_cols = pd.MultiIndex.from_product([['LTE Utilization %'], Sectors_Load.columns.levels[1], Sectors_Load.columns.levels[2]])
    Sectors_Utilization = pd.DataFrame(index=Sectors_Load.index, columns=utilization_cols)    
    for the_Date in Sectors_Load.columns.levels[1]:  # Iterate over dates
        for The_time in Sectors_Load.columns.levels[2]:  # Iterate over times
            active_users_col = ('L.Traffic.ActiveUser.Avg', the_Date, The_time)
            cell_thrp_col = ('Calculated Cell THRPT', the_Date, The_time)
            forecast_Col = ('Site Forecast %' , the_Date, The_time)
            upgrade_Impact = ('Upgrades Impact', the_Date, The_time)
            try:
                if not (is_Forecast or is_Upgrade ):
                    Sectors_Utilization[('LTE Utilization %', the_Date, The_time)] = Sectors_Load[active_users_col] / (Sectors_Load[cell_thrp_col] / qoshshld)
                elif is_Forecast and not is_Upgrade:
                    Sectors_Utilization[('LTE Utilization %', the_Date, The_time)] = ( Sectors_Load[forecast_Col] + 1) * Sectors_Load[active_users_col] / (Sectors_Load[cell_thrp_col] / qoshshld)
                if is_Upgrade and not is_Forecast:
                    Sectors_Utilization[('LTE Utilization %', the_Date, The_time)] = ( Sectors_Load[forecast_Col] + 1) * Sectors_Load[active_users_col] / (Sectors_Load[cell_thrp_col] * ( 1 + Sectors_Load[upgrade_Impact]) / qoshshld)    
            except:
                Sectors_Utilization[('LTE Utilization %', the_Date, The_time)] = 0
                # print("Util Set to 0")
    
    
    Sectors_Load = pd.concat([Sectors_Load, Sectors_Utilization], axis=1)
    Sectors_Load['High Load Times'] = (Sectors_Load.xs('LTE Utilization %', level=0, axis=1) > utiThshld/100).sum(axis=1)
    prb_col = Sectors_Load.columns[Sectors_Load.columns.get_level_values(0)=='BW(PRBs)']
    Sectors_Load['Max PRBs'] = Sectors_Load[prb_col].max(axis=1)
    # print(Sectors_Load.head)
    if is_Upgrade:
        Sectors_Load = Sectors_Load.drop(columns=['LTE Data Volume (TB)','Calculated Cell THRPT','L.Traffic.ActiveUser.Avg','BW(PRBs)','LTE Utilization %','Site Forecast %','Upgrades Impact'], level=0).reset_index()
    else:
        Sectors_Load = Sectors_Load.drop(columns=['LTE Data Volume (TB)','Calculated Cell THRPT','L.Traffic.ActiveUser.Avg','BW(PRBs)','LTE Utilization %','Site Forecast %'], level=0).reset_index()
    Sectors_Load.columns = ['_'.join(str(sub_col) if sub_col is not pd.NaT else 'NaT' for sub_col in col).strip() for col in Sectors_Load.columns]
    Sectors_Load.columns = ['Sector Code', 'High Load Times','Max PRBs']
    return Sectors_Load


def get_upgrade_Step(r,steps_DF,prev_Step,hrs_threshld,load_Col):
    if r[load_Col] != "":
        if int(r[load_Col]) >= hrs_threshld:
            for index,step in steps_DF[steps_DF['City'] == r['City']].sort_values('Step').iterrows():
                step_detail = [step['Step'],step['Step ID'],step['Impact']]
                step_no = int(step['Step'])
                if step['Step ID'] in r.index:
                    if step['Step ID'] == "MIMO Configuarion" and r[step['Step ID']] !="4Tx4Rx":
                        if prev_Step not in r.index:
                            return step_detail
                        if r[prev_Step] != step_detail and r[prev_Step] != "":
                            prev_Step_no = int(str(r[prev_Step]).split(',')[0][-1:])
                            if step_no > prev_Step_no:
                                return step_detail
                    elif step['Step ID'] != "MIMO Configuarion" and r[step['Step ID']] =="No":
                        if prev_Step not in r.index:
                            return step_detail
                        elif r[prev_Step] != step_detail and r[prev_Step] != "":
                            prev_Step_no = int(str(r[prev_Step]).split(',')[0][-1:])
                            if step_no > prev_Step_no:
                                return step_detail
                elif prev_Step == 'Step0':
                    return step_detail
                
                elif step['Step ID'] not in str(r[prev_Step]) and str(r[prev_Step])!="":
                    return step_detail
            return ""
        else:
            return ""
    else:
        return ""

def get_step_impact(r,Column_name):
    if r[Column_name] =="":
        return 0
    elif float(str(r[Column_name]).strip()) > 20:
        return float(str(r[Column_name]).strip())/float(str(r['Max PRBs']).strip())
    else:
        if str(r['L2600[40MHz]']) == "No":
            return float(str(r[Column_name]).strip()) 
        elif str(r['L2600[40MHz]']) == "Yes" and str(r['L2600_F3']) == "Yes":
            return (float(str(r['Max PRBs']).strip())-300) * (float(str(r[Column_name]).strip()))/float(str(r['Max PRBs']).strip())
        elif str(r['L2600[40MHz]']) == "Yes" and str(r['L2600_F3']) == "No":
            return (float(str(r['Max PRBs']).strip())-200) * (float(str(r[Column_name]).strip()))/float(str(r['Max PRBs']).strip())

def get_Sec_Tx_BW(r):
    TDD_PRBs = 0
    FDD_PRBs = 0
    HOS_Configured = 1
    Sec_BW = 0
    try:
        MIMO_Config = int(str(r['MIMO Configuarion']).strip()[0])
    except:
         MIMO_Config = 2

    if str(r['L2600[40MHz]']) == "Yes" and str(r['L2600_F3']) == "Yes":
        TDD_PRBs = 300
    elif str(r['L2600[40MHz]']) == "Yes" and str(r['L2600_F3']) == "No":
        TDD_PRBs = 200
    elif str(r['L2600[40MHz]']) == "No" and str(r['L2600_F3']) == "Yes":
        TDD_PRBs = 100
    else:
        TDD_PRBs = 0
    
    if str(r['HOS']) == "Yes":
        HOS_Configured = 2
    else:
        HOS_Configured = 1

    Sec_BW = (((int(r['Max PRBs']) - TDD_PRBs)/HOS_Configured) * 0.75 * MIMO_Config) + ((TDD_PRBs/100)*225)
    
    return Sec_BW

def get_upgraded_Sec_Tx_BW(r,req_Cols,steps_DF):
    Sec_BW = 0
    prbs = 0
    Mimo_Percentage = 1
    checker_Value = 0
    for upgrade in req_Cols:
        step = upgrade.replace(" Needed?", "")
        if r[upgrade] == 1:
            checker_Value = float(steps_DF[(steps_DF['Step ID'] == step) & (steps_DF['City'] == str(r['City']))]['PRBs Tx Impact'].iloc[0])
            if checker_Value < 25 :
                Mimo_Percentage = Mimo_Percentage + checker_Value
            else:
                prbs = prbs + checker_Value

    return float(str(r['Sector_Config Tx BW [Mbps]']).strip()) + prbs * 1.5 * Mimo_Percentage

st.set_page_config(
    page_title="EasyOptim - Mass Event Capacity Assessment Tool",
    layout="wide"
)
st.markdown(
    """
    <style>
    .header {
        background-color: #f8f9fa;
        padding: 20px;
        text-align: left;
        font-size: 30px;
        font-weight: bold;
        border-bottom: 2px solid #e0e0e0;
    }
    </style>
    <div class="header">
        EasyOptim - Mass Event Capacity Assessment Tool 
    </div>
    """,
    unsafe_allow_html=True,
)
st.write("**Instructions:**")
st.markdown("""
- **Technology Study**: Limited to `4G`.
- **Formulas**: Matching to `Ooredoo Group`.
- **Radio Counters**: `L.ChMeas.PRB.DL.Avail`, `Used Rank1 _Asiacell`, `Used Rank2 _Asiacell`, `Used Rank3 _Asiacell`, `Used Rank4 _Asiacell`, `Downlink EARFCN`
, `DL User Throughput_Asiacell`, `UL Interference_Asiacell`, `Average CQI`, `BW(PRBs)`, `DL IBLER_Asiacell`, `4G DL Traffic Volume (GB)_Asiacell`, `4G UL Traffic Volume (GB)_Asiacell`
, `HW_DL PRB Avg Utilization(%)`, `L.Traffic.ActiveUser.Avg`, `UL IBLER_Asiacell`, `LTECell Tx and Rx Mode`.
- **Radio Part**: Data must be collected `Hourly cell Level`, File must be CSV, must contain `eNodeB Name`,`LocalCell Id`.
- **Needed Radio Counters**: Limited to `<`, `>`, `=`, `<=`, `>=`.
- **Needed Tx/EtherPort Counters**: Etherport Counters file must contain `FEGE.RxMaxSpeed (Mbps)(Mbit/s)`, Tx Flow Control Counters File must contain `VS.RscGroup.FlowCtrol.DL.DropNum`, `VS.RscGroup.FlowCtrol.DL.ReceiveNum`.
- **Dates Condition**: All reports must contain dates for the Event [Start/End] and Normal Days [Start/End]
""")   
with st.expander("Specify conditions for the Capacity Tool ", expanded=True):
    event_cal_col,nor_cal_col,rad_thrshld_col,tx_thrshld_col = st.columns(4)
    event_cal_col.write("Previous Event Calender:")
    nor_cal_col.write("Normal Days Calender")
    rad_thrshld_col.write("Radio Thresholds/Upgrades Steps")
    tx_thrshld_col.write("Tx Thresholds/Flow Control Conditions")
    
    eventStartDate = event_cal_col.date_input("Start Date:",datetime.date(2024, 8, 6))
    eventEndDate = event_cal_col.date_input("End Date:",datetime.date(2024, 8, 25))

    normalStartDate = nor_cal_col.date_input("Start Date:",datetime.date(2024, 8, 1))
    normalEndDate = nor_cal_col.date_input("End Date:",datetime.date(2024, 8, 5))

    col1,col2 = rad_thrshld_col.columns(2)
    utiThshld=col1.number_input("OG 4G Utilization %:", min_value=10, max_value=1000, value=100,step=10)
    qoshshld=col2.number_input("QoS Throughput Mbps:", min_value=0.1, max_value=20.0, value=3.0,step=0.1)
    hrshshld=rad_thrshld_col.number_input("High Load Hours Threshold:", min_value=1, max_value=200, value=20,step=1)

    col3,col4 = tx_thrshld_col.columns(2)
    hrsTxEthershld = tx_thrshld_col.number_input("High Tx Clip Threshold Times", min_value=1, max_value=200, value=20,step=1)
    flwCtrlPercentage = col3.number_input("Flow Ctrl drop %", min_value=0.5, max_value=100.0, value=2.0,step=0.5)
    flwCtrlNumber = col4.number_input("Flow Ctrl drop No.", min_value=100, max_value=10000000, value=100000,step=100)

with st.expander("Upload input files :", expanded=True):
 
    col_1,col_2,col_3,col_4,col_5 = st.columns(5)
    filesiteScope = col_1.file_uploader("Study Sites Scope:", type=["xlsx"])
    fileArbForecast= col_2.file_uploader("Data Forecast:", type=["xlsx"])
    fileArbRadioCounters =col_3.file_uploader("Radio Counters:", type=["csv"])
    fileArbFlowTxCounters =col_4.file_uploader("Tx Flow Control Counters:", type=["csv"])
    fileArbEthernetCounters =col_5.file_uploader("Tx EtherPort & Utilization Counters:", type=["csv"])
    
    tool_ARBlteutil = st.button("Start", key="tool_ARBlteutil")

    if tool_ARBlteutil:
        arb_form = {
                'filesiteScope': filesiteScope,
                'fileArbForecast': fileArbForecast,
                'fileArbRadioCounters': fileArbRadioCounters,
                'fileArbFlowTxCounters': fileArbFlowTxCounters,
                'fileArbEthernetCounters': fileArbEthernetCounters,
                'eventStartDate': eventStartDate,
                'eventEndDate': eventEndDate,
                'normalStartDate': normalStartDate,
                'normalEndDate': normalEndDate,
                'utiThshld' : utiThshld,
                'qoshshld' : qoshshld,
                'hrshshld' : hrshshld,
                'hrsTxEthershld' : hrsTxEthershld,
                'flwCtrlPercentage' : flwCtrlPercentage,
                'flwCtrlNumber' : flwCtrlNumber,
            }
        dura = arb_Study(arb_form)
        st.write("Preparing Massive Event Capacity Assesment consumed a duration of:"+ dura)
        with open(Study_Output, "rb") as f:
            file_data = f.read()
            b64_file_data = base64.b64encode(file_data).decode()
            download_link = f'<a href="data:application/octet-stream;base64,{b64_file_data}" download="{os.path.basename(Study_Output)}">Click to download Study Output File {os.path.basename(Study_Output)}</a>'
        st.markdown(download_link, unsafe_allow_html=True) 

st.markdown(
    """
    <style>
    .footer {
        position: fixed;
        bottom: 0;
        width: 100%;
        background-color: #f8f9fa;
        padding: 10px 0;
        text-align: left;
        font-size: 16px;
        border-top: 2px solid #e0e0e0;
    }
    </style>
    <div class="footer">
        The Tool developed by Abdellatif Ahmed (abdellatif.ahmed@nokia.com)
        
    </div>
    
    """,
    unsafe_allow_html=True,
)


