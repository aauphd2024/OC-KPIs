# -*- coding: utf-8 -*-
"""
Created on Wed May 29 10:45:34 2024

@author: kamilla heimar andersen
kahean@build.aau.dk
"""

#%%
##############################################################################
### HEATING USE KPIs AND OC KPIs ###
##############################################################################

#%% Trim dataset to heating season only

list_months_heating_season = [1,2,3]
list_target_years = [2023]

for i, df in enumerate(dfs_merged):
    df = df[df['DateTime'].dt.month.isin(list_months_heating_season) & df['DateTime'].dt.year.isin(list_target_years)]
    dfs_merged[i] = df  # Just to be sure it changes the df

#%% Drop all data lines without occupancy data

for i, df in enumerate(dfs_merged):
    df = df.dropna(subset=['dominant_presence'])
    dfs_merged[i] = df

#%% Get apartment average occupancy over heating season

for i, df in enumerate(dfs_merged):
    occupancy_series = df['dominant_presence'].dropna()
    avrg_occupancy = (occupancy_series.mean())*100
    print(f"Average occupancy of apartment {i} over feb, march, nov, dec is: {avrg_occupancy} %")
    dfs_merged[i] = df

#%% Get only heating and occupancy data

dfs_heating_merged = dfs_merged.copy()

for i, df in enumerate(dfs_heating_merged):
    df = df[['DateTime', 'dominant_presence', 'heating_power_kw']].dropna()
    dfs_heating_merged[i] = df

#%% Compute KPI 1: monthly average heating use

for i, df in enumerate(dfs_heating_merged):
    KPI_1_monthly_average_heating_use = (df['heating_power_kw'].mean())*30.5*24
    print(f"The monthly average heating use (KPI 1) for apartment {i+1} is: {KPI_1_monthly_average_heating_use} kWh/month")

#%% Compute KPI 2: monthly average heating use per m2

list_surface_area = [55, 72, 55, 72, 72]

for i, df in enumerate(dfs_heating_merged):
    KPI_2_monthly_average_heating_use = (df['heating_power_kw'].mean())*30.5*24/list_surface_area[i]
    print(f"The monthly average heating use per m2 (KPI 2) for apartment {i+1} is: {KPI_2_monthly_average_heating_use} kWh/m2 per month")

#%% Compute OC-KPI 1: monthly heating use during occupied hours

for i, df in enumerate(dfs_heating_merged):
    df['heat_occ'] = df['heating_power_kw'] * df['dominant_presence']
    OC_KPI_1_monthly_average_heating_use_during_occ_h = (df['heat_occ'].mean())*30.5*24
    print(f"The monthly average heating use during occupied hours (OC KPI 1) for apartment {i+1} is: {OC_KPI_1_monthly_average_heating_use_during_occ_h} kWh/hour_occ per month")


#%% Compute OC-KPI 2: Monthly average heating use during occupied hours

for i, df in enumerate(dfs_heating_merged):
    df['heat_occ'] = df['heating_power_kw'] * df['dominant_presence']
    OC_KPI_2_monthly_average_heating_use_during_occ_h = (df['heat_occ'].mean())*30.5*24/list_surface_area[i]
    print(f"The monthly average heating use during occupied hours per m2 (OC KPI 2) for apartment {i+1} is: {OC_KPI_2_monthly_average_heating_use_during_occ_h} kWh/hour_occ per month per m2")

#%% Compute OC-KPI 3: Monthly average heating use during non-occupied hours

for i, df in enumerate(dfs_heating_merged):
    df['heat_occ'] = df['heating_power_kw'] * (1-df['dominant_presence'])
    OC_KPI_3_monthly_average_heating_use_during_non_occ_h = (df['heat_occ'].mean())*30.5*24/list_surface_area[i]
    print(f"The monthly average heating use during non-occupied hours per m2 (OC KPI 3) for apartment {i+1} is: {OC_KPI_3_monthly_average_heating_use_during_non_occ_h} kWh/hour_non_occ per month per m2")

#%% Compute OC-KPI 4: Share of heating use during occupied hours

for i, df in enumerate(dfs_heating_merged):
    tot_heat_use = df['heating_power_kw'].sum()
    df['heat_occ'] = df['heating_power_kw'] * df['dominant_presence']
    tot_heat_occ = df['heat_occ'].sum()
    share_heating_use_occ_hours = tot_heat_occ*100 / tot_heat_use
    print(f"The share of heating use during occupied hours (OC KPI 4) for apartment {i+1} is: {share_heating_use_occ_hours} %")

#%% Compute OC-KPI 5: Share of heating use during non-occupied hours

for i, df in enumerate(dfs_heating_merged):
    tot_heat_use = df['heating_power_kw'].sum()
    df['heat_occ'] = df['heating_power_kw'] * df['dominant_presence']
    tot_heat_occ = df['heat_occ'].sum()
    share_heating_use_non_occ_hours = 100-(tot_heat_occ*100 / tot_heat_use)
    print(f"The share of heating use during non-occupied hours (OC KPI 5) for apartment {i+1} is: {share_heating_use_non_occ_hours} %")

#%% Calculating degreeday from outdoor temperature DMI data

# Taking hourly df of DMI outdoor temperature and adding column corresponding to daily average temperature

outdoor_air_temperature_hourly['Date'] = outdoor_air_temperature_hourly['DateTime'].dt.date
outdoor_air_temperature_hourly['daily avrg temperature'] = outdoor_air_temperature_hourly['temp_mean_past1h']

for date in set(outdoor_air_temperature_hourly['Date']):
    subdf = outdoor_air_temperature_hourly[outdoor_air_temperature_hourly['Date']==date]
    daily_avrg_temp = subdf['temp_mean_past1h'].mean()
    outdoor_air_temperature_hourly.loc[outdoor_air_temperature_hourly['Date'] == date, 'daily avrg temperature'] = daily_avrg_temp

#%% Merge outdoor temperature df with heating df

for i, df in enumerate(dfs_heating_merged):
    new_df = pd.merge(df, outdoor_air_temperature_hourly, on='DateTime', how='outer')
    dfs_heating_merged[i] = new_df

#%% New heating merged df with outdoor mean temp data

dfs_heating_merged_2 = dfs_heating_merged.copy()

for i, df in enumerate(dfs_heating_merged_2):
    df = df.dropna()
    df.drop(['Date', 'temp_mean_past1h'], inplace=True, axis=1)
    dfs_heating_merged_2[i] = df

#%% Calculate degree day and add to df

for i, df in enumerate(dfs_heating_merged_2):
    max_temp = df['daily avrg temperature'].max()
    # print(f'Maximum daily average temperature in df {i} is: {max_temp} C')
    df['HDD'] = 16 - df['daily avrg temperature']
    df['heating_norm_HDD'] = df['heating_power_kw'] / df['HDD']
    non_OC_KPI_4_monthly_average_heating_use_norm_HDD_per_m2 = (df['heating_norm_HDD'].mean())*30.5*24/list_surface_area[i]
    print(f"The monthly average heating use norm HDD per m2 (non-OC KPI 3) for apartment {i+1} is: {non_OC_KPI_4_monthly_average_heating_use_norm_HDD_per_m2} kWh/HDD per m2")

#%% OC KPI 5: total heating / total occupied hours

for i, df in enumerate(dfs_heating_merged):
    total_heating_use = df['heating_power_kw'].sum()
    total_occupied_hours = df['dominant_presence'].sum()
    kpi5 = total_heating_use/total_occupied_hours
    print(kpi5)

#%% OC KPI 6: total heating / total occupied hours per m2

for i, df in enumerate(dfs_heating_merged):
    total_heating_use = df['heating_power_kw'].sum()
    total_occupied_hours = df['dominant_presence'].sum()
    kpi6 = total_heating_use/(total_occupied_hours*list_surface_area[i])
    print(kpi6)

#%% Get only heating, occupancy and setpoint data

dfs_heating_SP_merged = dfs_merged.copy()

for i, df in enumerate(dfs_heating_SP_merged):
    new_df = pd.merge(df, outdoor_air_temperature_hourly, on='DateTime', how='outer')
    dfs_heating_SP_merged[i] = new_df

for i, df in enumerate(dfs_heating_SP_merged):
    df = df[['DateTime', 'heating_power_kw', 'avg_setpoint', 'temp_mean_past1h', 'avg_temp']].dropna()
    dfs_heating_SP_merged[i] = df

#%% OC KPI 7: heating use normalized by setpoint diff with outdoor temperature

for i, df in enumerate(dfs_heating_SP_merged):
    df['kpi'] = df['heating_power_kw']/(df['avg_setpoint']-df['temp_mean_past1h'])
    kpi7 = df['kpi'].mean()
    print(kpi7)

#%% OC KPI 8: heating use normalized by setpoint diff with outdoor temperature per m2

for i, df in enumerate(dfs_heating_SP_merged):
    df['kpi'] = df['heating_power_kw']/((df['avg_setpoint']-df['temp_mean_past1h'])*list_surface_area[i])
    kpi8 = df['kpi'].mean()
    print(kpi8)

#%% Get all setpoints for each room

room_1_SP = pd.concat([stth_data_5min['DateTime'], stth_data_5min['Setpoint_masterbedroom_C']], axis=1)
room_1_SP = room_1_SP.dropna()
room_1_SP.rename(columns={'Setpoint_masterbedroom_C': 'setpoint'}, inplace=True)
room_2_SP = pd.concat([stth_data_5min['DateTime'], stth_data_5min['Setpoint_livingroomkichen_C']], axis=1)
room_2_SP = room_2_SP.dropna()
room_2_SP.rename(columns={'Setpoint_livingroomkichen_C': 'setpoint'}, inplace=True)

room_3_SP = pd.concat([sttv_data_5min['DateTime'], sttv_data_5min['Setpoint_masterbedroom_C']], axis=1)
room_3_SP = room_3_SP.dropna()
room_3_SP.rename(columns={'Setpoint_masterbedroom_C': 'setpoint'}, inplace=True)
room_4_SP = pd.concat([sttv_data_5min['DateTime'], sttv_data_5min['Setpoint_livingroomkichen_C']], axis=1)
room_4_SP = room_4_SP.dropna()
room_4_SP.rename(columns={'Setpoint_livingroomkichen_C': 'setpoint'}, inplace=True)
room_5_SP = pd.concat([sttv_data_5min['DateTime'], sttv_data_5min['Setpoint_livingroomkichen_C']], axis=1)
room_5_SP = room_5_SP.dropna()
room_5_SP.rename(columns={'Setpoint_livingroomkichen_C': 'setpoint'}, inplace=True)
room_6_SP = pd.concat([sttv_data_5min['DateTime'], sttv_data_5min['Setpoint_smallbedroom_C']], axis=1)
room_6_SP = room_6_SP.dropna()
room_6_SP.rename(columns={'Setpoint_smallbedroom_C': 'setpoint'}, inplace=True)

room_7_SP = pd.concat([enth_data_5min['DateTime'], enth_data_5min['Setpoint_masterbedroom_C']], axis=1)
room_7_SP = room_7_SP.dropna()
room_7_SP.rename(columns={'Setpoint_masterbedroom_C': 'setpoint'}, inplace=True)
room_8_SP = pd.concat([enth_data_5min['DateTime'], enth_data_5min['Setpoint_livingroomkichen_C']], axis=1)
room_8_SP = room_8_SP.dropna()
room_8_SP.rename(columns={'Setpoint_livingroomkichen_C': 'setpoint'}, inplace=True)

room_9_SP = pd.concat([totv_data_5min['DateTime'], totv_data_5min['Setpoint_masterbedroom_C']], axis=1)
room_9_SP = room_9_SP.dropna()
room_9_SP.rename(columns={'Setpoint_masterbedroom_C': 'setpoint'}, inplace=True)
room_10_SP = pd.concat([totv_data_5min['DateTime'], totv_data_5min['Setpoint_livingroomkichen_C']], axis=1)
room_10_SP = room_10_SP.dropna()
room_10_SP.rename(columns={'Setpoint_livingroomkichen_C': 'setpoint'}, inplace=True)
room_11_SP = pd.concat([totv_data_5min['DateTime'], totv_data_5min['Setpoint_livingroomkichen_C']], axis=1)
room_11_SP = room_11_SP.dropna()
room_11_SP.rename(columns={'Setpoint_livingroomkichen_C': 'setpoint'}, inplace=True)
room_12_SP = pd.concat([totv_data_5min['DateTime'], totv_data_5min['Setpoint_smallbedroom_C']], axis=1)
room_12_SP = room_12_SP.dropna()
room_12_SP.rename(columns={'Setpoint_smallbedroom_C': 'setpoint'}, inplace=True)

room_13_SP = pd.concat([entv_data_5min['DateTime'], entv_data_5min['Setpoint_masterbedroom_C']], axis=1)
room_13_SP = room_13_SP.dropna()
room_13_SP.rename(columns={'Setpoint_masterbedroom_C': 'setpoint'}, inplace=True)
room_14_SP = pd.concat([entv_data_5min['DateTime'], entv_data_5min['Setpoint_livingroomkichen_C']], axis=1)
room_14_SP = room_14_SP.dropna()
room_14_SP.rename(columns={'Setpoint_livingroomkichen_C': 'setpoint'}, inplace=True)
room_15_SP = room_14_SP.copy()
room_16_SP = pd.concat([entv_data_5min['DateTime'], entv_data_5min['Setpoint_smallbedroom_C']], axis=1)
room_16_SP = room_16_SP.dropna()
room_16_SP.rename(columns={'Setpoint_smallbedroom_C': 'setpoint'}, inplace=True)

df_all_rooms_SP = [room_1_SP, room_2_SP, room_3_SP, room_4_SP, room_5_SP, room_6_SP, room_7_SP, room_8_SP, room_9_SP, room_10_SP, room_11_SP, room_12_SP, room_13_SP, room_14_SP, room_15_SP, room_16_SP]

#%% Detect changes of setpoints in each room and count per room
print("Average number of setpoint changes per month")
for i, df in enumerate(df_all_rooms_SP):
    # df2 = df.copy()
    # df2['DateTime'] = df2['DateTime'] + timedelta(minutes=5)
    # df2.rename(columns={'setpoint': 'setpoint2'}, inplace=True)
    # df = pd.merge(df, df2, on='DateTime', how='inner')
    # df['diff_SP'] = abs(df['setpoint'] - df['setpoint2']) > 0.1
    # df.dropna()
    # count = (df['diff_SP'].sum()/(len(df['diff_SP'])*5*60))*30.5*24*60*60
    std = df['setpoint'].std()
    print(std)
    # print(count)

#%% Calculate the setpoint standard deviation in each apartment
list_room_apt_1 = [1,2]
list_room_apt_2 = [3,4,5,6]
list_room_apt_3 = [7,8]
list_room_apt_4 = [9,10,11,12]
list_room_apt_5 = [13,14,15,16]
list_apt = [list_room_apt_1, list_room_apt_2, list_room_apt_3, list_room_apt_4, list_room_apt_5]

print("average std dev of SP in different rooms of apartment")
for list_room in list_apt:
    res_df = df_all_rooms_SP[list_room[0]-1]['DateTime']
    for i in list_room:
        df = df_all_rooms_SP[i-1]
        res_df = pd.merge(df, res_df, on='DateTime', how='inner')
    
    res_df = res_df.drop(columns=['DateTime'])
    res_df['StdDev'] = res_df.apply(lambda row: np.std(row), axis=1)
    mean_res = res_df['StdDev'].mean()
    print(mean_res)

#%% Calculate average number of temperature setpoint changes per month
print("average number of temperature setpoint changes per month in each apartment")
for list_room in list_apt:
    total_count = 0
    for i in list_room:
        df = df_all_rooms_SP[i-1].copy()
        df2 = df.copy()
        df2['DateTime'] = df2['DateTime'] + timedelta(minutes=5)
        df2.rename(columns={'setpoint': 'setpoint2'}, inplace=True)
        df = pd.merge(df, df2, on='DateTime', how='inner')
        df['diff_SP'] = abs(df['setpoint'] - df['setpoint2']) > 0.1
        df.dropna()
        count = (df['diff_SP'].sum()/(len(df['diff_SP'])*5*60))*30.5*24*60*60
        total_count = total_count + count
    
    print(total_count)


#%%
##############################################################################
### ELECTRICITY USE KPIs AND OC KPIs ###
##############################################################################

#%% Compute KPI 1: monthly average el use

print("monthly el use: kWh/month")

for i, df in enumerate(dfs_el_merged):
    KPI_1_monthly_average_el_use = (df['eluse'].mean())*30.5*24
    print(KPI_1_monthly_average_el_use)

#%% Compute KPI 2: monthly average el use per m2

print("monthly el use per m2: kWh/m2")

list_surface_area = [55, 72, 55, 72, 72]

for i, df in enumerate(dfs_el_merged):
    KPI_2_monthly_average_el_use = (df['eluse'].mean())*30.5*24/list_surface_area[i]
    print(KPI_2_monthly_average_el_use)

#%% Compute OC-KPI 1: monthly el use during occupied hours

print("OC KPI 1 monthly el use during occpied hours: kWh/hour_occ")

for i, df in enumerate(dfs_el_merged):
    df['el_occ'] = df['eluse'] * df['occupancy_ground_truth']
    OC_KPI_1_monthly_average_el_use_during_occ_h = (df['el_occ'].mean())*30.5*24
    print(OC_KPI_1_monthly_average_el_use_during_occ_h)
    
#%% Compute OC-KPI: monthly el use during non-occupied hours

print("OC KPI monthly el use during non-occpied hours: kWh/hour_non_occ")

for i, df in enumerate(dfs_el_merged):
    df['el_non_occ'] = df['eluse'] * (1-df['occupancy_ground_truth'])
    OC_KPI = (df['el_non_occ'].mean())*30.5*24
    print(OC_KPI)

#%% Compute OC-KPI 2a: Monthly average el use per m2 during occupied hours

print("OC KPI 3: monthly average el use per m2 during occupied hours: kWh/m2 per hour occ")

for i, df in enumerate(dfs_el_merged):
    df['el_occ'] = df['eluse'] * df['occupancy_ground_truth']
    OC_KPI_2_monthly_average_el_use_during_occ_h = (df['el_occ'].mean())*30.5*24/list_surface_area[i]
    print(OC_KPI_2_monthly_average_el_use_during_occ_h)

#%% Compute OC-KPI 3b: Monthly average el use per m2 during non-occupied hours

print("OC KPI 3b: monthly average el use per m2 during non-occupied hours: kWh/m2 per hour non-occ")

for i, df in enumerate(dfs_el_merged):
    df['el_non_occ'] = df['eluse'] * (1-df['occupancy_ground_truth'])
    OC_KPI_2b_monthly_average_el_use_during_non_occ_h = (df['el_non_occ'].mean())*30.5*24/list_surface_area[i]
    print(OC_KPI_2b_monthly_average_el_use_during_non_occ_h)

#%% Compute OC-KPI: Share of el use during occupied hours

print("OC KPI: Share of el use during occupied hours")

for i, df in enumerate(dfs_el_merged):
    tot_el_use = df['eluse'].sum()
    df['el_occ'] = df['eluse'] * df['occupancy_ground_truth']
    tot_el_occ = df['el_occ'].sum()
    share_el_use_occ_hours = tot_el_occ*100 / tot_el_use
    print(share_el_use_occ_hours/100)

#%% Compute OC-KPI: Share of el use during non-occupied hours

print("OC KPI: Share of el use during non-occupied hours")

for i, df in enumerate(dfs_el_merged):
    tot_el_use = df['eluse'].sum()
    df['el_non_occ'] = df['eluse'] * (1-df['occupancy_ground_truth'])
    tot_el_non_occ = df['el_non_occ'].sum()
    share_el_use_non_occ_hours = tot_el_non_occ*100 / tot_el_use
    print(share_el_use_non_occ_hours/100)

#%% OC KPI: total el use/ total occupied hours

print("Total el use per total occupied hours: kWh/hour_occ")

for i, df in enumerate(dfs_el_merged):
    total_eluse_use = df['eluse'].sum()
    total_occupied_hours = df['occupancy_ground_truth'].sum()
    kpi = total_eluse_use/total_occupied_hours
    print(kpi)

#%% OC KPI: total el use / total occupied hours per m2

print("Total el use per total occupied hours per m2: kWh/hour_occ per m2")

for i, df in enumerate(dfs_el_merged):
    total_eluse_use = df['eluse'].sum()
    total_occupied_hours = df['occupancy_ground_truth'].sum()
    kpi = total_eluse_use/(total_occupied_hours*list_surface_area[i])
    print(kpi)

#%% Get new df with solar production

dfs_el_solar_merged = list_dfs.copy()

for i, df in enumerate(dfs_el_solar_merged):
    df = df[['DateTime', 'occupancy_ground_truth', 'eluse', 'sum_el_production']].dropna()
    dfs_el_solar_merged[i] = df

#%% non-OC KPI: Load matching (PV prod, el use)

print("non-OC KPI load matching for each apartment")

for i, df in enumerate(dfs_el_solar_merged):
    # Calculate the net load (consumption - production) for each time interval
    net_load = df['eluse'] - (df['sum_el_production'] / 24)  # 24 apartments in total
    
    # Determine the percentage of time intervals with a match or surplus
    matched_intervals = net_load[net_load <= 0].count()
    total_intervals = net_load.count()
    load_match_percentage = (matched_intervals / total_intervals)
    
    print(load_match_percentage)

#%% OC KPI: Load matching during occupancy hours (PV prod, el use)

print("OC KPI load matching during occupancy hours only")

for i, df in enumerate(dfs_el_solar_merged):
    subdf = df[df['occupancy_ground_truth'] == 1]
    # Calculate the net load (consumption - production) for each time interval
    net_load = subdf['eluse'] - (subdf['sum_el_production'] / 24)  # 24 apartments in total
    
    # Determine the percentage of time intervals with a match or surplus
    matched_intervals = net_load[net_load <= 0].count()
    total_intervals = net_load.count()
    load_match_percentage = (matched_intervals / total_intervals)
    
    print(load_match_percentage)

#%% OC KPI: Load matching during non-occupancy hours (PV prod, el use)

print("OC KPI load matching during non-occupancy hours only")

for i, df in enumerate(dfs_el_solar_merged):
    subdf = df[df['occupancy_ground_truth'] == 0]
    # Calculate the net load (consumption - production) for each time interval
    net_load = subdf['eluse'] - (subdf['sum_el_production'] / 24)  # 24 apartments in total
    
    # Determine the percentage of time intervals with a match or surplus
    matched_intervals = net_load[net_load <= 0].count()
    total_intervals = net_load.count()
    load_match_percentage = (matched_intervals / total_intervals)
    
    print(load_match_percentage)
    
#%%
##############################################################################
### HOT AND COLD WATER USE KPIs AND OC KPIs ###
##############################################################################

#%% Compute KPI 1: monthly average water use

# hotw
print("monthly hot water use: liter/month")
for i, df in enumerate(dfs_hotw_merged):
    KPI_1_monthly_average_hotw_use = (df['hotw'].mean())*30.5*24
    print(KPI_1_monthly_average_hotw_use)

# coldw
print("monthly cold water use: liter/month")
for i, df in enumerate(dfs_coldw_merged):
    KPI_1_monthly_average_coldw_use = (df['coldw'].mean())*30.5*24
    print(KPI_1_monthly_average_coldw_use)

#%% Compute OC-KPI 1: monthly water use during occupied hours

# hotw
print("OC KPI 1 monthly hotw use during occpied hours: liter/hour_occ")
for i, df in enumerate(dfs_hotw_merged):
    df['hotw_occ'] = df['hotw'] * df['dominant_presence']
    OC_KPI_1_monthly_average_hotw_use_during_occ_h = (df['hotw_occ'].mean())*30.5*24
    print(OC_KPI_1_monthly_average_hotw_use_during_occ_h)

# coldw
print("OC KPI 1 monthly coldw use during occpied hours: liter/hour_occ")
for i, df in enumerate(dfs_coldw_merged):
    df['coldw_occ'] = df['coldw'] * df['dominant_presence']
    OC_KPI_1_monthly_average_coldw_use_during_occ_h = (df['coldw_occ'].mean())*30.5*24
    print(OC_KPI_1_monthly_average_coldw_use_during_occ_h)
    
#%% Compute OC-KPI: monthly water use during non-occupied hours

#hotw
print("OC KPI monthly hotw use during non-occpied hours: liter/hour_non_occ")
for i, df in enumerate(dfs_hotw_merged):
    df['hotw_non_occ'] = df['hotw'] * (1-df['dominant_presence'])
    OC_KPI = (df['hotw_non_occ'].mean())*30.5*24
    print(OC_KPI)

# coldw
print("OC KPI monthly coldw use during non-occpied hours: liter/hour_non_occ")
for i, df in enumerate(dfs_coldw_merged):
    df['coldw_non_occ'] = df['coldw'] * (1-df['dominant_presence'])
    OC_KPI = (df['coldw_non_occ'].mean())*30.5*24
    print(OC_KPI)

#%% non-OC KPI monthly water use per month per m2

list_surface_area = [55, 72, 55, 72, 72]

# hotw
print("monthly hot water use: liter/month")
for i, df in enumerate(dfs_hotw_merged):
    KPI_1_monthly_average_hotw_use = (df['hotw'].mean())*30.5*24/list_surface_area[i]
    print(KPI_1_monthly_average_hotw_use)

# coldw
print("monthly cold water use: liter/month")
for i, df in enumerate(dfs_coldw_merged):
    KPI_1_monthly_average_coldw_use = (df['coldw'].mean())*30.5*24/list_surface_area[i]
    print(KPI_1_monthly_average_coldw_use)

#%% monthly water use during occupancy hours per m2

# hotw
print("OC KPI monthly hotw use during occpied hours per m2: liter/hour_occ per m2")
for i, df in enumerate(dfs_hotw_merged):
    df['hotw_occ'] = df['hotw'] * df['dominant_presence']
    KPI = (df['hotw_occ'].mean())*30.5*24/list_surface_area[i]
    print(KPI)

# coldw
print("OC KPI 1 monthly coldw use during occpied hours per m2: liter/hour_occ per m2")
for i, df in enumerate(dfs_coldw_merged):
    df['coldw_occ'] = df['coldw'] * df['dominant_presence']
    KPI = (df['coldw_occ'].mean())*30.5*24/list_surface_area[i]
    print(KPI)

#%% monthly water use during non-occupancy hours per m2

# hotw
print("OC KPI monthly hotw use during occpied hours per m2: liter/hour_occ per m2")
for i, df in enumerate(dfs_hotw_merged):
    df['hotw_occ'] = df['hotw'] * (1-df['dominant_presence'])
    KPI = (df['hotw_occ'].mean())*30.5*24/list_surface_area[i]
    print(KPI)

# coldw
print("OC KPI 1 monthly coldw use during occpied hours per m2: liter/hour_occ per m2")
for i, df in enumerate(dfs_coldw_merged):
    df['coldw_occ'] = df['coldw'] * (1-df['dominant_presence'])
    KPI = (df['coldw_occ'].mean())*30.5*24/list_surface_area[i]
    print(KPI)

#%% Share water usage during occupancy

# Hot water

print("OC KPI: Share of hot water use during occupied hours")

for i, df in enumerate(dfs_hotw_merged):
    tot_hotw_use = df['hotw'].sum()
    df['hotw_occ'] = df['hotw'] * df['dominant_presence']
    tot_hotw_occ = df['hotw_occ'].sum()
    share_hotw_use_occ_hours = tot_hotw_occ*100 / tot_hotw_use
    print(share_hotw_use_occ_hours/100)

# cold water

print("OC KPI: Share of cold water use during occupied hours")

for i, df in enumerate(dfs_coldw_merged):
    tot_coldw_use = df['coldw'].sum()
    df['coldw_occ'] = df['coldw'] * df['dominant_presence']
    tot_coldw_occ = df['coldw_occ'].sum()
    share_coldw_use_occ_hours = tot_coldw_occ*100 / tot_coldw_use
    print(share_coldw_use_occ_hours/100)

#%% Share water usage during non-occupancy

# Hot water

print("OC KPI: Share of hot water use during non-occupied hours")

for i, df in enumerate(dfs_hotw_merged):
    tot_hotw_use = df['hotw'].sum()
    df['hotw_occ'] = df['hotw'] * (1-df['dominant_presence'])
    tot_hotw_occ = df['hotw_occ'].sum()
    share_hotw_use_occ_hours = tot_hotw_occ*100 / tot_hotw_use
    print(share_hotw_use_occ_hours/100)

# cold water

print("OC KPI: Share of cold water use during non-occupied hours")

for i, df in enumerate(dfs_coldw_merged):
    tot_coldw_use = df['coldw'].sum()
    df['coldw_occ'] = df['coldw'] * (1-df['dominant_presence'])
    tot_coldw_occ = df['coldw_occ'].sum()
    share_coldw_use_occ_hours = tot_coldw_occ*100 / tot_coldw_use
    print(share_coldw_use_occ_hours/100)

#%% Total water use per occupied hours

# Hot water

print("Total hot water use per total occupied hours: L/hour_occ")

for i, df in enumerate(dfs_hotw_merged):
    total_hotw_use = df['hotw'].sum()
    total_occupied_hours = df['dominant_presence'].sum()
    kpi = total_hotw_use/total_occupied_hours
    print(kpi)
    
# cold water

print("Total cold water use per total occupied hours: L/hour_occ")

for i, df in enumerate(dfs_coldw_merged):
    total_coldw_use = df['coldw'].sum()
    total_occupied_hours = df['dominant_presence'].sum()
    kpi = total_coldw_use/total_occupied_hours
    print(kpi)

#%% Total water use per occupied hours per m2

# Hot water

print("Total hot water use per total occupied hours per m2: L/hour_occ per m2")

for i, df in enumerate(dfs_hotw_merged):
    total_hotw_use = df['hotw'].sum()
    total_occupied_hours = df['dominant_presence'].sum()
    kpi = total_hotw_use/(total_occupied_hours*list_surface_area[i])
    print(kpi)
    
# cold water

print("Total cold water use per total occupied hours per m2: L/hour_occ per m2")

for i, df in enumerate(dfs_coldw_merged):
    total_coldw_use = df['coldw'].sum()
    total_occupied_hours = df['dominant_presence'].sum()
    kpi = total_coldw_use/(total_occupied_hours*list_surface_area[i])
    print(kpi)

#%%
##############################################################################
### IEQ KPIs AND OC KPIs ###
##############################################################################

#%% Add new columns for clo and met depending on time of the day

def met_function(t):
    if t > 7 and t < 23:
        met = 1.2
    else:
        met = 0.8
    
    return met

def clo_function(t):
    if t > 7 and t < 23:
        clo = 0.57
    else:
        clo = 0.93
        
    return clo

for i, df in enumerate(list_dfs_all_rooms):
    df['time_of_the_day'] = df['DateTime'].dt.hour
    df['met'] = df['time_of_the_day'].apply(met_function)
    df['clo'] = df['time_of_the_day'].apply(clo_function)

#%%

# with several mets and clo

def met_function(t, scenario):
    met_values = {
        1: (1.2, 0.8),   # Example scenario 1
        2: (1.0, 0.7),   # Example scenario 2
        3: (1.4, 0.6),   # Example scenario 3
        4: (1.3, 0.9),   # Example scenario 4
        5: (1.1, 0.5)    # Example scenario 5
    }
    met_day, met_night = met_values[scenario]
    if 7 < t < 23:
        return met_day
    else:
        return met_night

def clo_function(t, scenario):
    clo_values = {
        1: (0.57, 0.93),
        2: (0.50, 0.95),
        3: (0.60, 0.90),
        4: (0.55, 0.85),
        5: (0.65, 0.80)
    }
    clo_day, clo_night = clo_values[scenario]
    if 7 < t < 23:
        return clo_day
    else:
        return clo_night

scenarios = {0: 1, 1: 2, 2: 3, 3: 4, 4: 5}  # Map index to a scenario

for i, df in enumerate(list_dfs_all_rooms):
    scenario = scenarios.get(i, 1)  # Default to scenario 1 if not mapped
    df['time_of_the_day'] = df['DateTime'].dt.hour
    df['met'] = df['time_of_the_day'].apply(lambda x: met_function(x, scenario))
    df['clo'] = df['time_of_the_day'].apply(lambda x: clo_function(x, scenario))

#%% Average operative room temperature

for i, df in enumerate(list_dfs_all_rooms):
    temp = df['air_temperature'].mean()
    print(temp)

#%% KPI average PMV
v = 0.1

for i, df in enumerate(list_dfs_all_rooms):
    tdb = np.array(df['air_temperature'])
    tr = np.array(df['air_temperature'])
    rh = np.array(df['relative_humidity'])
    met = np.array(df['met'])
    clo = np.array(df['clo'])
    
    # calculate relative air speed
    v_r = v_relative(v=v, met=met)
    
    # calculate dynamic clothing
    clo_d = clo_dynamic(clo=clo, met=met)
    results = pmv(tdb=tdb, tr=tr, vr=v_r, rh=rh, met=met, clo=clo_d, standard='ISO', units='SI', limit_inputs=False)
    
    df['PMV'] = results
    list_dfs_all_rooms[i] = df
    avrg_PMV = df['PMV'].mean()
    print(avrg_PMV)

#%% PMV during occupancy only

for i, df in enumerate(list_dfs_all_rooms):
    sub_df = df[df['predicted_labels_newdata']==1]
    kpi = sub_df['PMV'].mean()
    print(kpi)

#%% PMV during occupancy only

for i, df in enumerate(list_dfs_all_rooms):
    sub_df = df[df['predicted_labels_newdata']==1]
    kpi = sub_df['PMV'].mean()
    print(kpi)

#%% PMV during daytime only

for i, df in enumerate(list_dfs_all_rooms):
    sub_df = df[df['clo']==0.57]
    kpi = sub_df['PMV'].mean()
    print(kpi)

#%% PMV during nighttime only

for i, df in enumerate(list_dfs_all_rooms):
    sub_df = df[df['clo']==0.93]
    kpi = sub_df['PMV'].mean()
    print(kpi)

#%% PMV during occupancy during daytime only

for i, df in enumerate(list_dfs_all_rooms):
    sub_df = df[(df['clo']==0.57) & (df['predicted_labels_newdata']==1)]
    kpi = sub_df['PMV'].mean()
    print(kpi)

#%% PMV during occupancy during nighttime only

for i, df in enumerate(list_dfs_all_rooms):
    sub_df = df[(df['clo']==0.93) & (df['predicted_labels_newdata']==1)]
    kpi = sub_df['PMV'].mean()
    print(kpi)

#%% Average temperature during occupancy

for i, df in enumerate(list_dfs_all_rooms):
    sub_df = df[df['predicted_labels_newdata']==1]
    temp = sub_df['air_temperature'].mean()
    print(temp)

#%% Merge SP dfs with IEQ dfs

list_dfs_SP_IEQ = []

for i, df in enumerate(list_dfs_all_rooms):
    merged_df = pd.merge(df, df_all_rooms_SP[i], on='DateTime', how='inner')
    # rename the SP column
    list_column = list(df_all_rooms_SP[i].columns)
    old_name = list_column[1]
    merged_df = merged_df.rename(columns={old_name: 'setpoint'})
    list_dfs_SP_IEQ.append(merged_df)

#%% Melgaard's score function def

def melgaard_score (dT):
    a = 0.5  # K
    if dT < -a:
        score = 100*np.exp(-0.03353*((dT+a)**4)-0.2179*((dT+a)**2))
    elif dT > a:
        score = 100*np.exp(-0.03353*((dT-a)**4)-0.2179*((dT-a)**2))
    else:
        score = 100
       
    return score

#%% Calculate Melgaard score

for i, df in enumerate(list_dfs_SP_IEQ):
    df['dT'] = df['air_temperature']-df['setpoint']
    df['Melgaard_score'] = df['dT'].apply(melgaard_score)
    kpi = df['Melgaard_score'].mean()
    print(kpi)

#%% Calculate Melgaard score during occupancy

for i, df in enumerate(list_dfs_SP_IEQ):
    sub_df = df[df['predicted_labels_newdata'] == 1]
    kpi = sub_df['Melgaard_score'].mean()
    print(kpi)

#%% Calculate Melgaard score during occupancy and day

for i, df in enumerate(list_dfs_SP_IEQ):
    sub_df = df[(df['predicted_labels_newdata'] == 1) & (df['clo'] == 0.57)]
    kpi = sub_df['Melgaard_score'].mean()
    print(kpi)

#%% Calculate Melgaard score during occupancy and night

for i, df in enumerate(list_dfs_SP_IEQ):
    sub_df = df[(df['predicted_labels_newdata'] == 1) & (df['clo'] == 0.93)]
    kpi = sub_df['Melgaard_score'].mean()
    print(kpi)

#%% Calculate Melgaard score during day

for i, df in enumerate(list_dfs_SP_IEQ):
    sub_df = df[df['clo'] == 0.57]
    kpi = sub_df['Melgaard_score'].mean()
    print(kpi)

#%% Calculate Melgaard score during night

for i, df in enumerate(list_dfs_SP_IEQ):
    sub_df = df[df['clo'] == 0.93]
    kpi = sub_df['Melgaard_score'].mean()
    print(kpi)

#%%
##############################################################################
### END SCRIPT ###
##############################################################################