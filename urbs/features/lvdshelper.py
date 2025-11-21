from urbs.identify import *
import copy
import math
import numpy as np
import os
from datetime import datetime, date
import random

def remove_battery(data):
    # de-flexiblise electricity demand by disallowing battery investment
    data['storage'].loc[pd.IndexSlice[:, :, 'battery_private', :], ['inst-cap-p']] = 0
    data['storage'].loc[pd.IndexSlice[:, :, 'battery_private', :], ['inst-cap-c']] = 0
    data['storage'].loc[pd.IndexSlice[:, :, 'battery_private', :], ['cap-up-p']] = 0
    data['storage'].loc[pd.IndexSlice[:, :, 'battery_private', :], ['cap-up-c']] = 0
    return data

def remove_heat_storage(data):
    # de-flexiblise electricity demand by disallowing battery investment
    data['storage'].loc[pd.IndexSlice[:, :, 'heat_storage', :], ['cap-up-p']] = 0
    data['storage'].loc[pd.IndexSlice[:, :, 'heat_storage', :], ['cap-up-c']] = 0
    data['storage'].loc[pd.IndexSlice[:, :, 'heat_storage', :], ['inst-cap-p']] = 0
    data['storage'].loc[pd.IndexSlice[:, :, 'heat_storage', :], ['inst-cap-c']] = 0
    return data

def remove_mob_flexibility(data):
    # remove mobility flexibility
    for (stf, sit, sto, com) in data['storage'].index:
        if sto.startswith('mobility'):
            data['storage'].loc[(stf, sit, sto, com), 'inst-cap-p'] = 0
            data['storage'].loc[(stf, sit, sto, com), 'inst-cap-c'] = 0
            data['storage'].loc[(stf, sit, sto, com), 'cap-up-p'] = 0
            data['storage'].loc[(stf, sit, sto, com), 'cap-up-c'] = 0

    return data

def create_xls_file_labels(flexible, mode, data):
    coordination_text = '_coordinated'
    flexible_text = '_flexible' if flexible else '_inflexible'

    # 14a mode removed - no regulation text needed
    regulation_text = ''

    # Check if power_price_kw column exists and has non-NaN values
    power_price_series = data['site']['power_price_kw'].dropna()
    if len(power_price_series) > 0 and power_price_series.iloc[0] > 0.01:
        capacity_price_text = '_capacity_price'
    else:
        capacity_price_text = ''

    return coordination_text, flexible_text, regulation_text, capacity_price_text

def create_h5_file_labels(input_files, electrification):
    grid_text = input_files.split('_')[2]
    paradigm_text = input_files.split('_')[3][:-4]
    if electrification == 1:
        electrification_text = 'full'
    elif electrification == 0.5:
        electrification_text = 'half'
    elif electrification == 0.25:
        electrification_text = 'quarter'
    else:
        electrification_text = 'unknown'
    return grid_text, paradigm_text, electrification_text

# ---- 14a related functions
# removed def add_import_hp_bev_process(data):



# removed def add_electricity_hp_bev_commodity(data, comtype='Stock'):
    # add commodity "electricity_hp" to all sites


# removed def modify_hp_bev_processes(data):

# removed def add_elec_to_hp_bev_process(data):



def add_curtailment_heat(data):
    year = date.today().year

    # add process curtailment_heat to all buildings (same properties as curtailment)
    curtailment_heat_pro = data['process'][data['process'].index.get_level_values(2) == 'curtailment']
    curtailment_heat_pro = curtailment_heat_pro.copy(deep=True)
    curtailment_heat_pro.rename(index={'curtailment': 'curtailment_heat'}, level=2, inplace=True)
    data['process'] = pd.concat([data['process'], curtailment_heat_pro])
    data['process_commodity'].loc[year, 'curtailment_heat', 'space_heat', 'In'] = (1, np.nan)

    return data

# 14a coordinated
# removed def add_hp_bev_flows(data):
    ### copy transmission lines with resistance to model transmission lines for reactive power flows



# Function removed as it was only needed for uncoordinated optimization
# def distributed_building_specific_import_processes(data, mode):

# electrification functions
def unelectrify_heat_in_random(data, electrification):
    year = date.today().year
    demand_nodes = [sit for (sit, demand) in data['demand'].columns if demand == 'space_heat']
    random.seed(1)
    unelectrified_heat_nodes = random.sample(demand_nodes, int(len(demand_nodes) * (1 - electrification)))
    data['process'].loc[(year, unelectrified_heat_nodes, 'heatpump_air'), 'cap-up'] = 0

    # instead of HP, provide heat by another means, e.g. gas boiler
    data['commodity'].loc[(year, unelectrified_heat_nodes, 'common_heat'), 'price'] = 0.12  # assumed gas price
    data['commodity'].loc[(year, unelectrified_heat_nodes, 'common_heat'), 'max'] = np.inf # allow gas supply
    data['commodity'].loc[(year, unelectrified_heat_nodes, 'common_heat'), 'maxperhour'] = np.inf # allow gas supply
    return data

def remove_pv_in_random(data, electrification):
    year = date.today().year
    demand_nodes = [sit for (sit, demand) in data['demand'].columns if demand == 'space_heat']
    random.seed(2)
    disable_pv_nodes = random.sample(demand_nodes, int(len(demand_nodes) * (1 - electrification)))

    data['process'].loc[((data['process'].index.get_level_values(1).isin(disable_pv_nodes)) &
                         (data['process'].index.get_level_values(2).str.startswith('Rooftop PV'))), 'cap-up'] = 0
    return data

def unelectrify_mobility_in_random(data, electrification):
    year = date.today().year
    demand_nodes = [sit for (sit, demand) in data['demand'].columns if demand == 'space_heat']
    random.seed(3)
    all_cars = [col for col in data['demand'].columns if col[1].startswith('mobility')]
    unelectrified_cars = random.sample(all_cars, int(len(all_cars) * (1 - electrification)))

    for (site, car) in unelectrified_cars:
        car_idx = car[-1]
        data['process'].loc[(year, site, 'charging_station' + car_idx), 'inst-cap'] = 0
        data['process'].loc[
            (year, site, 'charging_station' + car_idx), 'cap-up'] = 0  # set charging_station capacity to zero
        data['commodity'].loc[(year, site, 'mobility' + car_idx, 'Demand'), 'price'] = 0.6  # 60 cent for public charging
        data['commodity'].loc[(year, site, 'mobility' + car_idx, 'Demand'), 'max'] = np.inf
        data['commodity'].loc[(year, site, 'mobility' + car_idx,
                               'Demand'), 'maxperhour'] = np.inf  # add stock availabiltiy for mobility commodity
    return data



def adopt_variable_tariffs(data, vartariff_nodes):
    year = date.today().year
    demand_nodes = [sit for (sit, demand) in data['demand'].columns if demand == 'space_heat']

    # rename import to import_var process for those nodes
    for building in vartariff_nodes:
        import_pro = data['process'][
            data['process'].index.get_level_values(2) == 'import']
        import_pro = import_pro[import_pro.index.get_level_values(1) == building]
        import_pro = import_pro.copy(deep=True)
        import_pro.rename(index={'import': 'import_var'}, level=2, inplace=True)
        data['process'].drop((year, building, 'import'), inplace=True, axis=0)
        data['process'] = pd.concat([data['process'], import_pro])

    # rename commodities "electricity_import" to "electricity_import_var" for those nodes
    for building in vartariff_nodes:
        electricity_import_com = data['commodity'][
            data['commodity'].index.get_level_values(2) == 'electricity_import']
        electricity_import_com = electricity_import_com[electricity_import_com.index.get_level_values(1) == building]
        electricity_import_com = electricity_import_com.copy(deep=True)
        electricity_import_com.rename(index={'electricity_import': 'electricity_import_var'}, level=2, inplace=True)
        data['commodity'].drop((year, building, 'electricity_import', 'Buy'), inplace=True, axis=0)
        data['commodity'] = pd.concat([data['commodity'], electricity_import_com])

    # add process-commodity for import_var (same as import, just different import commodity with variable prices)
    import_var_pro_com = data['process_commodity'][
        data['process_commodity'].index.get_level_values(1) == 'import']
    import_var_pro_com = import_var_pro_com.copy(deep=True)
    import_var_pro_com.rename(index={'electricity_import': 'electricity_import_var'}, level=2, inplace=True)
    import_var_pro_com.rename(index={'import': 'import_var'}, level=1, inplace=True)
    data['process_commodity'] = pd.concat([data['process_commodity'], import_var_pro_com])
    return data


#removed def set_curtailment_limits(data_grid_plan): #uncoordinated
    # set limits to DSO-side grid curtailment using the "availability" functionality
    # (it cannot be greater than the feed-in (minus demand) at each hour!)



### START - HOODS-Grid routines
# uncoordinated
#removed def delete_solar_supim_timeseries(data_grid_plan, building):


#removed def delete_charging_station_eff_factors(data_grid_plan, building):


#removed def delete_heatpump_eff_factors(data_grid_plan, building):


#removed def delete_non_electric_demand(data_grid_plan, building):
    #delete heat demands


#removed def shift_demand_to_elec(data_grid_plan, building, prob_cluster, mode, vartariff, vartariff_nodes=None):


#removed def delete_processes_for_hoods_grid(data_grid_plan, building, grid_curtailment):
    ### delete all processes besides Q-compensation, import and feed-in


#removed def delete_commodities_for_hoods_grid(data_grid_plan, building):



#removed def delete_procoms_for_hoods_grid(data_grid_plan, grid_curtailment):





# removed def add_hp_bev_regulation_process(data_grid_plan, data_bui, var_cost=0):



### START - HOODS-Bui-React routines
# removed def limit_hp_for_hoods_bui_react(data_hp_react, data, building, prob_cluster, prob_grid_plan):
    # 14a regulation functionality removed - HP limitation based on 14a regulation no longer available
    # This function previously implemented capacity limitations for heat pumps based on 14a regulation processes
    # The regulation processes hp_14a_steuve_regulate have been removed, so these limitations are no longer active
    # The function now returns data unchanged to maintain compatibility
    return data_hp_react

# removed def limit_bev_for_hoods_bui_react(data_hp_react, data, building, prob_cluster, prob_grid_plan, data_grid_plan):
    # 14a regulation functionality removed - BEV limitation based on 14a regulation no longer available
    # This function previously implemented capacity limitations for battery electric vehicles based on 14a regulation processes
    # The regulation processes bev_14a_steuve_regulate have been removed, so these limitations are no longer active
    # The function now returns data unchanged to maintain compatibility
    return data_hp_react


### Model new imaginary lines to enable reactive power flow on respective lines with defined resistance
def add_reactive_transmission_lines(microgrid_data_input):
    ### copy transmission lines with resistance to model transmission lines for reactive power flows
    reactive_transmission_lines = microgrid_data_input['transmission'][
        microgrid_data_input['transmission'].index.get_level_values(4) == 'electricity'][
        ~microgrid_data_input['transmission'].loc[:, 'resistance'].isna()]
    reactive_transmission_lines = reactive_transmission_lines.copy(deep=True)
    reactive_transmission_lines.rename(index={'electricity': 'electricity-reactive'}, level=4, inplace=True)
    ### set costs to zero as lines are not really built -

    reactive_transmission_lines.loc[:, ['inv-cost', 'fix-cost', 'var-cost', 'decom-saving']] *= 0
    ### set tra-block to NaN for reactive lines as they shouldn't have block constraints
    #reactive_transmission_lines.loc[:, 'tra-block'] = float('nan')
    ### concat new line data
    microgrid_data_input['transmission'] = pd.concat(
        [microgrid_data_input['transmission'], reactive_transmission_lines], sort=True)
    return microgrid_data_input


### Implement reactive power outputs as commodity according to predefined power factors for processes
def add_reactive_output_ratios(microgrid_data_input):
    pro_Q = microgrid_data_input['process'][microgrid_data_input['process'].loc[:, 'pf-min'] > 0]
    ratios_elec = microgrid_data_input['process_commodity'].loc[pd.IndexSlice[:, :, 'electricity', 'Out'], :]
    for process_idx, process in pro_Q.iterrows():
        for ratio_P_idx, ratio_P in ratios_elec.iterrows():
            if process_idx[2] == ratio_P_idx[1]:
                ratio_Q = ratios_elec.loc[pd.IndexSlice[:, ratio_P_idx[1], 'electricity', 'Out'], :].copy(deep=True)
                ratio_Q.rename(index={'electricity': 'electricity-reactive'}, level=2, inplace=True)
                microgrid_data_input['process_commodity'] = pd.concat(
                    [microgrid_data_input['process_commodity'], ratio_Q])
                microgrid_data_input['process_commodity'] = microgrid_data_input['process_commodity'] \
                    [~microgrid_data_input['process_commodity'].index.duplicated(keep='first')]
    return microgrid_data_input

# transdist functions removed - no longer needed for simplified model

### Merge main data with microgrid data
#removed def concatenate_with_micros(data, microgrid_data):



### store additional demand in cross scenario data to be used in subsequent scenarios
# removed def store_additional_demand(cross_scenario_data, mobility_transmission_shift, heat_transmission_shift):
    ###transform dicts into dataframe and summarize timeseries for regions
