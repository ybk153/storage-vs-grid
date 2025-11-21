import os
import time
import pandas as pd
import numpy as np

from pyomo.environ import SolverFactory
from .model import create_model
from .report import *
from .plot import *
from .input import *
from .validation import *
from .saveload import *
from .features import *
from .scenarios import *
import os
import multiprocessing as mp
import random
import numpy as np
import pprint


# run_worker function removed - this function was used for parallel processing of optimization tasks (uncoordinated).
# removed def run_worker(cluster, func_args, lp, xls, i, result_dir, return_dict, react=False, prob_bui=None, prob_grid_plan=None):

def prepare_result_directory(result_name):
    """ create a time stamped directory within the result folder.
    e.g. folder 20250723T1543-run.py
    Args:
        result_name: user specified result name

    Returns:
        a subfolder in the result folder 
    
    """
    # timestamp for result directory
    now = datetime.now().strftime('%Y%m%dT%H%M')

    # create result directory if not existent
    result_dir = os.path.join('result', '{}-{}'.format(now, result_name))
    if not os.path.exists(result_dir):
        os.makedirs(result_dir)

    return result_dir
# returns the result directory path

def setup_solver(optim, logfile='solver.log'):
    """ """
    if optim.name == 'gurobi':
        # reference with list of option names
        # http://www.gurobi.com/documentation/5.6/reference-manual/parameters
        optim.set_options("logfile={}".format(logfile))
        # optim.set_options("NumericFocus=3")
        # optim.set_options("Crossover=0")
        # optim.set_options("Method=1") # ohne method: concurrent optimization. Method=1 -> dual simplex
        optim.set_options("MIPFocus=1")  #
        optim.set_options("BarConvTol=1e-4")
        optim.set_options("FeasibilityTol=1e-2")
        optim.set_options("OptimalityTol=1e-2")
        optim.set_options("Threads=32")
        # optim.set_options("NodeMethod=2")
        # optim.set_options("Crossover=2")
        # optim.set_options("Presolve=0")
        # optim.set_options("timelimit=7200")  # seconds
        optim.set_options("MIPGap=1e-2")  # default = 1e-4
    elif optim.name == 'glpk':
        # reference with list of options
        # execute 'glpsol --help'
        optim.set_options("log={}".format(logfile))
        # optim.set_options("tmlim=7200")  # seconds
        # optim.set_options("mipgap=.0005")
    elif optim.name == 'cplexdirect' or optim.name == 'cplex_direct':
        pass
        # optim.options['threads'] = 32
        # optim.options['mip display'] = 5
        # optim.options['log'] = "={}".format(logfile)
    else:
        print("Warning from setup_solver: no options set for solver "
              "'{}'!".format(optim.name))
    return optim

def setup_solver_mip(optim, logfile='solver.log', precision='low', clusters=None, **gurobiparams):
    if optim.name == 'gurobi':
        # reference with list of option names
        # http://www.gurobi.com/documentation/5.6/reference-manual/parameters
        optim.set_options("logfile={}".format(logfile))
        # optim.set_options("NumericFocus=3")
        # optim.set_options("Crossover=0")
        optim.set_options("Method=3") # ohne method: concurrent optimization. Method=1 -> dual simplex
        optim.set_options("MIPFocus=1")  #
        for (key, value) in gurobiparams.items():
            optim.set_options(f"{key}={value}")
        if precision == 'high':
            optim.set_options("BarConvTol=1e-10")
            optim.set_options("FeasibilityTol=1e-9")
            optim.set_options("OptimalityTol=1e-9")
        if clusters is None:
            pass

    if optim.name == 'cplexdirect' or optim.name == 'cplex_direct':
        optim.options['threads'] = 32
        if "MIPGap" in gurobiparams.keys():
            optim.options['mip_tolerances_mipgap'] = gurobiparams['MIPGap']
        # optim.options['epgap'] = 0.01
    # reference with list of options
    # execute 'glpsol --help'
    return optim
# return optimizer

def run_lvds_opt(input_files, solver_name, timesteps, scenario, result_dir, dt,
                          objective,
                          report_tuples=None,
                          report_sites_name=None,
                          cross_scenario_data=None,
                          noTypicalPeriods=None,
                          hoursPerPeriod=None,
                          flexible=False,
                          lp=True,
                          xls=True,
                          assumelowq=True,
                          electrification=1,
                          bev_ratio=1,
                          hp_ratio=1,
                          pv_ratio=1,
                          vartariff=0,): # grid_curtailment, grid_op, parallel removed

    """ run an urbs model for given input, time steps and scenario

    Args:
        - input_files: filenames of input Excel spreadsheets
        - Solver: the user specified solver
        - timesteps: a list of timesteps, e.g. range(0,8761)
        - scenario: a scenario function that modifies the input data dict
        - result_dir: directory name for result spreadsheet and plots
        - dt: length of each time step (unit: hours)
        - objective: objective function chosen (either "cost" or "CO2")
        - plot_tuples: (optional) list of plot tuples (c.f. urbs.result_figures)
        - plot_sites_name: (optional) dict of names for sites in plot_tuples
        - plot_periods: (optional) dict of plot periods
          (c.f. urbs.result_figures)
        - report_tuples: (optional) list of (sit, com) tuples
          (c.f. urbs.report)
        - report_sites_name: (optional) dict of names for sites in
          report_tuples

    Returns:
        the urbs model instance
        :param assumelowq:
    """

    # Initial Setup and Data Reading
    year = date.today().year
    sce = scenario.__name__
    data = read_input(input_files, year)
    data, cross_scenario_data = scenario(data, cross_scenario_data)     # apply scenario function to modify the data
    validate_input(data)
    validate_dc_objective(data, objective)                              # relevant for CO2 objective only

    # Removed clusters list processing - not needed for coordinated optimization

    # identify the mode of the model
    mode = identify_mode(data)

    #create labels for created output files
    coordination_text, flexible_text, regulation_text, capacity_price_text = create_xls_file_labels(flexible, mode, data) # Removed uncoordinated parameter

    # adjustments in case flexibilities are deactivated (Global sheet)
    if not flexible:
        # delete all storages and flexibility
        data = remove_battery(data)
        data = remove_heat_storage(data)

        # 14a mode removed - mobility flexibility can now be removed unconditionally
        data = remove_mob_flexibility(data)
        mode['sto'] = False

    # identify the transformer and the main busbar nodes (transformer has the import process, main busbar the Q_comp)
    data['trafo_node'] = data['process'].query("Process == 'import'").index.get_level_values(1)[0]
    data['mainbusbar_node'] = data['process'].query("Process == 'Q_feeder_central'").index.get_level_values(1)[0]

    # if distribution network has to be modeled with reactive power
    if mode['acpf']:
        add_reactive_transmission_lines(data)

    add_reactive_output_ratios(data)

    # add curtailment option for heat (see lvdshelper.py).
    # acts to "ventilate" the building, in case the internal or solar gains heat it beyond the T_max
    add_curtailment_heat(data)

    # 14a mode removed - 14a commodity functionality no longer needed

    # 14a mode removed - hp_bev_flows no longer needed

    if mode['tsam']:
        # run timeseries aggregation method before creating model
        data, timesteps, weighting_order, cross_scenario_data = run_tsam(data, noTypicalPeriods,
                                                                         hoursPerPeriod,
                                                                         cross_scenario_data,
                                                                         mode['tsam_season'])
                                                                         # UHP parameter removed
        
        # Save TSAM results to Excel files in result directory
        print("Saving TSAM aggregated data to Excel files...")
        
        # Save aggregated demand data
        if 'demand' in data and not data['demand'].empty:
            demand_file = os.path.join(result_dir, 'tsam_aggregated_demand.xlsx')
            data['demand'].to_excel(demand_file)
            print(f"✅ Saved aggregated demand data: {demand_file}")
        
        # Save aggregated supply intermittency data
        if 'supim' in data and not data['supim'].empty:
            supim_file = os.path.join(result_dir, 'tsam_aggregated_supim.xlsx')
            data['supim'].to_excel(supim_file)
            print(f"✅ Saved aggregated supim data: {supim_file}")
        
        # Save aggregated efficiency factors
        if 'eff_factor' in data and not data['eff_factor'].empty:
            eff_file = os.path.join(result_dir, 'tsam_aggregated_eff_factor.xlsx')
            data['eff_factor'].to_excel(eff_file)
            print(f"✅ Saved aggregated eff_factor data: {eff_file}")
        
        # Save aggregated availability data
        if 'availability' in data and not data['availability'].empty:
            avail_file = os.path.join(result_dir, 'tsam_aggregated_availability.xlsx')
            data['availability'].to_excel(avail_file)
            print(f"✅ Saved aggregated availability data: {avail_file}")
        
        # Save aggregated buy/sell prices
        if 'buy_sell_price' in data and not data['buy_sell_price'].empty:
            price_file = os.path.join(result_dir, 'tsam_aggregated_buy_sell_price.xlsx')
            data['buy_sell_price'].to_excel(price_file)
            print(f"✅ Saved aggregated buy_sell_price data: {price_file}")
        
        # Save type period weights
        if 'type period' in data and not data['type period'].empty:
            weights_file = os.path.join(result_dir, 'tsam_type_period_weights.xlsx')
            data['type period'].to_excel(weights_file)
            print(f"✅ Saved type period weights: {weights_file}")
        
        # Save timesteps and weighting information
        tsam_info_file = os.path.join(result_dir, 'tsam_aggregation_info.xlsx')
        with pd.ExcelWriter(tsam_info_file) as writer:
            # Save timesteps
            timesteps_df = pd.DataFrame({'timesteps': list(timesteps)})
            timesteps_df.to_excel(writer, sheet_name='timesteps', index=False)
            
            # Save weighting order if available
            if weighting_order is not None:
                weighting_df = pd.DataFrame({'weighting_order': weighting_order})
                weighting_df.to_excel(writer, sheet_name='weighting_order', index=False)
            
            # Save cross scenario data if available
            if cross_scenario_data:
                for key, value in cross_scenario_data.items():
                    if isinstance(value, (list, np.ndarray)):
                        cross_df = pd.DataFrame({key: value})
                        # Truncate sheet name to 31 characters max
                        sheet_name = f'cross_{key}'[:31]
                        cross_df.to_excel(writer, sheet_name=sheet_name, index=False)
                    elif isinstance(value, (int, float, str)):
                        cross_df = pd.DataFrame({key: [value]})
                        # Truncate sheet name to 31 characters max
                        sheet_name = f'cross_{key}'[:31]
                        cross_df.to_excel(writer, sheet_name=sheet_name, index=False)
        
        print(f"✅ Saved TSAM aggregation info: {tsam_info_file}")
        print("📊 All TSAM aggregated data saved successfully!")
        
    else:
        # tsam disabled, just filter the time series according to the defined time steps
        weighting_order = None
        data['demand'] = data['demand'][data['demand'].index.get_level_values(1).isin(timesteps)]
        data['supim'] = data['supim'][data['supim'].index.get_level_values(1).isin(timesteps)]
        # Check if eff_factor has data and multiple index levels before filtering
        if not data['eff_factor'].empty and data['eff_factor'].index.nlevels > 1:
            data['eff_factor'] = data['eff_factor'][data['eff_factor'].index.get_level_values(1).isin(timesteps)]
        # Check if availability has data and multiple index levels before filtering
        if not data['availability'].empty and data['availability'].index.nlevels > 1:
            data['availability'] = data['availability'][data['availability'].index.get_level_values(1).isin(timesteps)]
        # Check if buy_sell_price has data and multiple index levels before filtering
        if not data['buy_sell_price'].empty and data['buy_sell_price'].index.nlevels > 1:
            data['buy_sell_price'] = data['buy_sell_price'][data['buy_sell_price'].index.get_level_values(1).isin(timesteps)]
        # UHP timestep filtering removed - functionality no longer needed

    # user can define a percentual electrification rate (0<= electrification <<1) that removes electrification measures
    # in random buildings
    # alternatively, adjust pv, hp, or bev penetration individually through the variables pv_ratio, bev_ratio, hp_ratio

    if electrification < 1 or pv_ratio < 1:
        data = remove_pv_in_random(data, electrification)

    if electrification < 1 or bev_ratio < 1:
        data = unelectrify_mobility_in_random(data, electrification)

    if electrification < 1 or hp_ratio < 1:
        data = unelectrify_heat_in_random(data, electrification)

    # allow participation to variable grid tariffs.  0<=vartariff<=1: share of prosumers which opt to variable tariffs.
    if vartariff > 0:
        random.seed(4)
        demand_nodes = set([sit for (sit, demand) in data['demand'].columns])
        vartariff_nodes = random.sample(demand_nodes, int(len(demand_nodes) * (vartariff)))
        data = adopt_variable_tariffs(data, vartariff_nodes)

    # if non-zero capacity prices (power_price_kw index in Global) are defined:
    # assign these for each site (power_price_kw column in Site) and adjust the import prices slightly to compensate
    if data['global_prop'].loc[pd.IndexSlice[:, 'power_price_kw'], 'value'].iloc[0] > 0:
        kwh_per_peakkw = 1000 # assumed average annual kWh consumption to peak kW ratio
        demand_nodes = [sit for (sit, demand) in data['demand'].columns if demand == 'space_heat']
        for building in demand_nodes:
            data['site'].loc[year, building]['power_price_kw'] = \
            data['global_prop'].loc[pd.IndexSlice[:, 'power_price_kw'], 'value'].iloc[0]
            
        # optional: adjust the import prices slightly to compensate (so that the total payments do not increase much)
        data['buy_sell_price']['electricity_import'] = data['buy_sell_price']['electricity_import'] - \
                                                       data['global_prop'].loc[
                                                           pd.IndexSlice[:, 'power_price_kw'], 'value'].iloc[
                                                           0] / kwh_per_peakkw

    # data is constructed finally, now to solve the HOODS-Sys problem
    prob = create_model(data,                               # data from input_path in main 
                        dt=dt,                              # from main
                        timesteps=timesteps,                # from main
                        objective='cost',                   # from main
                        weighting_order=weighting_order,    # from run_lvds_opt()/tsam
                        assumelowq=assumelowq,              # from main
                        hoursPerPeriod=hoursPerPeriod,      # from main
                        # grid_plan_model parameter removed - always use full co-optimization
                        dual=False)

    # write lp file # lp writing needs huge RAM capacities for bigger models
    if lp:
        prob.write('{}{}{}{}_step1.lp'.format(sce,
                                              coordination_text,
                                              flexible_text,
                                              regulation_text),
                   io_options={'symbolic_solver_labels': True})
    log_filename = os.path.join(result_dir, '{}.log').format(sce)
    optim = SolverFactory(solver_name)
    optim = setup_solver_mip(optim, logfile=log_filename, MIPGap=0.05, ConcurrentMIP=6, Threads=24)
    result = optim.solve(prob, tee=True, report_timing=True)

    # create h5 file label by using grid name, model type etc.
    # grid_text, paradigm_text, electrification_text = create_h5_file_labels(input_files, electrification) # lvdshelper.py

    # save results to h5/ HDF5 format for efficient storage
    save(prob, os.path.join(result_dir, '{}_step1.h5'.format(sce)), manyprob=False)
    # optionally create Excel reports with detailed results
    # Note: report function currently only supports Excel format, so skip if using .h5
    if xls:
        try:
            # Try to create Excel report (change .h5 to .xlsx for report)
            report_filename = os.path.join(result_dir, '{}_step1.xlsx'.format(sce))
            report(prob, report_filename,
                   report_tuples=report_tuples,             # year-site-commodity combinations to report
                   report_sites_name=report_sites_name)     # names for sites in report_tuples
        except Exception as e:
            print(f"Warning: Could not create Excel report: {e}")
            print("Main results are saved in the .h5 file")
               

        
    return prob