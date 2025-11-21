import os
import shutil
import urbs
from urbs.runfunctions import *
from multiprocessing import freeze_support
import pandas as pd
import glob
import pyomo.environ as pyomo


import warnings
if __name__ == '__main__':
    freeze_support()

    warnings.filterwarnings("ignore", category=RuntimeWarning)
    warnings.filterwarnings("ignore", category=UserWarning)

    input_files = '2025.xlsx'  # for single year file name, for intertemporal folder name
    input_dir = 'Input'
    input_path = os.path.join(input_dir, input_files)       # if single year file input_path = 'Input/2025.xlsx' 
    # if intertemporal folder name input_path = 'Input/intertemporal'

    result_name = os.path.basename(__file__)                 # returns run.py
    result_dir = urbs.prepare_result_directory(result_name)  # name + time stamp

    # #copy input file to result directory
    try:
        shutil.copytree(input_path, os.path.join(result_dir, input_dir))
    except NotADirectoryError:
        shutil.copyfile(input_path, os.path.join(result_dir, input_files))

    # #copy run file to result directory
    shutil.copy(__file__, result_dir)

    # objective function
    objective = 'cost'  # set either 'cost' or 'CO2' as objective

    # Choose Solver (cplex, glpk, gurobi, ...)
    solver = 'gurobi'

    # simulation timesteps
    (offset, length) = (0, 35136)  # time step selection
    #(offset, length) = (0, 2688)  # time step selection
    timesteps = range(offset, offset + length + 1)
    dt = 0.25  # length of each time step (unit: 15 minutes = 0.25 hours)

    # detailed reporting commodity/sites
    if os.path.isdir(input_path):
        glob_input = os.path.join(input_path, '*.xlsx')
        input_files = sorted(glob.glob(glob_input))     # e.g., 'Input/intertemporal/2025.xlsx', 'Input/intertemporal/2026.xlsx'
    else:
        input_files = [input_path]                      # 'Input/2025.xlsx'

    for filename in input_files:
        print("Reading for site names and mode")
        with pd.ExcelFile(filename) as xls:
            demand = xls.parse('Demand').set_index(['t'])
            print("Site reading complete")

            global_props       = xls.parse('Global').set_index('Property')
            tsam               = global_props.loc['tsam']['value']
            noTypicalPeriods   = int(global_props.loc['noTypicalPeriods']['value'])
            hoursPerPeriod     = int(global_props.loc['hoursPerPeriod']['value'])
            flexible           = global_props.loc['flexible']['value']
            lp                 = global_props.loc['lp']['value']
            excel              = global_props.loc['excel']['value']
            electrification    = global_props.loc['electrification']['value']
            vartariff          = global_props.loc['vartariff']['value']

    # rural 1.1 with 14 buses
    # sites = [f'Bus_{i}' for i in range(1, 15)]

    # ruralsemirurb with 43 buses
    sites = [f'Bus_{i}' for i in range(1, 44)]

    # urban with 58 buses
    # sites = [f'Bus_{i}' for i in range(1, 59)]

    commodities = ['electricity', 'electricity-reactive']
    
    # optional: define names for sites in report_tuples
    report_tuples = [(2025, site, com) for site in sites for com in commodities]
    report_sites_name = {site: site for site in sites}

    
    # plotting commodities/sites
    #plot_tuples = []
    # optional: define names for sites in plot_tuples
    #plot_sites_name = {}

    # plotting timesteps
    # plot_periods = {
    #     'all': timesteps[1:]
    # }
    # time_series_for_aggregation = {'demand': ['electricity', ']}
    # select scenarios to be run
    scenarios = [
        flex_all
    ]

    cross_scenario_data = dict()

    # Only coordinated optimization is supported

    def export_variables_to_excel(model, result_dir):
        """Export all non-zero variables to one Excel file with multiple sheets"""
        
        excel_path = os.path.join(result_dir, 'all_variables.xlsx')
        
        with pd.ExcelWriter(excel_path, engine='openpyxl') as writer:
            sheet_count = 0
            
            for var_component in model.component_objects(pyomo.Var):
                var_name = var_component.name
                
                # Collect non-zero values
                data = []
                for key in var_component:
                    val = var_component[key].value
                    if val is not None and abs(val) > 1e-6:
                        if isinstance(key, tuple):
                            data.append(list(key) + [val])
                        else:
                            data.append([key, val])
                
                if data:
                    # Create DataFrame
                    if isinstance(list(var_component.keys())[0], tuple):
                        key_len = len(list(var_component.keys())[0])
                        columns = [f'key_{i}' for i in range(key_len)] + ['value']
                    else:
                        columns = ['key', 'value']
                    
                    df = pd.DataFrame(data, columns=columns)
                    
                    # Excel sheet names have limitations (max 31 chars, no special chars)
                    sheet_name = var_name[:31].replace('/', '_').replace('\\', '_').replace(':', '_')
                    
                    # Save to Excel sheet
                    df.to_excel(writer, sheet_name=sheet_name, index=False)
                    sheet_count += 1
                    print(f"Added {len(data)} {var_name} variables to sheet '{sheet_name}'")
            
            print(f"\n✅ Created Excel file: {excel_path}")
            print(f"📊 Total sheets: {sheet_count}")
            print(f"📁 File size: {os.path.getsize(excel_path) / 1024:.1f} KB")

    def export_model_components_to_text(model, result_dir):
        """Export all model components (sets, parameters, variables, constraints) to text files"""
        
        # 1. Export Sets to text file
        sets_file = os.path.join(result_dir, 'model_sets.txt')
        with open(sets_file, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("MODEL SETS\n")
            f.write("=" * 60 + "\n\n")
            
            sets_count = 0
            for set_component in model.component_objects(pyomo.Set):
                set_name = set_component.name
                f.write(f"Set: {set_name}\n")
                f.write("-" * 40 + "\n")
                
                try:
                    if set_component.is_indexed():
                        for key in set_component:
                            f.write(f"  Index [{key}]: {list(set_component[key])}\n")
                            sets_count += len(set_component[key])
                    else:
                        elements = list(set_component)
                        f.write(f"  Elements: {elements}\n")
                        sets_count += len(elements)
                except Exception as e:
                    f.write(f"  ERROR: Could not read set - {e}\n")
                
                f.write("\n")
            
            f.write(f"\nTotal set elements: {sets_count}\n")
        
        # 2. Export Parameters to text file
        params_file = os.path.join(result_dir, 'model_parameters.txt')
        with open(params_file, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("MODEL PARAMETERS\n")
            f.write("=" * 60 + "\n\n")
            
            params_count = 0
            for param_component in model.component_objects(pyomo.Param):
                param_name = param_component.name
                f.write(f"Parameter: {param_name}\n")
                f.write("-" * 40 + "\n")
                
                try:
                    for key in param_component:
                        val = param_component[key].value if hasattr(param_component[key], 'value') else param_component[key]
                        f.write(f"  {key}: {val}\n")
                        params_count += 1
                except Exception as e:
                    f.write(f"  ERROR: Could not read parameter - {e}\n")
                
                f.write("\n")
            
            f.write(f"\nTotal parameters: {params_count}\n")
        
        # 3. Export Variables to text file
        vars_file = os.path.join(result_dir, 'model_variables.txt')
        with open(vars_file, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("MODEL VARIABLES\n")
            f.write("=" * 60 + "\n\n")
            
            vars_count = 0
            nonzero_count = 0
            for var_component in model.component_objects(pyomo.Var):
                var_name = var_component.name
                f.write(f"Variable: {var_name}\n")
                f.write("-" * 40 + "\n")
                
                try:
                    for key in var_component:
                        val = var_component[key].value
                        lower = var_component[key].lb
                        upper = var_component[key].ub
                        
                        if val is not None and abs(val) > 1e-6:
                            nonzero_count += 1
                            f.write(f"  {key}: value={val}, bounds=[{lower}, {upper}] *NON-ZERO*\n")
                        else:
                            f.write(f"  {key}: value={val}, bounds=[{lower}, {upper}]\n")
                        vars_count += 1
                except Exception as e:
                    f.write(f"  ERROR: Could not read variable - {e}\n")
                
                f.write("\n")
            
            f.write(f"\nTotal variables: {vars_count}\n")
            f.write(f"Non-zero variables: {nonzero_count}\n")
        
        # 4. Export Constraints to text file
        constraints_file = os.path.join(result_dir, 'model_constraints.txt')
        with open(constraints_file, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("MODEL CONSTRAINTS\n")
            f.write("=" * 60 + "\n\n")
            
            constraints_count = 0
            for con_component in model.component_objects(pyomo.Constraint):
                con_name = con_component.name
                f.write(f"Constraint: {con_name}\n")
                f.write("-" * 40 + "\n")
                
                try:
                    for key in con_component:
                        constraint = con_component[key]
                        
                        # Get constraint bounds and body
                        lower = constraint.lower
                        upper = constraint.upper
                        body = constraint.body
                        
                        # Try to evaluate body value
                        try:
                            body_value = pyomo.value(body) if body is not None else None
                        except:
                            body_value = "Cannot evaluate"
                        
                        f.write(f"  {key}: {lower} <= {body_value} <= {upper}\n")
                        constraints_count += 1
                except Exception as e:
                    f.write(f"  ERROR: Could not read constraint - {e}\n")
                
                f.write("\n")
            
            f.write(f"\nTotal constraints: {constraints_count}\n")
        
        # 5. Export Model Summary to text file
        summary_file = os.path.join(result_dir, 'model_summary.txt')
        with open(summary_file, 'w') as f:
            f.write("=" * 60 + "\n")
            f.write("MODEL SUMMARY\n")
            f.write("=" * 60 + "\n\n")
            
            total_sets = len(list(model.component_objects(pyomo.Set)))
            total_params = len(list(model.component_objects(pyomo.Param)))
            total_vars = len(list(model.component_objects(pyomo.Var)))
            total_constraints = len(list(model.component_objects(pyomo.Constraint)))
            
            # Count total elements
            total_var_elements = sum(len(list(var.keys())) for var in model.component_objects(pyomo.Var))
            total_con_elements = sum(len(list(con.keys())) for con in model.component_objects(pyomo.Constraint))
            
            f.write(f"Total Sets: {total_sets}\n")
            f.write(f"Total Parameters: {total_params}\n")
            f.write(f"Total Variables: {total_vars}\n")
            f.write(f"Total Constraints: {total_constraints}\n")
            f.write(f"Total Variable Elements: {total_var_elements}\n")
            f.write(f"Total Constraint Elements: {total_con_elements}\n\n")
            
            f.write("Set Names:\n")
            for s in model.component_objects(pyomo.Set):
                f.write(f"  - {s.name}\n")
            
            f.write("\nParameter Names:\n")
            for p in model.component_objects(pyomo.Param):
                f.write(f"  - {p.name}\n")
            
            f.write("\nVariable Names:\n")
            for v in model.component_objects(pyomo.Var):
                var_count = len(list(v.keys()))
                nonzero_count = sum(1 for k in v.keys() if v[k].value is not None and abs(v[k].value) > 1e-6)
                f.write(f"  - {v.name}: {var_count} total, {nonzero_count} non-zero\n")
            
            f.write("\nConstraint Names:\n")
            for c in model.component_objects(pyomo.Constraint):
                con_count = len(list(c.keys()))
                f.write(f"  - {c.name}: {con_count} constraints\n")
        
        print(f"\n✅ Created model component text files:")
        print(f"   📄 Sets: {sets_file}")
        print(f"   📄 Parameters: {params_file}")
        print(f"   📄 Variables: {vars_file}")
        print(f"   📄 Constraints: {constraints_file}")
        print(f"   📄 Summary: {summary_file}")
        
        print(f"\n📊 Model Summary:")
        print(f"   Total Sets: {total_sets}")
        print(f"   Total Parameters: {total_params}")
        print(f"   Total Variables: {total_vars}")
        print(f"   Total Constraints: {total_constraints}")
        print(f"   Total Variable Elements: {total_var_elements}")
        print(f"   Total Constraint Elements: {total_con_elements}")

    
    for scenario in scenarios:
            prob = run_lvds_opt(input_path, solver, timesteps,
                         scenario, result_dir, dt,
                         objective,
                         report_tuples=report_tuples,
                         cross_scenario_data=cross_scenario_data,
                         report_sites_name=report_sites_name,
                         noTypicalPeriods=noTypicalPeriods,
                         hoursPerPeriod=hoursPerPeriod,
                         flexible=1,
                         lp=lp,
                         xls=excel,
                         assumelowq=1,
                         electrification=electrification,
                         vartariff=vartariff,)
            export_variables_to_excel(prob, result_dir)
            
            # Save complete model structure to file (commented out due to pandas conflict)
            # model_file = os.path.join(result_dir, 'model_structure.txt')
            # with open(model_file, 'w') as f:
            #     prob.pprint(ostream=f)
            # print(f"✅ Model structure saved to: {model_file}")
            
            # Export model components to text files
            export_model_components_to_text(prob, result_dir)
            
            # Quick model summary to console
            print("\n" + "="*50)
            print("MODEL SUMMARY")
            print("="*50)
            print(f"Sets: {len(list(prob.component_objects(pyomo.Set)))}")
            print(f"Parameters: {len(list(prob.component_objects(pyomo.Param)))}")
            print(f"Variables: {len(list(prob.component_objects(pyomo.Var)))}")
            print(f"Constraints: {len(list(prob.component_objects(pyomo.Constraint)))}")
            
            # List constraint names
            print("\nConstraint Names:")
            for con in prob.component_objects(pyomo.Constraint):
                con_count = len(list(con.keys()))
                print(f"  {con.name}: {con_count} constraints")
            
            # List variable names
            print("\nVariable Names:")
            for var in prob.component_objects(pyomo.Var):
                var_count = len(list(var.keys()))
                nonzero_count = sum(1 for k in var.keys() if var[k].value is not None and abs(var[k].value) > 1e-6)
                print(f"  {var.name}: {var_count} total, {nonzero_count} non-zero")
            print("="*50)
            
       # prob, prob_grid_plan, prob_hp_react, cross_scenario_data = urbs.run_lvds_opt(input_path, solver, timesteps,