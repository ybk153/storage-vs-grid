import pandas as pd

def identify_mode(data):
    """ Identify the urbs mode that is needed for running the current Input

    Args:
        data: input data dictionary

    Features:
        Intertemporal, Transmission, Storage, Buy Sell (Price), Time
        Variable efficiency, Expansion (4 values for process, transmission,
        storage capacity and storage power expansion)

    Returns:
        mode dictionary; contains bool values that define the urbs mode
        m.mode['exp'] will be initialized with 'True' if the corresponing mode
        (e.g. transmission) is also enabled and later updated through
        identify_expansion(m)

    """

    # create modes
    mode = {
        'int': False,                   # intertemporal
        'tra': False,                   # transmission
        'sto': False,                   # storage
        # DSM mode removed - demand side management functionality no longer needed
        'bsp': False,                   # buy sell price
        'tve': False,                   # time variable efficiency
        'ava': False,
        'dcpf': False,                  # dc power flow
        'acpf': False,                  # ac power flow
        'tdy': False,                   # type periods
        'tsam': False,                  # time series aggregation method
        'tsam_season': False,
        'onoff': False,                 # on/off processes
        'minfraction': False,           # processes with minimum working load
        'exp': {                        # expansion
                'pro': True,
                'tra': False,
                'sto-c': False,
                'sto-p': False},
        'power_price': False,
        }
        # UHP mode removed - no longer needed
        # 14a mode removed - this functionality is no longer needed
        # evu_sperre mode removed

    # if number of support timeframes > 1
    if len(data['global_prop'].index.levels[0]) > 1:
        mode['int'] = True
    if not data['transmission'].empty:
        mode['tra'] = True
        mode['exp']['tra'] = True
    if not data['storage'].empty:
        mode['sto'] = True
        mode['exp']['sto-c'] = True
        mode['exp']['sto-p'] = True
    # DSM mode check removed - this functionality is no longer needed
    if not data['buy_sell_price'].empty:
        mode['bsp'] = True
    if not data['eff_factor'].empty:
        mode['tve'] = True
    if not data['availability'].empty:
        mode['ava'] = True
    if 'resistance' in data['transmission'].keys():
        if any(data['transmission']['resistance'] > 0):
            mode['acpf'] = True
    if 'reactance' in data['transmission'].keys():
        if any(data['transmission']['reactance'] > 0):
            mode['dcpf'] = True
    if any(data['type period']['weight_typeperiod'] > 0):
        mode['tdy'] = True
    if data['global_prop'].loc[pd.IndexSlice[:,'tsam'],'value'].iloc[0]:
        mode['tsam'] = True
    if data['global_prop'].loc[pd.IndexSlice[:,'tsam_season'],'value'].iloc[0]:
        mode['tsam_season'] = True
    if 'on-off' in data['process'].keys():
        if any(data['process']['on-off'] == 1):
            mode['onoff'] = True
    if 'min-fraction' in data['process'].keys():
        if any(data['process']['min-fraction'] > 0):
            mode['minfraction'] = True
    if len(data['site'][data['site']['power_price_kw'] > 0]):
        mode['power_price'] = True
    # Coordinated optimization only

    # 14a mode check removed - this functionality is no longer needed

    # UHP mode check removed - this functionality is no longer needed

    return mode


def identify_expansion(const_unit_df, inst_cap_df):
    """ Identify if the model will be with expansion. The criterion for which
        no expansion is possible is "inst-cap == cap-up" for all
        support timeframes

        Here the the number of items in dataframe with constant units will be
        compared to the the number of items to which 'inst-cap' is given

    """
    if const_unit_df.count() == inst_cap_df.count():
        return False
    else:
        return True

