import copy
import traceback

import requests
import pandas as pd
from pathlib import Path
from uuid import uuid4
from io import StringIO
from esdl.esdl_handler import EnergySystemHandler
from datetime import datetime as dt, timedelta as td
from esdl import DateTimeProfile, QuantityAndUnitType, PhysicalQuantityEnum, UnitEnum, MultiplierEnum, ProfileElement, \
    ElectricityCommodity, PowerPlant, EnergySystem, Services, DrivenByProfile, InPort, DrivenByDemand, Service, ProfileReference

# Constants
ETM_url = 'https://beta-engine.energytransitionmodel.com/api/v3/scenarios/{}/curves/{}'
ELECTRICITY_PRICE_CSV = 'electricity_price.csv'
MERIT_ORDER_CSV = 'merit_order.csv'
TIME_FORMAT = '%Y-%m-%d %H:%M'
TIME_FORMAT_merit = '%d-%m-%Y %H:%M'
base_date = dt.strptime("2018-12-31 23:00", TIME_FORMAT)
time_step = td(hours=1)
powerplant_profiles = ["Power Plant Biomass", "Power Plant Coal", "Power Plant Gas Small", "Power Plant Gas Large",
                       "Power Plant Nuclear", "Power Plant Other"]
carrier_ids = ["RTLH_ODO", "RTLG_ODO", "RTLH_NODO", "RTLG_NODO", "HTLH", "HTLG",
               "H2_new", "H2_local", "H2_Hvision", "C", "BM", "W", "GM"]
essim_etm_mapping = {"RTLH_ODO": "Power Plant Gas Large",
                     "GM": "Power Plant Gas Large",
                     "RTLG_ODO": "Power Plant Gas Large",
                     "RTLH_NODO": "Power Plant Gas Large",
                     "RTLG_NODO": "Power Plant Gas Large",
                     "HTLH": "Power Plant Gas Large",
                     "HTLG": "Power Plant Gas Large",
                     "H2_new": "Power Plant Gas Small",
                     "H2_local": "Power Plant Gas Small",
                     "H2_Hvision": "Power Plant Gas Small",
                     "C": "Power Plant Coal",
                     "BM": "Power Plant Biomass",
                     "W": "Power Plant Other"
                     }


def make_reference(profile: DateTimeProfile):
    return ProfileReference(id=str(uuid4()), name='Profile reference to {}'.format(profile.name), reference=profile)


def update_profiles(ESDL_string, merit_order):
    esh = EnergySystemHandler()
    es = esh.load_from_string(ESDL_string)
    # Dataframe. This is equal for all scenario's and years
    grouping_dict = {
        "energy_chp_ultra_supercritical_coal.output (MW)": "Power Plant Coal",
        "energy_chp_ultra_supercritical_cofiring_coal.output (MW)": "Power Plant Coal",
        "energy_chp_ultra_supercritical_lignite.output (MW)": "Power Plant Coal",
        "energy_power_combined_cycle_ccs_coal.output (MW)": "Power Plant Coal",
        "energy_power_combined_cycle_coal.output (MW)": "Power Plant Coal",
        "energy_power_supercritical_coal.output (MW)": "Power Plant Coal",
        "energy_power_ultra_supercritical_ccs_coal.output (MW)": "Power Plant Coal",
        "energy_power_ultra_supercritical_coal.output (MW)": "Power Plant Coal",
        "energy_power_ultra_supercritical_cofiring_coal.output (MW)": "Power Plant Coal",
        "industry_chp_ultra_supercritical_coal.output (MW)": "Power Plant Coal",
        "energy_power_combined_cycle_ccs_network_gas.output (MW)": "Power Plant Gas Large",
        "energy_power_combined_cycle_hydrogen.output (MW)": "Power Plant Gas Large",
        "energy_power_combined_cycle_network_gas.output (MW)": "Power Plant Gas Large",
        "industry_chp_engine_gas_power_fuelmix.output (MW)": "Power Plant Gas Large",
        "industry_chp_turbine_gas_power_fuelmix.output (MW)": "Power Plant Gas Large",
        "energy_chp_local_engine_biogas.output (MW)": "Power Plant Gas Small",
        "energy_chp_local_engine_network_gas.output (MW)": "Power Plant Gas Small",
        "energy_power_engine_network_gas.output (MW)": "Power Plant Gas Small",
        "energy_power_turbine_hydrogen.output (MW)": "Power Plant Gas Small",
        "energy_power_turbine_network_gas.output (MW)": "Power Plant Gas Small",
        "energy_power_ultra_supercritical_network_gas.output (MW)": "Power Plant Gas Small",
        "industry_chp_combined_cycle_gas_power_fuelmix.output (MW)": "Power Plant Gas Small",
        "energy_power_nuclear_gen2_uranium_oxide.output (MW)": "Power Plant Nuclear",
        "energy_power_nuclear_gen3_uranium_oxide.output (MW)": "Power Plant Nuclear",
        "energy_chp_local_wood_pellets.output (MW)": "Power Plant Biomass",
        "energy_chp_supercritical_waste_mix.output (MW)": "Power Plant Other",
        "energy_power_engine_diesel.output (MW)": "Power Plant Other",
        "energy_power_geothermal.output (MW)": "Power Plant Other",
        "energy_power_hydro_mountain.output (MW)": "Power Plant Other",
        "energy_power_hydro_river.output (MW)": "Power Plant Other",
        "energy_power_supercritical_waste_mix.output (MW)": "Power Plant Other",
        "energy_power_ultra_supercritical_crude_oil.output (MW)": "Power Plant Other",
        "energy_power_ultra_supercritical_lignite.output (MW)": "Power Plant Other",
        "energy_power_ultra_supercritical_oxyfuel_ccs_lignite.output (MW)": "Power Plant Other",
        "industry_chp_wood_pellets.output (MW)": "Power Plant Other",
    }

    # Sum and make deepcopy of merit order
    merit_order_grouped = merit_order.groupby(grouping_dict, axis=1).sum()
    merit_order_grouped_copy = copy.deepcopy(merit_order_grouped)

    # Normalize value in the copied dataframe
    for name, column in merit_order_grouped.items():
        for index, value in column.iteritems():
            column_max = column.max() * 1.1
            normalized_value = value / column_max
            if normalized_value == 0:
                normalized_value = 0.0001
            merit_order_grouped_copy.at[index, name] = normalized_value

    time_range = pd.date_range('2018-12-31 23:00:00', periods=8760, freq='H', name='Time')
    merit_order_grouped_copy.index = time_range

    profile_dict = {}
    processed_profiles = []

    for powerplant_profile in powerplant_profiles:
        dt_profile = DateTimeProfile(id=str(uuid4()), name=powerplant_profile)
        percent_per_hour = QuantityAndUnitType(id=str(uuid4()), description='ProductionInPercentage',
                                               physicalQuantity=PhysicalQuantityEnum.COEFFICIENT,
                                               unit=UnitEnum.PERCENT, perMultiplier=MultiplierEnum.NONE)
        dt_profile.profileQuantityAndUnit = percent_per_hour
        # Process dataframe to add elements to DateTimeProfile
        for date, value in merit_order_grouped_copy[powerplant_profile].iteritems():
            from_time = date
            to_time = from_time + time_step
            dt_profile.element.append(ProfileElement(from_=from_time, to=to_time, value=float(value)))
        profile_dict[powerplant_profile] = dt_profile

    for asset in es.eAllContents():
        if isinstance(asset, PowerPlant):
            if asset.port[0].carrier.id not in carrier_ids:
                continue

            pp_outport = None
            for port in asset.port:
                if isinstance(port, InPort):
                    port_carrier = port.carrier.id
                    carrier_id = essim_etm_mapping[port_carrier]
                    pp_inport = port
                if port.carrier.name == 'Electricity':
                    pp_outport = port
                    break
            if pp_outport is None:
                print('PowerPlant {} has no electricity out ports... skipping!'.format(asset.id))
                continue

            dbp = DrivenByProfile(id='DbP_{}'.format(asset.id))
            dbp.energyAsset = asset
            dbp.port = pp_inport
            profile = profile_dict[carrier_id]
            if profile in processed_profiles:
                profile = make_reference(profile)
            else:
                processed_profiles.append(profile)
            dbp.profile = profile
            es.services.service.append(dbp)
            asset.controlStrategy = dbp

    return esh.to_bytesio()


def update_esdl(scenario_id: int, ESDL_string: str):
    # Retrieve scenario-specific electricity price CSV from ETM
    r = requests.get(ETM_url.format(scenario_id, ELECTRICITY_PRICE_CSV))
    if r.status_code != 200:
        raise ValueError('Could not get price information for scenario {}'.format(scenario_id))
    lines = r.text.splitlines()

    # Remove header row
    lines.pop(0)

    # Open ESDL file for processing
    esh = EnergySystemHandler()
    es = esh.load_from_string(ESDL_string)
    if es.energySystemInformation is None:
        raise ValueError('Energy System Information missing in this ESDL')
    if es.energySystemInformation.carriers is None:
        raise ValueError('Carriers undefined in this ESDL')

    # Find the electricity commodity
    electricity_carrier = None
    for carrier in es.energySystemInformation.carriers.carrier:
        if isinstance(carrier, ElectricityCommodity):
            electricity_carrier = carrier
            break
    if electricity_carrier is None:
        raise ValueError('No Electricity commodity defined in this ESDL')

    # Create a DateTimeProfile with quantity and unit set to Eur per MW
    dt_profile = DateTimeProfile(id=str(uuid4()), name='ElectricityPriceProfile')
    euro_per_mw = QuantityAndUnitType(id=str(uuid4()), description='PriceInEuros',
                                      physicalQuantity=PhysicalQuantityEnum.COST,
                                      unit=UnitEnum.EURO, perMultiplier=MultiplierEnum.MEGA, perUnit=UnitEnum.WATT)
    dt_profile.profileQuantityAndUnit = euro_per_mw
    profile_time = base_date

    # Process CSV to add elements to DateTimeProfile
    for line in lines:
        data = line.split(',')
        from_time = profile_time
        to_time = from_time + time_step
        dt_profile.element.append(ProfileElement(from_=from_time, to=to_time, value=float(data[1])))
        profile_time = to_time
    # Attach price profile to ElectricityCommodity's cost attribute
    electricity_carrier.cost = dt_profile

    # Save as modified ESDL file
    return esh.to_string()
