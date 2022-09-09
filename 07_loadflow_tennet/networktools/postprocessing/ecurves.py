import warnings

import numpy as np
import pandas as pd

from .discounter import discount_market_curves


def aggregate_mv_substations(sites, network, exceptions=None):
    """
    Aggregate sites that are connected to MV substations to
    HV substations to match network topology.

    ASSUMPTION: It is not possible to connect a MV substation
    with an EHV substation.

    Parameters
    ----------
    sites : DataFrame
        DataFrame with properties of sites, must include
        substation column.
    network : pandapowerNet
        Network model in which the existence of substations
        can be validated.
    exceptions : dict, default None
        Optional dictonairy that maps cases when MV station
        does not match with HV stations.

    Returns
    -------
    sites : DataFrame
        DataFrame with properties of sites in which all
        MV-substations have been subsituted with
        HV-substations.
    """

    if exceptions is None:
        exceptions = {}

    # copy design
    sites = sites.copy()

    # extract info from substation
    pattern = "^(?P<station>[a-zA-Z]*)(?P<voltage>[0-9]*)(?P<remaining>.*)$"
    station = sites.substation.str.extract(pattern)

    # subset marked voltage levels
    marked = station[station.voltage.astype('int64') < 150]
    marked = marked.station.map(exceptions)

    # fillna with original station and add 150kV notation
    marked = marked.fillna(station.station)
    marked = marked + '150'

    # update design
    sites.substation.update(marked)

    # check for errors
    sstation = sites.substation
    if not sstation.isin(network.bus.sShort).all():
        errors = sstation[~sstation.isin(network.bus.sShort)].unique()
        raise KeyError(f'cannot find substation {list(errors)} in network')

    return sites


def aggregate_essim_sectorcurves(curves, sites, exceptions=None):
    """
    Aggregate nodal sectorcurves from ESSIM curves based on site
    configuration. The resulting curves are at the same resolution
    as the regionalized ETM curves and can be used for discounting.

    Parameters
    ----------
    curves : DataFrame
        ESSIM power curves with hours in index and
        sites in columns.
    sites : DataFrame
        Site configurations with sites in index and
        substation, capacity and sector in columns.
    exceptions: dict, default None
        Optional exceptions when assigining sectors.

    Return
    ------
    curves : DataFrame
        ESSIM curves with hours and nodes in index and
        sectors in columns."""

    # preserve original
    curves = curves.copy(deep=True)

    # check curve mapping
    curves, sites = _check_curve_mapping(curves, sites, cname='ESSIM-curves',
                                         mname='site-mapping')

    # prepare midx
    names = ['sector', 'node']
    frame = sites[['sector', 'substation']]

    # make midx mapping
    midx = pd.MultiIndex.from_frame(frame, names=names)
    midx = dict(zip(frame.index, midx))

    # assign sectors and group columns
    curves.columns = curves.columns.map(midx)
    curves = curves.groupby(level=(0, 1), axis=1).sum()

    # stack node to index
    curves = curves.stack(level=1)
    curves.index.names = ['hour', 'node']

    # groupby sectors
    curves = curves.groupby(level=0, axis=1).sum()

    # extract load and sgen
    demand = curves.where(curves > 0, 0)
    supply = -curves.where(-curves > 0, 0)

    # handle exceptions
    if exceptions is not None:
        # map expections
        regular = dict(zip(sites.sector, sites.sector))
        regular.update(exceptions['Supply'])

        # update supply sectors
        supply.columns = supply.columns.map(regular)
        supply = supply.groupby(level=0, axis=1).sum()

        # map expections
        regular = dict(zip(sites.sector, sites.sector))
        regular.update(exceptions['Demand'])

        # update demand sectors
        demand.columns = demand.columns.map(regular)
        demand = demand.groupby(level=0, axis=1).sum()

    # prepare midx maker
    names = ['product', 'sector']
    func = pd.MultiIndex.from_product

    # assign correct products
    demand.columns = func([['Demand'], demand.columns], names=names)
    supply.columns = func([['Supply'], supply.columns], names=names)

    # concat products
    curves = pd.concat([demand, supply], axis=1)

    return curves


def process_ESSIM_ecurves(curves, sites, network):
    """Helper function to process ESSIM curves for
    electricity

    Parameters
    ----------
    curves : DataFrame
        ESSIM power curves with hours in index and
        sites in columns.
    sites : DataFrame
        Site configurations with sites in index and
        substation, capacity and sector in columns.
    network : pandapowerNet
        Network model in which the existence of substations
        can be validated.

    Return
    ------
    curves : DataFrame
        ESSIM curves with hours and nodes in index and
        sectors in columns."""

    # node aggregation exceptions
    node_exceptions = {
    }

    # sector aggregation exceptions
    sector_exceptions = {
    }

    # aggregate mv substations to hv substations
    sites = aggregate_mv_substations(sites, network, node_exceptions)
    curves = aggregate_essim_sectorcurves(curves, sites, sector_exceptions)

    return curves


def categorize_ETM_curves(curves, cat, columns, carrier=None,
                          names=None, *args, **kwargs):
    """categorize the curves based on a categorization table. The
    (kw)args are passed to pandas.read_csv if the categorization is
    passed as a filestring.

    Parameters
    ----------
    curves : DataFrame
        ETM curves that are categorized with hours in index
        and ETM output keys in columns. Time column should be
        excluded from curves.
    cat : DataFrame
        Categorization that specifies the relation between the
        ETM output keys and the desired new columns.
    carrier : str
        Optional argument to specify a specific carrier that
        is subsetted from the carrier column.
    names : list
        Optional argument to specify the names of the resulting
        multiindex columns.

    Return
    ------
    ccurves : DataFrame
        Categorized ETM curves.
    """

    # load categorization
    if isinstance(cat, str):
        cat = pd.read_csv(cat, *args, **kwargs)

    # check columns
    columns = list(columns)
    if not isinstance(columns, list):
        raise TypeError('"columns" must be of type list')

    if carrier is not None:
        cat = cat[cat.carrier == carrier]

    if names is None:
        names = columns

    # check curves mapping
    # curves, cat = _check_ETM_categorization(curves, cat)
    curves, cat = _check_curve_mapping(curves, cat, cname='ETM-curves',
                                       mname='categorization')

    # preserve original
    ccurves = curves.copy(deep=True)

    # set new multiindex column
    arrays = [curves.columns.map(cat[col]) for col in columns]
    ccurves.columns = pd.MultiIndex.from_arrays(arrays, names=names)

    # aggregate to specified columns
    levels = [x for x in range(len(columns))]
    ccurves = ccurves.groupby(level=levels, axis=1).sum()

    return ccurves


def _check_curve_mapping(curves, mapping, cname='curves', mname='mapping'):
    """check if all required mappings are present and used"""

    for item in curves.columns[~curves.columns.isin(mapping.index)]:
        raise KeyError(f"'{item}' not in {mname} while in {cname}")

    for item in mapping.index[~mapping.index.isin(curves.columns)]:
        raise KeyError(f"'{item}' not in {cname} while in {mname}")

    return curves, mapping


def regionalize_ETM_curves(curves, reg, warn=True, *args, **kwargs):
    """regionalize the curves based on a regionalization table. The
    (kw)args are passed to pandas.read_csv if the regionalization is
    passed as a filestring.

    Parameters
    ----------
    curves : DataFrame
        Categorized ETM curves.
    reg : DataFrame
        Regionalization table with nodes in index and
        sectors in columns.
    warn : bool, default True
        Optional argument to warn when not all
        regionalization columns are present in the
        passed ETM curves.

     Return
     ------
     curves : DataFrame
         Regionalized ETM curves.
     """

    # load regioanlization
    if isinstance(reg, str):
        reg = pd.read_csv(reg, *args, **kwargs)

    # check curves mapping
    curves, reg = _check_ETM_regionalization(curves, reg, warn)

    # prepare new index
    names = ['hour', 'node']
    levels = [curves.index, reg.index]

    # prepare new dataframe
    columns = curves.columns
    index = pd.MultiIndex.from_product(levels, names=names)
    values = np.repeat(curves.values, reg.index.size, axis=0)

    # match index structure of regionalization
    curves = pd.DataFrame(values, index=index, columns=columns)

    # make regionalized hourly curves
    sreg = reg[curves['Supply'].columns]
    curves['Supply'] = sreg.mul(curves['Supply'], level=1)

    dreg = reg[curves['Demand'].columns]
    curves['Demand'] = dreg.mul(curves['Demand'], level=1)

    return curves


def _check_ETM_regionalization(curves, reg, warn=True):
    """check if all required mappings are present and used"""

    ccolumns = curves.columns.get_level_values(level=1)

    # warn if True
    if warn is True:
        # check is regionalization contains all curve headers
        for item in reg.columns[~reg.columns.isin(ccolumns)]:
            warnings.warn(f'"{item}" not in categorized curves ' +
                          'while in regionalization')

    # check if curves contains all regionalization headers
    for item in ccolumns[~ccolumns.isin(reg.columns)]:
        raise ValueError(f'"{item}" not in regionalization while ' +
                         f'in categorized curves')

    # check if regionalizations add up to 1.000
    sums = round(reg.sum(axis=0), 3)
    for idx, value in sums[sums != 1].iteritems():
        raise ValueError(f'"{idx}" regionalization sums to ' +
                         f'{value: .3f} instead of 1.000')

    return curves, reg


def process_ETM_curves(curves, cat, reg, carrier):
    """Helper function to process the ETM curves

    Parameters
    ----------
    curves : DataFrame
        ETM curves that are categorized with hours in index
        and ETM output keys in columns. Time column should be
        excluded from curves.
    cat : DataFrame
        Categorization that specifies the relation between the
        ETM output keys and the desired new columns.
    reg : DataFrame
        Regionalization table with nodes in index and
        sectors in columns.
    carrier : str
        Optional argument to specify a specific carrier that
        is subsetted from the carrier column.

     Return
     ------
     curves : DataFrame
         Regionalized ETM curves."""

    # specify parameters
    warn = False
    columns = ['product', 'sector']

    # categorize ETM curves
    curves = categorize_ETM_curves(curves, cat, columns, carrier)
    curves = regionalize_ETM_curves(curves, reg, warn)

    return curves

def postprocess_ecurves(ETM, ESSIM, cat, reg, sites, network):
    """
    Parameters
    ----------
    ETM : DataFrame
        ETM curves that are categorized with hours in index
        and ETM output keys in columns. Time column should be
        excluded from curves.
    ESSIM : DataFrame
        ESSIM power curves with hours in index and
        sites in columns.
    cat : DataFrame
        Categorization that specifies the relation between the
        ETM output keys and the desired new columns.
    reg : DataFrame
        Regionalization table with nodes in index and
        sectors in columns.
    sites : DataFrame
        Site configurations with sites in index and
        substation, capacity and sector in columns.
    network : pandapowerNet
        Network model in which the existence of substations
        can be validated.

    Return
    ------
    power : DataFrame
        Nodal power that can be passed to loadflow module.
    """
    # specify parameters
    carrier = 'Electricity'
    nodes = 'HICxxx'

    # preprocess curves
    ESSIM = process_ESSIM_ecurves(ESSIM, sites, network)
    ETM = process_ETM_curves(ETM, cat, reg, carrier)

    # discount curves and aggregate
    ETM = discount_market_curves(ETM, ESSIM, nodes)

    return ETM


def chunkit(df, size):
    """STACKOVERFLOW"""

    chunks = len(df) // size
    if len(df) % size != 0:
        chunks += 1

    for i in range(chunks):
        yield df[i * size: (i + 1) * size]


def make_nodal_ecurves(ETM, ESSIM, cat, reg, sites, network, size=1000):
    """Iterate over curves in chunks to reduce memory presure

    Could probably be improved, but seems to keep memory usage under 1GB.

    Parameters
    ----------
    ETM : DataFrame
        ETM curves that are categorized with hours in index
        and ETM output keys in columns. Time column should be
        excluded from curves.
    ESSIM : DataFrame
        ESSIM power curves with hours in index and
        sites in columns.
    cat : DataFrame
        Categorization that specifies the relation between the
        ETM output keys and the desired new columns.
    reg : DataFrame
        Regionalization table with nodes in index and
        sectors in columns.
    sites : DataFrame
        Site configurations with sites in index and
        substation, capacity and sector in columns.
    network : pandapowerNet
        Network model in which the existence of substations
        can be validated.
    size : int, default 1000
        size of chuncks.

    Return
    ------
    power : DataFrame
        Nodal power that can be passed to loadflow module.
    """

    # prepare function
    args = (cat, reg, sites, network)
    func = postprocess_ecurves

    # make chunksized iterator objects to iterate over
    iterator = zip(chunkit(ETM, size), chunkit(ESSIM, size))
    power = [func(cETM, cESSIM, *args) for cETM, cESSIM in iterator]

    # concat results
    power = pd.concat(power)

    return power