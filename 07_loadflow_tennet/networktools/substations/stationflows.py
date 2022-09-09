import numpy as np
import pandas as pd

def subset_mv_substations(sites):
    """
    Subset the sites that are placed in the mv-domain.
    
    Parameters
    ----------
    sites : DataFrame
        DataFrame with properties of sites, must include
        substation column.
    
    Return
    ------
    sites : DataFrame
        Subset of site configurations that are placed in 
        the mv-domain.
    """
    
    # extract info from substation
    pattern = "^(?P<station>[a-zA-Z]*)(?P<voltage>[0-9]*)(?P<remaining>.*)$"
    station = sites.substation.str.extract(pattern)
    
    # subset marked voltage levels
    marked = station[station.voltage.astype('int64') < 150]
    sites = sites.loc[marked.index]

    return sites

def evaluate_flow(curves, sites):
    """Aggregate flows for passed substations.
    
    Parameters
    ----------
    curves : DataFrame
        ESSIM power curves with hours in index and
        sites in columns.
    sites : DataFrame
        DataFrame with properties of sites, must include 
        substation column.
        
    Return
    ------
    flow : DataFrame
        ESSIM flows aggregated per substation node with 
        hours in index and nodes in columns.
    """
    
    # replace sites with substation names
    curves = curves[sites.index]
    curves.columns = curves.columns.map(sites.substation)
    
    # group substations and sum flow
    flow = curves.groupby(level=0, axis=1).sum()
    flow.columns.name = 'node'

    return flow
    
def get_nx_capacities(substations, n):
    """Calculate n-x capacities of substations based on the total
    capacity of the trafos at a substation and the number of
    trafos placed at a substation.
    
    ASSUMPTION
    ----------
    All trafos in a substation are assummed to have the same 
    capacity, as the total capacity is devided over the number
    of trafos in the substation, before taking one out of service.
    
    Parameters
    ----------
    substations : DataFrame
        DataFrame with the properties of substations, must
        include a total capacity of the substation and
        the number of trafos present at the substation.
    n : integer
        Reduncancy for which the capacities are evaluated. Use
        n=1 for n-1 redundancy and n=2 for n-2 redundancy.
        
    Return
    ------
    nx : Series
        Series with the n-x redundancy capacities of the passed
        substations.
    """
    
    # reference columns
    capacity = substations.capacity
    trafos = substations.trafos
    
    # evaluate capacities
    redundancy = trafos.where(trafos-n > 0, 0)
    nx = (capacity / trafos) * redundancy
    
    return nx

def assess_overload(flow, nx):
    """Assess the overload flows at a substation based
    on the available capacity at a substation.
    
    Parameters
    ----------
    flow : DataFrame
        ESSIM flows aggregated per substation node with
        hours in index and nodes in columns.
    nx : Series
        Series with the n-x capacities of the substations
        in the columns of the passed flow.
        
    Return
    ------
    overload : DataFrame
        DataFrame with the overload powers of each substation
        at each hour. Hours in index and substations in columns."""
    
    # evaluate upward and downward overloads
    overload = flow.where(flow > nx, np.nan) - nx
    downward = flow.where(flow < -nx, np.nan) + nx
    
    # merge information and replace nans
    overload.update(downward)
    overload = overload.fillna(np.nan)
    
    return overload
    
def evaluate_mv_substations(ESSIM, sites):
    """Evaluate all mv-substations
    
    Parameters
    ----------
    ESSIM : DataFrame
        ESSIM power curves with hours in index and
        sites in columns.
    sites : DataFrame
        DataFrame with properties of sites, must include 
        substation column.
    
    Return
    ------
    flow : DataFrame
        ESSIM flows aggregated per substation node with hours in 
        index and nodes in columns.
    overload : DataFrame
        DataFrame with the overload powers of each substation
        at each hour. Hours in index and substations in columns."""
        
    # subset sstations and aggregate mv-curves
    sites = subset_mv_substations(sites)
    flow = evaluate_flow(ESSIM, sites)
    
    return flow

def evaluate_substation_overload(substations, flow, n, factor):
    """Evaluate the overload for the passed substation flows.
    
    The results contains the performance for the volume, median, frequency 
    and (weighed) score of the overload.
    
    Parameters
    ----------
    substations : DataFrame
        DataFrame with the properties of substations, must
        include a total capacity of the substation and
        the number of trafos present at the substation.
    flow : DataFrame
        ESSIM flows aggregated per substation node with hours in 
        index and nodes in columns.
    n : integer, default 1
        Reduncancy for which the capacities are evaluated. Use
        n=0 for n-0 redundancy, n=1 for n-1 redundancy, etc.
    factor : float or Series
        Factor with which the overload score is 
        multiplied for evaluation of the calculated
        overload score.
    
    Return
    ------
    performance : DataFrame
        Dataframe that specifies the different overload
        results based on the passed flows and network."""
    
    # evaluate capacity and assess overload
    capacity = get_nx_capacities(substations, n)
    overload = assess_overload(flow, capacity)
    
    # evaluate overload stats
    median = overload.median()
    frequency = overload.count()
    volume = overload.sum()
    
    # evaluate overload scores
    scores = median * frequency / 8760
    weighed = scores * factor
    
    # assign names to recognize
    volume.name = 'overloadVolume'
    median.name = 'overloadMedian'
    frequency.name = 'overloadFreq'
    scores.name = 'overloadScore'
    weighed.name = 'overloadCalculated'

    # make results dataframe
    frames = [volume, median, frequency, scores, weighed]
    performance = pd.concat(frames, axis=1)
    
    # add elementype
    performance.insert(loc=0, column='elementType', value='substation')
    performance.insert(loc=1, column='elementCapacity', value=capacity)
    performance.insert(loc=2, column='weighFactor', value=factor)
    
    # assign linename as index
    performance.index.name = 'elementName'
    
    return performance.fillna(0)
