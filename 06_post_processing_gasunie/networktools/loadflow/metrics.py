import math
import numpy as np
import pandas as pd

def evaluate_element_overload(element, network, flows, capacity, factor):
    """Evaluate the overload for a network element in a passed network.
    
    The results contains the performance for the volume, median, frequency 
    and (weighed) score of the overload.
    
    Parameters
    ----------
    element : str
        The name of the element for which the overload
        is evaluated.
    network : pandapowernetwork
        Pandapower network object. Elements must match
        with the element index in the passed flows.
    flows : DataFrame
        Flow at each loading element of the passed
        network. Elements must match with the element
        index of the passed network.
    capacity : Series
        Series that specifies the capacity of each
        of the passed element item in the network.
    factor : float or Series
        Factor with which the overload score is 
        multiplied for evaluation of the calculated
        overload score.
    
    Return
    ------
    performance : DataFrame
        Dataframe that specifies the different overload
        results based on the passed flows and network."""
                
    # get network element and subset flows
    elmnt = getattr(network, element)
    flows = flows[element].abs()

    # check for matching indices
    if not (flows.columns == elmnt.index).all():
        raise ValueError('mismatching indices between ' + 
                         'passed flows and network.')
    
    # evaluate overload values
    overload = flows.sub(capacity)
    overload = overload.where(overload > 0, np.nan)
    
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
    performance.insert(loc=0, column='elementType', value=element)
    performance.insert(loc=1, column='elementCapacity', value=capacity)
    performance.insert(loc=2, column='weighFactor', value=factor)
    
    # assign linename as index
    performance.index = performance.index.map(elmnt.name)
    performance.index.name = 'elementName'
    
    return performance.fillna(0)
    
    
def evaluate_line_overload(network, flows):
    """Evaluate the line overload scores for the passed flows.
    
    Parameters
    ----------
    network : pandapowernetwork
        Pandapower network object. Elements must match
        with the element index in the passed flows.
    flows : DataFrame
        Flow at each loading element of the passed
        network. Elements must match with the element
        index of the passed network.
        
    Return
    ------
    performance : DataFrame
        Dataframe that specifies the different overload
        results based on the passed flows and network."""
    
    # set element
    element = 'line'
    
    # get factor from network
    factor = network.line.length_km

    # get line voltages
    busvoltage = network.bus.vn_kv
    linevoltage = network.line.from_bus.map(busvoltage)
    
    # fill missings with to bus
    linevoltage = linevoltage.fillna(network.line.to_bus.map(busvoltage))
    
    # evaluate thermal capacities
    capacity = linevoltage * network.line.max_i_ka * math.sqrt(3)
    
    # evaluate overload
    performance = evaluate_element_overload(element, network, 
                                            flows, capacity, factor)
        
    return performance


def evaluate_trafo_overload(network, flows):
    """Evaluate the trafo overload scores for the passed flows.
    
    Parameters
    ----------
    network : pandapowernetwork
        Pandapower network object. Elements must match
        with the element index in the passed flows.
    flows : DataFrame
        Flow at each loading element of the passed
        network. Elements must match with the element
        index of the passed network.
        
    Return
    ------
    performance : DataFrame
        Dataframe that specifies the different overload
        results based on the passed flows and network."""
        
    # set element
    element = 'trafo'
    factor = 13.3
    
    # evaluate thermal capacities
    capacity = network.trafo.sn_mva
    
    # evaluate overload
    performance = evaluate_element_overload(element, network, 
                                            flows, capacity, factor)
        
    return performance


def evaluate_network_overload(network, flows):
    """Evaluate the line amd trafo overload scores for the passed flows.
    
    Parameters
    ----------
    network : pandapowernetwork
        Pandapower network object. Elements must match
        with the element index in the passed flows.
    flows : DataFrame
        Flow at each loading element of the passed
        network. Elements must match with the element
        index of the passed network.
        
    Return
    ------
    performance : DataFrame
        Dataframe that specifies the different overload
        results based on the passed flows and network."""
    
    # evaluate scores
    trafos = evaluate_trafo_overload(network, flows)
    lines = evaluate_line_overload(network, flows)
    
    # merge results
    performance = pd.concat([lines, trafos])
    
    return performance