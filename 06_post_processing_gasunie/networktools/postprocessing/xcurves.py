import pandas as pd

from .discounter import discount_market_curves

def aggregate_essim_xcurves(curves, sites):
    """simplified aggration for ESSIM curves based on
    sites design.
    
    Parameters
    ----------
    curves : DataFrame
        ESSIM curves per site with asset name in columns 
        and hours in rows.
    sites : DataFrame
        Configuration of sites with asset names in index
        and a substation column that specifies the node
        to which the site is connected.
        
    Return
    ------
    curves : DataFrame
        Aggregated ESSIM curves with a Demand and Supply profile
        for each hour on each node."""
    
    # assign substation
    curves = curves.copy(deep=True)
    curves.columns = curves.columns.map(sites.substation)
    
    # group by substations
    curves = curves.groupby(level=0, axis=1).sum()
    
    demand = curves.where(curves > 0, 0)
    supply = -curves.where(-curves > 0, 0)
    
    demand = demand.stack(level=0)
    demand.name = 'Demand'
    
    supply = supply.stack(level=0)
    supply.name = 'Supply'
    
    curves = pd.concat([demand, supply], axis=1)
    curves.index.names = ['hour', 'node']
    
    return curves

def make_nodal_xcurves(ESSIM, sites):
    """Helper function to make curves for molecules.
    
    Parameters
    ----------
    ESSIM : DataFrame
        ESSIM curves per site with asset name in columns 
        and hours in rows.
    sites : DataFrame
        Configuration of sites with asset names in index
        and a substation column that specifies the node
        to which the site is connected.
    
    Return
    ------
    power : DataFrame
        Nodal power curves.
    """
    
    # aggregate nodal demand and supply
    power = aggregate_essim_xcurves(ESSIM, sites)
    power = power['Supply'] - power['Demand']
    
    return power.unstack()

def make_mca_format(curves):
    """Format files according to MCA.
    
    Parameters
    ----------
    curves : DataFrame
        Discounted nodal powers.
        
    Return
    ------
    curves : DataFrame
        MCA-formatted discounted nodal powers.
    """
    
    # remove node index
    curves.columns.name = None
    
    # convert to periodindex
    periods = pd.date_range('2050/01/01 00:00', periods=8760, freq='H')

    names = ['dag', 'uur']
    arrays = [periods.dayofyear, periods.hour]
    curves.index = pd.MultiIndex.from_arrays(arrays, names=names)

    return curves

def make_nodal_hcurves(ESSIM, sites):
    """Make curves for hydrogen.
    
    Parameters
    ----------
    ESSIM : DataFrame
        ESSIM curves per site with asset name in columns 
        and hours in rows.
    sites : DataFrame
        Configuration of sites with asset names in index
        and a substation column that specifies the node
        to which the site is connected.
    
    Return
    ------
    curves : DataFrame
        MCA-formatted discounted nodal powers for hydrogen.
    """
    
    # aggregate essim curves
    curves = make_nodal_xcurves(ESSIM, sites)
    curves = make_mca_format(curves)
    
    return curves

def make_nodal_mcurves(ESSIM, sites):
    """Make curves for hydrogen.
    
    Parameters
    ----------
    ESSIM : DataFrame
        ESSIM curves per site with asset name in columns 
        and hours in rows.
    sites : DataFrame
        Configuration of sites with asset names in index
        and a substation column that specifies the node
        to which the site is connected.
    
    Return
    ------
    curves : DataFrame
        MCA-formatted discounted nodal powers for hydrogen.
    """
    
    curves = make_nodal_xcurves(ESSIM, sites)
    curves = make_mca_format(curves)
    
    return curves

def make_MCA_input_frame(mESSIM, msites, hESSIM, hsites):
    """merge mcurves and hcurves
    Parameters
    ----------
    mESSIM : DataFrame
        ESSIM methane curves per site with asset name in columns 
        and hours in rows.
    msites : DataFrame
        Configuration of methane sites with asset names in index
        and a substation column that specifies the node
        to which the site is connected.
    hESSIM : DataFrame
        ESSIM hyrogen curves per site with asset name in columns 
        and hours in rows.
    hsites : DataFrame
        Configuration of hyrogen sites with asset names in index
        and a substation column that specifies the node
        to which the site is connected.
    
    Return
    ------
    xcurves : DataFrame
        MCA-formatted discounted nodal powers for methane 
        and hydrogen in a single frame.
    """
    
    mcurves = make_nodal_mcurves(mESSIM, msites)
    hcurves = make_nodal_hcurves(hESSIM, hsites)
    
    xcurves = pd.concat([mcurves, hcurves], axis=1)
    
    return xcurves