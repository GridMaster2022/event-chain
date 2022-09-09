import logging
import pandas as pd

logger = logging.getLogger(__name__)

def validate_discountability(ETM, ESSIM):
    """Validate if ESSIM residual balance can be
    discounted with the ETM results. 
    
    This function checks if the absolute residual 
    ESSIM volumes are smaller than or equal to the 
    Supply or Demand group of the ETM curves
    
    Parameters
    ----------
    ETM : DataFrame
        Regionalized ETM curves.
    ESSIM : DataFrame
        Regionalized ESSIM curves.
        
    Make sure that the midx-formats of the rows and
    columns of ETM and ESSIM are the same.
    
    Return
    ------
    passed : bool
        Boolean that specifies if the curves
        can be discounted or not.
    """
    
    vETM = ETM.groupby(level=0, axis=1).sum()
    vETM = vETM.groupby(level=0, axis=0).sum()
    
    vESSIM = ESSIM.groupby(level=0, axis=1).sum()
    vESSIM = vESSIM.groupby(level=0, axis=0).sum()
    
    # check demand balance
    if not (vETM['Demand'] >= vESSIM['Demand']).all():
        logging.error('BalanceError: ESSIM demand larger ' + 'than ETM supply')

    # check supply balance
    if not (vETM['Supply'] >= vESSIM['Supply']).all():
        logging.error('BalanceError: ESSIM supply larger ' + 'than ETM supply')

    # check overall balance
    if not ((vETM['Supply'] - vETM['Demand']) >= 
            (vETM['Supply'] - vETM['Demand'])).all():
        logging.error('BalanceError: ESSIM residual larger ' + 'than ETM residual')
        
    return True


def discount_market_curves(ETM, ESSIM, nodes=None):
    """discount the ETM and ESSIM curves based on a proportional
    allocation of the difference that has to be discounted.
    
    Assumption
    ----------
    1. Discounting can cause power imbalances in cases where
    the ESSIM results contain volumes that are greater than
    the volumes that are present in the non-ESSIM related
    nodes in the ETM results. In these cases any differences
    are discounted over the demand/supply group.
    
    2. All ETM results regionalized towards the ESSIM
    region are overwritten by the ESSIM results. This
    means that any ETM volumes allocated to the ESSIM
    area that are not included in ESSIM are overwritten.
    
    Parameters
    ----------
    ETM* : DataFrame
        Regionalized ETM curves.
    ESSIM* : DataFrame
        Regionalized ESSIM curves.
    nodes : list
        A list of nodes that are in the regionalized
        ETM curves that are included in the ESSIM area.
        
    *Make sure that the midx-formats of the rows and
    columns of ETM and ESSIM are the same.
    
    Returns
    -------
    curves : DataFrame
        Discounted curves."""

    # check if curves can be discounted
    balanced = validate_discountability(ETM, ESSIM)
        
    # default nodes list
    if nodes is None:
        nodes = []
    
    if isinstance(nodes, str):
        nodes = [nodes]
    
    # reference nodes in ESSIM and ETM
    ETMnodes = ETM.index.unique(level=1)
    ESSIMnodes = ESSIM.index.unique(level=1)

    # specify conditions
    c1 = ESSIMnodes.isin(ETMnodes)
    c2 = ESSIMnodes.isin(nodes)
    
    # check for duplicates
    if (c1 & ~c2).any():
        nodes = list(ESSIMnodes[c1 & ~c2])
        raise ValueError(f'"{nodes}" present in ESSIM and ETM, while not ' + 
                        'excepted in nodes argument.')

    # check for wrong node exceptions
    errors = [node for node in nodes if node not in ETMnodes]
    if len(errors) > 0:
        raise ValueError(f'"{errors}" not in ETM while excepted ' + 
                         'in nodes argument')
    
    # check if levels are names the same
    if not ETM.columns.names == ESSIM.columns.names:
        raise ValueErrror('ETM and ESSIM have different column names')
        
    # check if levels are named the same
    if not ETM.index.names == ETM.index.names:
        raise ValueError('ETM and ESSIM have different index names')
    
    # with exceptions
    if len(nodes) > 0:
            
        # prepare midx args
        names = ['hour', 'node']
        hours = ESSIM.index.unique(level=0)

        # make hic/non-hic slicer for ETM
        func = pd.MultiIndex.from_product
        midx = func([hours, nodes], names=names)
                
        # seperate HIC and non-HIC values in ETM
        hETM = ETM.loc[midx]
        nETM = ETM[~ETM.index.isin(hETM.index)]
        
        # check if nodes are left
        if nETM.empty:
            raise ValueError('No nodes left in ETM to discount volumes')
                        
        # aggregate hourly volumes in HIC from ESSIM/ETM
        hETM = hETM.groupby(level=0).sum()
        hESSIM = ESSIM.groupby(level=0).sum()

        # subtract volumes from ESSIM that are
        # already allocated to the ESSIM area
        discount = (hESSIM - hETM).fillna(hESSIM)
        discount = discount.groupby(level=0).sum()

    # without exceptions
    else:
        
        # copy ETM and set ESSIM as discount
        nETM = ETM.copy(deep=True)
        discount = ESSIM.groupby(level=0).sum()
    
    # check for imbalances in hourly values
    reference = nETM.groupby(level=0).sum()
    
    # set imbalance if imbalanced found
    if (reference < discount).any().any():
        balanced = False
                                                
    # subset all ETM nodes not in HIC
    rowtotals = nETM.groupby(level=0, axis=0).sum()
    proportion = nETM.div(rowtotals).fillna(0)

    # discount HIC volumes with ETM volumes
    nETM -= proportion.mul(discount)

    # merge ETM and ESSIM
    curves = pd.concat([nETM, ESSIM])
    curves = curves.sort_index().fillna(0)
    
    # try to handle imbalance
    if balanced is False:
        
        # temporary fix
        if 0 in curves.index:
            logger.warn('DiscountWarning: Cannot balance all over sectors, ' +
                        'balancing residuals over product groups instead.')
            
        # aggregate original ETM data
        ETM = ETM.groupby(level='hour').sum()
        ETM = ETM.groupby(level='product', axis=1).sum()

        # aggregate resulting curves as well
        curves = curves.groupby('hour').sum()
        curves = curves.groupby('product', axis=1).sum()

        # solving disbalances between groups
        nETM = nETM.groupby('product', axis=1).sum()
        nETM += nETM / nETM.groupby(level=0).sum() * (ETM-curves)

        # aggregate ESSIM as well
        ESSIM = ESSIM.groupby('product', axis=1).sum()
        curves = pd.concat([nETM, ESSIM])
    
    # group over product
    curves = curves.groupby(level=0, axis=1).sum()
    curves = curves.Supply - curves.Demand
             
    # unstack node
    curves = curves.unstack(level=1)
    
    return curves
