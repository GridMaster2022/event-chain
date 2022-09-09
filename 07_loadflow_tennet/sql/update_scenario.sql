INSERT INTO loadflow_tennet (scenarioId, investmentPlan, networkId, calculationState, postProcessingTennetLocation, tennetLoadFlowLocation, tennetMetricslocation)
VALUES (%(scenarioId)s, %(investmentPlan)s, %(networkId)s, %(calculationState)s, %(postProcessingTennetLocation)s, %(tennetLoadFlowLocation)s, %(tennetMetricslocation)s)
    ON DUPLICATE KEY UPDATE
        calculationState = VALUES(calculationState),
        tennetLoadFlowLocation= VALUES(tennetLoadFlowLocation),
        tennetMetricslocation= VALUES(tennetMetricslocation);