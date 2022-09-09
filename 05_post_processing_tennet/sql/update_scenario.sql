INSERT INTO loadflow_tennet (scenarioId, investmentPlan, networkId, calculationState, postProcessingTennetLocation)
VALUES (%(scenarioId)s, %(investmentPlan)s, %(networkId)s, %(calculationState)s, %(postProcessingTennetLocation)s)
    ON DUPLICATE KEY UPDATE
        calculationState = VALUES(calculationState),
        postProcessingTennetLocation= VALUES(postProcessingTennetLocation);