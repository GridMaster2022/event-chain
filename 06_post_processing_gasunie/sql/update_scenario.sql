INSERT INTO loadflow_gasunie (scenarioId, networkId, gasunieInvestmentModel, calculationState, postProcessingGasunieLocation, postProcessingGasunieAssignmentLocation)
VALUES (%(scenarioId)s, %(networkId)s, %(gasunieInvestmentModel)s, %(calculationState)s, %(postProcessingGasunieLocation)s, %(postProcessingGasunieAssignmentLocation)s)
    ON DUPLICATE KEY UPDATE
        calculationState = VALUES(calculationState),
        postProcessingGasunieLocation= VALUES(postProcessingGasunieLocation),
        postProcessingGasunieAssignmentLocation= VALUES(postProcessingGasunieAssignmentLocation),
        gasunieInvestmentModel = VALUES(gasunieInvestmentModel);