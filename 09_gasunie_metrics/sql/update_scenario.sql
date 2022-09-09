INSERT INTO loadflow_gasunie (scenarioId, gasunieInvestmentModel, networkId, calculationState, postProcessingGasunieLocation, postProcessingGasunieAssignmentLocation, gasunieLoadFlowLocation, gasunieMetricsLocationH2, gasunieMetricsLocationCH4)
VALUES (%(scenarioId)s, %(gasunieInvestmentModel)s, %(networkId)s, %(calculationState)s, %(postProcessingGasunieLocation)s, %(postProcessingGasunieAssignmentLocation)s, %(gasunieLoadFlowLocation)s, %(gasunieMetricsLocationH2)s, %(gasunieMetricsLocationCH4)s)
    ON DUPLICATE KEY UPDATE
        calculationState = VALUES(calculationState),
        gasunieInvestmentModel = VALUES(gasunieInvestmentModel),
        networkId = VALUES(networkId),
        gasunieMetricsLocationCH4= VALUES(gasunieMetricsLocationCH4),
        gasunieLoadFlowLocation= VALUES(gasunieLoadFlowLocation),
        gasunieMetricsLocationH2= VALUES(gasunieMetricsLocationH2);