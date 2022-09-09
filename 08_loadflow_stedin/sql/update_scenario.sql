INSERT INTO loadflow_stedin (scenarioId, stedinDesign, calculationState, stedinLoadFlowLocation, stedinOverloadLocation)
VALUES (%(scenarioId)s, %(stedinDesign)s, %(calculationState)s, %(stedinLoadFlowLocation)s, %(stedinOverloadLocation)s)
    ON DUPLICATE KEY UPDATE
        calculationState = VALUES(calculationState),
        stedinLoadFlowLocation= VALUES(stedinLoadFlowLocation),
        stedinOverloadLocation= VALUES(stedinOverloadLocation);