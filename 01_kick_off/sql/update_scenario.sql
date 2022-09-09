UPDATE scenario_overview
SET calculationState= 'kickedOff'
WHERE scenarioUuid = %(scenarioUuid)s