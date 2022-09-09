UPDATE scenario_overview
SET calculationState= 'esdlUpdated', updatedEsdlLocation=%(updatedEsdlLocation)s
WHERE scenarioId = %(scenarioId)s