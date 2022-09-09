UPDATE scenario_overview
SET calculationState= 'essimExported', essimExportGasunieLocation=%(essimExportGasunieLocation)s, essimExportTennetLocation=%(essimExportTennetLocation)s
WHERE scenarioId = %(scenarioId)s