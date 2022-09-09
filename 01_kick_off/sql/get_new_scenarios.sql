SELECT *
FROM scenario_overview
WHERE calculationState = '{}'
GROUP BY scenarioUuid
LIMIT {}