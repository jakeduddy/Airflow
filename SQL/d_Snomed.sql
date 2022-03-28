CREATE OR REPLACE VIEW `oncasearch.Citeline.d_Snomed` AS

SELECT DISTINCT
trialId
, trialSnomed.snomedId
, trialSnomed.name AS SnomedName
FROM 
    `oncasearch.Citeline.trial`
    , UNNEST(trialTherapeuticAreas) AS triTherapeuticAreas
    , UNNEST(triTherapeuticAreas.trialDiseases) AS trialDiseases
    , UNNEST(trialDiseases.trialSnomed) AS trialSnomed