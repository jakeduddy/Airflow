CREATE OR REPLACE VIEW `oncasearch.Citeline.d_Diseases` AS

SELECT
trialId
, trialDiseases.id AS DiseaseId
, trialDiseases.name AS DiseaseName
FROM 
    `oncasearch.Citeline.trial`
    , UNNEST(trialTherapeuticAreas) AS triTherapeuticAreas
    , UNNEST(triTherapeuticAreas.trialDiseases) AS trialDiseases