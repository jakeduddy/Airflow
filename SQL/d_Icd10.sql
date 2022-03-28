CREATE OR REPLACE VIEW `oncasearch.Citeline.d_Icd10` AS

SELECT DISTINCT
trialId
, trialIcd10.icd10Id
, trialIcd10.name AS ICD10Name
FROM 
    `oncasearch.Citeline.trial`
    , UNNEST(trialTherapeuticAreas) AS triTherapeuticAreas
    , UNNEST(triTherapeuticAreas.trialDiseases) AS trialDiseases
    , UNNEST(trialDiseases.trialIcd10) AS trialIcd10