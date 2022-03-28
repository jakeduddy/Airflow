CREATE OR REPLACE VIEW `oncasearch.Citeline.d_Icd9` AS

SELECT DISTINCT
trialId
, trialIcd9.icd9Id
, trialIcd9.name AS ICD9Name
FROM 
    `oncasearch.Citeline.trial`
    , UNNEST(trialTherapeuticAreas) AS triTherapeuticAreas
    , UNNEST(triTherapeuticAreas.trialDiseases) AS trialDiseases
    , UNNEST(trialDiseases.trialIcd9) AS trialIcd9