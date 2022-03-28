CREATE OR REPLACE VIEW `oncasearch.Citeline.d_MeshTerms` AS

SELECT DISTINCT
trialId
, trialMeshTerms.meshid
, trialMeshTerms.name AS MeshTermsName
FROM 
    `oncasearch.Citeline.trial`
    , UNNEST(trialTherapeuticAreas) AS triTherapeuticAreas
    , UNNEST(triTherapeuticAreas.trialDiseases) AS trialDiseases
    , UNNEST(trialDiseases.trialMeshTerms) AS trialMeshTerms