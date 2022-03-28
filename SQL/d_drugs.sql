CREATE OR REPLACE VIEW `oncasearch.Citeline.d_drugs` AS

select
trialId
, trialPrimaryDrugsTested.drugId
, trialPrimaryDrugsTested.drugName
, trialPrimaryDrugsTested.drugPrimaryName
, ARRAY_TO_STRING(drugParentNames, ', ') AS drugParentNames
, ARRAY_TO_STRING(drugNames, ', ') AS drugNames
, drugTherapeuticClasses
, true AS isPrimaryDrug
FROM 
    `oncasearch.Citeline.trial`
    , UNNEST( trialPrimaryDrugsTested ) AS trialPrimaryDrugsTested
    , UNNEST( trialPrimaryDrugsTested.drugTherapeuticClasses ) AS drugTherapeuticClasses

UNION ALL

select
trialId
, trialOtherDrugsTested.drugId
, trialOtherDrugsTested.drugName
, trialOtherDrugsTested.drugPrimaryName
, ARRAY_TO_STRING(drugParentNames, ', ') AS drugParentNames
, ARRAY_TO_STRING(drugNames, ', ') AS drugNames
, drugTherapeuticClasses
, false AS isPrimaryDrug
FROM 
    `oncasearch.Citeline.trial`
    , UNNEST( trialOtherDrugsTested ) AS trialOtherDrugsTested
    , UNNEST( trialOtherDrugsTested.drugTherapeuticClasses ) AS drugTherapeuticClasses