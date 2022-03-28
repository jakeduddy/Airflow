CREATE OR REPLACE VIEW `oncasearch.Citeline.d_CT_GovMeshTerm` AS

select
trialId
,trialCtGovMeshTerms
FROM 
    `oncasearch.Citeline.trial`
    ,UNNEST( trialCtGovMeshTerms ) AS trialCtGovMeshTerms