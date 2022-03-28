CREATE OR REPLACE VIEW `oncasearch.Citeline.d_trialTags` AS

select
trialId
,trialTags
FROM 
    `oncasearch.Citeline.trial`
    ,UNNEST( trialTags ) AS trialTags