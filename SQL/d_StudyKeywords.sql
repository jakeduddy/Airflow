CREATE OR REPLACE VIEW `oncasearch.Citeline.d_StudyKeywords` AS

select
trialId
,trialStudyKeywords
FROM 
    `oncasearch.Citeline.trial`
    ,UNNEST( trialStudyKeywords ) AS trialStudyKeywords