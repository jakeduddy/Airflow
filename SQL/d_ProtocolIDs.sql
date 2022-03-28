CREATE OR REPLACE VIEW `oncasearch.citeline.d_ProtocolIDs` AS

select
trialId
,trialProtocolIDs
FROM 
    `oncasearch.citeline.Trial`
    ,UNNEST( trialProtocolIDs ) AS trialProtocolIDs