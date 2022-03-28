CREATE OR REPLACE VIEW `oncasearch.Citeline.f_invDiseaseTiers` AS

SELECT
investigatorId
,invDiseaseTiers.name
,invDiseaseTiers.tier
FROM 
    `oncasearch.Citeline.investigator`
    , UNNEST(investigatorDiseaseTiers) AS invDiseaseTiers