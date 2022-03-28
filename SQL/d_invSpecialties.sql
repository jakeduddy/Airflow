CREATE OR REPLACE VIEW `oncasearch.Citeline.d_invSpecialties` AS

SELECT
investigatorId
, invSpecialties
FROM 
    `oncasearch.Citeline.investigator`
    , UNNEST(investigatorSpecialties) AS invSpecialties