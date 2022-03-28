CREATE OR REPLACE VIEW `oncasearch.Citeline.d_Sponsors` AS

select
trialId
,trialSponsors.name AS SponsorName
,trialSponsors.type AS SponsorType 
FROM 
    `oncasearch.Citeline.trial`
    ,UNNEST( trialSponsors ) AS trialSponsors