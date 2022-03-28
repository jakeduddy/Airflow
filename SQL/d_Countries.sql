CREATE OR REPLACE VIEW `oncasearch.Citeline.d_Countries` AS

SELECT DISTINCT
trialCountries AS CountryName
FROM 
    `oncasearch.Citeline.trial`
    ,UNNEST(trialCountries) AS trialCountries

UNION DISTINCT 

SELECT DISTINCT
investigatorLocation.country AS CountryName
FROM `oncasearch.Citeline.investigator`

UNION DISTINCT

SELECT DISTINCT
organizationCountryName AS CountryName
FROM `oncasearch.Citeline.organization`