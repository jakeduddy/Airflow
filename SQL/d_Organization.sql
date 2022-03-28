CREATE OR REPLACE VIEW `oncasearch.Citeline.d_Organization` AS

SELECT
organizationId
, recordUrl OrganizationURL
, organizationName
, organizationType
, ARRAY_TO_STRING(organizationPhoneNumbers, ', ') organizationPhoneNumbers
, organizationLocation.city organizationCity
, organizationLocation.state organizationState
--, organizationLocation.country 
, organizationCountryName organizationCountry 
, organizationGeoLocation.lat organizationLat
, organizationGeoLocation.lon organizationLon
FROM 
    `oncasearch.Citeline.organization`