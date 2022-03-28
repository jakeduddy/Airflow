CREATE OR REPLACE VIEW `oncasearch.citeline.d_Investigator` AS

SELECT
investigatorId
, recordUrl investigatorURL
, investigatorFirstName || ' ' || investigatorLastName investigatorName
-- , invPrimaryOrganization.id
, investigatorLocation.city investigatorCity
, investigatorLocation.state investigatorState
, investigatorLocation.country investigatorCountry
, investigatorGeoLocation.lat investigatorLat
, investigatorGeoLocation.lon investigatorLon
, ARRAY_TO_STRING(investigatorEmails, ', ') investigatorEmails
, ARRAY_TO_STRING(investigatorPhoneNumbers, ', ') investigatorPhoneNumbers
, ARRAY_TO_STRING(investigatorFaxes, ', ') investigatorFaxes
, ARRAY_TO_STRING(investigatorDegrees, ', ') investigatorDegrees
, ARRAY_TO_STRING(investigatorSpecialties, ', ') investigatorSpecialties
FROM 
    `oncasearch.citeline.Investigator`
    -- , UNNEST(investigatorPrimaryOrganization) as invPrimaryOrganization