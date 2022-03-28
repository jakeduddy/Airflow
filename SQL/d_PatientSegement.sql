CREATE OR REPLACE VIEW `oncasearch.Citeline.d_PatientSegment` AS

SELECT DISTINCT
trialId
, trialPatientSegments.id PatientSegmentId
, trialPatientSegments.name AS PatientSegmentName
FROM 
    `oncasearch.Citeline.trial`
    , UNNEST(trialTherapeuticAreas) AS triTherapeuticAreas
    , UNNEST(triTherapeuticAreas.trialDiseases) AS trialDiseases
    , UNNEST(trialDiseases.trialPatientSegments) AS trialPatientSegments