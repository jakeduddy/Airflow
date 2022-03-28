CREATE OR REPLACE VIEW `oncasearch.Citeline.f_Trial` AS

SELECT
trialId
-- , recordUrl AS TrialURL
-- , trialTitle
-- , trialStatus
-- , trialPhase
-- , trialStartDate
-- , trialPrimaryCompletionDate
-- , trialPrimaryEndpointsReported
-- , trialObjective
-- , trialInclusionCriteria
-- , trialExclusionCriteria
-- , trialStudyDesign
-- , trialPatientPopulation
-- , trialTreatmentPlan
-- , trialPriorConcurrentTherapy
, trialTargetAccrual
, trialTargetAccrualText
, trialActualAccrual
, trialActualAccrualText
, trialPatientsPerSitePerMonth
--, trialIdentifiedSites
, trialReportedSites
-- , ARRAY_TO_STRING(trialOutcomes, ', ') AS trialOutcomes     -- (array) not sure if string_agg? or seperate dimension
-- , trialOutcomeDetails
-- , ARRAY_TO_STRING( trialTherapeuticAreas.trialDiseases.name, ', ') AS DiseaseNames
-- , ARRAY_TO_STRING( trialTherapeuticAreas.trialDiseases.trialMeshTerms.name, ', ') AS MeshTermsNames
-- , ARRAY_TO_STRING( trialSponsors.name, ', ') AS SponsorNames
-- , ARRAY_TO_STRING(trialAssociatedCRO , ', ') AS trialAssociatedCRO 
-- , ARRAY_TO_STRING(trialPatientDispositions , ', ') AS trialPatientDispositions 
-- , trialRecordType
, trialTiming.actualTrialStartDate
, trialTiming.anticipatedTrialStartDate
, trialTiming.actualEnrollmentDuration
, trialTiming.anticipatedEnrollmentDuration
, trialTiming.actualEnrollmentPeriodCloseDate
, trialTiming.anticipatedEnrollmentPeriodCloseDate
, trialTiming.actualTreatmentDuration
, trialTiming.anticipatedTreatmentDuration
, trialTiming.actualPrimaryCompletionDate
, trialTiming.anticipatedPrimaryCompletionDate
, trialTiming.totalTrialDuration
, trialTiming.actualTotalTrialCloseDate
, trialTiming.anticipatedTotalTrialCloseDate
, trialTiming.actualPrimaryEndpointsReported
, trialTiming.anticipatedPrimaryEndpointsReported
-- , trialPatientCriteria.minAge PatientCriteriamMinAge
-- , trialPatientCriteria.maxAge PatientCriteriamMaxAge
-- , trialPatientCriteria.minAgeUnits PatientCriteriamMinAgeUnits
-- , trialPatientCriteria.maxAgeUnits PatientCriteriamMaxAgeUnits
-- , trialPatientCriteria.gender PatientCriteriaGender
-- , ARRAY_TO_STRING(trialSupportingUrls, ', ') trialSupportingUrls
FROM 
    `oncasearch.Citeline.trial`


-- bmtPrimaryDrugsTested
--     drugId
--     drugNameId
--     ppDrugNameId
--     drugName
--     bmtDrugId
--     bmtBrandName
-- bmtOtherDrugsTested
--     drugId
--     drugNameId
--     ppDrugNameId
--     drugName
--     bmtDrugId
--     bmtBrandName
-- ctGovListedLocations
--     country
--     sitesCount
-- trialNotes
    -- details
    -- date
-- trialPrimaryEndpoint                Should the trial end point group group, subgroup be a dimension?
--     details                         To determine the 1-year survival rates of patients treated with paclitaxel, gemcitabine, and radiation with or without R115777.
--     primaryEndpoints
--         primaryEndpointGroup        Efficacy
--         primaryEndpointSubGroup     primaryEndpointSubGroup
--         primaryEndpoint             primaryEndpoint
-- trialOtherEndpoint
--     details
--     otherEndpoints
--         otherEndpointGroup
--         otherEndpointSubGroup
--         otherEndpoint
-- trialResults
--     date
--     details
-- biomarkers
--     trialTitle
--         caption
--         synonyms        (array)
--         commonUses      (array)
--     trialObjective
--         caption
--         synonyms        (array)
--         commonUses      (array)
--     trialPatientPopulation
--         caption
--         synonyms        (array)
--         commonUses      (array)
--     trialInclusionCriteria
--         caption
--         synonyms        (array)
--         commonUses      (array)
--     trialExclusionCriteria
--         caption
--         synonyms        (array)
--         commonUses      (array)
--     trialPrimaryEndpoint
--         caption
--         synonyms        (array)
--         commonUses      (array)
--     trialOtherEndpoint
--          caption
--         synonyms        (array)
--         commonUses      (array)
--     trialResults
--         caption
--         synonyms        (array)
--         commonUses      (array)