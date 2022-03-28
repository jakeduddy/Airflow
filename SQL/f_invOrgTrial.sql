CREATE OR REPLACE VIEW `oncasearch.Citeline.f_invOrgTrial` AS

WITH Investigator AS (
    SELECT
    investigatorId
    , invTrialOrganizations.organizationId AS organizationId
    , trialId AS trialId
    FROM
        `oncasearch.Citeline.investigator`
        , UNNEST(investigatorTrialOrganizations) as invTrialOrganizations
        , UNNEST(invTrialOrganizations.trialId) as trialId
    INNER JOIN `oncasearch.Citeline.trial` AS t
        on trialId = t.trialId
)
, Organization_W_Inv AS (
    SELECT
    organizationId
    , orgPrimaryInvestigators AS investigatorId
    , 'Primary' AS InvestigatorAffilication
    FROM 
        `oncasearch.Citeline.organization`
        , UNNEST(organizationPrimaryInvestigators) as orgPrimaryInvestigators
    UNION DISTINCT
    SELECT
    organizationId
    , orgAffiliatedInvestigators AS investigatorId
    , 'Affilicated' AS InvestigatorAffilication
    FROM 
        `oncasearch.Citeline.organization`
        , UNNEST(organizationAffiliatedInvestigators) as orgAffiliatedInvestigators
)
, Organization AS (
    SELECT 
    organizationId
    ,orgTrials.id as trialId
    FROM 
        `oncasearch.Citeline.organization`
        ,UNNEST( organizationTrials ) AS orgTrials
    where orgTrials.id in (Select trialId from `oncasearch.Citeline.trial`)
)
, Org_trial_Inv AS (
    Select 
    COALESCE( i.investigatorId, o.investigatorId) AS investigatorId
    , COALESCE( i.organizationId, o.organizationId) AS organizationId
    , i.trialId
    , o.InvestigatorAffilication
    FROM Organization_W_Inv o
    INNER JOIN Investigator i
        ON i.investigatorId = o.investigatorId
        and i.organizationId = o.organizationId
)

select 
o.organizationId
,o.trialId
,x.investigatorId
,x.InvestigatorAffilication
from Organization o
left join Org_trial_Inv x
    ON o.organizationId = x.organizationId
    and o.trialId = x.trialId