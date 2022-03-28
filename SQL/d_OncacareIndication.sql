CREATE OR REPLACE VIEW `oncasearch.Citeline.d_OncacareIndication` AS

WITH Pivoted AS (
    SELECT
    trialId
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)head/neck') AS Solid_Tumour__Head_and_Neck
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)lung*cell') AS Solid_Tumour__Lung
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)esophageal') AS Solid_Tumour__Gastrointestinal__Esophageal
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)gastric') AS Solid_Tumour__Gastrointestinal__Gastric
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)colorectal') AS Solid_Tumour__Gastrointestinal__Colorectal
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)anal') AS Solid_Tumour__Gastrointestinal__Anal
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)breast') AS Solid_Tumour__Breast
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)ovarian') AS Solid_Tumour__Genitourinary__Ovarian
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)endometrial') AS Solid_Tumour__Genitourinary__Endometrial
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)vulvar') AS Solid_Tumour__Genitourinary__Vulval
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)cervical') AS Solid_Tumour__Genitourinary__Cervical
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)vaginal') AS Solid_Tumour__Genitourinary__Vaginal
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)fallopian') AS Solid_Tumour__Genitourinary__Fallopian_Tube
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)peritoneal') AS Solid_Tumour__Genitourinary__Peritoneal
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)renal') AS Solid_Tumour__Genitourinary__Renal
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)bladder') AS Solid_Tumour__Genitourinary__Bladder
    , REGEXP_CONTAINS(trialMeshTerms.name, r'(?i)urologic neoplasms') AS Solid_Tumour__Genitourinary__Urothelial
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)prostate') AS Solid_Tumour__Genitourinary__Prostate
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)testicul') AS Solid_Tumour__Genitourinary__Testicular
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)penile') AS Solid_Tumour__Genitourinary__Penile
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)liver') AS Solid_Tumour__Hepatobiliary__Hepatic
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)pancrea') AS Solid_Tumour__Hepatobiliary__Pancreatic
    , REGEXP_CONTAINS(trialMeshTerms.name, r'(?i)(gallbladder neoplasms|biliary tract neoplasms)') AS Solid_Tumour__Hepatobiliary__Gallbladder_and_Biliary_Tract
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)(sarcoma|GIST)') AS Solid_Tumour__Sarcoma
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)skin*cell') OR REGEXP_CONTAINS(trialDiseases.name, r'(?i)melanoma') AS Solid_Tumour__Skin_cancer
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)thyroid') AS Solid_Tumour__Thyroid
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)mesothel') AS Solid_Tumour__Mesothelioma
    , REGEXP_CONTAINS(trialDiseases.name, r'(CNS|blastoma)') AS Solid_Tumour__Brain_and_Nervous_System
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)neuroendocr') AS Solid_Tumour__Neuroendocrine
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)unspeci*tumor') AS Solid_Tumour__Unspecified_Solid_Tumour
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)leukem') AS Haematological_Cancers__Leukaemia
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)lymphoma') AS Haematological_Cancers__Lymphoma
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)myeloma') AS Haematological_Cancers__Myeloma
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)myelodys') AS Haematological_Cancers__Myelodysplastic_Syndrome
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)myeloprolif') AS Haematological_Cancers__Myeloproliferative_Neoplasms
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)(unspecifi*hematol)') AS Haematological_Cancers__Unspecified_Haematological_Cancer
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)metastat') AS Metastatic_Cancer
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)supportive') AS Supportive_Care
    , REGEXP_CONTAINS(trialDiseases.name, r'(?i)(unspecified cancer|n/a)') AS Unspecified_Cancer
    , (REGEXP_CONTAINS(primaryDrugTherapeuticClasses, r'(?i)^gene therapy$') OR REGEXP_CONTAINS(primaryDrugTherapeuticClasses, r'(?i)gene delivery vector'))
        OR (REGEXP_CONTAINS(otherDrugTherapeuticClasses, r'(?i)^gene therapy$') OR REGEXP_CONTAINS(otherDrugTherapeuticClasses, r'(?i)gene delivery vector')) AS Cell_and_Gene_Therapy__Gene_Therapy
    , REGEXP_CONTAINS(primaryDrugTherapeuticClasses, r'(?i)(cellular therapy*stem cell)') OR REGEXP_CONTAINS(otherDrugTherapeuticClasses, r'(?i)(cellular therapy*stem cell)') AS Cell_and_Gene_Therapy__Cellular_Therapy__Stem_Cell
    , REGEXP_CONTAINS(primaryDrugTherapeuticClasses, r'(?i)(cellular therapy*other)') OR REGEXP_CONTAINS(otherDrugTherapeuticClasses, r'(?i)(cellular therapy*other)') AS Cell_and_Gene_Therapy__Cellular_Therapy__Other
    , REGEXP_CONTAINS(primaryDrugTherapeuticClasses, r'(?i)(cellular therapy*lymphocyte)') OR REGEXP_CONTAINS(otherDrugTherapeuticClasses, r'(?i)(cellular therapy*lymphocyte)') AS Cell_and_Gene_Therapy__Cellular_Therapy__Tumour_Infiltrating_Lymphocyte
    , REGEXP_CONTAINS(primaryDrugTherapeuticClasses, r'(?i)(cellular therapy*T cell)') OR REGEXP_CONTAINS(otherDrugTherapeuticClasses, r'(?i)(cellular therapy*T cell)') AS Cell_and_Gene_Therapy__Cellular_Therapy__T_Cell_Receptor
    , REGEXP_CONTAINS(primaryDrugTherapeuticClasses, r'(?i)chimaeric antigen') OR REGEXP_CONTAINS(otherDrugTherapeuticClasses, r'(?i)chimaeric antigen') AS Cell_and_Gene_Therapy__Chimaeric_Antigen_Receptor
    ,(
        (   
            REGEXP_CONTAINS(trialDiseases.name, r'(?i)(pediatric|children|infant|neonatal|adolescent|childhood|)')  # (?i) case insensitive 
            OR REGEXP_CONTAINS(trialDiseases.name, r'(?i)(neonatal brain|neonatal abstinence|infant respiratory)') -- won't return cos' they're for therapeutic areas of CNS and Autoimmune/Inflammation, not in Oncology 
        ) OR (
            REGEXP_CONTAINS(trialPatientSegments.name, r'(?i)juvenile')  
            AND NOT REGEXP_CONTAINS(trialPatientSegments.name, r'(?i)myoclonic/juvenile') -- won't return cos' is of disease - epilepsy
        ) OR (
            trialPatientCriteria.maxAge <= 21.0 
            OR REGEXP_CONTAINS(trialPatientCriteria.minAgeUnits, r'(?i)(days|weeks|months')  -- why?
            OR REGEXP_CONTAINS(trialPatientCriteria.maxAgeUnits, r'(?i)(days|weeks|months')
        )
    )
    OR (
        (
            REGEXP_CONTAINS(trialPatientPopulation, r'(?i)(pediatric|paediatric|childhood|juvenile)')
            OR REGEXP_CONTAINS(trialTitle, r'(?i)(pediatric|paediatric|childhood|juvenile)')
        )
        AND NOT REGEXP_CONTAINS(trialPatientPopulation, r'(?i)^(excluding children|father children|bear children)$')
    )
    OR (
            REGEXP_CONTAINS(trialTitle, r'(?i)infant|neonate|toddler') OR 
            REGEXP_CONTAINS(trialPatientPopulation, r'(?i)infant|neonate|toddler') OR 
            REGEXP_CONTAINS(trialTitle, r'(?i)^babies$') OR 
            REGEXP_CONTAINS(trialPatientPopulation, r'(?i)^babies$')
    )
    OR (
        REGEXP_CONTAINS(trialTitle, r'(?i)adolescen|teenage') OR
        REGEXP_CONTAINS(trialPatientPopulation, r'(?i)adolescen|teenage') OR
        REGEXP_CONTAINS(trialTitle, r'(?i)^youth$') OR
        REGEXP_CONTAINS(trialPatientPopulation, r'(?i)^youth$') OR 
        -- was getting bad results because teen/teens usually forms part of many words so must make sure there's space before after them when searching so they serve as standalones
        REGEXP_CONTAINS(trialTitle, r'(?i)^(teens|teen)$') OR
        REGEXP_CONTAINS(trialPatientPopulation, r'(?i)^(teens|teen)$')
    ) AS Paediatric_Cancers
    FROM 
        `oncasearch.Citeline.trial`
        , UNNEST(trialTherapeuticAreas) AS triTherapeuticAreas
        , UNNEST(triTherapeuticAreas.trialDiseases) AS trialDiseases
        , UNNEST(trialDiseases.trialMeshTerms) AS trialMeshTerms
        , UNNEST(trialDiseases.trialPatientSegments) AS trialPatientSegments
        , UNNEST(trialPrimaryDrugsTested) AS trialPrimaryDrugsTested
        , UNNEST(trialPrimaryDrugsTested.drugTherapeuticClasses) AS primaryDrugTherapeuticClasses
        LEFT JOIN UNNEST(trialOtherDrugsTested) AS trialOtherDrugsTested
        LEFT JOIN UNNEST(trialOtherDrugsTested.drugTherapeuticClasses) AS otherDrugTherapeuticClasses
)
,UnPivoted AS (
    SELECT
    *
    FROM Pivoted
    UNPIVOT(IsInGroup FOR TA IN (
        Solid_Tumour__Head_and_Neck
        , Solid_Tumour__Lung
        , Solid_Tumour__Gastrointestinal__Esophageal
        , Solid_Tumour__Gastrointestinal__Gastric
        , Solid_Tumour__Gastrointestinal__Colorectal
        , Solid_Tumour__Gastrointestinal__Anal
        , Solid_Tumour__Breast
        , Solid_Tumour__Genitourinary__Ovarian
        , Solid_Tumour__Genitourinary__Endometrial
        , Solid_Tumour__Genitourinary__Vulval
        , Solid_Tumour__Genitourinary__Cervical
        , Solid_Tumour__Genitourinary__Vaginal
        , Solid_Tumour__Genitourinary__Fallopian_Tube
        , Solid_Tumour__Genitourinary__Peritoneal
        , Solid_Tumour__Genitourinary__Renal
        , Solid_Tumour__Genitourinary__Bladder
        , Solid_Tumour__Genitourinary__Urothelial
        , Solid_Tumour__Genitourinary__Prostate
        , Solid_Tumour__Genitourinary__Testicular
        , Solid_Tumour__Genitourinary__Penile
        , Solid_Tumour__Hepatobiliary__Hepatic
        , Solid_Tumour__Hepatobiliary__Pancreatic
        , Solid_Tumour__Hepatobiliary__Gallbladder_and_Biliary_Tract
        , Solid_Tumour__Sarcoma
        , Solid_Tumour__Skin_cancer
        , Solid_Tumour__Thyroid
        , Solid_Tumour__Mesothelioma
        , Solid_Tumour__Brain_and_Nervous_System
        , Solid_Tumour__Neuroendocrine
        , Solid_Tumour__Unspecified_Solid_Tumour
        , Haematological_Cancers__Leukaemia
        , Haematological_Cancers__Lymphoma
        , Haematological_Cancers__Myeloma
        , Haematological_Cancers__Myelodysplastic_Syndrome
        , Haematological_Cancers__Myeloproliferative_Neoplasms
        , Haematological_Cancers__Unspecified_Haematological_Cancer
        , Metastatic_Cancer
        , Supportive_Care
        , Unspecified_Cancer
        , Cell_and_Gene_Therapy__Gene_Therapy
        , Cell_and_Gene_Therapy__Cellular_Therapy__Stem_Cell
        , Cell_and_Gene_Therapy__Cellular_Therapy__Other
        , Cell_and_Gene_Therapy__Cellular_Therapy__Tumour_Infiltrating_Lymphocyte
        , Cell_and_Gene_Therapy__Cellular_Therapy__T_Cell_Receptor
        , Cell_and_Gene_Therapy__Chimaeric_Antigen_Receptor
        , Paediatric_Cancers
        )
    )
)
,TASplit AS (
    SELECT 
    trialId
    , split(TA, '__') AS TA -- replace __ with : then replace _ with spcae
    , IsInGroup
    FROM UnPivoted
    WHERE IsInGroup
)
, TASplit2 AS (
    SELECT DISTINCT
    trialId
    , REPLACE(TA[SAFE_OFFSET(0)], '_', ' ') AS indicationSuperType
    , REPLACE(TA[SAFE_OFFSET(1)], '_', ' ') AS indicationMainType
    , REPLACE(TA[SAFE_OFFSET(2)], '_', ' ') AS indicationSubType
    FROM TASplit
)

SELECT 
*
from TASplit2
