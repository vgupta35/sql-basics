/**************************************************************************************************/
/****************************View Based ETL for Canada Coverage Assessment*************************/
/**************************************************************************************************/

-----------------------------------TABLE OF CONTENTS------------------------------------------------
/* I. Indicator Staging */
--A. TStagingIndicator
--B. VTIndicator
--C. VTTheme
--D. VTIndicatorXTheme
--E. VTModifier
--F. VTIndicatorXModifier
--G. VTRule
--H. VTIndicatorXRule
--I. TThemeDescription


/* II. Product Staging */
--A. TProduct
--B.TIndicatorXProductGroup
---XB. TIndicatorXProduct (No Longer Used)
--C. VIndicator
--D. View Overwrite



/* III. Display Data */
--A. Indicator Inventory
--B. Pivot Charts
--C. Unmapped Products
--D. High Risk Non-Conducive (not used)
--E. Rule Mapping Master
--F. Products with Data Feed (not used)
--G. Product Rule Coverage (not used)
--H. Expanded Rule Mapping
--I. Theme Description
--J. Indicators, Themes, and Products
--K. Products and Rules



/**************************************************************************************************/
/***********************************I. Indicator Staging*******************************************/
/**************************************************************************************************/

/*

This section details the process of breaking the raw indicator data (Staging Table)
into seaprate portions to apply ETL (Extract Transform Load).
Views(V) applied to most tables to automate ETL process of broken down parts.




							(V)TIndicator	
							(V)TTheme				(V)TindicatorXTheme
TIndicatorStaging  ---> 	(V)TModifier			(V)TindicatorXModifier
							(V)TRule				TIndicatorXRule
							TThemeDescription	
							
							
							
							
*/ 


/**************************************************************************************************/
--A. TStagingIndicator
/**************************************************************************************************/

/*

This staging table represents the latest raw indicator data to be imported 
and broken into separate tables.


Note: The source of this staging table was imported using Excel/Access. 
Consider the data types below prior to loading in. 

*/

--DROP TABLE [dbo].[TStagingIndicator]
CREATE TABLE [dbo].[TStagingIndicator]
(
	[Segment] [nvarchar] (50) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[IndicatorID] [int] IDENTITY(1,1) NOT NULL,
	[IndicatorRefID] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Red Flag Theme 1] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Red Flag Theme 2] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Red Flag Theme 3] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Navigant Rule Template] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Indicator] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Priority] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[IsApplicableToBank] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[IsConduciveToAutomatedMonitoring] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[OracleRule] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[InOracleMVP1] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[CoverageInclOracleMVP1AndFortent] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Logical Scenario] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[BusinessLine] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Rule ID] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[NavigantRule] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Rule Priority] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Modifiers] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Rule Psuedocode] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ProposedRuleName] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FortentCoverage] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FortentRule] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Missing Parameter in Mantas Scenario, If any] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Expected Coverage] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[RevisionHistory] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[RuleCoverageNotes] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[FortentRuleCoverageNotes] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[IsDuplicate] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
SET IDENTITY_INSERT [dbo].[TStagingIndicator] ON;
INSERT INTO [dbo].[TStagingIndicator]
(
    Segment,
    IndicatorID,
    IndicatorRefID,
    [Red Flag Theme 1],
    [Red Flag Theme 2],
    [Red Flag Theme 3],
    [Navigant Rule Template],
    Indicator,
    Priority,
    IsApplicableToBank,
    IsConduciveToAutomatedMonitoring,
    OracleRule,
    InOracleMVP1,
    CoverageInclOracleMVP1AndFortent,
    [Logical Scenario],
    BusinessLine,
    [Rule ID],
    NavigantRule,
    [Rule Priority],
    Modifiers,
    [Rule Psuedocode],
    ProposedRuleName,
    FortentCoverage,
    FortentRule,
    [Missing Parameter in Mantas Scenario, If any],
    [Expected Coverage],
    RevisionHistory,
    RuleCoverageNotes,
    FortentRuleCoverageNotes, 
	IsDuplicate
)


SELECT
	[Segment],
	ID AS IndicatorID,
	[Indicator ID] AS IndicatorRefID,
	[Red Flag Theme 1],
	[Red Flag Theme 2],
	[Red Flag Theme 3],
	[Navigant Rule Template],
	[Indicator],
	[Priority], --Limit 1-3
	[Applicable to BNS?] AS IsApplicableToBank,
	[Conducive to Automated Monitoring?] AS IsConduciveToAutomatedMonitoring, --Limit T/F
	CONVERT(nvarchar(MAX),[Proposed Oracle Scenario]) AS OracleRule,
	[MVP1?] AS InOracleMVP1, --Limit to T/F, no N/A
	[Coverage After MVP1] AS CoverageInclOracleMVP1AndFortent,
	[Logical Scenario],
	[Business Line] AS BusinessLine,
	[Rule ID],
	[Rule Name] AS NavigantRule,
	[Rule Priority],
	[Modifiers],
	[Rule Psuedocode],
	[Proposed Rule Name] AS ProposedRuleName,
	[Current Fortent Coverage Level] AS FortentCoverage,
	[Fortent Rule (s) providing coverage] AS FortentRule,
	[Missing Parameter in Mantas Scenario, If any],
	[Expected Coverage],
	[Revision History] AS RevisionHistory,
	[Notes on Oracle Coverage] AS RuleCoverageNotes,
	[Notes on Fortent Coverage] AS FortentRuleCoverageNotes, 
	[IsDuplicate] AS IsDuplicate
--Insert Updated Base Indicator Table Here
FROM [dbo].[XTIndicatorCA20181116];

SET IDENTITY_INSERT TStagingIndicator OFF;

--SELECT * FROM TStagingIndicator;

/**************************************************************************************************/
--B. VTIndicator
/**************************************************************************************************/

/*

This view takes all indicator related information from staging
and applies transformations as necessary. Like all other views 
after it, it extracts information from the raw staging table.

*/ 

ALTER VIEW VTIndicator AS

SELECT
	IndicatorID, 
	CASE WHEN IsConduciveToAutomatedMonitoring = 'Wealth' THEN 'Retail/Wealth' ELSE Segment END AS Segment,
	--[# Indicator Row],
	CONVERT(VARCHAR(50),IndicatorRefID) AS IndicatorRefID, 
	CONVERT(NVARCHAR(2000),Indicator) AS Indicator,
	CONVERT(INT,Priority) AS [Priority], --Limit 1-3
	CASE WHEN IsApplicableToBank IN ('Y') THEN 1 WHEN IsApplicableToBank IN ('N','N/A') THEN 0 ELSE NULL END AS IsApplicableToBank,
	CASE WHEN IsConduciveToAutomatedMonitoring IN ('Y', 'Wealth') THEN 1 
		WHEN IsConduciveToAutomatedMonitoring IN ('N', 'N/A') THEN 0 END AS IsConduciveToAutomatedMonitoring, --Limit T/F
	CASE WHEN InOracleMVP1 = 'Y' THEN 1 ELSE 0 END AS InOracleMVP1, --Limit to T/F, no N/A
	CASE WHEN CoverageInclOracleMVP1AndFortent NOT IN ('Full', 'Partial') THEN NULL ELSE CoverageInclOracleMVP1AndFortent END AS CoverageInclOracleMVP1AndFortent,
	CASE WHEN FortentCoverage IN ('Full', 'Full?', 'Y') THEN 'Full' WHEN FortentCoverage IN ('Partial') THEN 'Partial' ELSE NULL END AS FortentCoverage,
	--FortentCoverageByRule,
	--[Logical Scenario] AS LogicalScenario,
	BusinessLine,
	RevisionHistory,
	RuleCoverageNotes,
	FortentRuleCoverageNotes, 
	CASE IsDuplicate WHEN 'Y' THEN 1 WHEN 'N' THEN 0 END AS IsDuplicate
FROM [dbo].[TStagingIndicator];

/**************************************************************************************************/
--C. VTTheme
/**************************************************************************************************/

/*

This view extracts all possible themes from the Red Flag Themes columns in staging.
The values are standardized and assigned an ID. 

*/ 


ALTER VIEW dbo.VTTheme AS

WITH TempData AS (
	SELECT
	DISTINCT Theme
	FROM [dbo].[TStagingIndicator]
	UNPIVOT (Theme for ThemePrior IN 
		(
		[Red Flag Theme 1], 
		[Red Flag Theme 2], 
		[Red Flag Theme 3]
		)) upvt
)

, ThemeTemp AS (

	----------------------------------------------------------------------------------------------------
	SELECT 'Watch List' AS ThemeInitial					, 'Watch Lists/Keywords' AS NewTheme UNION ALL
	SELECT 'Watch Lists'								, 'Watch Lists/Keywords' UNION ALL
	SELECT 'Watch Lists/Keywords'						, 'Watch Lists/Keywords' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Structuring'								, 'Structuring/Threshold Avoidance' UNION ALL
	SELECT 'Structuring/ Threshold Avoidance'			, 'Structuring/Threshold Avoidance' UNION ALL
	SELECT 'Threshold Avoidance'						, 'Structuring/Threshold Avoidance' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Third Parties'								, 'Third Party Payment' UNION ALL
	SELECT 'Third Party Payment'						, 'Third Party Payment' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Loans'										, 'Loan' UNION ALL
	SELECT 'Loan '										, 'Loan' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Profiling'									, 'Profiling' UNION ALL
	SELECT 'Historical Deviation'						, 'Profiling' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Rapid Movement of Funds'					, 'Velocity' UNION ALL
	SELECT 'Velocity'									, 'Velocity' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT '?'											, 'N/A' UNION ALL
	SELECT 'N/A'										, 'N/A' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'High Risk Geography (HRG)'					, 'High Risk Geography (HRG)' UNION ALL
	SELECT 'Geography '									, 'High Risk Geography (HRG)' UNION ALL
	SELECT 'High Risk Jurisdictions'					, 'High Risk Geography (HRG)' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Insurance'									, 'Insurance' UNION ALL
	SELECT 'Insurance - Borrowing Against New Policy'	, 'Insurance' UNION ALL
	SELECT 'Insurance - Canceled Policy'				, 'Insurance'
)

, RevisedTheme AS (

SELECT DISTINCT 
	COALESCE(ThemeTemp.NewTheme, TempData.Theme) AS Theme 
FROM TempData
LEFT JOIN ThemeTemp ON TempData.Theme = ThemeTemp.ThemeInitial
)

SELECT ROW_NUMBER() OVER (ORDER BY Theme) ThemeID, Theme 
FROM RevisedTheme;

--SELECT * FROM VTTheme;

/**************************************************************************************************/
--D. VTIndicatorXTheme
/**************************************************************************************************/

/*

This view ties standardized themes to the indicators. 
Theme Priority is assigned based on which Red Flag Theme field the value was pulled from. 

*/ 




ALTER VIEW dbo.VTIndicatorXTheme AS

WITH ThemeTemp AS (

	----------------------------------------------------------------------------------------------------
	SELECT 'Watch List' AS ThemeInitial					, 'Watch Lists/Keywords' AS NewTheme UNION ALL
	SELECT 'Watch Lists'								, 'Watch Lists/Keywords' UNION ALL
	SELECT 'Watch Lists/Keywords'						, 'Watch Lists/Keywords' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Structuring'								, 'Structuring/Threshold Avoidance' UNION ALL
	SELECT 'Structuring/ Threshold Avoidance'			, 'Structuring/Threshold Avoidance' UNION ALL
	SELECT 'Threshold Avoidance'						, 'Structuring/Threshold Avoidance' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Third Parties'								, 'Third Party Payment' UNION ALL
	SELECT 'Third Party Payment'						, 'Third Party Payment' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Loans'										, 'Loan' UNION ALL
	SELECT 'Loan '										, 'Loan' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Profiling'									, 'Profiling' UNION ALL
	SELECT 'Historical Deviation'						, 'Profiling' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Rapid Movement of Funds'					, 'Velocity' UNION ALL
	SELECT 'Velocity'									, 'Velocity' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT '?'											, 'N/A' UNION ALL
	SELECT 'N/A'										, 'N/A' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'High Risk Geography (HRG)'					, 'High Risk Geography (HRG)' UNION ALL
	SELECT 'Geography '									, 'High Risk Geography (HRG)' UNION ALL
	SELECT 'High Risk Jurisdictions'					, 'High Risk Geography (HRG)' UNION ALL
	----------------------------------------------------------------------------------------------------
	SELECT 'Insurance'									, 'Insurance' UNION ALL
	SELECT 'Insurance - Borrowing Against New Policy'	, 'Insurance' UNION ALL
	SELECT 'Insurance - Canceled Policy'				, 'Insurance'
)


, TempData AS (
	SELECT
	DISTINCT IndicatorID, Theme.ThemeID, CAST(SUBSTRING(ThemePrior, 16, 1) AS int) AS Priority
	FROM [dbo].[TStagingIndicator]
	UNPIVOT (Theme for ThemePrior IN 
		(
		[Red Flag Theme 1], 
		[Red Flag Theme 2], 
		[Red Flag Theme 3]
		)) upvt
	LEFT JOIN ThemeTemp ON upvt.Theme = ThemeTemp.ThemeInitial
	LEFT JOIN VTTheme Theme ON Theme.Theme = COALESCE(ThemeTemp.NewTheme, upvt.Theme)
) 


SELECT ROW_NUMBER() OVER (ORDER BY IndicatorID, ThemeID) IndicatorXThemeID, TempData.* FROM TempData;

--SELECT * FROM VTIndicatorXTheme;

/**************************************************************************************************/
--E. VTModifier
/**************************************************************************************************/

/*

This view extracts all possible modifiers from the modifier column in staging.
Semicolons imposed to multiple modifiers per row. Subsequently split to extract full value.
The values are standardized and assigned an ID. 

*/ 



ALTER VIEW dbo.VTModifier AS

WITH TempData AS (
   SELECT --IndicatorID, 
   CONVERT(XML,'<x>'+REPLACE(REPLACE(Modifiers, ',', ';'),';','</x><x>')+'</x>') AS XMLData
   FROM [dbo].TStagingIndicator
   WHERE Modifiers IS NOT NULL
)

, Modifiers AS (
	SELECT DISTINCT --IndicatorID, 
	LTRIM(RTRIM(Element.Loc.value('.','varchar(50)'))) AS [ModifierName]
	FROM   TempData  
	CROSS APPLY XMLData.nodes('/x') as Element(Loc)
)

SELECT ROW_NUMBER() OVER (ORDER BY [ModifierName]) ModifierID, ModifierName FROM Modifiers;

--SELECT * FROM VTModifier;

/**************************************************************************************************/
--F. VTIndicatorXModifier
/**************************************************************************************************/

/*

This view ties standardized modifiers to the indicators. 

*/ 



ALTER VIEW dbo.VTIndicatorXModifier AS

WITH TempData AS
(
   SELECT 
		IndicatorID, 
		Modifiers,
   CONVERT(XML,'<x>'+REPLACE(REPLACE(Modifiers, ',', ';'),';','</x><x>')+'</x>') AS XMLData
   FROM dbo.TStagingIndicator
   WHERE Modifiers IS NOT NULL
)
--INSERT INTO dbo.TIndicatorXModifier (IndicatorID, ModifierID) 

, IndMod AS (
	SELECT 
		IndicatorID
		, LTRIM(RTRIM(Element.Loc.value('.','varchar(50)'))) as ModifierName
	FROM TempData
	CROSS APPLY XMLData.nodes('/x') as Element(Loc)
)

SELECT 
	ROW_NUMBER() OVER (ORDER BY IndicatorID, ModifierID) AS IndicatorXModifierID, 
	IndicatorID, 
	ModifierID
FROM   IndMod
LEFT JOIN VTModifier ON IndMod.ModifierName = VTModifier.ModifierName
WHERE IndMod.ModifierName IS NOT NULL;

--SELECT * FROM VTIndicatorXModifier;

/**************************************************************************************************/
--G. VTRule
/**************************************************************************************************/

/* 

This view extracts all rules from Fortent, Navigant, and Oracle in the staging table. 
Semicolons imposed to multiple rules per row. Subsequently split to extract full value.
The values are standardized, categorized (Oracle, Navigant. etc.) and assigned an ID.

The main use of this table is to identify the Oracle Template 
to apply in place of the Fortent decomission. 

*/


ALTER VIEW dbo.VTRule AS
WITH OracleRules AS (
	SELECT 'Anticipatory Profile - Expected Activity' AS RuleName					, 'Anomalies in Behavior' AS Category, 'Expected vs Actual Transactional Levels: Deviation from Profile/Expected vs Actual Transactional Levels: Large Transactions' AS ProposedTypology UNION ALL
	SELECT 'Anticipatory Profile - Income'											, 'Anomalies in Behavior'						, 'Expected vs Actual Transactional Levels: Deviation from Profile/Expected vs Actual Transactional Levels: Large Transactions' UNION ALL
	SELECT 'Anticipatory Profile - Source Of Funds'									, 'Anomalies in Behavior'						, 'Expected vs Actual Transactional Levels: Deviation from Profile' UNION ALL
	SELECT 'CIB: Foreign Activity'													, 'Anomalies in Behavior'						, 'Expected vs Actual Transactional Levels: Deviation from Profile' UNION ALL
	SELECT 'CIB: High Risk Geography Activity'										, 'Anomalies in Behavior'						, 'Change in Behavior: High Risk Geography Activity' UNION ALL
	SELECT 'CIB: Product Utilization Shift'											, 'Anomalies in Behavior'						, 'Change in Behavior: Product Channel/Utilization Shift' UNION ALL
	SELECT 'CIB: Significant Change from Previous Average Activity'					, 'Anomalies in Behavior'						, 'Change in Behavior: Significant Change from Previous Average Activity' UNION ALL
	SELECT 'CIB: Significant Change From Previous Peak Activity'					, 'Anomalies in Behavior'						, 'Change in Behavior: Significant Change from Previous Peak Activity' UNION ALL
	SELECT 'Deposits/Withdrawals in Same or Similar Amounts'						, 'Anomalies in Behavior'						, 'Flow through of Funds: Rapid Movement of Incoming / Outgoing Funds' UNION ALL
	SELECT 'Deviation From Peer Group - Product Utilization'						, 'Anomalies in Behavior'						, 'Change in Behavior: Product Channel/Utilization Shift' UNION ALL
	SELECT 'Deviation from Peer Group - Total Activity'								, 'Anomalies in Behavior'						, 'Change in Behavior: Significant Change from Previous Peak Activity' UNION ALL
	SELECT 'Escalation in Inactive Account'											, 'Anomalies in Behavior'						, 'Change in Behavior: Escalation of Inactive Account' UNION ALL
	SELECT 'Large Depreciation of Account Value'									, 'Anomalies in Behavior'						, 'Expected vs Actual Transactional Levels: Large Transactions.' UNION ALL
	SELECT 'Rapid Movement Of Funds - All Activity'									, 'Anomalies in Behavior'						, 'Flow through of Funds: Rapid Movement of Incoming / Outgoing Funds' UNION ALL
	SELECT 'Rapid Movement Of Funds - Funds Transfers'								, 'Anomalies in Behavior'						, 'Flow through of Funds: Rapid Movement of Incoming / Outgoing Funds' UNION ALL
	SELECT 'Terrorist Financing'													, 'Anomalies in Behavior'						, 'Anomalies in Behavior' UNION ALL
	SELECT 'Transactions In Round Amounts'											, 'Anomalies in Behavior'						, 'Round Amount Activity: Transactions in Round Amounts' UNION ALL
	SELECT 'Anomalies in ATM, Bank Card: Excessive Withdrawals'						, 'ATM, Debit, Bank Card, and Credit Scenarios'	, 'Change in Behavior: Product Channel/Utilization Shift' UNION ALL
	SELECT 'Anomalies In ATM, Bank Card: Foreign Transactions'						, 'ATM, Debit, Bank Card, and Credit Scenarios'	, 'Change in Behavior: High Risk Geography Activity' UNION ALL
	SELECT 'Anomalies In ATM, Bank Card: Structured Cash Deposits'					, 'ATM, Debit, Bank Card, and Credit Scenarios'	, 'Structuring: Structuring' UNION ALL
	SELECT 'Early Payoff or Paydown of a Credit Product'							, 'ATM, Debit, Bank Card, and Credit Scenarios'	, 'Early Payoff / Pay Down: Early Payoff / Paydown' UNION ALL
	SELECT 'Rapid Loading And Redemption Of Stored Value Cards'						, 'ATM, Debit, Bank Card, and Credit Scenarios'	, 'Flow through of Funds: Rapid Movement of Incoming / Outgoing Funds' UNION ALL
	SELECT 'Custom Scenario'														, 'Custom Scenario'								, 'Custom Scenario' UNION ALL
	SELECT 'Address Associated with Multiple, Recurring External Entities'			, 'Hidden Relationships'						, 'Recurring Relationship: Recurring Originators / Beneficiaries' UNION ALL
	SELECT 'External Entity Associated With Multiple, Recurring Addresses'			, 'Hidden Relationships'						, 'Recurring Relationship: Recurring Originators / Beneficiaries' UNION ALL
	SELECT 'External Entity Identifier Associated With Multiple, Recurring Names'	, 'Hidden Relationships'						, 'Recurring Relationship: Recurring Originators / Beneficiaries' UNION ALL
	SELECT 'External Entity Name Associated With Multiple, Recurring Identifiers'	, 'Hidden Relationships'						, 'Recurring Relationship: Recurring Originators / Beneficiaries' UNION ALL
	SELECT 'Journals Between Unrelated Accounts'									, 'Hidden Relationships'						, 'Transactions without Economic Value: Transactions with Unknown Purpose/Third Parties and Intermediaries: Third Party Transactions' UNION ALL
	SELECT 'Known Remitter/Beneficiary Names In Checks, Monetary Instruments'		, 'Hidden Relationships'						, 'Recurring Relationship: Recurring Originators / Beneficiaries' UNION ALL
	SELECT 'Networks Of Accounts, Entities, And Customers'							, 'Hidden Relationships'						, 'Hidden Relationship: Networks of Hidden Relationships' UNION ALL
	SELECT 'Pattern of Funds Transfers Between Correspondent Banks'					, 'Hidden Relationships'						, 'Hidden Relationship: Patterns of Funds Transfers' UNION ALL
	SELECT 'Patterns Of Funds Transfers Between Customers And External Entities'	, 'Hidden Relationships'						, 'Hidden Relationship: Patterns of Funds Transfers' UNION ALL
	SELECT 'Patterns of Funds Transfers Between Internal Accounts and Customers'	, 'Hidden Relationships'						, 'Hidden Relationship: Patterns of Funds Transfers' UNION ALL
	SELECT 'Patterns Of Recurring Originators/Beneficiaries In Funds Transfers'		, 'Hidden Relationships'						, 'Hidden Relationship: Patterns of Funds Transfers' UNION ALL
	SELECT 'Unknown Remitter/Beneficiary Names In Checks, Monetary Instruments'		, 'Hidden Relationships'						, 'Third Parties and Intermediaries: Third Party Transactions' UNION ALL
	SELECT 'High Risk Transactions: Focal High Risk Entity'							, 'High Risk Geographies and Entities'			, 'High Risk Entity: High Risk Entities' UNION ALL
	SELECT 'High Risk Transactions: High Risk Counter Party'						, 'High Risk Geographies and Entities'			, 'High Risk Entity: High Risk Entities/High Risk Geography: High Risk Geography' UNION ALL
	SELECT 'High Risk Transactions: High Risk Geography'							, 'High Risk Geographies and Entities'			, 'High Risk Geography: High Risk Geography' UNION ALL
	SELECT 'Hub and Spoke'															, 'Hub and Spoke'								, 'Hidden Relationship: Patterns of Funds Transfers' UNION ALL
	SELECT 'CIB: Inactive To Active Customers'										, 'Institutional Anti Money Laundering Scenarios', 'Change in Behavior: Escalation of Inactive Account' UNION ALL
	SELECT 'CIB: Significant Change in Trade/Transaction Activity'					, 'Institutional Anti Money Laundering Scenarios', 'CIB: Significant Change in Trade/Transaction Activity' UNION ALL
	SELECT 'Customers Engaging in Offsetting Trades'								, 'Institutional Anti Money Laundering Scenarios', 'Transactions without Economic Value: Transactions with Unknown Purpose' UNION ALL
	SELECT 'Frequent Changes To Instructions'										, 'Institutional Anti Money Laundering Scenarios', 'Instruction Amendments or Re-submissions: Frequent Changes to Instructions' UNION ALL
	SELECT 'Hidden Relationships'													, 'Institutional Anti Money Laundering Scenarios', 'Hidden Relationship: Networks of Hidden Relationships/Hidden Relationship: Patterns of Funds Transfers' UNION ALL
	SELECT 'High Risk Electronic Transfers'											, 'Institutional Anti Money Laundering Scenarios', 'High Risk Entity: High Risk Entities' UNION ALL
	SELECT 'High Risk Instructions'													, 'Institutional Anti Money Laundering Scenarios', 'Institutional Anti Money Laundering Scenarios' UNION ALL
	SELECT 'Manipulation of Account/Customer Data Followed by Instruction Changes'	, 'Institutional Anti Money Laundering Scenarios', 'Instruction Amendments or Re-submissions: Frequent Changes to Instructions' UNION ALL
	SELECT 'Movement of Funds without Corresponding Trade'							, 'Institutional Anti Money Laundering Scenarios', 'Attempt to Conceal Identity: Suspicious Business Structure Indicators' UNION ALL
	SELECT 'Trades In Securities With Near-Term Maturity, Exchange Of Assets'		, 'Institutional Anti Money Laundering Scenarios', 'Transactions without Economic Value: Trades in Securities with Near-Term Maturity, Exchange of Assets' UNION ALL
	SELECT 'Change In Beneficiary/Owner Followed By Surrender'						, 'Insurance Scenarios'							, 'Attempt to Conceal Identity: Frequent Change of Beneficiaries' UNION ALL
	SELECT 'Customer Borrowing Against New Policy'									, 'Insurance Scenarios'							, 'Insurance Scenarios' UNION ALL
	SELECT 'Insurance Policies with Refunds'										, 'Insurance Scenarios'							, 'Insurance Scenarios' UNION ALL
	SELECT 'Policies with Large Early Removal'										, 'Insurance Scenarios'							, 'Early Payoff / Pay Down: Early Payoff / Paydown' UNION ALL
	SELECT 'Externally Matched Names'												, 'Other Money Laundering Behaviors'			, 'Other Money Laundering Behaviors' UNION ALL
	SELECT 'Large Reportable Transactions'											, 'Other Money Laundering Behaviors'			, 'Expected vs Actual Transactional Levels: Large Transactions' UNION ALL
	SELECT 'Patterns of Sequentially Numbered Checks, Monetary Instruments'			, 'Other Money Laundering Behaviors'			, 'Sequentially Numbered Monetary Instruments: Sequentially Numbered Monetary Instruments' UNION ALL
	SELECT 'Single Or Multiple Cash Transactions: Large Significant Transactions'	, 'Other Money Laundering Behaviors'			, 'Expected vs Actual Transactional Levels: Large Transactions' UNION ALL
	SELECT 'Single Or Multiple Cash Transactions: Possible CTR'						, 'Other Money Laundering Behaviors'			, 'Expected vs Actual Transactional Levels: Large Transactions' UNION ALL
	SELECT 'Structuring: Avoidance of Reporting Thresholds'							, 'Other Money Laundering Behaviors'			, 'Structuring: Structuring' UNION ALL
	SELECT 'Structuring: Deposits/Withdrawals Of Mixed Monetary Instruments'		, 'Other Money Laundering Behaviors'			, 'Structuring: Structuring' UNION ALL
	SELECT 'Structuring: Potential Structuring In Cash And Equivalents'				, 'Other Money Laundering Behaviors'			, 'Structuring: Structuring'
)

, TempOracleMapping AS (
	SELECT 'CIB - Deviation from Previous Average Activity' 					AS IndicatorName, 'CIB: Significant Change From Previous Average Activity' AS OracleName UNION ALL
	SELECT 'CIB - Product Utilization Shift' 									AS IndicatorName, 'CIB: Product Utilization Shift' AS OracleName UNION ALL
	SELECT 'CIB: Product Utlization Shift' 										AS IndicatorName, 'CIB: Product Utilization Shift' AS OracleName UNION ALL
	SELECT 'Deviation from Peer Group -Total Activity' 							AS IndicatorName, 'Deviation From Peer Group - Total Activity' AS OracleName UNION ALL
	SELECT 'Deviation from Peer Group: Total Activity' 							AS IndicatorName, 'Deviation From Peer Group - Total Activity' AS OracleName UNION ALL
	SELECT 'Deviation from Peer Group: Product Utilization' 					AS IndicatorName, 'Deviation From Peer Group - Product Utilization' AS OracleName UNION ALL
	SELECT 'High Risk Transactions: High Risk Focal Entity' 					AS IndicatorName, 'High Risk Transactions: High Risk Counter Party' AS OracleName UNION ALL
	SELECT 'Large Reportable Transactions [Customer Focus]' 					AS IndicatorName, 'Large Reportable Transactions' AS OracleName UNION ALL
	SELECT ' Large Reportable Transactions'										AS IndicatorName, 'Large Reportable Transactions' AS OracleName UNION ALL
	SELECT 'Networks of Accounts, Entities and Customers' 						AS IndicatorName, 'Networks Of Accounts, Entities, And Customers' AS OracleName UNION ALL
	SELECT 'Pattern of Funds Transfers between Internal Accounts and Customers' AS IndicatorName, 'Patterns Of Funds Transfers Between Internal Accounts And Customers' AS OracleName UNION ALL
	SELECT 'Pattern of Funds Transfers between Customers and External Entities' AS IndicatorName, 'Patterns of Funds Transfers between Customers and External Entities' AS OracleName UNION ALL
	SELECT 'Rapid Movement of Funds – All Activity' 							AS IndicatorName, 'Rapid Movement Of Funds - All Activity' AS OracleName UNION ALL
	SELECT 'Rapid Movement Funds - All Activity' 								AS IndicatorName, 'Rapid Movement Of Funds - All Activity' AS OracleName UNION ALL
	SELECT '1. Patterns of Sequentially Numbered Checks, Monetary Instruments 2. Manual Control' AS IndicatorName, 'Patterns of Sequentially Numbered Checks, Monetary Instruments' AS OracleName UNION ALL
	SELECT '1. Patterns of Sequentially Numbered Checks, Monetary Instruments 2.  Manual Control' AS IndicatorName, 'Patterns of Sequentially Numbered Checks, Monetary Instruments' AS OracleName UNION ALL
	SELECT '1. Patterns of Sequentially Numbered Checks, Monetary Instruments2.  Manual Control' AS IndicatorName, 'Patterns of Sequentially Numbered Checks, Monetary Instruments' AS OracleName UNION ALL
	SELECT 'Deposits/Withdrawals in Same/Similar Amounts'						AS IndicatorName, 'Deposits/Withdrawals In Same Or Similar Amounts' AS OracleName UNION ALL
	SELECT 'Automated PUPID report' 											AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Custom' 															AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Custom Scenario' 													AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Large Credit Refunds (requires customization of scenario)' 			AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Large Positive Credit Card Balances (requires customization)' 		AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Manual control' 													AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Missing Counter Party Details (Custom Scenario)' 					AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Missing Counter Party Details (Requires Customization of Scenario)' AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Multiple Jurisdictions (custom scenario)' 							AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Multiple Jurisdictions (Requires Customization of Scenario)' 		AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Nested Correspondent Rule (Requires Customization of Scenario)' 	AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Nested Correspondent Rule  (Requires Customization of Scenario)'	AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'No corresponding Mantas scenario' 									AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Wire Structuring (Requires Customization of Scenario)' 				AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Wire Structuring  (Requires Customization of Scenario)'				AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Wire Structuring (requires customization of scenario)' 				AS IndicatorName, 'Custom Scenario' AS OracleName 
)

, RuleTempData AS (

	SELECT 'Navigant' AS Source,CONVERT(XML,'<x>'+REPLACE(NavigantRule,';','</x><x>')+'</x>') AS XMLData FROM dbo.TStagingIndicator WHERE NavigantRule IS NOT NULL UNION ALL
	SELECT 'Oracle', 			CONVERT(XML,'<x>'+REPLACE(OracleRule,';','</x><x>')+'</x>') FROM dbo.TStagingIndicator WHERE OracleRule IS NOT NULL UNION ALL
	SELECT 'Fortent', 			CONVERT(XML,'<x>'+REPLACE(FortentRule,';','</x><x>')+'</x>')FROM dbo.TStagingIndicator WHERE FortentRule IS NOT NULL

)

, CrossApply AS (
	SELECT DISTINCT
		Source, 
		REPLACE(REPLACE(LTRIM(RTRIM(Element.Loc.value('.','varchar(255)'))),CHAR(13), ''),CHAR(10),'') AS RuleName
	FROM RuleTempData
	CROSS APPLY XMLData.nodes('/x') as Element(Loc)
	WHERE Element.Loc.value('.','varchar(255)') NOT IN ('N/A', ' ')
)

, ReFiltered AS (
	SELECT DISTINCT 
		CASE WHEN CrossApply.Source = 'Oracle' THEN COALESCE(TempMap.OracleName,CrossApply.RuleName)
		ELSE CrossApply.RuleName END AS RuleName, 
		CrossApply.Source
	FROM CrossApply
	LEFT JOIN TempOracleMapping TempMap 
		ON TempMap.IndicatorName = CrossApply.RuleName AND CrossApply.Source = 'Oracle'


)

SELECT 
	ROW_NUMBER() OVER (ORDER BY OracleRules.Category, Rules.RuleName) AS RuleID
	, Rules.RuleName
	, OracleRules.Category
	, Rules.Source
	, OracleRules.ProposedTypology
FROM
	(
	SELECT RuleName, 'Oracle' AS Source FROM OracleRules UNION
	SELECT RuleName, Source 			FROM ReFiltered) Rules
LEFT JOIN OracleRules ON Rules.RuleName = OracleRules.RuleName AND Rules.Source = 'Oracle';

--SELECT * FROM dbo.VTRule;


/**************************************************************************************************/
--H. VTIndicatorXRule
/**************************************************************************************************/

/*

This table ties all sourced rules to indicators, but also assigns a proposed rule as put forward 
by the Fortent Decomission project plan wich maps to the original rules. Each of these proposed 
rules are assigned a coverage value to represent if their Oracle counterpart has been implemented.
This table also maps similar proposed rules to behavioral themes for higher level analysis. 

Notes: 
	- Due to memory issues, this code has two parts: 
		1. Update the Temp table. 
		2. Truncate and replace data. 
	- Prior to code, Excel Formula provided to convert Excel Tables 
		to SQL tables row by row for smaller tables to bypass import function.


*/



--ALTER VIEW dbo.VTIndicatorXRule AS 

--Trailing Space Excel to SQL Alignment
--="SELECT '"&A2&"'"&REPT(" ", 250-LEN(A2))&", '"&B2&"'"&REPT(" ", 100 - LEN(B2))&"UNION ALL"
--="SELECT '"&A2&"'"&REPT(" ", 75-LEN(A2))&", '"&B2&"'"&REPT(" ", 75 - LEN(B2))&", '"&C2&"'"&REPT(" ", 10-LEN(C2))&"UNION ALL"

DROP TABLE #TempMatch;

WITH TempData AS (
       
   SELECT IndicatorID 
		  , CONVERT(XML,'<x>' + REPLACE(ProposedRuleName,';','</x><x>') + '</x>') AS ProposedRuleNameXML
		  , CONVERT(XML,'<x>' + REPLACE(OracleRule,';','</x><x>') + '</x>') AS OracleRuleXML		  
		  , CONVERT(XML,'<x>' + REPLACE(NavigantRule,';','</x><x>') + '</x>') AS NavigantRuleXML
		  , CONVERT(XML,'<x>' + REPLACE(FortentRule,';','</x><x>') + '</x>') AS FortentRuleXML
		  
   FROM TStagingIndicator

)


, TempOracleMapping AS (
	SELECT 'CIB - Deviation from Previous Average Activity' 					AS IndicatorName, 'CIB: Significant Change From Previous Average Activity' AS OracleName UNION ALL
	SELECT 'CIB - Product Utilization Shift' 									AS IndicatorName, 'CIB: Product Utilization Shift' AS OracleName UNION ALL
	SELECT 'CIB: Product Utlization Shift' 										AS IndicatorName, 'CIB: Product Utilization Shift' AS OracleName UNION ALL
	SELECT 'Deviation from Peer Group -Total Activity' 							AS IndicatorName, 'Deviation From Peer Group - Total Activity' AS OracleName UNION ALL
	SELECT 'Deviation from Peer Group: Total Activity' 							AS IndicatorName, 'Deviation From Peer Group - Total Activity' AS OracleName UNION ALL
	SELECT 'Deviation from Peer Group: Product Utilization' 					AS IndicatorName, 'Deviation From Peer Group - Product Utilization' AS OracleName UNION ALL
	SELECT 'High Risk Transactions: High Risk Focal Entity' 					AS IndicatorName, 'High Risk Transactions: High Risk Counter Party' AS OracleName UNION ALL
	SELECT 'Large Reportable Transactions [Customer Focus]' 					AS IndicatorName, 'Large Reportable Transactions' AS OracleName UNION ALL
	SELECT ' Large Reportable Transactions'										AS IndicatorName, 'Large Reportable Transactions' AS OracleName UNION ALL
	SELECT 'Networks of Accounts, Entities and Customers' 						AS IndicatorName, 'Networks Of Accounts, Entities, And Customers' AS OracleName UNION ALL
	SELECT 'Pattern of Funds Transfers between Internal Accounts and Customers' AS IndicatorName, 'Patterns Of Funds Transfers Between Internal Accounts And Customers' AS OracleName UNION ALL
	SELECT 'Pattern of Funds Transfers between Customers and External Entities' AS IndicatorName, 'Patterns of Funds Transfers between Customers and External Entities' AS OracleName UNION ALL
	SELECT 'Rapid Movement of Funds – All Activity' 							AS IndicatorName, 'Rapid Movement Of Funds - All Activity' AS OracleName UNION ALL
	SELECT 'Rapid Movement Funds - All Activity' 								AS IndicatorName, 'Rapid Movement Of Funds - All Activity' AS OracleName UNION ALL
	SELECT '1. Patterns of Sequentially Numbered Checks, Monetary Instruments 2. Manual Control' AS IndicatorName, 'Patterns of Sequentially Numbered Checks, Monetary Instruments' AS OracleName UNION ALL
	SELECT '1. Patterns of Sequentially Numbered Checks, Monetary Instruments 2.  Manual Control' AS IndicatorName, 'Patterns of Sequentially Numbered Checks, Monetary Instruments' AS OracleName UNION ALL
	SELECT '1. Patterns of Sequentially Numbered Checks, Monetary Instruments2.  Manual Control' AS IndicatorName, 'Patterns of Sequentially Numbered Checks, Monetary Instruments' AS OracleName UNION ALL
	SELECT 'Deposits/Withdrawals in Same/Similar Amounts'						AS IndicatorName, 'Deposits/Withdrawals In Same Or Similar Amounts' AS OracleName UNION ALL
	SELECT 'Automated PUPID report' 											AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Custom' 															AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Custom Scenario' 													AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Large Credit Refunds (requires customization of scenario)' 			AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Large Positive Credit Card Balances (requires customization)' 		AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Manual control' 													AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Missing Counter Party Details (Custom Scenario)' 					AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Missing Counter Party Details (Requires Customization of Scenario)' AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Multiple Jurisdictions (custom scenario)' 							AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Multiple Jurisdictions (Requires Customization of Scenario)' 		AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Nested Correspondent Rule (Requires Customization of Scenario)' 	AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Nested Correspondent Rule  (Requires Customization of Scenario)'	AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'No corresponding Mantas scenario' 									AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Wire Structuring (Requires Customization of Scenario)' 				AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Wire Structuring  (Requires Customization of Scenario)'				AS IndicatorName, 'Custom Scenario' AS OracleName UNION ALL
	SELECT 'Wire Structuring (requires customization of scenario)' 				AS IndicatorName, 'Custom Scenario' AS OracleName 
)




, ProposedMapping AS (
	SELECT 'Address Associated with Multiple, Recurring External Entities' AS RuleName                                                                                                                                                                                 , 'Address Associated with Multiple, Recurring External Entities' AS ProposedName                       , 'Network of Customers' AS ProposedRuleTheme         UNION ALL
	SELECT 'Single Address for Multiple Entities'                                                                                                                                                                             										   , 'Address Associated with Multiple, Recurring External Entities'                      				   , 'Network of Customers' 					         UNION ALL
	SELECT 'All Activity In/All Activity Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                  , 'All Activity In/All Activity Out'                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'All Activity In/All Activity Out'																																																						   , 'All Activity In/All Activity Out'                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'All Activity In/Cash Out'																																																								   , 'All Activity In/Cash Out'																			   , 'Velocity'                                          UNION ALL
	SELECT 'Anomalies in ATM, Bank Card: Excessive Withdrawals'                                                                                                                                                                                                        , 'Anomalies in ATM, Bank Card: Excessive Withdrawals'                                                  , 'Patterning'                                        UNION ALL
	SELECT 'Anomalies in ATM, Bank Card: Foreign Transactions'                                                                                                                                                                                                         , 'Anomalies in ATM, Bank Card: Foreign Transactions'                                                   , 'Patterning'                                        UNION ALL
	SELECT 'Anomalies in ATM, Bank Card: Foreign Transactions [note that this scenario was not recommended for Priority 1, but this indicator specifically maps to it.]'                                                                                               , 'Anomalies in ATM, Bank Card: Foreign Transactions'                                                   , 'Patterning'                                        UNION ALL
	SELECT 'Automated PUPID Report'                                                                                                                                                                                                                                    , 'Automated PUPID Report'                                                                              , 'Identity Concealment'                              UNION ALL
	SELECT 'Bearer Instrument In/Wire Out'																																																						       , 'Bearer Instrument In/Wire Out'                                                                       , 'Velocity'                                          UNION ALL
	SELECT 'Cash Deposit in Correspondent Account [using template Large Reportable Transactions]'                                                                                                                                                                      , 'Cash Deposit in Correspondent Account'                                                               , 'Patterning'                                        UNION ALL
	SELECT 'Cash In Followed by Credit Card Payment [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                           , 'Cash In/Credit Card Payment Out'                                                                     , 'Velocity'                                          UNION ALL
	SELECT 'Cash In, Cash Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                                 , 'Cash In/Cash Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Cash Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                                  , 'Cash In/Cash Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Cash Out'                                                                                                                                                                                  														   , 'Cash In/Cash Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Cash-Check In/Wire Out'                                                                                                                                                                                  												   , 'Cash-Check In/Wire Out'                                                                              , 'Velocity'                                          UNION ALL	
	SELECT 'Cash In/Internal Transfer Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                     , 'Cash In/Internal Transfer Out'                                                                       , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Internal Transfer Out'																																																							   , 'Cash In/Internal Transfer Out'                                                                       , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Monetary Instrument Out [using Rapid Movement of Funds - All Activity template]'                                                                                                                                                                   , 'Cash In/Monetary Instrument Out'                                                                     , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Monetary Instrument Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                   , 'Cash In/Monetary Instrument Out'                                                                     , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Monetary Instrument Out'                                                                                                                                                                   														   , 'Cash In/Monetary Instrument Out'                                                                     , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Monetary Instrument Purchase Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                          , 'Cash In/Monetary Instrument Out'                                                                     , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Wire or Transfer Out [using template Rapid Movement of Funds – All Activity]'                                                                                                                                                                      , 'Cash In/Monetary Instrument Out'                                                                     , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Wire Out'                                                                                                                                                                                                                                          , 'Cash In/Wire Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Wire Out (using template Rapid Movement of Funds - All Activity)'                                                                                                                                                                                  , 'Cash In/Wire Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Wire Out [using Rapid Movement of Funds - All Activity template]'                                                                                                                                                                                  , 'Cash In/Wire Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Wire Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                                  , 'Cash In/Wire Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Wire Out [using template Rapid Movement of Funds – All Activity]'                                                                                                                                                                                  , 'Cash In/Wire Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Wire, Monetary Instrument or Cheque Out'                                                                                                                                                                                                           , 'Cash In/Wire Out; Cash In/Monetary Instrument Out'                                                   , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Wires Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                                 , 'Cash In/Wire Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Cash In/Purchases Out [using template Rapid Movement of Funds - All Activity]'																																											   , 'Cash In/Monetary Instrument Out'                                                                     , 'Velocity'                                          UNION ALL
	SELECT 'Cash Purchase of Monetary Instrument [using template Rapid Movement of Funds – All Activity]'                                                                                                                                                              , 'Cash In/Monetary Instrument Out'                                                                     , 'Velocity'                                          UNION ALL
	SELECT 'CIB - Product Utilization Shift'                                                                                                                                                                                                                           , 'CIB: Product Utilization Shift'                                                                      , 'Profiling'                                         UNION ALL
	SELECT 'CIB: High Risk Geography Activity [note that this scenario was not previously recommended but maps specifically to this indicator.]'                                                                                                                       , 'CIB: High Risk Geography'                                                                            , 'High Risk Geography (HRG)'              			 UNION ALL
	SELECT 'CIB: Product Utilization Shift'                                                                                                                                                                                                                            , 'CIB: Product Utilization Shift'                                                                      , 'Profiling'                                         UNION ALL
	SELECT 'CIB: Product Utilization Shift [using template [CIB: Product Utilization Shift]'                                                                                                                                                                           , 'CIB: Product Utilization Shift'                                                                      , 'Profiling'                                         UNION ALL
	SELECT 'CIB: Product Utlization Shift'                                                                                                                                                                                                                             , 'CIB: Product Utilization Shift'                                                                      , 'Profiling'                                         UNION ALL
	SELECT 'CIB: Product Utlization Shift [using template CIB: Product Utlization Shift]'                                                                                                                                                                              , 'CIB: Product Utilization Shift'                                                                      , 'Profiling'                                         UNION ALL
	SELECT 'CIB: Significant Change from Previous Average Activity'                                                                                                                                                                                                    , 'CIB: Significant Change from Previous Average Activity'                                              , 'Profiling'                                         UNION ALL
	SELECT 'CIB: Significant Change from Previous Average Activity  CIB: Product Utilization Shift'                                                                                                                                                                    , 'CIB: Significant Change from Previous Average Activity'                                              , 'Profiling'                                         UNION ALL
	SELECT 'CIB: Significant Change from Previous Peak Activity [note that this scenario was not recommended for Priority 1, but this indicator specifically maps to it]'                                                                                              , 'CIB: Significant Change from Previous Peak Activity'                                                 , 'Profiling'                                         UNION ALL
	SELECT 'CIB: Significant Change in Trade/Transaction Activity'                                                                                                                                                                                                     , 'CIB: Significant Change in Trade/Transaction Activity'                                               , 'Profiling'                                         UNION ALL
	SELECT 'Credit Card Payment Followed by Cash Advance [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                      , 'Credit Card Payment Followed By Cash Advance'                                                        , 'Velocity'                                          UNION ALL
	SELECT 'Currency Exchange Followed by Outgoing Wire (using template Rapid Movement of Funds - All Activity)'                                                                                                                                                       , 'Currency Exchange Followed by Wire'                                                                  , 'Velocity'                                          UNION ALL
	SELECT 'Customer Borrowing Against New Policy'                                                                                                                                                                                                                     , 'Customer Borrowing Against New Policy'                                                               , 'Borrowing/Refunds'                                 UNION ALL
	SELECT 'Customers Engaging in Offsetting Trades'                                                                                                                                                                                                                   , 'Customer Engaging in Offsetting Trades'                                                              , 'Manipulation'                                      UNION ALL
	SELECT 'Deposits/Withdrawals in Same/Similar Amounts'                                                                                                                                                                                                              , 'Deposits/Withdrawals in Similar Amounts'                                                             , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Deviation from Peer Group - Product Utilization [note that this rule was not previously recommended but it maps specifically to this indicator]'                                                                                                           , 'Deviation from Peer Group: Product Utilization'                                                      , 'Profiling'                                         UNION ALL
	SELECT 'Deviation from Peer Group - Product Utilization [note that this scenario was not recommended for Priority 1, but this indicator specifically maps to it]'                                                                                                  , 'Deviation from Peer Group: Product Utilization'                                                      , 'Profiling'                                         UNION ALL
	SELECT 'Deviation from Peer Group: Product Utilization'                                                                                                  																										   , 'Deviation from Peer Group: Product Utilization'                                                      , 'Profiling'                                         UNION ALL
	SELECT 'Deviation from Peer Group: Total Activity'                                                                                                                                                                                                                 , 'Deviation from Peer Group: Total Activity'                                                           , 'Profiling'                                         UNION ALL
	SELECT 'Deviation from Peer Group - Total Activity'                                                                                                                                                                                                                , 'Deviation from Peer Group: Total Activity'                                                           , 'Profiling'                                         UNION ALL
	SELECT 'Deviation from Peer Group -Total Activity'                                                                                                                                                                                                                 , 'Deviation from Peer Group: Total Activity'                                                           , 'Profiling'                                         UNION ALL
	SELECT 'Deviation from Peer Group -Total Activity  CIB - Deviation from Previous Average Activity'                                                                                                                                                                 , 'Deviation from Peer Group: Total Activity; CIB: Significant Change from Previous Average Activity'   , 'Profiling'                                         UNION ALL
	SELECT 'Domestic Wire In/International Wire Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                           , 'Domestic Wire In/International Wire Out'                                                             , 'Velocity'                                          UNION ALL
	SELECT 'Early Payoff or Paydown of a Credit Product'                                                                                                                                                                                                               , 'Early Payoff of a Credit Product'                                                                    , 'Early Redemption'                                  UNION ALL
	SELECT 'Early Payoff of a Credit Product'                                                                                                                                                                                                               , 'Early Payoff of a Credit Product'																			   , 'Early Redemption'                                  UNION ALL
	SELECT 'Early Redemption'																																																										   , 'Early Redemption'																					   , 'Early Redemption'                                  UNION ALL
	SELECT 'Electronic Payment/Cheque In, Cash Out'                                                                                                                                                                                                                    , 'Electronic Payment In/Cash Out; Cheque In/Cash Out'                                                  , 'Velocity'                                          UNION ALL
	SELECT 'Escalation in Inactive Account'                                                                                                                                                                                                                            , 'Escalation in Inactive Account'                                                                      , 'Profiling'                                         UNION ALL
	SELECT 'Foreign Currency Exchange followed by Wire Out [using template Rapid Movement of Funds - All Activity]  High Risk Transactions: High Risk Geography [using template High Risk Transactions: High Risk Geography]'                                          , 'Foreign Exchange Followed by Wire Out'                                                               , 'Velocity'                                          UNION ALL
	SELECT 'Foreign Exchange followed by Wire Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                             , 'Foreign Exchange Followed by Wire Out'                                                               , 'Velocity'                                          UNION ALL
	SELECT 'Foreign Exchange Followed by Wire Out'																																																					   , 'Foreign Exchange Followed by Wire Out'                                                               , 'Velocity'                                          UNION ALL	
	SELECT 'Frequent ATM Deposits [using template Large Reportable Transactions]'                                                                                                                                                                                      , 'Frequent ATM Deposits'                                                                               , 'Patterning'                                        UNION ALL
	SELECT 'Hidden Relationships'                                                                                                                                                                                                                                      , 'Hidden Relationships'                                                                                , 'Network of Customers'                              UNION ALL
	SELECT 'High Risk Electronic Transfers'                                                                                                                                                                                                                            , 'High Risk Electronic Transfers'                                                                      , 'High Risk Geography (HRG)'                         UNION ALL
	SELECT 'High Risk Instructions'                                                                                                                                                                                                                                    , 'High Risk Instructions'                                                                              , 'High Risk Geography (HRG)'                         UNION ALL
	SELECT 'High Risk Transactions: High Risk Counter Party'                                                                                                                                                                                                           , 'High Risk Transactions: High Risk Counter Party'                                                     , 'High Risk Geography (HRG)'                         UNION ALL
	SELECT 'High Risk Transactions: High Risk Focal Entity'                                                                                                                                                                                                            , 'High Risk Transactions: High Risk Focal Entity'                                                      , 'High Risk Geography (HRG)'                         UNION ALL
	SELECT 'High Risk Transactions: High Risk Geography'                                                                                                                                                                                                               , 'High Risk Transactions: High Risk Geography'                                                         , 'High Risk Geography (HRG)'                         UNION ALL
	SELECT 'High Risk Transactions: High Risk Geography  Hub and Spoke'                                                                                                                                                                                                , 'High Risk Transactions: High Risk Geography; Hub and Spoke'                                          , 'High Risk Geography (HRG); Funneling'              UNION ALL
	SELECT 'High Risk Transactions: High Risk Geography [using template High Risk Transactions: High Risk Geography]'                                                                                                                                                  , 'High Risk Transactions: High Risk Geography'                                                         , 'High Risk Geography (HRG)'                         UNION ALL
	SELECT 'Hub and Spoke'                                                                                                                                                                                                                                             , 'Hub and Spoke'                                                                                       , 'Funneling'                                         UNION ALL
	SELECT 'Insurance Policies with Refunds'                                                                                                                                                                                                                           , 'Insurance Policies with Refunds'                                                                     , 'Borrowing/Refunds'                                 UNION ALL
	SELECT 'Internal Transfer In/Wire Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                     , 'Internal Transfer In/Wire Out '                                                                      , 'Velocity'                                          UNION ALL
	SELECT 'Journals Between Unrelated Accounts'                                                                                                                                                                                                                       , 'Journals Between Unrelated Accounts'                                                                 , 'Network of Customers'                              UNION ALL
	SELECT 'Large Cash Transaction [using template Large Reportable Transaction]'                                                                                                                                                                                      , 'Large Cash Transactions'                                                                             , 'Patterning'                                        UNION ALL
	SELECT 'Large Cash Transactions'                                                                                                                                                                                                                                   , 'Large Cash Transactions'                                                                             , 'Patterning'                                        UNION ALL
	SELECT 'Large Cash Transactions [using template Large Reportable Transactions]'                                                                                                                                                                                    , 'Large Cash Transactions'                                                                             , 'Patterning'                                        UNION ALL
	SELECT 'Large Cash Transactions [using template Large Reportable Transactions]  Large Monetary Instrument Transactions  [using template Large Reportable Transactions]  Large International Wire Transactions  [using template Large Reportable Transactions]'     , 'Large Cash Transactions'                                                                             , 'Patterning'                                        UNION ALL
	SELECT 'Large Credit Refunds (requires customization of scenario)'                                                                                                                                                                                                 , 'Large Credit Refunds'                                                                                , 'Borrowing/Refunds'                                 UNION ALL
	SELECT 'Large Credit Refunds'                                                                                                                                                                                                 									   , 'Large Credit Refunds'                                                                                , 'Borrowing/Refunds'                                 UNION ALL
	SELECT 'Large Currency Exchange [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                           , 'Large Currency Exchange'                                                                             , 'Patterning'                                        UNION ALL
	SELECT 'Large Currrency Exchange (using either Rapid Movement of Funds – All Activity or Large Reportable Transactions template]'                                                                                                                                  , 'Large Currency Exchange'                                                                             , 'Patterning'                                        UNION ALL
	SELECT 'Large Currrency Exchange'                                                                                                                                  																								   , 'Large Currency Exchange'                                                                             , 'Patterning'                                        UNION ALL
	SELECT 'Large Depreciation of Account Value'                                                                                                                                                                                                                       , 'Large Depreciation of Account Value'                                                                 , 'Profiling'                                         UNION ALL
	SELECT 'Large Foreign Currency Purchase [using template Large Reportable Transactions]'                                                                                                                                                                            , 'Large Currency Exchange'                                                                             , 'Patterning'                                        UNION ALL
	SELECT 'Large Hydro Bill Payment [using Large Reportable Transactions template]'                                                                                                                                                                                   , 'Large Hydro Bill Payment'                                                                            , 'Lack of Economic Purpose'                          UNION ALL
	SELECT 'Large Monetary Instrument Transactions [using template Large Reportable Transactions]'                                                                                                                                                                     , 'Large Monetary Instrument Transactions'                                                              , 'Patterning'                                        UNION ALL
	SELECT 'Large Payments to Online Payment Services [using template Large Reportable Transactions]'                                                                                                                                                                  , 'Large Payments to Online Payment Services'                                                           , 'Lack of Economic Purpose'                          UNION ALL
	SELECT 'Large Positive Credit Card Balances (requires customization)'                                                                                                                                                                                              , 'Large Positive Credit Card Balances'                                                                 , 'Lack of Economic Purpose'                          UNION ALL
	SELECT 'Large Wire Transaction [using template Large Reportable Transactions]'                                                                                                                                                                                     , 'Large Wire Transfers'                                                                                , 'Lack of Economic Purpose'                          UNION ALL
	SELECT 'Large Wire Transactions [using template Large Reportable Transactions]'                                                                                                                                                                                    , 'Large Wire Transfers'                                                                                , 'Lack of Economic Purpose'                          UNION ALL
	SELECT 'Large Wire Transfer [using template Large Reportable Transactions]'                                                                                                                                                                                        , 'Large Wire Transfers'                                                                                , 'Lack of Economic Purpose'                          UNION ALL
	SELECT 'Large Wire Transfers [using Large Reportable Transactions template]  Large Check Transactions [using Large Reportable Transactions template]'                                                                                                              , 'Large Wire Transfers'                                                                                , 'Lack of Economic Purpose'                          UNION ALL
	SELECT 'Manipulation of Account/Customer Data Followed by Instruction Changes'                                                                                                                                                                                     , 'Manipulation of Account/Customer Data Followed by Instruction Changes'                               , 'Manipulation'                                      UNION ALL
	SELECT 'Micro Structuring [using template Structuring: Potential Structuring in Cash and Equivalents]'                                                                                                                                                             , 'Micro Structuring'                                                                                   , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Missing Counter Party Details (Custom Scenario)'                                                                                                                                                                                                           , 'Missing Counter Party Details'                                                                       , 'Identity Concealment'                              UNION ALL
	SELECT 'Missing Counter Party Details (Requires Customization of Scenario)'                                                                                                                                                                                        , 'Missing Counter Party Details'                                                                       , 'Identity Concealment'                              UNION ALL
	SELECT 'Missing Counter Party Details'                                                                                                                                                                                        									   , 'Missing Counter Party Details'                                                                       , 'Identity Concealment'                              UNION ALL
	SELECT 'Monetary Instrument In/Monetary Instrument Out [using Rapid Movement of Funds - All Activity template]'                                                                                                                                                    , 'Monetary Instrument In/Monetary Instrument Out'                                                      , 'Velocity'                                          UNION ALL
	SELECT 'Monetary Instrument In/Wire Out [using Rapid Movement of Funds - All Activity template]'																																								   , 'Monetary Instrument In/Wire Out'																	   , 'Velocity'                                          UNION ALL
	SELECT 'Third Party Monetary Instrument Deposits'																																								   												   , 'Monetary Instrument In/Monetary Instrument Out'													   , 'Velocity'                                          UNION ALL
	SELECT 'Monetary Instrument Structuring [using template Deposits/Withdrawals in Same or Similar Amounts]'                                                                                                                                                          , 'Monetary Instrument Structuring'                                                                     , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Monetary Instrument Structuring'                                                                                                                                                          																   , 'Monetary Instrument Structuring'                                                                     , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Movement of Funds without Corresponding Trade'                                                                                                                                                                                                             , 'Movement of Funds without Corresponding Trade'                                                       , 'Manipulation'                                      UNION ALL
	SELECT 'Multiple Jurisdictions (custom scenario)'                                                                                                                                                                                                                  , 'Multiple Jurisdictions'                                                                              , 'High Risk Geography (HRG)'                         UNION ALL
	SELECT 'Multiple Jurisdictions (Requires Customization of Scenario)'                                                                                                                                                                                               , 'Multiple Jurisdictions'                                                                              , 'High Risk Geography (HRG)'                         UNION ALL
	SELECT 'Multiple Jurisdictions'                                                                                                                                                                                               									   , 'Multiple Jurisdictions'                                                                              , 'High Risk Geography (HRG)'                         UNION ALL
	SELECT 'Nested Correspondent Rule  (Requires Customization of Scenario)'                                                                                                                                                                                           , 'Nested Correspondent Rule'                                                                           , 'Identity Concealment'                              UNION ALL
	SELECT 'Networks of Accounts, Entities and Customers'                                                                                                                                                                                                              , 'Networks of Accounts, Entities, and Customers'                                                       , 'Network of Customers'                              UNION ALL
	SELECT 'Networks of Accounts, Entities, and Customers'                                                                                                                                                                                                             , 'Networks of Accounts, Entities, and Customers'                                                       , 'Network of Customers'                              UNION ALL
	SELECT 'Pattern of Funds Transfers Between Correspondent Banks'                                                                                                                                                                                                    , 'Pattern of Funds Transfers between Correspondent Banks'                                              , 'Exclusive Relationship'                            UNION ALL
	SELECT 'Pattern of Funds Transfers between Internal Accounts and Customers'                                                                                                                                                                                        , 'Pattern of Funds Transfers between Internal Accounts and Customers'                                  , 'Exclusive Relationship'                            UNION ALL
	SELECT 'Pattern of Funds Transfers between Customers and External Entities'                                                                                                                                                                                        , 'Pattern of Funds Transfers between Customers and External Entities'                                  , 'Exclusive Relationship'                            UNION ALL
	SELECT 'Patterns of Funds Transfers between Customers and External Entities'                                                                                                                                                                                       , 'Pattern of Funds Transfers between Customers and External Entities'                                  , 'Exclusive Relationship'                            UNION ALL
	SELECT 'Patterns of Funds Transfers Between Internal Accounts and Customers'                                                                                                                                                                                       , 'Pattern of Funds Transfers between Internal Accounts and Customers'                                  , 'Exclusive Relationship'                            UNION ALL
	SELECT 'Patterns of Recurring Originators/ Beneficiaries in Funds Transfers'                                                                                                                                                                                       , 'Pattern of Funds Transfers between Recurring Originators/Beneficiaries'                              , 'Exclusive Relationship'                            UNION ALL
	SELECT 'Patterns of Recurring Originators/Beneficiaries in Funds Transfers'                                                                                                                                                                                        , 'Pattern of Funds Transfers between Recurring Originators/Beneficiaries'                              , 'Exclusive Relationship'                            UNION ALL
	SELECT 'Patterns of Sequentially Numbered Checks, Monetary Instruments'                                                                                                                                                                                            , 'Pattern of Sequentially Numbered Checks'                                                             , 'Patterning'                                        UNION ALL
	SELECT 'Pattern of Sequentially Numbered Checks'																																																				   , 'Pattern of Sequentially Numbered Checks'                                                             , 'Patterning'                                        UNION ALL
	SELECT 'Policies with Large Early Removal'                                                                                                                                                                                                                         , 'Policies with Large Early Removal'                                                                   , 'Early Redemption'                                  UNION ALL
	SELECT 'Rapid Movement Of Funds - All Activity'                                                                                                                                                                                                                    , 'Rapid Movement of Funds – All Activity'                                                              , 'Velocity'                                          UNION ALL
	SELECT 'Rapid Movement of Funds – All Activity'                                                                                                                                                                                                                    , 'Rapid Movement of Funds – All Activity'                                                              , 'Velocity'                                          UNION ALL
	SELECT 'Return of Cheques [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                                 , 'Return of Cheques'                                                                                   , 'Lack of Economic Purpose'                          UNION ALL
	SELECT 'Structuring: Avoidance of Reporting Thresholds'                                                                                                                                                                                                            , 'Structuring: Avoidance of Reporting Thresholds'                                                      , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Structuring: Avoidance of Reporting Thresholds [using template Structuring: Potential Structuring in Cash and Equivalents]'                                                                                                                                , 'Structuring: Avoidance of Reporting Thresholds'                                                      , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Structuring: Potential Structuring in Cash and Equivalents'                                                                                                                                                                                                , 'Structuring: Potential Structuring in Cash and Equivalents'                                          , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Structuring: Potential Structuring in Cash and Equivalents  Large Cash Transactions [using template Large Reportable Transactions]'                                                                                                                        , 'Structuring: Potential Structuring in Cash and Equivalents; Large Cash Transactions'                 , 'Structuring/Threshold Avoidance; Patterning'       UNION ALL
	SELECT 'Structuring: Potential Structuring in Cash and Equivalents [using template Structuring: Potential Structuring in Cash and Eq'                                                                                                                              , 'Structuring: Potential Structuring in Cash and Equivalents'                                          , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Structuring: Potential Structuring in Cash and Equivalents [using template Structuring: Potential Structuring in Cash and Equivalents]'                                                                                                                    , 'Structuring: Potential Structuring in Cash and Equivalents'                                          , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Terrorist Financing'                                                                                                                                                                                                                                       , 'Terrorist Financing'                                                                                 , 'Funneling'                                         UNION ALL
	SELECT 'Wire In/Cash Out [using Rapid Movement of Funds - All Activity]'                                                                                                                                                                                           , 'Wire In/Cash Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Wire In/Cash Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                                  , 'Wire In/Cash Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Wire In/Cash Out'                                                                                                                                                                                  														   , 'Wire In/Cash Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Wire In/Wire or Transfer Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                      , 'Wire In/Wire Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Wire In/Wire Out'                                                                                                                                                                                                                                          , 'Wire In/Wire Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Wire In/Wire Out (using template Rapid Movement of Funds – All Activity)'                                                                                                                                                                                  , 'Wire In/Wire Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Wire In/Wire Out [using template Rapid Movement of Funds - All Activity]'                                                                                                                                                                                  , 'Wire In/Wire Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Wire In/Wire Out [using template Rapid Movement of Funds - All Activity]  Cheque or Monetary Instrument In/Wire Out [using template Rapid Movement of Funds - All Activity]'                                                                               , 'Wire In/Wire Out'                                                                                    , 'Velocity'                                          UNION ALL
	SELECT 'Wire Structring [using template Deposits/Withdrawals in Same or Similar Amounts]'                                                                                                                                                                          , 'Wire Structuring'                                                                                    , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Wire Structuring [using template Deposits/Withdrawals in Same or Similar Amounts]'                                                                                                                                                                         , 'Wire Structuring'                                                                                    , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Wire Structuring'                                                                                                                                                                                                                                          , 'Wire Structuring'                                                                                    , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Wire Structuring  (Requires Customization of Scenario)'                                                                                                                                                                                                    , 'Wire Structuring'                                                                                    , 'Structuring/Threshold Avoidance'                   UNION ALL
	SELECT 'Wire Structuring (requires customization of scenario)'                                                                                                                                                                                                     , 'Wire Structuring'                                                                                    , 'Structuring/Threshold Avoidance'
)

, InitialClean AS (
		SELECT IndicatorID
		, LTRIM(RTRIM(
		REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(p.r.value('.' , 'varchar(5000)'),'1.',''),'2.',''),CHAR(13),''),CHAR(10),''),'3.',''),'4.',''))) AS [Value]
		, p.r.value('for $i in . return count(../*[. << $i]) + 1', 'int') AS Position
		FROM TempData
			  CROSS APPLY ProposedRuleNameXML.nodes('//x') P(r)
		WHERE LTRIM(RTRIM(p.r.value('.' , 'varchar(5000)'))) != '' 
) 


, TempProposedXML AS (       
	SELECT 
		InitialClean.IndicatorID
		, CONVERT(XML,'<x>' + REPLACE(COALESCE(ProposedMapping.ProposedName, InitialClean.Value),';','</x><x>') + '</x>') AS ProposedRuleNameXML 
		, CONVERT(XML,'<x>' + REPLACE(COALESCE(ProposedMapping.ProposedRuleTheme, InitialClean.Value),';','</x><x>') + '</x>') AS ProposedRuleThemeXML
		, Position
	FROM InitialClean
	LEFT JOIN ProposedMapping ON InitialClean.Value = ProposedMapping.RuleName
	--ORDER BY 1, 2

)

, TempProposedRule AS (
	SELECT 
		IndicatorID
		, LTRIM(RTRIM(Element.Loc.value('.' , 'varchar(5000)'))) AS Value
		, Position + Element.Loc.value('for $i in . return count(../*[. << $i]) + 1', 'int') - 1 AS Position
	FROM TempProposedXML
	CROSS APPLY ProposedRuleNameXML.nodes('//x') Element(loc)
	--ORDER BY 2,1
)

, TempProposedTheme AS (
	SELECT 
		IndicatorID
		, LTRIM(RTRIM(Element.Loc.value('.' , 'varchar(5000)'))) AS Value
		, Position + Element.Loc.value('for $i in . return count(../*[. << $i]) + 1', 'int') - 1 AS Position
	FROM TempProposedXML
	CROSS APPLY ProposedRuleThemeXML.nodes('//x') Element(loc)
	--ORDER BY 1,2
)

, TempProposedRaw AS (
	SELECT COALESCE(TempProposedRule.IndicatorID, TempProposedTheme.IndicatorID) AS IndicatorID
		 , TempProposedRule.Value AS ProposedRule
		 , TempProposedTheme.Value AS ProposedTheme
		 , CASE WHEN TempProposedTheme.Value = '' THEN TempProposedTheme.Position ELSE ISNULL(TempProposedTheme.Position,TempProposedRule.Position) END AS Position
	FROM TempProposedRule 
	FULL OUTER JOIN TempProposedTheme 
		ON TempProposedRule.IndicatorID = TempProposedTheme.IndicatorID
		AND TempProposedRule.Position = TempProposedTheme.Position
	--ORDER BY 1, 2
)

, TempProposed AS (

	SELECT TempProposedRaw.IndicatorID
		, ISNULL(TempProposedRaw.ProposedRule, TempProposedRule.Value) AS ProposedRule
		, ISNULL(TempProposedRaw.ProposedTheme, TempProposedTheme.Value) AS ProposedTheme
		, TempProposedRaw.Position
	FROM TempProposedRaw 
	LEFT OUTER JOIN TempProposedTheme 
		ON TempProposedRaw.IndicatorID = TempProposedTheme.IndicatorID
		AND TempProposedTheme.Position = 1
		AND ISNULL(TempProposedRaw.ProposedTheme, '') = ''
	LEFT OUTER JOIN TempProposedRule 
		ON TempProposedRaw.IndicatorID = TempProposedRule.IndicatorID
		AND TempProposedRule.Position = 1
		AND ISNULL(TempProposedRaw.ProposedRule, '') = ''
	--ORDER BY 1, 2

)

, TempOracleRule AS (
	SELECT IndicatorID
	, REPLACE(REPLACE(LTRIM(RTRIM(p.r.value('.' , 'varchar(5000)'))),CHAR(13), ''),CHAR(10),'') AS [Value] -- add your rule name fixes
	, p.r.value('for $i in . return count(../*[. << $i]) + 1', 'int') AS Position
	FROM TempData
		  CROSS APPLY OracleRuleXML.nodes('//x') P(r)

)

, TempMatch AS (
	SELECT COALESCE(TempProposed.IndicatorID, TempOracleRule.IndicatorID) AS IndicatorID
	, TempProposed.ProposedRule AS ProposedRuleName
	, TempProposed.ProposedTheme AS ProposedRuleTheme
	, TempProposed.Position AS ProposedPosition
	, COALESCE(TempMap.OracleName, TempOracleRule.Value) AS [OracleRuleName] --Apply Cleaning Map
	, TempOracleRule.Position AS OraclePosition
	FROM TempProposed
	FULL OUTER JOIN TempOracleRule ON TempProposed.IndicatorID = TempOracleRule.IndicatorID
			AND TempProposed.Position = TempOracleRule.Position 
	LEFT OUTER JOIN TempOracleMapping TempMap
		ON TempMap.IndicatorName = TempOracleRule.[Value]
		
)

SELECT * INTO #TempMatch FROM TempMatch;

TRUNCATE TABLE dbo.TIndicatorXRule;

SET IDENTITY_INSERT dbo.TIndicatorXRule ON;

WITH TempData AS (
       
   SELECT IndicatorID 
		  , CONVERT(XML,'<x>' + REPLACE(ProposedRuleName,';','</x><x>') + '</x>') AS ProposedRuleNameXML
		  , CONVERT(XML,'<x>' + REPLACE(OracleRule,';','</x><x>') + '</x>') AS OracleRuleXML		  
		  , CONVERT(XML,'<x>' + REPLACE(NavigantRule,';','</x><x>') + '</x>') AS NavigantRuleXML
		  , CONVERT(XML,'<x>' + REPLACE(FortentRule,';','</x><x>') + '</x>') AS FortentRuleXML
		  
   FROM TStagingIndicator

) 

, TempNavForData AS (
	SELECT DISTINCT
		IndicatorID 
		, Source 
		, REPLACE(REPLACE(LTRIM(RTRIM(Element.Loc.value('.','varchar(255)'))),CHAR(13), ''),CHAR(10),'') AS RuleName
	FROM 
		(SELECT IndicatorID, 'Fortent' AS Source, FortentRuleXML AS RuleXML FROM TempData  UNION ALL
		SELECT IndicatorID, 'Navigant' AS Source, NavigantRuleXML AS RuleXML FROM TempData ) Rules
	CROSS APPLY RuleXML.nodes('/x') as Element(Loc)
	WHERE Element.Loc.value('.','varchar(255)') NOT IN ('N/A', ' ')
		
)


, OracleFinal AS (

	SELECT TempMatch.IndicatorID
		, COALESCE(TempMatch.OracleRuleName, TempMatchOracle.OracleRuleName) AS OracleRuleNameEnhanced
		, COALESCE(TempMatch.ProposedRuleName, TempMatchProposed.ProposedRuleName) AS ProposedRuleNameEnhanced
		, COALESCE(TempMatch.ProposedRuleTheme, TempMatchProposed.ProposedRuleTheme) AS ProposedThemeNameEnhanced
		, ISNULL(TempMatch.ProposedPosition, TempMatch.OraclePosition) AS POSITION
	FROM #TempMatch TempMatch
	LEFT OUTER JOIN #TempMatch TempMatchOracle
		ON TempMatchOracle.IndicatorID = TempMatch.IndicatorID 
		AND TempMatchOracle.OraclePosition = 1 
		AND COALESCE(TempMatch.OracleRuleName, 'N/A') = 'N/A'  -- use the first position if missing
	LEFT OUTER JOIN #TempMatch TempMatchProposed 
		ON TempMatchProposed.IndicatorID = TempMatch.IndicatorID 
		AND TempMatchProposed.ProposedPosition = 1 
		AND TempMatch.ProposedRuleName IS NULL  -- use the first position if missing
) 

, ProposedCoverage AS 
(
	SELECT 'Address Associated with Multiple, Recurring External Entities' AS ProposedRule , 'Address Associated with Multiple, Recurring External Entities' AS OracleTemplate , 'Partial' AS InOracle UNION ALL
	SELECT 'Address Associated with Multiple, Recurring External Entities'			    , 'Customers Engaging in Offsetting Trades'									   , 'Partial'	 UNION ALL
	SELECT 'Address Associated with Multiple, Recurring External Entities'			    , 'Hub and Spoke'															   , 'Partial'   UNION ALL	
	SELECT 'All Activity In/All Activity Out'                                           , 'Rapid Movement Of Funds - All Activity'                                     , 'Partial'   UNION ALL
	SELECT 'All Activity In/Cash Out'													, 'Transactions In Round Amounts'											   , 'Partial'   UNION ALL
	SELECT 'All Activity In/Cash Out'													, 'Rapid Movement Of Funds - All Activity'									   , 'None'      UNION ALL
	SELECT 'Anomalies in ATM, Bank Card: Excessive Withdrawals'                         , 'Anomalies in ATM, Bank Card: Excessive Withdrawals'                         , 'None'      UNION ALL
	SELECT 'Anomalies in ATM, Bank Card: Foreign Transactions'                          , 'Anomalies In ATM, Bank Card: Foreign Transactions'                          , 'None'      UNION ALL
	SELECT 'Automated PUPID Report'                                                     , 'Custom Scenario'                                                            , 'Partial'   UNION ALL
	SELECT 'Bearer Instrument In/Wire Out'												, 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Cash Deposit in Correspondent Account'                                      , 'Large Reportable Transactions'                                              , 'Full'      UNION ALL
	SELECT 'Cash In/Cash Out'                                                           , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Cash In/Credit Card Payment Out'                                            , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Cash In/Internal Transfer Out'                                              , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Cash In/Monetary Instrument Out'                                            , 'Rapid Movement Of Funds - All Activity'                                     , 'Full'      UNION ALL
	SELECT 'Cash In/Wire Out'                                                           , 'Rapid Movement Of Funds - All Activity'                                     , 'Full'      UNION ALL
	SELECT 'Cheque In/Cash Out'                                                         , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'CIB: High Risk Geography'                                                   , 'CIB: High Risk Geography Activity'                                          , 'Full'      UNION ALL
	SELECT 'CIB: Product Utilization Shift'                                             , 'CIB: Product Utilization Shift'                                             , 'Full'      UNION ALL
	SELECT 'CIB: Significant Change from Previous Average Activity'                     , 'CIB: Product Utilization Shift'                                             , 'Full'      UNION ALL
	SELECT 'CIB: Significant Change from Previous Average Activity'                     , 'CIB: Significant Change from Previous Average Activity'                     , 'Full'      UNION ALL
	SELECT 'CIB: Significant Change from Previous Peak Activity'                        , 'CIB: Significant Change From Previous Peak Activity'                        , 'Full'      UNION ALL
	SELECT 'CIB: Significant Change in Trade/Transaction Activity'                      , 'CIB: Significant Change in Trade/Transaction Activity'                      , 'Full'      UNION ALL
	SELECT 'Credit Card Payment Followed By Cash Advance'                               , 'Rapid Movement Of Funds - All Activity'                                     , 'Partial'   UNION ALL
	SELECT 'Currency Exchange Followed by Wire'                                         , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Customer Borrowing Against New Policy'                                      , 'Customer Borrowing Against New Policy'                                      , 'None'      UNION ALL
	SELECT 'Customer Engaging in Offsetting Trades'                                     , 'Customers Engaging in Offsetting Trades'                                    , 'None'      UNION ALL
	SELECT 'Deposits/Withdrawals in Similar Amounts'                                    , 'Deposits/Withdrawals in Same or Similar Amounts'                            , 'Full'      UNION ALL
	SELECT 'Deviation from Peer Group: Product Utilization'                             , 'Deviation From Peer Group - Product Utilization'                            , 'Full'      UNION ALL
	SELECT 'Deviation from Peer Group: Total Activity'                                  , 'Deviation from Peer Group - Total Activity'                                 , 'Full'      UNION ALL
	SELECT 'Domestic Wire In/International Wire Out'                                    , 'Rapid Movement Of Funds - All Activity'                                     , 'Full'      UNION ALL
	SELECT 'Early Payoff of a Credit Product'                                           , 'Early Payoff or Paydown of a Credit Product'                                , 'None'      UNION ALL
	SELECT 'Early Redemption'															, 'Early Payoff or Paydown of a Credit Product'                                , 'None'      UNION ALL
	SELECT 'Electronic Payment In/Cash Out'                                             , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Escalation in Inactive Account'                                             , 'Escalation in Inactive Account'                                             , 'Full'      UNION ALL
	SELECT 'Foreign Exchange Followed by Wire Out'                                      , 'High Risk Transactions: High Risk Geography'                                , 'None'      UNION ALL
	SELECT 'Foreign Exchange Followed by Wire Out'                                      , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Frequent ATM Deposits'                                                      , 'Large Reportable Transactions'                                              , 'Partial'   UNION ALL
	SELECT 'Hidden Relationships'                                                       , 'Hidden Relationships'                                                       , 'None'      UNION ALL
	SELECT 'High Risk Electronic Transfers'                                             , 'High Risk Electronic Transfers'                                             , 'None'      UNION ALL
	SELECT 'High Risk Instructions'                                                     , 'High Risk Instructions'                                                     , 'None'      UNION ALL
	SELECT 'High Risk Transactions: High Risk Counter Party'                            , 'High Risk Transactions: High Risk Counter Party'                            , 'None'      UNION ALL
	SELECT 'High Risk Transactions: High Risk Focal Entity'                             , 'High Risk Transactions: High Risk Counter Party'                            , 'Partial'   UNION ALL
	SELECT 'High Risk Transactions: High Risk Geography'                                , 'High Risk Transactions: High Risk Geography'                                , 'Full'      UNION ALL
	SELECT 'Hub and Spoke'                                                              , 'Hub and Spoke'                                                              , 'Full'      UNION ALL
	SELECT 'Insurance Policies with Refunds'                                            , 'Insurance Policies with Refunds'                                            , 'None'      UNION ALL
	SELECT 'Internal Transfer In/Wire Out'                                              , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Journals Between Unrelated Accounts'                                        , 'Journals Between Unrelated Accounts'                                        , 'None'      UNION ALL
	SELECT 'Large Cash Transactions'                                                    , 'Large Reportable Transactions'                                              , 'Full'      UNION ALL
	SELECT 'Large Credit Refunds'                                                       , 'Custom Scenario'                                                            , 'Partial'   UNION ALL
	SELECT 'Large Currency Exchange'                                                    , 'Large Reportable Transactions'                                              , 'None'      UNION ALL
	SELECT 'Large Currency Exchange'                                                    , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Large Depreciation of Account Value'                                        , 'Large Depreciation of Account Value'                                        , 'None'      UNION ALL
	SELECT 'Large Hydro Bill Payment'                                                   , 'Large Reportable Transactions'                                              , 'None'      UNION ALL
	SELECT 'Large Monetary Instrument Transactions'                                     , 'Large Reportable Transactions'                                              , 'None'      UNION ALL
	SELECT 'Large Payments to Online Payment Services'                                  , 'Large Reportable Transactions'                                              , 'None'      UNION ALL
	SELECT 'Large Positive Credit Card Balances'                                        , 'Custom Scenario'                                                            , 'None'      UNION ALL
	SELECT 'Large Wire Transfers'                                                       , 'Large Reportable Transactions'                                              , 'Full'      UNION ALL
	SELECT 'Manipulation of Account/Customer Data Followed by Instruction Changes'      , 'Manipulation of Account/Customer Data Followed by Instruction Changes'      , 'None'      UNION ALL
	SELECT 'Micro Structuring'                                                          , 'Structuring: Potential Structuring in Cash and Equivalents'                 , 'Full'      UNION ALL
	SELECT 'Missing Counter Party Details'                                              , 'Custom Scenario'                                                            , 'None'      UNION ALL
	SELECT 'Monetary Instrument In/Monetary Instrument Out'                             , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Monetary Instrument In/Monetary Instrument Out'                             , 'Structuring: Deposits/Withdrawals Of Mixed Monetary Instruments'            , 'None'      UNION ALL
	SELECT 'Monetary Instrument In/Wire Out'											, 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL	
	SELECT 'Monetary Instrument Structuring'                                            , 'Deposits/Withdrawals in Same or Similar Amounts'                            , 'Partial'   UNION ALL
	SELECT 'Movement of Funds without Corresponding Trade'                              , 'Movement of Funds without Corresponding Trade'                              , 'None'      UNION ALL
	SELECT 'Multiple Jurisdictions'                                                     , 'Custom Scenario'                                                            , 'Partial'   UNION ALL
	SELECT 'Nested Correspondent Rule'                                                  , 'Custom Scenario'                                                            , 'Partial'   UNION ALL
	SELECT 'Networks of Accounts, Entities, and Customers'                              , 'Networks Of Accounts, Entities, And Customers'                              , 'None'      UNION ALL
	SELECT 'Pattern of Funds Transfers between Correspondent Banks'                     , 'Pattern of Funds Transfers Between Correspondent Banks'                     , 'Partial'   UNION ALL
	SELECT 'Pattern of Funds Transfers between Customers and External Entities'         , 'Patterns Of Funds Transfers Between Customers And External Entities'        , 'Full'      UNION ALL
	SELECT 'Pattern of Funds Transfers between Internal Accounts and Customers'         , 'Patterns of Funds Transfers Between Internal Accounts and Customers'        , 'Full'      UNION ALL
	SELECT 'Pattern of Funds Transfers between Recurring Originators/Beneficiaries'     , 'Patterns Of Recurring Originators/Beneficiaries In Funds Transfers'         , 'Full'      UNION ALL
	SELECT 'Pattern of Sequentially Numbered Checks'                                    , 'Patterns of Sequentially Numbered Checks, Monetary Instruments'             , 'None'      UNION ALL
	SELECT 'Policies with Large Early Removal'                                          , 'Policies with Large Early Removal'                                          , 'None'      UNION ALL
	SELECT 'Rapid Movement of Funds – All Activity'                                     , 'Custom Scenario'                                                            , 'None'      UNION ALL
	SELECT 'Rapid Movement of Funds – All Activity'                                     , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Return of Cheques'                                                          , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Structuring: Avoidance of Reporting Thresholds'                             , 'Structuring: Avoidance of Reporting Thresholds'                             , 'Full'      UNION ALL
	SELECT 'Structuring: Avoidance of Reporting Thresholds'                             , 'Structuring: Potential Structuring in Cash and Equivalents'                 , 'Full'      UNION ALL
	SELECT 'Structuring: Avoidance of Reporting Thresholds'                             , 'Escalation in Inactive Account'											   , 'Full'      UNION ALL
	SELECT 'Structuring: Avoidance of Reporting Thresholds'                             , 'Deposits/Withdrawals in Same or Similar Amounts'							   , 'Full'      UNION ALL
	SELECT 'Structuring: Potential Structuring in Cash and Equivalents'                 , 'Deposits/Withdrawals in Same or Similar Amounts'                            , 'Full'      UNION ALL
	SELECT 'Structuring: Potential Structuring in Cash and Equivalents'                 , 'Structuring: Potential Structuring in Cash and Equivalents'                 , 'Full'      UNION ALL
	SELECT 'Terrorist Financing'                                                        , 'Terrorist Financing'                                                        , 'None'      UNION ALL
	SELECT 'Wire In/Cash Out'                                                           , 'Rapid Movement Of Funds - All Activity'                                     , 'None'      UNION ALL
	SELECT 'Wire In/Wire Out'                                                           , 'Rapid Movement Of Funds - All Activity'                                     , 'Full'      UNION ALL
	SELECT 'Wire Structuring'                                                           , 'Custom Scenario'                                                            , 'Full'      UNION ALL
	SELECT 'Wire Structuring'                                                           , 'Structuring: Potential Structuring in Cash and Equivalents'                 , 'Full'		 UNION ALL 
	SELECT 'Wire Structuring'                                                           , 'Deposits/Withdrawals in Same or Similar Amounts'                            , 'Full'

)


INSERT INTO dbo.TIndicatorXRule (IndicatorXRuleID, IndicatorID, RuleID, ProposedRuleName, ProposedTheme, InOracle, Focus)
SELECT
	ROW_NUMBER() OVER (ORDER BY Rules.IndicatorID, VTRule.RuleID) AS IndicatorXRuleID
	, Rules.IndicatorID
	, VTRule.RuleID
	, Rules.ProposedRuleName
	, Rules.ProposedTheme
	, ProposedCoverage.InOracle
	, NULL 
	
	--, Rules.RuleName
	--, VTRule.Source RuleSource
	--, Rules.Source GenSource
	--, OracleTSD.Category

FROM (
	SELECT
		IndicatorID
		, OracleRuleNameEnhanced AS RuleName
		, 'Oracle' AS Source
		, ProposedRuleNameEnhanced AS ProposedRuleName
		, ProposedThemeNameEnhanced AS ProposedTheme
	FROM OracleFinal UNION ALL
	SELECT
		IndicatorID
		, RuleName
		, Source
		, NULL 
		, NULL
	FROM TempNavForData ) Rules
LEFT OUTER JOIN VTRule ON  Rules.RuleName = VTRule.RuleName
LEFT OUTER JOIN ProposedCoverage 
	ON VTRule.RuleName = ProposedCoverage.OracleTemplate AND VTRule.Source = 'Oracle'
	AND Rules.ProposedRuleName = ProposedCoverage.ProposedRule
--LEFT OUTER JOIN OracleRules ON Rules.RuleName = OracleRules.RuleName --Add in From TRule Code to QA
WHERE Rules.RuleName NOT IN ('N/A', ' ');
SET IDENTITY_INSERT dbo.TIndicatorXRule OFF;

--SELECT * FROM TIndicatorXRule;

/**************************************************************************************************/
--I. TThemeDescription
/**************************************************************************************************/

/*

This table lists all proposed rule mapped behavioral themes 
and provides a detailed description of each. 

*/ 


WITH ThemeDescription AS (
	SELECT 1 AS ThemeID , 'Borrowing/Refunds' AS ThemeName , 'Customer Borrowing against new policy, insurance policies with refunds and large credit refunds.' AS ThemeDescription                                                                                                                                                                                                                                                                                                                                                                                                                                              UNION ALL
	SELECT 2    , 'Early Redemption'                   , 'Early repayment or redemption is a behavior typically associated with long-term lending or investment products where there is a sudden, early pay off or cancellation of the product to layer and then integrate funds.'                                                                                                                                                                                                                                                                                                                                              UNION ALL
	SELECT 3    , 'Exclusive Relationship'             , 'Exclusive relationship is a behavior that is typically associated a high amount of transaction activity between those accounts which reveals a previously unknown link between unrelated accounts.'                                                                                                                                                                                                                                                                                                                                                                    UNION ALL
	SELECT 4    , 'Funneling'                          , 'Funneling is a patterning type behavior where multiple parties access funds through a single party and vice versa often giving access to funds in a different jurisdiction. This includes Many-to-One, One-to-Many, Many-to-Many complex transaction patter behaviors.'                                                                                                                                                                                                                                                                                                UNION ALL
	SELECT 5    , 'High Risk Geography (HRG)'          , 'This theme involves transactions to or from jurisdictions of concern for money laundering, offshore jurisdictions, and/or jurisdictions known to be tax havens as these locations are known for lax money laundering regulations or locations that are not transparent with regard to the originator or beneficiary of funds.'                                                                                                                                                                                                                                         UNION ALL
	SELECT 6    , 'Identity Concealment'               , 'This theme involves a pattern of behavior where the transaction originator attempts to conceal his/her identity or the beneficiary’s identity such as transactions which request payment in cash or fund transfers missing key information on the parties to the transfer. Lack of transparency into the originator, source of funds, or beneficiary is an indicia of potential money laundering.'                                                                                                                                                                     UNION ALL
	SELECT 7    , 'Lack of Economic Purpose'           , 'This pattern of behavior reflects transactions generally that appear to have little to no economic purpose.  For example, wire transfers that are directed through numerous countries, transactions which take a circuitous route (funds come in and out of the account in a circular fashion), and a party sending multiple wires to the same recipient on the same day for differing amounts rather than as part of one single transaction.'                                                                                                                       UNION ALL
	SELECT 8    , 'Manipulation'                       , 'Manipulation is typically evidenced by an effort to create misleading, artificial, or false activity in the market place for the purpose of capturing a gain in price or to avoid a loss.'                                                                                                                                                                                                                                                                                                                                                                           UNION ALL
	SELECT 9    , 'Network of Customers'               , 'This pattern of behavior involves coordinated efforts by customers to move funds or securities, such as transfers of securities between unrelated accounts or fund transfers between seemingly unrelated accounts or third parties.'                                                                                                                                                                                                                                                                                                                                   UNION ALL
	SELECT 10   , 'Patterning'                         , 'Patterning typically involves the identification of a series of general transaction behavior that is indicative of illicit activity.  For example, a domestic account that has a large number of foreign ATM deposits or withdrawals or a business account is funded through frequent large cash deposits.'                                                                                                                                                                                                                                                        UNION ALL
	SELECT 11   , 'Profiling'                          , 'This theme relates to transactions out of line with the historical or anticipated activity of the customer or customer’s peer group, which could indicate potentially illicit behavior.'                                                                                                                                                                                                                                                                                                                                                                               UNION ALL
	SELECT 12   , 'Structuring/Threshold Avoidance'    , 'Reporting thresholds have been established by regulators to create mandatory levels at which transaction activity information must be collected and reported. Any attempt to circumvent or structure transactions to avoid these reporting thresholds can be an indicia of suspicious behavior. Common examples are receiving cash deposits on two consecutive days to avoid a single day reporting threshold, or multiple low-dollar transactions sent to the same recipient that aggregate to an amount above a mandated threshold.'								 UNION ALL
	SELECT 13   , 'Velocity'                           , 'Velocity patterns typically involve transactions that occur in rapid succession, such as the request for payment in cash immediately upon the deposit or receipt of a funds transfer or transactions involving foreign exchanges followed shortly with a wiring of funds to high-risk jurisdictions. Transactions of this type typically indicate an account is being used to facilitate a transfer or act as a pass through for a transaction.'
)
SELECT * INTO dbo.TThemeDescription FROM ThemeDescription;



/**************************************************************************************************/
/***********************************II. Product Staging********************************************/
/**************************************************************************************************/

/* 

This process entails importing bank product data and merging with the indicator staging data
to create an indicator to product mapping via designated product groups. 

The final indicator view in this section collates all tables into an enhanced version
of the staging file with cleaned data. 


TProduct 			
TIndicatorStaging	-->     TIndicatorXProduct
(or Staging proxy)


Tindicator
TindicatorXModifier
TindicatorXTheme
TIndicatorXRule
TIndicatorXProduct  -->		VIndicator
TRule
TTheme
TModifier
TProduct


*/



/**************************************************************************************************/
--A. TProduct
/**************************************************************************************************/

/* 

This table represents the latest product data to be imported.

Note: 
	--The source of this staging table was imported using Excel/Access. 
	--Consider the data types below prior to loading in.
	--Products which identify with multiple Product Groupings split into separate rows.  

*/


DROP TABLE dbo.TProduct;

CREATE TABLE [dbo].[TProduct]
(
	[ProductID] [int] NOT NULL IDENTITY(1, 1),
	--[StagingID] [nvarchar] (10) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Segment] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[BusinessUnit] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[System] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Type] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ProductOrService] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ProductOrServiceDescription] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ProductGrouping] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[ProductSubGrouping] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[IsConduciveToAutomatedMonitoring] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	--[IsEvaluatedInCoverageAssessment] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	--[IsMVP1] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[UnderlyingProductTxnsToBeMonitored] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Comment] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Question] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Currency] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[IsProductOrService] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Inherent risk has been identified in regulatory guidance or indu] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Product has been highlighted in recent AML enforcement actions, ] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Transactions related to this product may result in cross-border ] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Product features enable unrelated or third parties to make or re] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Product features enable anonymity in the transaction] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Product features enable change in customer ownership/sponsorship] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Elevated risks are associated with the method in which the produ] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[1 Branch] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[2 ScotiaOnline] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[3 ScotiaConnect] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[4 Mobile] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[5 ABM] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[6 Night Deposit, Mail, Courier] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[7 IVR] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[8 Call Center] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[9 SWIFT Wires] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[10 Non-SWIFT Payments] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[11 Wealth Channels/PIC] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[12 Commercial Channels (RM/Fulfillment Team)] [nvarchar] (255) COLLATE SQL_Latin1_General_CP1_CI_AS NULL,
	[Comments] [nvarchar] (max) COLLATE SQL_Latin1_General_CP1_CI_AS NULL, 
	[Rating] [int] 
) ON [PRIMARY] TEXTIMAGE_ON [PRIMARY]
GO
SET IDENTITY_INSERT [dbo].[TProduct] ON;

WITH TempData AS (
	SELECT
		[ID], 
		CONVERT(XML,'<x>' + REPLACE([Product Grouping (separate by ";" if more than one)],';','</x><x>') + '</x>') AS ProductXML
	FROM XTProduct20181220
)

, SplitGrouping AS (

	SELECT 
		[ID], 
		LTRIM(RTRIM(p.r.value('.', 'varchar(50)'))) AS ProductGrouping
	FROM TempData
	CROSS APPLY ProductXML.nodes('/x') as P(r) 
)

, ProductGroupETL AS
(
SELECT 'EFT' AS InitialGrouping				, 'Electronic Funds Transfer'		AS ProposedGrouping UNION ALL
SELECT 'Moblie/Internet Payment'			, 'Mobile/Internet Payment'		UNION ALL
SELECT 'Depository Accounts - Investment'	, 'Depository Account - Investment' 
)

INSERT INTO dbo.TProduct
(
    ProductID,
	Segment,
    BusinessUnit,
    System,
    Type,
    ProductOrService,
    ProductOrServiceDescription,
    ProductGrouping,
    ProductSubGrouping,
    IsConduciveToAutomatedMonitoring,
    --IsEvaluatedInCoverageAssessment,
    --IsMVP1,
    UnderlyingProductTxnsToBeMonitored,
    Comment,
    Question,
    Currency,
    IsProductOrService,
    [Inherent risk has been identified in regulatory guidance or indu],
    [Product has been highlighted in recent AML enforcement actions, ],
    [Transactions related to this product may result in cross-border ],
    [Product features enable unrelated or third parties to make or re],
    [Product features enable anonymity in the transaction],
    [Product features enable change in customer ownership/sponsorship],
    [Elevated risks are associated with the method in which the produ],
    [1 Branch],
    [2 ScotiaOnline],
    [3 ScotiaConnect],
    [4 Mobile],
    [5 ABM],
    [6 Night Deposit, Mail, Courier],
    [7 IVR],
    [8 Call Center],
    [9 SWIFT Wires],
    [10 Non-SWIFT Payments],
    [11 Wealth Channels/PIC],
    [12 Commercial Channels (RM/Fulfillment Team)],
    Comments, 
	Rating
)


SELECT 
	ROW_NUMBER() OVER (ORDER BY [Segment],[Product/Service],COALESCE(ProductGroupETL.ProposedGrouping,SplitGrouping.ProductGrouping)) AS ProductID,
	--Staging.ProductID AS StagingID,
	[Segment] AS Segment,
	[CB Business Unit / GBM Business Line] AS BusinessUnit,
	[System] AS System,
	[Type] AS Type,
	[Product/Service] AS ProductOrService,
	[Product/Service Description] AS ProductOrServiceDescription,
	COALESCE(ProductGroupETL.ProposedGrouping,SplitGrouping.ProductGrouping) AS ProductGrouping,
	[Product Sub-Grouping (separate by ";" if more than one)] AS ProductSubGrouping,
	[Conducive to Automated Monitoring (AML surveillance specific)?] AS IsConduciveToAutomatedMonitoring,
	--[Evaluated During Coverage Assessment?] AS IsEvaluatedInCoverageAssessment,
	--[Covered by MVP1?] AS IsMVP1,
	[Where applicable, Are Underlying Products/Transactions Monitored] AS UnderlyingProductTxnsToBeMonitored,
	[Review Comment] AS Comment,
	[Question] AS Question,
	[Currency] AS Currency,
	[Product or Service] AS IsProductOrService,
	[Inherent risk has been identified in regulatory guidance or indu],
	[Product has been highlighted in recent AML enforcement actions, ],
	[Transactions related to this product may result in cross-border ],
	[Product features enable unrelated or third parties to make or re],
	[Product features enable anonymity in the transaction],
	[Product features enable change in customer ownership/sponsorship],
	[Elevated risks are associated with the method in which the produ],
	[1 Branch],
	[2 ScotiaOnline],
	[3 ScotiaConnect],
	[4 Mobile],
	[5 ABM],
	[6 Night Deposit, Mail, Courier],
	[7 IVR],
	[8 Call Center],
	[9 SWIFT Wires],
	[10 Non-SWIFT Payments],
	[11 Wealth Channels/PIC],
	[12 Commercial Channels (RM/Fulfillment Team)],
	[Comments], 
	CASE Rating WHEN 'High' THEN 1 WHEN 'Moderate' THEN 2 WHEN 'Low' THEN 3 END AS Rating
FROM dbo.XTProduct20181220 Staging
LEFT JOIN SplitGrouping ON Staging.[ID] = SplitGrouping.[ID]
LEFT JOIN ProductGroupETL ON SplitGrouping.ProductGrouping = ProductGroupETL.InitialGrouping;
--WHERE Staging.Segment NOT IN ('GBM')
SET IDENTITY_INSERT TProduct OFF;

--SELECT * FROM TProduct;

/**************************************************************************************************/
--B.TIndicatorXProductGroup
/**************************************************************************************************/

/* 

This table replaces the TIndicatorXProduct table as our predefined Product Groups will map
to a client's product list via a Product Group ID. Older data was used in creating this table.

*/


CREATE TABLE [dbo].[TIndicatorXProductGroup]
(
[IndicatorXProductGroupID] [int] NOT NULL IDENTITY(1, 1),
[IndicatorID] [int] NULL,
[ProductGroupID] [int] NULL,
[CoverageLevel] [int] NULL
) ON [PRIMARY]
GO
ALTER TABLE [dbo].[TIndicatorXProductGroup] 
ADD CONSTRAINT [PK_TIndicatorXProductGroup_1] PRIMARY KEY CLUSTERED ([IndicatorXProductGroupID]) WITH (FILLFACTOR=85) ON [PRIMARY]
GO

WITH GroupingIDs AS (
	SELECT DISTINCT ROW_NUMBER() OVER (ORDER BY ProductGrouping) AS ProductGroupingID, ProductGrouping
	FROM (SELECT DISTINCT ProductGrouping FROM TProduct) a
)

, GroupingComplete AS (
	SELECT DISTINCT TProduct.ProductGrouping, TIndicatorXProduct.*, GroupingIDs.ProductGroupingID 
	FROM TProduct
	INNER JOIN  TIndicatorXProduct ON TProduct.ProductID = TIndicatorXProduct.ProductID
	LEFT JOIN GroupingIDs ON TProduct.ProductGrouping = GroupingIDs.ProductGrouping
	)

INSERT INTO dbo.TIndicatorXProductGroup
(
    IndicatorID,
    ProductGroupID,
    CoverageLevel
)

SELECT DISTINCT 
	IndicatorID
	, GroupingComplete.ProductGroupingID
	, GroupingComplete.CoverageLevel
FROM GroupingComplete

SELECT * FROM TIndicatorXProductGroup;



/**************************************************************************************************/
---XB. TIndicatorXProduct (No Longer Used)
/**************************************************************************************************/

/* 

This table applies product group coverage from the indicator staging table 
to the products in the product table. 

Note: 
	--The script should respond dynamically to different amounts of product groups so long as 
	the non-product group fields are specified. 


*/


--Using updated VIndicator output
DECLARE @IndStagingTable VARCHAR(100);
DECLARE @V_SQL nvarchar(max);
SET IDENTITY_INSERT TIndicatorXProduct ON;
SET @IndStagingTable = 'XTIndicator20181220'
SET @V_SQL = '
WITH OrdinalReference AS (
	SELECT * 
	FROM INFORMATION_SCHEMA.COLUMNS
	WHERE TABLE_NAME = N'''+@IndStagingTable+'''
)

, Products AS (
SELECT COLUMN_NAME AS Product
FROM OrdinalReference
WHERE COLUMN_NAME NOT IN 
	(
		''IndicatorID'',
		''IndicatorRefID'',
		''Indicator'',
		''Segment'',
		''Business Line'',
		''Is Applicable To Bank'',
		''Is Conducive To Automated Monitoring'',
		''Priority'',
		''Red Flag Theme 1'',
		''Red Flag Theme 2'',
		''Red Flag Theme 3'',
		''Oracle Template'',
		''Proposed Rule Name'',
		''Navigant Template'',
		''Fortent Template'',
		''Modifier'',
		''In Oracle MVP1'',
		''Coverage Including Oracle MVP1 And Fortent'',
		''Rule Coverage Notes'',
		''Fortent Coverage'',
		''Fortent Rule Coverage Notes'',
		''Revision History'',
		''Rating'',
		''IsDuplicate''
	)
	AND ORDINAL_POSITION > 
		(SELECT ORDINAL_POSITION 
		FROM OrdinalReference 
		WHERE COLUMN_NAME = ''IsDuplicate''))
		
SELECT @ProdIn = ''''+LEFT(ProdGroup, LEN(ProdGroup)-1)+''''
FROM (
	SELECT CONVERT(VARCHAR(5000),ProdGroup) ProdGroup FROM 
		(SELECT
			(SELECT DISTINCT 
				''[''+Product+''], ''
			FROM Products
			ORDER BY 1
			FOR XML PATH('''')) ProdGroup ) ProdList
	) ListStore;'

--PRINT @V_SQL;
--EXEC (@V_SQL);
DECLARE @ProductList NVARCHAR(max);
SET @ProductList = ''
exec sp_executesql @V_SQL, N'@ProdIn nvarchar(4000) out', @ProductList OUT;
--PRINT @ProductList;

DECLARE @W_SQL NVARCHAR(MAX); 

SET @W_SQL = '
TRUNCATE TABLE TIndicatorXProduct;

WITH IndicatorMapping AS (
SELECT 
	TIndicator.[IndicatorID],
	ProductCoverage, 
	ProductType
FROM  '+@IndStagingTable+' 
UNPIVOT (ProductCoverage for ProductType IN ('+@ProductList+')) upvt 
LEFT JOIN TIndicator ON upvt.IndicatorRefID = TIndicator.IndicatorRefID AND upvt.Segment = TIndicator.Segment
WHERE ISNULL(TIndicator.IsConduciveToAutomatedMonitoring,2) <> 0
)


INSERT INTO dbo.TIndicatorXProduct (IndicatorXProductID, IndicatorID, ProductID, CoverageLevel)	
SELECT 
	ROW_NUMBER() OVER (ORDER BY IndicatorMapping.[IndicatorID], TProduct.ProductID) IndicatorXProductID
	, IndicatorMapping.[IndicatorID]
	, TProduct.ProductID
	--, IndicatorMapping.ProductType
	--, TProduct.ProductOrService
	--, TProduct.ProductGrouping
	, IndicatorMapping.ProductCoverage AS CoverageLevel
FROM IndicatorMapping
LEFT JOIN dbo.TProduct ON IndicatorMapping.ProductType = TProduct.ProductGrouping;
';

--PRINT @W_SQL;
EXEC (@W_SQL);
SET IDENTITY_INSERT TIndicatorXProduct OFF;

--SELECT * FROM TIndicatorXProduct;


/**************************************************************************************************/
--C. VIndicator
/**************************************************************************************************/

/* 

This view combines all other tables into a single view which standardizes the staging table data.


*/


DECLARE @Lists TABLE (ListType VARCHAR(50), List VARCHAR(5000));
INSERT @Lists (ListType, List)

--Create All Dynamic Lists
SELECT 'Theme', ''+LEFT(RedFlag, LEN(RedFlag)-1)+'' 
FROM (
	SELECT CONVERT(VARCHAR(1000),RedFlag) RedFlag FROM 
		(SELECT
			(SELECT DISTINCT 
				'[Red Flag Theme '+CONVERT(VARCHAR(1),Priority)+'], '
			FROM TIndicatorXTheme
			ORDER BY 1
			FOR XML PATH('')) RedFlag ) ThemeList
	) ListStore UNION ALL
--Create Product Grouping List
SELECT 'Product Group', ''+LEFT(ProdGroup, LEN(ProdGroup)-1)+'' 
FROM (
	SELECT CONVERT(VARCHAR(5000),ProdGroup) ProdGroup FROM 
		(SELECT
			(SELECT DISTINCT 
				'['+ProductGrouping+'], '
			FROM TProduct
			ORDER BY 1
			FOR XML PATH('')) ProdGroup ) ProdList
	) ListStore

DECLARE @V_SQL varchar(8000)
SET @V_SQL = '';

SET @V_SQL = @V_SQL+'ALTER VIEW dbo.VIndicator AS
WITH RuleList AS (

	SELECT 
		IndicatorID

		, (SELECT DISTINCT RuleName+'';'' 
			FROM TIndicatorXRule
			LEFT JOIN TRule ON TIndicatorXRule.RuleID = TRule.RuleID
			WHERE Source = ''Oracle'' AND TIndicatorXRule.IndicatorID = TInd.IndicatorID
			ORDER BY 1
			FOR XML PATH('''')) AS  OracleTemplate
		, (SELECT DISTINCT ProposedTheme+'';'' 
			FROM TIndicatorXRule
			WHERE TIndicatorXRule.IndicatorID = TInd.IndicatorID AND ProposedTheme <> ''''
			ORDER BY 1
			FOR XML PATH('''')) AS ProposedThemes
		, (SELECT DISTINCT ProposedRuleName+'';'' 
			FROM TIndicatorXRule
			WHERE TIndicatorXRule.IndicatorID = TInd.IndicatorID AND ProposedRuleName <> ''''
			ORDER BY 1
			FOR XML PATH('''')) AS ProposedTemplate
		,  (SELECT DISTINCT RuleName+'';'' 
			FROM TIndicatorXRule
			LEFT JOIN TRule ON TIndicatorXRule.RuleID = TRule.RuleID
			WHERE Source = ''Navigant'' AND TIndicatorXRule.IndicatorID = TInd.IndicatorID
			ORDER BY 1
			FOR XML PATH('''')) AS  NavigantTemplate
		,  (SELECT DISTINCT RuleName+'';'' 
			FROM TIndicatorXRule
			LEFT JOIN TRule ON TIndicatorXRule.RuleID = TRule.RuleID
			WHERE Source = ''Fortent'' AND TIndicatorXRule.IndicatorID = TInd.IndicatorID
			ORDER BY 1
			FOR XML PATH('''')) AS  FortentTemplate

		,   (SELECT DISTINCT ModifierName+'';'' 
			FROM TIndicatorXModifier
			LEFT JOIN TModifier ON TIndicatorXModifier.ModifierID = TModifier.ModifierID
			WHERE TIndicatorXModifier.IndicatorID = TInd.IndicatorID
			ORDER BY 1
			FOR XML PATH('''')) AS  ModifierName
	FROM TIndicator TInd
)

, ProductGroupList AS (
	SELECT DISTINCT ProductGrouping, ProductGroupID FROM TProduct )

, CoverageSetup AS (
SELECT DISTINCT
	TIndicator.*
	, ProductGroupList.ProductGrouping
	, CoverageLevel
	, Theme
	, ''Red Flag Theme ''+CONVERT(VARCHAR(1),TIndicatorXTheme.Priority) AS FlagPriority
	, LEFT(ModifierName,LEN(ModifierName)-1) AS Modifier
	, LEFT(ProposedThemes,LEN(ProposedThemes)-1) AS ProposedThemes
	, LEFT(OracleTemplate, LEN(OracleTemplate)-1) AS OracleTemplate
	, LEFT(ProposedTemplate, LEN(ProposedTemplate)-1) AS ProposedTemplate
	, LEFT(NavigantTemplate, LEN(NavigantTemplate)-1) AS NavigantTemplate
	, LEFT(FortentTemplate, LEN(FortentTemplate)-1) AS FortentTemplate
FROM TIndicator
LEFT JOIN RuleList ON TIndicator.IndicatorID = RuleList.IndicatorID
LEFT JOIN TIndicatorXTheme ON TIndicator.IndicatorID = TIndicatorXTheme.IndicatorID
LEFT JOIN TIndicatorXProductGroup ON TIndicatorXProductGroup.IndicatorID = TIndicator.IndicatorID
LEFT JOIN ProductGroupList ON TIndicatorXProductGroup.ProductGroupID = ProductGroupList.ProductGroupID
LEFT JOIN TTheme ON TIndicatorXTheme.ThemeID = TTheme.ThemeID
)

, FullView AS ( 
SELECT *
FROM CoverageSetup
PIVOT (MAX([Theme]) FOR [FlagPriority] IN ('
--Incorporating Themes

DECLARE @ThemeList VARCHAR(100)
SELECT @ThemeList = List FROM @Lists WHERE ListType = 'Theme'
SET @V_SQL = @V_SQL+@ThemeList;


SET @V_SQL = @V_SQL+')) ThemePivot

PIVOT (MAX(CoverageLevel) FOR [ProductGrouping] IN ('

--Incorporating Product Groups
DECLARE @ProductGroup VARCHAR(5000)
SELECT @ProductGroup = List FROM @Lists WHERE ListType = 'Product Group'
SET @V_SQL = @V_SQL+@ProductGroup+')) pvt
)

SELECT
	IndicatorID
     , IndicatorRefID
     , Indicator
	 , Segment

       -- Core
       , BusinessLine
       , IsApplicableToBank
       , IsConduciveToAutomatedMonitoring
       , Priority, 

		-- Red Flags
		'+@ThemeList+'
		--, [Red Flag Rule 1]
		--, [Red Flag Rule 2]
		--, [Red Flag Rule 3]

       -- Rules
     , OracleTemplate
     , ProposedTemplate AS [ProposedRuleName]
     , NavigantTemplate
     , FortentTemplate
     , ProposedThemes AS [Themes]
	 , Modifier

       -- Client Specific Meta Data
       , InOracleMVP1
       , CoverageInclOracleMVP1AndFortent
       , RuleCoverageNotes

       , FortentCoverage
       , FortentRuleCoverageNotes
       
       , RevisionHistory
	   , IsDuplicate,'+ 
	   @ProductGroup+'
FROM FullView';

--PRINT @V_SQL;
EXEC (@V_SQL);

/**************************************************************************************************/
--D. View Overwrite
/**************************************************************************************************/

/* 

These series of scripts refreshes tables with ETL views in the Indicator Staging phase.

*/


TRUNCATE TABLE dbo.TIndicator;
SET IDENTITY_INSERT dbo.TIndicator ON; 
INSERT INTO dbo.TIndicator
(
    IndicatorID,
	Segment,
    IndicatorRefID,
    Indicator,
    Priority,
    IsApplicableToBank,
    IsConduciveToAutomatedMonitoring,
    InOracleMVP1,
    CoverageInclOracleMVP1AndFortent,
    FortentCoverage,
    BusinessLine,
    RevisionHistory,
    RuleCoverageNotes,
    FortentRuleCoverageNotes,
    IsDuplicate
)

SELECT * FROM dbo.VTIndicator;
SET IDENTITY_INSERT dbo.TIndicator OFF; 
/**************************************************************************************************/

TRUNCATE TABLE dbo.TTheme; 
SET IDENTITY_INSERT dbo.TTheme ON;
INSERT INTO dbo.TTheme (ThemeID, Theme)
SELECT * FROM dbo.VTTheme;
SET IDENTITY_INSERT dbo.TTheme OFF;
/**************************************************************************************************/

TRUNCATE TABLE dbo.TIndicatorXTheme; 
SET IDENTITY_INSERT dbo.TIndicatorXTheme ON;
INSERT INTO dbo.TIndicatorXTheme (IndicatorXThemeID, IndicatorID, ThemeID, PRIORITY)
SELECT * FROM dbo.VTIndicatorXTheme;
SET IDENTITY_INSERT dbo.TIndicatorXTheme OFF;
/**************************************************************************************************/

TRUNCATE TABLE dbo.TModifier; 
SET IDENTITY_INSERT dbo.TModifier ON;
INSERT INTO dbo.TModifier (ModifierID, ModifierName)
SELECT * FROM dbo.VTModifier;
SET IDENTITY_INSERT dbo.TModifier OFF;
/**************************************************************************************************/

TRUNCATE TABLE dbo.TIndicatorXModifier; 
SET IDENTITY_INSERT dbo.TIndicatorXModifier ON;
INSERT INTO dbo.TIndicatorXModifier (IndicatorXModifierID, IndicatorID, ModifierID)
SELECT * FROM dbo.VTIndicatorXModifier;
SET IDENTITY_INSERT dbo.TIndicatorXModifier OFF;
/**************************************************************************************************/

TRUNCATE TABLE dbo.TRule; 
SET IDENTITY_INSERT dbo.TRule ON;
INSERT INTO dbo.TRule (RuleID, RuleName, Category, SOURCE, ProposedTypology)
SELECT * FROM dbo.VTRule;
SET IDENTITY_INSERT dbo.TRule OFF;
/**************************************************************************************************/

TRUNCATE TABLE dbo.TIndicatorXRule; 
SET IDENTITY_INSERT dbo.TIndicatorXRule ON;
INSERT INTO dbo.TIndicatorXRule (IndicatorXRuleID, IndicatorID, RuleID, ProposedRuleName, Focus)
SELECT *, NULL FROM dbo.VTIndicatorXRule;
SET IDENTITY_INSERT dbo.TIndicatorXRule OFF;


/**************************************************************************************************/
/***********************************III. Display Data**********************************************/
/**************************************************************************************************/


/* 

The scripts below form the basis for the pivots and tables 
which populate the coverage assessment.

*/ 


/**************************************************************************************************/
--A. Indicator Inventory
/**************************************************************************************************/

--Used to Display all data at an Indicator Level. 

SELECT 
	VIndicator.[IndicatorID] AS IndicatorID,
	[IndicatorRefID] AS IndicatorRefID,
	[Indicator] AS Indicator,
	--[Segment] AS Segment,
	--[BusinessLine] AS [Business Line],
	CASE WHEN [IsApplicableToBank] = 1 THEN 'Y' WHEN [IsApplicableToBank] = 0 THEN 'N' ELSE NULL END AS [Is Applicable To Bank],
	CASE WHEN [IsConduciveToAutomatedMonitoring] = 1 THEN 'Y' WHEN [IsConduciveToAutomatedMonitoring] = 0 THEN 'N' ELSE NULL END AS [Is Conducive To Automated Monitoring],
	[Priority] AS [Indicator Priority],
	Themes, 
	--[Red Flag Theme 1] AS [Red Flag Theme 1],
	--[Red Flag Theme 2] AS [Red Flag Theme 2],
	--[Red Flag Theme 3] AS [Red Flag Theme 3],
	[OracleTemplate] AS [Oracle Template],
	[ProposedRuleName] AS [Proposed Rule Name],
	--[NavigantTemplate] AS [Navigant Template],
	--[FortentTemplate] AS [Fortent Template],
	--[Modifier] AS [Modifier],
	--CASE WHEN [InOracleMVP1] = 1 THEN 'Y' WHEN [InOracleMVP1] = 0 THEN 'N' ELSE NULL END AS [In Oracle MVP1],
	--[CoverageInclOracleMVP1AndFortent] AS [Coverage Including Oracle MVP1 And Fortent],
	--[RuleCoverageNotes] AS [Rule Coverage Notes],
	--[FortentCoverage] AS [Fortent Coverage],
	--[FortentRuleCoverageNotes] AS [Fortent Rule Coverage Notes],
	--[RevisionHistory] AS [Revision History],
	CASE WHEN IsDuplicate = 1 THEN 'Y' WHEN IsDUplicate = 0 THEN 'N' ELSE NULL END AS IsDuplicate ,
	[Cash Management] AS [Cash Management],
	[Correspondent Banking] AS [Correspondent Banking],
	[Credit Card] AS [Credit Card],
	--[Custody] AS [Custody],
	[Depository Account - Investment] AS [Depository Account - Investment],
	[Depository Account] AS [Depository Account],
	[Derivative] AS [Derivative],
	[Electronic Funds Transfer] AS [Electronic Funds Transfer],
	[Foreign Exchange] AS [Foreign Exchange],
	[Insurance Non Cash Value] AS [Insurance Non Cash Value],
	[Insurance] AS [Insurance],
	[Investment] AS [Investment],
	[Loan] AS [Loan],
	[Line of Credit] AS [Line of Credit],
	[Mobile/Internet Payment] AS [Mobile/ Internet Payment],
	--[Monetary Instrument] AS [Monetary Instrument],
	[Mortgage] AS [Mortgage],
	[Overdraft] AS [Overdraft],
	[Metals] AS [Metals],
	[RDC] AS [RDC],
	[Service] AS [Service],
	[Trade Finance] AS [Trade Finance],
	[Trust Services] AS [Trust Services]
FROM dbo.VIndicator
WHERE IsDuplicate = 0
ORDER BY IndicatorID;


/**************************************************************************************************/
--B. Pivot Charts
/**************************************************************************************************/

--Used to create heatmaps across Rule, Theme, and Product


WITH TProd AS (
	SELECT DISTINCT
			TIndicator.IndicatorID, 
			Tindicator.IndicatorRefID,
			Tindicator.Indicator, 
			TIndicator.Segment,
			Tindicator.BusinessLine,
			CASE TIndicator.IsApplicableToBank WHEN 0 THEN 'N' WHEN 1 THEN 'Y' END AS IsApplicableToBank,
			CASE TIndicator.IsConduciveToAutomatedMonitoring WHEN 0 THEN 'N' WHEN 1 THEN 'Y' END AS IsConduciveToAutomatedMonitoring,
			--MIN(TIndicator.Priority) OVER (PARTITION BY ProposedRuleName) AS Priority,
			TIndicator.Priority AS [Indicator Priority],
			TIndicatorXRule.ProposedRuleName, 
			TIndicatorXRule.ProposedTheme,
			TRule.RuleName AS OracleRule,
			CASE TIndicator.IsDuplicate WHEN 1 THEN 'Y' WHEN 0 THEN 'N' END AS IsDuplicate,
			TProduct.ProductDescription,
			--TProduct.Rating,
			TProduct.ProductGrouping, 
			CASE TIndicatorXRule.InOracle WHEN 'Full' THEN 1 WHEN 'Partial' THEN 2 WHEN 'None' THEN 3 END AS Coverage, 
			--LEFT(NewSegments.ProposedSegment,LEN(NewSegments.ProposedSegment)-1) AS [Product Segments], 
			TProduct.BusinessLine AS [Product Segment],
			--SegmentMapping.ProposedSegment AS [Product Segment],
			TIndicatorXRule.InOracle AS [Oracle Rule Indicator]
	FROM dbo.TIndicator
	LEFT OUTER JOIN dbo.TIndicatorXProductGroup ON  TIndicator.IndicatorID = TIndicatorXProductGroup.IndicatorID  
	LEFT OUTER JOIN dbo.TIndicatorXRule ON TIndicatorXProductGroup.IndicatorID = TIndicatorXRule.IndicatorID
	INNER JOIN dbo.TProduct ON TIndicatorXProductGroup.ProductGroupID = TProduct.ProductGroupID
	--LEFT OUTER JOIN NewSegments ON TProduct.ProductGrouping = NewSegments.ProductGrouping
	--LEFT OUTER JOIN SegmentMapping ON TProduct.Segment = SegmentMapping.Segment
	INNER JOIN TRule ON TIndicatorXRule.RuleID = TRule.RuleID AND TRule.Source = 'Oracle'
	WHERE --TProduct.ProductDescription IS NOT NULL 
		 TIndicatorXRule.ProposedRuleName IS NOT NULL 
		AND TIndicatorXRule.InOracle IS NOT NULL
)


SELECT 
	TProd.*
	, 1 AS Value
	, 'Category' AS Category
	, MIN([Indicator Priority]) OVER (PARTITION BY ProposedRuleName) AS Priority
	--, CASE WHEN AVG(TProd.Coverage) OVER (PARTITION BY [Product Segment], TProd.ProposedTheme, TProd.ProposedRuleName, TProd.ProductGrouping) < 3 THEN NCHAR(9679) END AS DotCoverage
	, CASE WHEN AVG(TProd.Coverage) OVER (PARTITION BY [Product Segment], TProd.ProposedTheme, TProd.ProposedRuleName, TProd.ProductGrouping) <= 3 THEN 1 END AS DotCoverage
FROM TProd;




/**************************************************************************************************/
--C. Unmapped Products
/**************************************************************************************************/

--All Products without Indicators

WITH AvailableGroups AS (
	SELECT DISTINCT TProduct.ProductGroupID 
	FROM TIndicatorXProductGroup 
	INNER JOIN TProduct 
		ON TIndicatorXProductGroup.ProductGroupID = TProduct.ProductGroupID
)

SELECT DISTINCT
	Source, ProductGrouping
FROM TProduct 
WHERE ProductGroupID NOT IN 
	(SELECT DISTINCT ProductGroupID FROM AvailableGroups)
AND ProductGrouping <> 'N/A'
ORDER BY 1,2;

/**************************************************************************************************/
--D. High Risk Non-Conducive (not used)
/**************************************************************************************************/

--All high risk products not conducive to monitoring (no longer considered)

WITH SegmentMapping AS (

	SELECT 'Commercial' AS Segment, 'CB - Commercial' AS ProposedSegment UNION ALL
	SELECT 'Retail'                                                                     , 'CB - Retail'         UNION ALL
	SELECT 'Global Banking and Markets (GBM)'                                           , 'GBM'                      UNION ALL
	SELECT 'Global Banking and Markets (GBM), Commercial Banking and small business'    , 'CB - Commercial'         UNION ALL
	SELECT 'Small Business'                                                             , 'CB - Small Business'         UNION ALL
	SELECT 'Financial Institutions'                                                     , 'Financial Institutions'   UNION ALL
	SELECT 'Commercial/Corp/Small Business'                                             , 'CB - Commercial'         UNION ALL
	SELECT 'Commercial/Corp'                                                            , 'CB - Commercial'         UNION ALL
	SELECT 'Retail '                                                                    , 'CB - Retail'         UNION ALL
	SELECT 'Wealth'                                                                     , 'Wealth'                   UNION ALL
	SELECT 'GBM'                                                                        , 'GBM'                      UNION ALL
	SELECT 'Group Treasury'                                                             , 'Group Treasury'
	)

SELECT DISTINCT SegmentMapping.ProposedSegment, ProductGrouping, ProductOrService 
FROM dbo.TProduct 
LEFT JOIN SegmentMapping ON TProduct.Segment = SegmentMapping.Segment 
LEFT JOIN TIndicatorXProduct ON TProduct.ProductID = TIndicatorXProduct.ProductID
WHERE Rating = 1 AND IsConduciveToAutomatedMonitoring = 'N';



/**************************************************************************************************/
--E. Rule Mapping Master
/**************************************************************************************************/

--Lists important Proposed Rule Data and notes

WITH RuleComments AS (
	SELECT 'All Activity In/All Activity Out' AS ProposedRuleName                       , 'Rapid Movement Of Funds - All Activity' AS OracleTemplate                   , 'Current RMF does not do All In/All Out' AS Comment UNION ALL
	SELECT 'CIB: Product Utilization Shift'                                             , 'CIB: Product Utilization Shift'                                             , 'Fortent Decommission (Security Blanket)'           UNION ALL
	SELECT 'CIB: Significant Change from Previous Average Activity'                     , 'CIB: Product Utilization Shift'                                             , 'Fortent Decommission (Security Blanket)'           UNION ALL
	SELECT 'CIB: Significant Change from Previous Average Activity'                     , 'CIB: Significant Change from Previous Average Activity'                     , 'Fortent Decommission (Security Blanket)'           UNION ALL
	SELECT 'CIB: Significant Change from Previous Average Activity'                     , 'CIB: Significant Change From Previous Peak Activity'                        , 'Fortent Decommission (Security Blanket)'           UNION ALL
	SELECT 'CIB: Significant Change in Trade/Transaction Activity'                      , 'CIB: Significant Change in Trade/Transaction Activity'                      , 'Fortent Decommission (Security Blanket)'           UNION ALL
	SELECT 'Deposits/Withdrawals in Similar Amounts'                                    , 'Deposits/Withdrawals in Same or Similar Amounts'                            , 'Rename to the Structuring of Wires Rule'           UNION ALL
	SELECT 'Deviation from Peer Group: Product Utilization'                             , 'Deviation From Peer Group - Product Utilization'                            , 'Fortent Decommission (Security Blanket)'           UNION ALL
	SELECT 'Deviation from Peer Group: Total Activity'                                  , 'Deviation from Peer Group - Total Activity'                                 , 'Fortent Decommission (Security Blanket)'           UNION ALL
	SELECT 'Domestic Wire In/International Wire Out'                                    , 'Rapid Movement Of Funds - All Activity'                                     , 'Fortent Decommission'                              UNION ALL
	SELECT 'Escalation in Inactive Account'                                             , 'Escalation in Inactive Account'                                             , 'Fortent Decommission (Security Blanket)'           UNION ALL
	SELECT 'Foreign Exchange Followed by Wire Out'                                      , 'High Risk Transactions: High Risk Geography'                                , 'Revist Mapping'                                    UNION ALL
	SELECT 'Foreign Exchange Followed by Wire Out'                                      , 'Rapid Movement Of Funds - All Activity'                                     , 'Revist Mapping'                                    UNION ALL
	SELECT 'Frequent ATM Deposits'                                                      , 'Large Reportable Transactions'                                              , 'Large Cash Transactions'                           UNION ALL
	SELECT 'Large Hydro Bill Payment'                                                   , 'Large Reportable Transactions'                                              , 'Revisit Mapping (Conducive?)'                      UNION ALL
	SELECT 'Large Wire Transfers'                                                       , 'Large Reportable Transactions'                                              , 'Fortent Decommission'                              UNION ALL
	SELECT 'Micro Structuring'                                                          , 'Structuring: Potential Structuring in Cash and Equivalents'                 , 'Fortent 5 - AMA Custom'                            UNION ALL
	SELECT 'Monetary Instrument In/Monetary Instrument Out'                             , 'Rapid Movement Of Funds - All Activity'                                     , 'Revist Mapping'                                    UNION ALL
	SELECT 'Monetary Instrument Structuring'                                            , 'Deposits/Withdrawals in Same or Similar Amounts'                            , 'Fortent Decommission'                              UNION ALL
	SELECT 'Multiple Jurisdictions'                                                     , 'Custom Scenario'                                                            , 'Partial due to credit from GBP Tool'               UNION ALL
	SELECT 'Nested Correspondent Rule'                                                  , 'Custom Scenario'                                                            , 'Partial due to credit from GBP Tool'               UNION ALL
	SELECT 'Pattern of Sequentially Numbered Checks'                                    , 'Patterns of Sequentially Numbered Checks, Monetary Instruments'             , 'RDC Workstream'                                    UNION ALL
	SELECT 'Rapid Movement of Funds – All Activity'                                     , 'Custom Scenario'                                                            , 'Revist Mapping'                                    UNION ALL
	SELECT 'Rapid Movement of Funds – All Activity'                                     , 'Rapid Movement Of Funds - All Activity'                                     , 'Revist Mapping'                                    UNION ALL
	SELECT 'Structuring: Avoidance of Reporting Thresholds'                             , 'Structuring: Avoidance of Reporting Thresholds'                             , 'Remap to d/w template'                             UNION ALL
	SELECT 'Structuring: Potential Structuring in Cash and Equivalents'                 , 'Deposits/Withdrawals in Same or Similar Amounts'                            , 'Fortent 5 - AMA custom'                            UNION ALL
	SELECT 'Wire In/Wire Out'                                                           , 'Rapid Movement Of Funds - All Activity'                                     , 'Fortent Decommission'                              UNION ALL
	SELECT 'Wire Structuring'                                                           , 'Custom Scenario'                                                            , 'Fortent 5 - AMA custom'                            UNION ALL
	SELECT 'Wire Structuring'                                                           , 'Deposits/Withdrawals in Same or Similar Amounts'                            , 'Fortent 5 - AMA custom'
)

, AutomatedMonitoring AS (
	SELECT 'Multiple Jurisdictions' AS ProposedRuleName      , 'Custom Scenario' AS OracleTemplate , 'Y' AS [Manual/Automated Monitoring] UNION ALL
	SELECT 'Nested Correspondent Rule'   , 'Custom Scenario'  , 'Y'
)

SELECT DISTINCT
	TIndicatorXRule.ProposedTheme
	, TIndicatorXRule.ProposedRuleName
	, TRule.RuleName AS OracleRuleTemplate
	, TIndicatorXRule.InOracle
	, RuleComments.Comment
	, AutomatedMonitoring.[Manual/Automated Monitoring]
	, MIN(TIndicator.Priority) OVER (PARTITION BY TIndicatorXRule.ProposedRuleName) AS Priority
FROM TIndicatorXRule
INNER JOIN TIndicator ON TIndicatorXRule.IndicatorID = TIndicator.IndicatorID
INNER JOIN TRule ON TIndicatorXRule.RuleID = TRule.RuleID AND Source = 'Oracle'
LEFT JOIN AutomatedMonitoring ON TIndicatorXRule.ProposedRuleName = AutomatedMonitoring.ProposedRuleName 
	AND TRule.RuleName = AutomatedMonitoring.OracleTemplate
LEFT JOIN RuleComments ON TIndicatorXRule.ProposedRuleName = RuleComments.ProposedRuleName 
	AND TRule.RuleName = RuleComments.OracleTemplate
WHERE IsConduciveToAutomatedMonitoring = 1 AND IsApplicableToBank = 1 AND IsDuplicate = 0;

/**************************************************************************************************/
--F. Products with Data Feed (not used)
/**************************************************************************************************/

--Lists all products with Datafeeds

WITH SegmentMapping AS (

	SELECT 'Commercial' AS Segment                                                      , 'CB - Commercial' AS ProposedSegment UNION ALL
	SELECT 'Retail'                                                                     , 'CB - Retail'         UNION ALL
	SELECT 'Global Banking and Markets (GBM)'                                           , 'GBM'                      UNION ALL
	SELECT 'Global Banking and Markets (GBM), Commercial Banking and small business'    , 'CB - Commercial'         UNION ALL
	SELECT 'Small Business'                                                             , 'CB - Small Business'         UNION ALL
	SELECT 'Financial Institutions'                                                     , 'Financial Institutions'   UNION ALL
	SELECT 'Commercial/Corp/Small Business'                                             , 'CB - Commercial'         UNION ALL
	SELECT 'Commercial/Corp'                                                            , 'CB - Commercial'         UNION ALL
	SELECT 'Retail '                                                                    , 'CB - Retail'         UNION ALL
	SELECT 'Wealth'                                                                     , 'Wealth'                   UNION ALL
	SELECT 'GBM'                                                                        , 'GBM'                      UNION ALL
	SELECT 'Group Treasury'                                                             , 'Group Treasury'
	)


SELECT DISTINCT 
	SegmentMapping.ProposedSegment,
	DataFeed.[Type MC] AS DataColumn,
	TProduct.*
	 FROM TProduct
LEFT JOIN XTProductDataFeed20181220 DataFeed
	ON TProduct.Segment = DataFeed.Segment 
	AND TProduct.System = DataFeed.System 
	AND TProduct.BusinessUnit = DataFeed.[Business Unit]
	AND TProduct.ProductOrService = DataFeed.[Product/Service]
	AND TProduct.ProductOrServiceDescription = DataFeed.[Product/Service Description]
LEFT JOIN SegmentMapping ON TProduct.Segment = SegmentMapping.Segment 
ORDER BY 1,2,3,4,5;

/**************************************************************************************************/
--G. Product Rule Coverage (not used)
/**************************************************************************************************/

--Initally used for high level data about products and product segments,

WITH SegmentMapping AS (

	SELECT 'Commercial' AS Segment                                                      , 'CB - Commercial' AS ProposedSegment UNION ALL
	SELECT 'Retail'                                                                     , 'CB - Retail'         UNION ALL
	SELECT 'Global Banking and Markets (GBM)'                                           , 'GBM'                      UNION ALL
	SELECT 'Global Banking and Markets (GBM), Commercial Banking and small business'    , 'CB - Commercial'         UNION ALL
	SELECT 'Small Business'                                                             , 'CB - Small Business'         UNION ALL
	SELECT 'Financial Institutions'                                                     , 'Financial Institutions'   UNION ALL
	SELECT 'Commercial/Corp/Small Business'                                             , 'CB - Commercial'         UNION ALL
	SELECT 'Commercial/Corp'                                                            , 'CB - Commercial'         UNION ALL
	SELECT 'Retail '                                                                    , 'CB - Retail'         UNION ALL
	SELECT 'Wealth'                                                                     , 'Wealth'                   UNION ALL
	SELECT 'GBM'                                                                        , 'GBM'                      UNION ALL
	SELECT 'Group Treasury'                                                             , 'Group Treasury'
	)

, Initial AS (
	SELECT DISTINCT
		SegmentMapping.ProposedSegment,
		ProductGrouping, 
		ProposedRuleName, 
		ProposedTheme, 
		RuleName, 
		InOracle, 
		Source, 
		MIN(TIndicator.Priority) OVER (PARTITION BY ProposedRuleName) AS Priority,
		TIndicator.IsConduciveToAutomatedMonitoring, 
		TIndicator.IsApplicabletoBank,
		TIndicator.IsDuplicate,
		MAX(CASE TProduct.IsConduciveToAutomatedMonitoring WHEN 'Y' THEN 1 WHEN 'N' THEN 0 END) OVER (PARTITION BY ProductGrouping) AS ProductConducive,
		MAX(CASE TProduct.IsConduciveToAutomatedMonitoring WHEN 'Y' THEN 1 WHEN 'N' THEN 0 END) OVER (PARTITION BY ProductGrouping, SegmentMapping.ProposedSegment) AS ProductSegmentConducive,
		DENSE_RANK() OVER (PARTITION BY ProductGrouping ORDER BY TProduct.ProductID ASC ) +
			DENSE_RANK() OVER (PARTITION BY ProductGrouping ORDER BY TProduct.ProductID DESC) - 1 AS TotalProducts,
		DENSE_RANK() OVER (PARTITION BY ProductGrouping, SegmentMapping.ProposedSegment ORDER BY TProduct.ProductID ASC ) +
			DENSE_RANK() OVER (PARTITION BY ProductGrouping, SegmentMapping.ProposedSegment ORDER BY TProduct.ProductID DESC) - 1 AS TotalSegmentProducts,
		CASE WHEN DataFeed.[Product/Service] IS NOT NULL THEN TProduct.ProductID ELSE NULL END AS DataProdID, 
		--COUNT(DISTINCT TProduct.ProductID) OVER (PARTITION BY (ProductGrouping)) AS [Total Products], 
		--COUNT(DISTINCT (CASE WHEN DataFeed.[Product/Service] IS NOT NULL THEN TProduct.ProductID END)) OVER (PARTITION BY (ProductGrouping)) AS [Data Products],  
		MIN(CASE InOracle WHEN 'Full' THEN 1 WHEN 'Partial' THEN 2 WHEN 'None' THEN 3 ELSE 4 END) OVER (PARTITION BY ProductGrouping) AS CoverageScore, 
		MIN(Rating) OVER (PARTITION BY ProductGrouping) AS Rating, 
		MIN(Rating) OVER (PARTITION BY ProductGrouping, SegmentMapping.ProposedSegment) AS SegmentRating
	FROM TProduct
	LEFT JOIN SegmentMapping ON TProduct.Segment = SegmentMapping.Segment
	LEFT JOIN XTProductDataFeed20181220 DataFeed
	ON TProduct.Segment = DataFeed.Segment 
	AND TProduct.System = DataFeed.System 
	AND TProduct.BusinessUnit = DataFeed.[Business Unit]
	AND TProduct.ProductOrService = DataFeed.[Product/Service]
	AND TProduct.ProductOrServiceDescription = DataFeed.[Product/Service Description]
	LEFT JOIN TIndicatorXProduct ON TProduct.ProductID = TIndicatorXProduct.ProductID
	LEFT JOIN TIndicator ON TIndicatorXProduct.IndicatorID = TIndicator.IndicatorID
	LEFT JOIN TIndicatorXRule ON TIndicatorXProduct.IndicatorID = TIndicatorXRule.IndicatorID
	LEFT JOIN TRule ON TIndicatorXRule.RuleID = TRule.RuleID
	WHERE (Source IS NULL OR InOracle IS NOT NULL) 
		--AND TIndicator.IsConduciveToAutomatedMonitoring = 1 
		--AND TIndicator.IsApplicabletoBank = 1 
		--AND TIndicator.IsDuplicate = 0
)

SELECT
	*
	, DENSE_RANK() OVER (PARTITION BY ProductGrouping ORDER BY DataProdID ASC ) +
		DENSE_RANK() OVER (PARTITION BY ProductGrouping ORDER BY DataProdID DESC) - 2 AS DataProducts
	, DENSE_RANK() OVER (PARTITION BY ProductGrouping, Initial.ProposedSegment ORDER BY DataProdID ASC ) +
		DENSE_RANK() OVER (PARTITION BY ProductGrouping, Initial.ProposedSegment ORDER BY DataProdID DESC) - 2 AS DataSegmentProducts
	, CASE CoverageScore WHEN 1 THEN 'Full' WHEN 2 THEN 'Partial' WHEN 3 THEN 'None' WHEN 4 THEN 'No Rules' END AS CoverageProduct
FROM Initial;

/**************************************************************************************************/
--H. Expanded Rule Mapping
/**************************************************************************************************/

--Rule Data Responsible for generating high level charts

WITH ProductSegment AS (
	SELECT DISTINCT
		Total.ProductGrouping
		, Total.BusinessLine AS ProposedSegment
		, Total.ProductGroupID
		, DENSE_RANK() OVER (PARTITION BY Total.ProductGrouping ORDER BY Total.ProductID ASC) - 1 +
			DENSE_RANK() OVER (PARTITION BY Total.ProductGrouping ORDER BY Total.ProductID DESC) AS TotalProducts
	FROM dbo.TProduct Total

)

, RawResults AS (
SELECT DISTINCT
	ProductSegment.ProductGrouping 
	, ProductSegment.ProposedSegment AS Segment
	, ProductSegment.TotalProducts
	--, ProductSegment.HighRiskProducts
	, TIndicatorXRule.ProposedTheme
	, TIndicatorXRule.ProposedRuleName
	, TRule.RuleName AS OracleRuleTemplate
	, TIndicator.IndicatorID
	, IndicatorRefID
	, Indicator AS [Indicator Description]
	, TIndicatorXRule.InOracle
	, CASE MIN(TIndicator.Priority) OVER (PARTITION BY ProposedRuleName) WHEN 1 THEN 'Required' WHEN 2 THEN 'Suggested' WHEN 3 THEN 'Suggested' END AS HighLevelPriority
	, CASE TIndicatorXRule.InOracle WHEN 'Full' THEN 1 WHEN 'Partial' THEN 2 WHEN 'None' THEN 3 END AS Coverage
	, CASE InOracle WHEN 'Full' THEN 'Deployed' WHEN 'Partial' THEN 'Not Deployed' WHEN 'None' THEN 'Not Deployed' END AS DeployedRules
	, TIndicator.Priority AS [Indicator Priority]
	--, AVG(CASE TIndicatorXRule.InOracle WHEN 'Full' THEN 1 WHEN 'Partial' THEN 2 WHEN 'None' THEN 3 END) OVER (PARTITION BY ProductSegment.ProposedSegment, TIndicatorXRule.ProposedTheme) AS ThemeCoverage
FROM TIndicatorXRule
INNER JOIN TIndicator ON TIndicatorXRule.IndicatorID = TIndicator.IndicatorID
INNER JOIN TRule ON TIndicatorXRule.RuleID = TRule.RuleID AND Source = 'Oracle'
LEFT JOIN TIndicatorXProductGroup ON TIndicator.IndicatorID = TIndicatorXProductGroup.IndicatorID
--LEFT JOIN TProduct ON TIndicatorXProduct.ProductID = TProduct.ProductID
INNER JOIN ProductSegment ON TIndicatorXProductGroup.ProductGroupID = ProductSegment.ProductGroupID 
WHERE TIndicator.IsConduciveToAutomatedMonitoring = 1 AND TIndicator.IsApplicableToBank = 1 AND TIndicator.IsDuplicate = 0
)

SELECT 
	RawResults.*
	, AVG(Coverage) OVER (PARTITION BY Segment, ProposedTheme) AS ThemeCoverageNum
	, CASE AVG(Coverage) OVER (PARTITION BY ProductGrouping, ProposedTheme) WHEN 1 THEN 'Covered' WHEN 3 THEN 'Not Covered' ELSE
	(CASE WHEN AVG(Coverage) OVER (PARTITION BY ProductGrouping, ProposedTheme) BETWEEN 1 AND 3 THEN 'Limited Coverage' END) END AS ThemeLabelProduct
	, CASE AVG(Coverage) OVER (PARTITION BY Segment, ProposedTheme) WHEN 1 THEN 'Covered' WHEN 3 THEN 'Not Covered' ELSE
		(CASE WHEN AVG(Coverage) OVER (PARTITION BY Segment, ProposedTheme) BETWEEN 1 AND 3 THEN 'Limited Coverage' END) END AS ThemeLabel
	--Product Grouping Metrics
	, DENSE_RANK() OVER (PARTITION BY ProductGrouping ORDER BY Segment ASC) - 1 +
		DENSE_RANK() OVER (PARTITION BY ProductGrouping ORDER BY Segment DESC) AS ProductSegments
	, DENSE_RANK() OVER (PARTITION BY ProductGrouping ORDER BY ProposedTheme ASC) - 1 +
		DENSE_RANK() OVER (PARTITION BY ProductGrouping ORDER BY ProposedTheme DESC) AS ProductThemes
	, DENSE_RANK() OVER (PARTITION BY ProductGrouping ORDER BY ProposedRuleName ASC) - 1 +
		DENSE_RANK() OVER (PARTITION BY ProductGrouping ORDER BY ProposedRuleName DESC) AS ProductRules
	--Segment Metrics
	, DENSE_RANK() OVER (PARTITION BY Segment ORDER BY ProductGrouping ASC) - 1 +
		DENSE_RANK() OVER (PARTITION BY Segment ORDER BY ProductGrouping DESC) AS SegmentProducts
	, DENSE_RANK() OVER (PARTITION BY Segment ORDER BY ProposedTheme ASC) - 1 +
		DENSE_RANK() OVER (PARTITION BY Segment ORDER BY ProposedTheme DESC) AS SegmentThemes
	, DENSE_RANK() OVER (PARTITION BY Segment, HighLevelPriority ORDER BY ProposedRuleName ASC) - 1 +
		DENSE_RANK() OVER (PARTITION BY Segment, HighLevelPriority ORDER BY ProposedRuleName DESC) AS SegmentRules
	--Priority Metrics
	, DENSE_RANK() OVER (PARTITION BY HighLevelPriority ORDER BY ProductGrouping ASC) - 1 +
		DENSE_RANK() OVER (PARTITION BY HighLevelPriority ORDER BY ProductGrouping DESC) AS PriorityProducts
	, DENSE_RANK() OVER (PARTITION BY HighLevelPriority ORDER BY ProposedRuleName ASC) - 1 +
		DENSE_RANK() OVER (PARTITION BY HighLevelPriority ORDER BY ProposedRuleName DESC) AS PriorityRules
	, MIN([Indicator Priority]) OVER (PARTITION BY ProposedRuleName) AS Priority
FROM RawResults;

/**************************************************************************************************/
--I. Theme Description
/**************************************************************************************************/

--Showcases Theme Descriptions as applicable to Segments

SELECT DISTINCT 
	TThemeDescription.*
	, TProduct.BusinessLine AS Segment
	, Priority AS ThemePriority
	--, ProposedRuleName
FROM dbo.TThemeDescription
INNER JOIN TIndicatorXRule ON TIndicatorXRule.ProposedTheme = TThemeDescription.ThemeName
INNER JOIN TIndicator ON TIndicatorXRule.IndicatorID = TIndicator.IndicatorID
INNER JOIN TIndicatorXProductGroup ON TIndicator.IndicatorID = TIndicatorXProductGroup.IndicatorID
INNER JOIN TProduct ON TIndicatorXProductGroup.ProductGroupID = TProduct.ProductGroupID;


/**************************************************************************************************/
--J. Indicators, Themes, and Products
/**************************************************************************************************/

--Truncated VIndicator View with essentals and only conducive indicators.

SELECT 
	--VIndicator.[IndicatorID] AS IndicatorID,
	[IndicatorRefID] AS IndicatorRefID,
	[Indicator] AS Indicator,
	[Priority] AS [Indicator Priority],
	Themes, 
	[Cash Management] AS [Cash Management],
	[Correspondent Banking] AS [Correspondent Banking],
	[Credit Card] AS [Credit Card],
	--[Custody] AS [Custody],
	[Depository Account - Investment] AS [Depository Account - Investment],
	[Depository Account] AS [Depository Account],
	[Derivative] AS [Derivative],
	[Electronic Funds Transfer] AS [Electronic Funds Transfer],
	[Foreign Exchange] AS [Foreign Exchange],
	[Insurance Non Cash Value] AS [Insurance Non Cash Value],
	[Insurance] AS [Insurance],
	[Investment] AS [Investment],
	[Loan] AS [Loan],
	[Line of Credit] AS [Line of Credit],
	[Mobile/Internet Payment] AS [Mobile/ Internet Payment],
	--[Monetary Instrument] AS [Monetary Instrument],
	[Mortgage] AS [Mortgage],
	[Overdraft] AS [Overdraft],
	[Metals] AS [Metals],
	[RDC] AS [RDC],
	[Service] AS [Service],
	[Trade Finance] AS [Trade Finance],
	[Trust Services] AS [Trust Services]
FROM dbo.VIndicator
WHERE IsConduciveToAutomatedMonitoring = 1 AND IsApplicableToBank = 1 AND IsDuplicate = 0
ORDER BY IndicatorID;

/**************************************************************************************************/
--K. Products and Rules
/**************************************************************************************************/

--Rules to Products Mapping


SELECT DISTINCT
	--TProduct.ProductID
	TProduct.BusinessLine AS Segment
	, TProduct.ProductID
	, TProduct.ProductGrouping
	, TProduct.SystemName
	--, TProduct.ProductOrService
	, TProduct.ProductDescription
	--, TProduct.IsConduciveToAutomatedMonitoring
	--, CASE TProduct.Rating WHEN 1 THEN 'High' WHEN 2 THEN 'Moderate' WHEN 3 THEN 'Low' END AS Rating
	, ProposedTheme
	, ProposedRuleName
	, RuleName AS OracleTemplate
FROM dbo.TProduct
INNER JOIN dbo.TIndicatorXProductGroup ON TProduct.ProductGroupID = TIndicatorXProductGroup.ProductGroupID
INNER JOIN dbo.TIndicator ON TIndicatorXProductGroup.IndicatorID = TIndicator.IndicatorID
INNER JOIN dbo.TIndicatorXRule ON TIndicatorXProductGroup.IndicatorID = TIndicatorXRule.IndicatorID AND ProposedRuleName IS NOT NULL
INNER JOIN dbo.TRule ON TIndicatorXRule.RuleID = TRule.RuleID AND TRule.Source = 'Oracle'
WHERE TIndicator.IsConduciveToAutomatedMonitoring = 1 AND IsApplicableToBank = 1 AND IsDuplicate = 0;


