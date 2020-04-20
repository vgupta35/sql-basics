/**************************************************************************************************/
/****************************Setting up Test Cases for Banco Popular ******************************/
/**************************************************************************************************/

-----------------------------------TABLE OF CONTENTS------------------------------------------------

--A. Input Thresholds
--B. Generate Tests
--C. Display Edge Tests
--D. Bring all thresholds for each test case
--E. Final Outputs


/**************************************************************************************************/
/**************************************************************************************************/
/**************************************************************************************************/


USE BancoPopularPR
GO


IF OBJECT_ID('tempdb..#TEdgeTests')		IS NOT NULL DROP TABLE #TEdgeTests; 
IF OBJECT_ID('tempdb..#ThresholdsUnpivoted')		IS NOT NULL DROP TABLE #ThresholdsUnpivoted; 

/**************************************************************************************************/
--A. Input Thresholds
/**************************************************************************************************/

---Segments defined as variables which create different thresholds. 
---Splits used when there are multiple ways to achieve a threshold.
---Threshold name follows 'Min/Max%Unit' syntax to faciliate interpretation.

WITH TThresholds AS (                                
	SELECT 'Rule 1: ATH Movil CIB' AS RuleName		, 'Business' AS Segment , 'MinActiveMonths' AS Threshold     , 6 AS Value              , '' AS Split                             UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Business'            , 'MinCurrentAmount'		 , 10000          , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Business'            , 'MinCurrentCount'			 , 30             , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Business'            , 'MinDeviations'			 , 2              , 'IncomingAmount, OutgoingAmount, IncomingCount, OutgoingCount'UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Business'            , 'MinProfileAmount'		 , 10000          , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Business'            , 'MinProfileCount'			 , 30             , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Personal'            , 'MaxDeviations'			 , 7              , 'IncomingAmount, OutgoingAmount, IncomingCount, OutgoingCount'UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Personal'            , 'MinActiveMonths'			 , 6              , ''                                                            UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Personal'            , 'MinCurrentAmount'		 , 3200           , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Personal'            , 'MinCurrentCount'			 , 30             , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Personal'            , 'MinDeviations'			 , 2              , 'IncomingAmount, OutgoingAmount, IncomingCount, OutgoingCount'UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Personal'            , 'MinDollarIncrease'		 , 1000.01        , 'IncomingAmount, OutgoingAmount'                              UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Personal'            , 'MinProfileAmount'		 , 3000           , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 1: ATH Movil CIB'					, 'Personal'            , 'MinProfileCount'			 , 30             , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'B-Smart'             , 'MinActiveMonths'          , 6              , ''                                                            UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'B-Smart'             , 'MinCountIncreaseRatio'    , 5              , 'IncomingCount, OutgoingCount'                                UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'B-Smart'             , 'MinCurrentAmount'         , 60000          , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'B-Smart'             , 'MinCurrentCount'          , 1              , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'B-Smart'             , 'MinDeviations'            , 5              , 'IncomingAmount, OutgoingAmount, IncomingCount, OutgoingCount'UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'B-Smart'             , 'MinDollarIncrease'        , 30000          , 'IncomingAmount, OutgoingAmount, IncomingCount, OutgoingCount'UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'B-Smart'             , 'MinDollarIncreaseRatio'   , 5              , 'IncomingAmount, OutgoingAmount'                              UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'B-Smart'             , 'MinProfileAmount'         , 5000           , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'B-Smart'             , 'MinProfileCount'          , 10             , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Business'            , 'MinActiveMonths'          , 6              , ''                                                            UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Business'            , 'MinCountIncreaseRatio'    , 5              , 'IncomingCount, OutgoingCount'                                UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Business'            , 'MinCurrentAmount'         , 160000         , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Business'            , 'MinCurrentCount'          , 1              , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Business'            , 'MinDeviations'            , 5              , 'IncomingAmount, OutgoingAmount, IncomingCount, OutgoingCount'UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Business'            , 'MinDollarIncrease'        , 80000          , 'IncomingAmount, OutgoingAmount, IncomingCount, OutgoingCount'UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Business'            , 'MinDollarIncreaseRatio'   , 5              , 'IncomingAmount, OutgoingAmount'                              UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Business'            , 'MinProfileAmount'         , 5000           , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Business'            , 'MinProfileCount'          , 10             , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Personal'            , 'MinActiveMonths'          , 6              , ''                                                            UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Personal'            , 'MinCountIncreaseRatio'    , 5              , 'IncomingCount, OutgoingCount'                                UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Personal'            , 'MinCurrentAmount'         , 35000          , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Personal'            , 'MinCurrentCount'          , 1              , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Personal'            , 'MaxCurrentCount'          , 40             , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Personal'            , 'MinDeviations'            , 5              , 'IncomingAmount, OutgoingAmount, IncomingCount, OutgoingCount'UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Personal'            , 'MaxDeviations'            , 90             , 'IncomingAmount, OutgoingAmount, IncomingCount, OutgoingCount'UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Personal'            , 'MinDollarIncrease'        , 30000          , 'IncomingAmount, OutgoingAmount, IncomingCount, OutgoingCount'UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Personal'            , 'MinDollarIncreaseRatio'   , 5              , 'IncomingAmount, OutgoingAmount'                              UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Personal'            , 'MinProfileAmount'         , 5000           , 'Incoming, Outgoing'                                          UNION ALL
	SELECT 'Rule 5: Historical Profile Deviation'	, 'Personal'            , 'MinProfileCount'          , 10             , 'Incoming, Outgoing'                                          
                                     

)

/**************************************************************************************************/
--B. Generate Tests
/**************************************************************************************************/
---Depending on Threshold prefix, tests and target value effects established.
, FloorCeiling AS (
	SELECT 'Min' AS Direction, -1 AS Effect,'Below Floor'	AS EdgeTest UNION ALL
	SELECT 'Min' AS Direction, 0 AS Effect,'At Floor'		AS EdgeTest UNION ALL
	SELECT 'Max' AS Direction, 0 AS Effect,'At Ceiling'		AS EdgeTest UNION ALL
	SELECT 'Max' AS Direction, 1 AS Effect,'Above Ceiling'	AS EdgeTest
)

---Depending on Threshold suffix, magnitude of edge determined. 
, EdgeQuant AS (
	SELECT 'Amount' AS Qualifier, .01 AS DiffValue UNION ALL
	SELECT 'Count'	, 1		UNION ALL
	SELECT 'Ratio'	, .01	UNION ALL
	SELECT 'Days'	, 1		UNION ALL
	SELECT 'Months'	, 1		UNION ALL 
	SELECT 'Increase', .01 	UNION ALL
	SELECT 'Deviations', .01 
)

---Splits divided into separate thresholds hyphenated with split.
---Split is only used on edge. When below floor/above ceiling, all possible triggers are off edge.
---Ex. For MinAmount w/ Incoming/Outgoing: Below Floor both, Only Incoming At Floor, Only Outgoing At Floor 

, TempData AS (
SELECT
	CAST((11 + DENSE_RANK() OVER (ORDER BY TThresholds.RuleName)) AS VARCHAR) AS RuleNum
	, RuleName
	, Segment
	, Threshold AS TargetName
	, Value AS TargetValue
	, CONVERT(XML,'<x>'+REPLACE(COALESCE(Split,''),',','</x><x>')+'</x>') AS XMLSplit
FROM TThresholds
)

, TSetup AS (
	SELECT DISTINCT 
		TempData.RuleNum
		, TempData.RuleName
		, TempData.Segment
		, NULLIF(LTRIM(RTRIM(Element.Loc.value('.','varchar(50)'))),'') AS [SplitResults]
		, TempData.TargetName
		, FloorCeiling.EdgeTest
		, TempData.TargetValue + FloorCeiling.Effect*EdgeQuant.DiffValue AS TargetValue
		, EdgeQuant.Qualifier AS DataType
		, FloorCeiling.Direction
		FROM   TempData  
		CROSS APPLY XMLSplit.nodes('/x') as Element(Loc)
		JOIN EdgeQuant ON TempData.TargetName LIKE '%'+EdgeQuant.Qualifier
		JOIN FloorCeiling ON TempData.TargetName LIKE FloorCeiling.Direction+'%'
)

/**************************************************************************************************/
--C. Display Edge Tests
/**************************************************************************************************/

SELECT DISTINCT
	RuleNum+'.'+CAST(DENSE_RANK() OVER (PARTITION BY TSetup.RuleNum ORDER BY TSetup.Segment, TSetup.TargetName
	, TSetup.TargetValue, CASE WHEN TSetup.EdgeTest LIKE 'At %' THEN TSetup.SplitResults ELSE NULL END) AS VARCHAR) AS TestID
	 ,TSetup.RuleNum
     ,TSetup.RuleName
     ,TSetup.Segment
     ,CASE WHEN TSetup.EdgeTest LIKE 'At %' THEN TSetup.SplitResults ELSE NULL END AS SplitResults
     ,TSetup.TargetName
	 ,TSetup.TargetName+(CASE WHEN SplitResults IS NULL THEN '' ELSE '-'+SplitResults END) AS TargetFullName
     ,TSetup.EdgeTest
     ,TSetup.TargetValue
	 ,TSetup.DataType
INTO #TEdgeTests
FROM TSetup;


WITH BaselineThresholds AS (
	SELECT DISTINCT
		RuleName
		, Segment
		, SplitResults
		, TargetName
		, TargetFullName
		, TargetValue 
	FROM #TEdgeTests WHERE EdgeTest LIKE 'At %'
)
/**************************************************************************************************/
--D. Bring all thresholds for each test case
/**************************************************************************************************/

	SELECT DISTINCT
			#TEdgeTests.RuleName,
			TestID,
			CASE WHEN EdgeTest LIKE 'At %' THEN #TEdgeTests.TargetFullName ELSE  #TEdgeTests.TargetName END AS TestValue,
			#TEdgeTests.EdgeTest AS TestType,
			#TEdgeTests.Segment,
			BaselineThresholds.TargetFullName AS ThresholdName,
			--BaselineThresholds.SplitResults AS Filter,
			CASE WHEN EdgeTest LIKE 'At %' THEN
				CASE WHEN #TEdgeTests.TargetFullName = BaselineThresholds.TargetFullName THEN 'Test Group'
				WHEN COALESCE(#TEdgeTests.SplitResults,'') = COALESCE(BaselineThresholds.SplitResults,'') THEN 'Control Group'
				WHEN COALESCE(#TEdgeTests.SplitResults,'') IN 
					(SELECT COALESCE(SplitResults,'') FROM BaselineThresholds BT 
					WHERE BT.TargetName = BaselineThresholds.TargetName
					AND BT.RuleName = BaselineThresholds.RuleName
					AND BT.Segment = BaselineThresholds.Segment) THEN 'Altered Group'
				ELSE 'Control Group' END
			WHEN #TEdgeTests.TargetName = BaselineThresholds.TargetName THEN 'Test Group'
			ELSE 'Control Group' END AS Marker,	
			CASE WHEN EdgeTest LIKE 'At %' THEN
				CASE WHEN #TEdgeTests.TargetFullName = BaselineThresholds.TargetFullName THEN #TEdgeTests.TargetValue 
				WHEN COALESCE(#TEdgeTests.SplitResults,'') = COALESCE(BaselineThresholds.SplitResults,'') THEN BaselineThresholds.TargetValue
				WHEN COALESCE(#TEdgeTests.SplitResults,'') IN 
					(SELECT COALESCE(SplitResults,'') FROM BaselineThresholds BT 
					WHERE BT.TargetName =BaselineThresholds.TargetName
					AND BT.RuleName = BaselineThresholds.RuleName
					AND BT.Segment = BaselineThresholds.Segment
					) THEN
					CASE WHEN BaselineThresholds.TargetName LIKE 'Min%' THEN
						(SELECT MIN(TargetValue) FROM #TEdgeTests TEdge 
						WHERE BaselineThresholds.RuleName = TEdge.RuleName
						AND BaselineThresholds.Segment = TEdge.Segment
						AND BaselineThresholds.TargetName = TEdge.TargetName)
					WHEN BaselineThresholds.TargetName LIKE 'Max%' THEN
						(SELECT MAX(TargetValue) FROM #TEdgeTests TEdge 
						WHERE BaselineThresholds.RuleName = TEdge.RuleName
						AND BaselineThresholds.Segment = TEdge.Segment
						AND BaselineThresholds.TargetName = TEdge.TargetName) END
				ELSE BaselineThresholds.TargetValue END
			WHEN #TEdgeTests.TargetName = BaselineThresholds.TargetName THEN #TEdgeTests.TargetValue 
			ELSE BaselineThresholds.TargetValue
			END AS ThresholdValue,
			CAST(RIGHT(TestID, LEN(TestID)-1 - LEN(RuleNum)) AS INT) AS CaseNumber,
			'Threshold_'+CAST(DENSE_RANK() OVER (PARTITION BY TestID ORDER BY BaselineThresholds.TargetFullName) AS VARCHAR) AS ThresholdNum,
			'Marker_'+CAST(DENSE_RANK() OVER (PARTITION BY TestID ORDER BY BaselineThresholds.TargetFullName) AS VARCHAR) AS MarkerNum,
			'Value_'+CAST(DENSE_RANK() OVER (PARTITION BY TestID ORDER BY BaselineThresholds.TargetFullName) AS VARCHAR) AS ValueNum
	INTO #ThresholdsUnpivoted
	FROM #TEdgeTests
	JOIN BaselineThresholds 
		ON #TEdgeTests.RuleName = BaselineThresholds.RuleName 
		AND #TEdgeTests.Segment = BaselineThresholds.Segment;
	--ORDER BY #TEdgeTests.RuleName, CAST(RIGHT(TestID, LEN(TestID)-2) AS INT), BaselineThresholds.TargetFullName, ThresholdValue

DECLARE @ThresholdList VARCHAR(MAX); 

WITH Names AS (
	SELECT DISTINCT Name
	,  CAST(SUBSTRING(Name, CHARINDEX('_',Name)+1,LEN(Name)) AS INT) AS NumOrder
	, CASE SUBSTRING(Name, 1,CHARINDEX('_',Name)-1) 
		WHEN 'Threshold' THEN 'A' WHEN 'Marker' THEN 'B' WHEN 'Value' THEN 'C' END AS ValOrder
	FROM #ThresholdsUnpivoted
	CROSS APPLY(VALUES(ThresholdNum, ThresholdName),(MarkerNum, Marker),(ValueNum, CAST(ThresholdValue	AS VARCHAR))) c(Name, Value)

)


SELECT @ThresholdList = LEFT(CONVERT(VARCHAR(5000),GroupA),LEN(CONVERT(VARCHAR(5000),GroupA))-1) FROM 
		(SELECT
			(SELECT
				'['+Name+'], '
			FROM Names
			ORDER BY NumOrder, ValOrder
			FOR XML PATH('')) GroupA ) List
			;


DECLARE @V_SQL NVARCHAR(MAX); 

SET @V_SQL = '

	WITH UnpivotedData AS (
		SELECT RuleName,
			   TestID,
			   TestValue,
			   TestType,
			   Segment,
			   c.Name,
			   c.Value
		FROM #ThresholdsUnpivoted 
		CROSS APPLY(VALUES(ThresholdNum, ThresholdName),(MarkerNum, Marker),(ValueNum, CAST(ThresholdValue	AS VARCHAR))) c(Name, Value)
	)


	SELECT pvt.*
	FROM UnpivotedData
	PIVOT (MAX(Value) 
	FOR NAME IN ('+@ThresholdList+')) pvt
	ORDER BY RuleName, CAST(RIGHT(TestID, LEN(TestID)-CHARINDEX(''.'',TestID)) AS INT)' ;
	
--PRINT(@V_SQL);
/**************************************************************************************************/
--E. Final Outputs
/**************************************************************************************************/

---Final Doc shows all Thresholds in Proper Test Case Format.
EXEC(@V_SQL);

---Shows all tests and target values for relevant variables.
SELECT * FROM #TEdgeTests
ORDER BY RuleName, CAST(RIGHT(TestID, LEN(TestID)-CHARINDEX('.',TestID)) AS INT)


---Shows all thresholds for each test and their relationship to the edge case (unpivoted format).
SELECT RuleName,
       TestID,
       TestValue,
       TestType,
       Segment,
       ThresholdName,
       Marker,
       ThresholdValue 
FROM #ThresholdsUnpivoted
ORDER BY RuleName, CAST(RIGHT(TestID, LEN(TestID)-CHARINDEX('.',TestID)) AS INT), ThresholdName

