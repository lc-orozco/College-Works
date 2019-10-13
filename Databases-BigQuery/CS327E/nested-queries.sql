--(Nested Query #1) An aggregate query that returns the most often occuring crime
SELECT Primary_Type, Crime_Identification FROM `chicago_crime_2001.Primary_Type_Counts`
GROUP BY Primary_Type, Crime_Identification
HAVING Crime_Identification in (SELECT MAX(`chicago_crime_2001.Primary_Type_Counts`.Crime_Identification) FROM `chicago_crime_2001.Primary_Type_Counts`) 

--(Nested Query #2) An aggregate query that returns returns the least often occuring crime
SELECT Primary_Type, Crime_Identification FROM `chicago_crime_2001.Primary_Type_Counts`
GROUP BY Primary_Type, Crime_Identification
HAVING Crime_Identification in (SELECT MIN(`chicago_crime_2001.Primary_Type_Counts`.Crime_Identification) FROM `chicago_crime_2001.Primary_Type_Counts`) 

--(Nested Query #3) An aggregate query w/ subquery that returns crimes committed at the first date of entry
SELECT Primary_Type, Date 
FROM chicago_crime_2001.crime_data_aggregated 
WHERE date = (SELECT MIN(Date) FROM `chicago_crime_2001.crime_data_aggregated` )

--(Nested Query #4) An aggregate query w/ subquery that returns crimes committed at the last date of entry
SELECT Primary_Type, Date 
FROM chicago_crime_2001.crime_data_aggregated 
WHERE date = (SELECT MAX(Date) FROM `chicago_crime_2001.crime_data_aggregated` )

--(Nested Query #5) A join query w/subquery that returns crimes from Crime_Data_DateTime & crime_data_aggregated with "NARCOTICS" type crimes
SELECT a.Case_Number, a.DateTime, a.Primary_Type 
FROM chicago_crime_2001.Crime_Data_DateTime a
LEFT JOIN chicago_crime_2001.crime_data_aggregated  b 
ON a.Case_Number = b.Case_Number 
WHERE a.Primary_Type = 'NARCOTICS'

--(Nested Query #6) A join query w/subquery that returns crimes from Crime_Data_DateTime & crime_data_aggregated with "OBSCENITY" & "SEX OFFENSE" type crimes
SELECT a.Primary_Type, a.Block
FROM chicago_crime_2001.Crime_Primary_Type_byBlock a
JOIN chicago_crime_2001.crime_data_aggregated  b 
ON a.Block = b.Block
WHERE a.Primary_Type = 'OBSCENITY' 
OR a.Primary_Type = 'SEX OFFENSE'