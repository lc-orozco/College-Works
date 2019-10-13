--Written by Raleigh Melancon
--First Query:
--This query will return the average of crimes and the average of abandoned vehicles by ward. Crime will be assessed if Arrest was true. Abandoned vehicles will assume the vehicle was abandoned if the Status of a ticket was Completed or Closed. A ParDo will be done in order to cast Status as a boolean.

CREATE VIEW `handy-limiter-230219:chicago_abandoned_2001.Averages_of_crime_and_abandoned`
AS SELECT AVG(a.Number_Of_Abandoned_Cars) as Average_Abandoned, AVG(Crime) as Average_Crime
FROM `handy-limiter-230219.chicago_crime_2001.Crime_Data_APR_Revised` c, `handy-limiter-230219.chicago_abandoned_2001.number_abandoned_cars_ward` a
LIMIT 10000

--Second Query:
--This query will return the ward with the MAXIMUM COUNT of crimes JOINED by the number of abandoned cars. This will be helpful in determining the most crime ridden ward and comparing that to the number of abandoned cars in that ward. A ParDo transform will be applied to count the number of abandoned cars so as to join them to the number of crimes.

CREATE VIEW `handy-limiter-230219:chicago_abandoned_2001.Averages_of_crime_and_abandoned`
AS SELECT Ward, Number_Of_Abandoned_Cars 
FROM `handy-limiter-230219.chicago_abandoned_2001.number_abandoned_cars_ward`
WHERE Number_Of_Abandoned_Cars = 
(SELECT MAX(Number_Of_Abandoned_Cars) FROM `handy-limiter-230219.chicago_abandoned_2001.number_abandoned_cars_ward`)
LIMIT 10000


--Third Query:
--This query will return the ward with the MINIMUM COUNT of Crimes JOINED by the number of abandoned cars for each respective ward. This will be helpful in determining the least crime ridden ward and comparing that to the number of abandoned cars in that ward. A ParDo transform will be applied to count the number of abandoned cars so as to join them to the number of crimes.

CREATE VIEW `handy-limiter-230219:chicago_abandoned_2001.Averages_of_crime_and_abandoned`
AS SELECT Ward, Number_Of_Abandoned_Cars 
FROM `handy-limiter-230219.chicago_abandoned_2001.number_abandoned_cars_ward`
WHERE Number_Of_Abandoned_Cars = 
(SELECT MIN(Number_Of_Abandoned_Cars) FROM `handy-limiter-230219.chicago_abandoned_2001.number_abandoned_cars_ward`)
LIMIT 10000

--Written by Luis C. Orozco
--Fourth Query:
--This query has already been written and ran, however, it will still require an Apache Beam Transformation in order to meet the criteria for the current Milestone—which is, as of today, #9. The query creates a table with the Case Number of a crime and if an Arrest was done or not from the chicago_crime_2001.Crime_Data_Aggregated table, as well as the Request Number of an Abandoned Vehicle ticket along if the Status of said ticket was Completed or not from the chicago_abandoned_2001.abandoned_data_aggregated table. It joins the two tables by Ward. The required ParDo transformation for this query will require that dashes are removed from Request_Number in order to make the key type an Int, cast Case_Number as an Int as well.

CREATE VIEW `handy-limiter-230219.chicago_crime_2001.Crime_Arrests_AbandonedCars_Completion_byWardDate`
AS SELECT c.Date, c.Ward, c.Case_Number, c.Arrest, a.Request_Number, a.Complete
FROM `handy-limiter-230219.chicago_crime_2001.Crime_Data_APR_Revised` c
JOIN `handy-limiter-230219.chicago_abandoned_2001.Abandoned_Data_APR_Revised` a
ON a.Ward = c.Ward AND a.Date = c.Date
WHERE c.Case_Number IS NOT NULL AND c.Arrest IS NOT NULL AND c.Ward IS NOT NULL AND a.Request_Number IS NOT NULL AND a.Complete IS NOT NULL
LIMIT 10000

--Fifth Query:
--This query will return the count of actual threating crimes and the count of abandoned vehicles by Ward. The two tables chicago_crime_2001.Crime_Data_Aggregated table and chicago_abandoned_2001.abandoned_data_aggregated table will be joined by Ward. In order to know how many crimes and how many abandoned vehicles are in a ward, we will first assume that a crime with resulted in an Arrest taking place is a threatening crime (i.e. Arrest = true). Then for abandoned vehicles we will assume the vehicle was abandoned if the Status of a ticket was Completed or Closed. This last part will get trickier and that is why a ParDo will be done in order to cast Status as a Boolean—transforming a value of "Complete" to true and a value of "Open" to false.

CREATE VIEW `handy-limiter-230219.chicago_crime_2001.Crime_Count`
AS SELECT COUNT(c.Arrest = true) as Crime, c.Ward
FROM `handy-limiter-230219.chicago_crime_2001.Crime_Data_APR_Revised` c
GROUP BY c.Ward
LIMIT 100000

CREATE VIEW `handy-limiter-230219.chicago_crime_2001.Abandoned_Count`
AS SELECT COUNT(a.Complete = true) as Count_Abandoned, a.Ward
FROM `handy-limiter-230219.chicago_abandoned_2001.Abandoned_Data_APR_Revised` a
GROUP BY a.Ward
LIMIT 100000

--Sixth Query:
--This query will return the average count of arrests and the average count of abandoned vehicles on a given timeframe (either month or day, whichever seems to deliver more output of statistic interest) by Ward. The two tables will be joined on multiple criteria, being joined by both Ward and Date. There could possibly have to be created four more tables deriving from the former ones previously mentioned, two tables from the different former datasets organized by Wards respectively and then two more tables organized by Date respectively. In order for this query to run, Date from chicago_crime_2001.Crime_Data_Aggregated must be first formatted into a valid Date type column—we will have to remove hours and minutes from Date in order to account for the Date column in chicago_abandoned_2001.abandoned_data_aggregated missing hourly information. Date from abandoned_data_aggregated must also be casted as a Date type column.
