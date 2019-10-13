--inner join if County value in 2017 matches County value in 2007
SELECT `handy-limiter-230219.gun_deaths.gun_deaths2017`.County FROM `handy-limiter-230219.gun_deaths.gun_deaths2017` JOIN `handy-limiter-230219.gun_deaths.gun_deaths2007` ON (`handy-limiter-230219.gun_deaths.gun_deaths2017`.County = `handy-limiter-230219.gun_deaths.gun_deaths2007`.County)

--outer join if County value in 2017 matches County value in 2007
SELECT `handy-limiter-230219.gun_deaths.gun_deaths2017`.County FROM `handy-limiter-230219.gun_deaths.gun_deaths2017` LEFT JOIN `handy-limiter-230219.gun_deaths.gun_deaths2007` ON (`handy-limiter-230219.gun_deaths.gun_deaths2017`.County = `handy-limiter-230219.gun_deaths.gun_deaths2007`.County)

--inner join if County_Code value in 2017 matches County_Code value in 2007
SELECT `handy-limiter-230219.gun_deaths.gun_deaths2017`.County_Code FROM `handy-limiter-230219.gun_deaths.gun_deaths2017` JOIN `handy-limiter-230219.gun_deaths.gun_deaths2007` ON (`handy-limiter-230219.gun_deaths.gun_deaths2017`.County_Code = `handy-limiter-230219.gun_deaths.gun_deaths2007`.County_Code)

--outer join if County_Code value in 2017 matches County_Code value in 2007
SELECT `handy-limiter-230219.gun_deaths.gun_deaths2017`.County_Code FROM `handy-limiter-230219.gun_deaths.gun_deaths2017` LEFT JOIN `handy-limiter-230219.gun_deaths.gun_deaths2007` ON (`handy-limiter-230219.gun_deaths.gun_deaths2017`.County_Code = `handy-limiter-230219.gun_deaths.gun_deaths2007`.County_Code)

--inner join if Deaths value in 2017 matches Deaths value in 2007
SELECT `handy-limiter-230219.gun_deaths.gun_deaths2017`.Deaths FROM `handy-limiter-230219.gun_deaths.gun_deaths2017`  JOIN `handy-limiter-230219.gun_deaths.gun_deaths2007` ON (`handy-limiter-230219.gun_deaths.gun_deaths2017`.Deaths = `handy-limiter-230219.gun_deaths.gun_deaths2007`.Deaths)

--inner join if Deaths value in 2017 matches County_Code value in 2007, returns null
SELECT `handy-limiter-230219.gun_deaths.gun_deaths2017`.Deaths FROM `handy-limiter-230219.gun_deaths.gun_deaths2017`  JOIN `handy-limiter-230219.gun_deaths.gun_deaths2007` ON (`handy-limiter-230219.gun_deaths.gun_deaths2017`.Deaths = `handy-limiter-230219.gun_deaths.gun_deaths2007`.County_Code)

--outer join if Deaths value in 2017 matches Population value in 2007
SELECT `handy-limiter-230219.gun_deaths.gun_deaths2017`.Deaths FROM `handy-limiter-230219.gun_deaths.gun_deaths2017` LEFT JOIN `handy-limiter-230219.gun_deaths.gun_deaths2007` ON (`handy-limiter-230219.gun_deaths.gun_deaths2017`.Deaths = `handy-limiter-230219.gun_deaths.gun_deaths2007`.Population)