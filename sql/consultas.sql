--
-- 1. Rendimiento academico entre paises
--
select country_name as "Country",
    "meanscoreinmathematics(pisa)" as "Math PISA",
    "meanscoreinreading(pisa)" as "Reading PISA",
    "meanscoreinscience(pisa)" as "Science PISA"
from kpi_pais_año
where area = 'Total';

--
-- 2. Desercion estudiantil vs rendimiento estudiantil
--
select
    "meanscoreinmathematics(pisa)" as "Math PISA",
    "meanscoreinreading(pisa)" as "Reading PISA",
    "meanscoreinscience(pisa)" as "Science PISA",
    dtrunc("earlyschooldropoutrate" * 100) || '%' as "Drop rate"
from kpi_pais_año
where area = 'Total' and sex = 'Total';

--
-- 3. Asistencia vs rendimiento estudiantil
--
drop view grades;
create view grades as
select
    dtrunc(("meanscoreinreading(pisa)"
     + "meanscoreinmathematics(pisa)"
     + "meanscoreinscience(pisa)") / 3) as grades,
    country_code
    country_code
from kpi_pais_año;

select
    country_name,
    avg(p."grossattendancerateprimaryeducation") as "Average Primary Attendance",
    avg(p."grossattendanceratesecondaryeducation") as "Average Secondary Attendance",
    avg(p."grossattendanceratetertiaryeducation") as "Average Tertiary Attendance",
    avg(g.grades) as "Average grades"
from grades as g inner join kpi_comparacion_regional as p
    on g.country_code = p.country_code
where p.area = 'Total' and p.sex = 'Total'
group by country_name

--
-- 4. Pobreza vs. rendimiento estudiantil
--
select
    percentageofthepopulationinpoverty * 100 as poverty_rate,
    grades
from grades as g inner join kpi_comparacion_regional as r
    on g.country_code = r.country_code
order by poverty_rate;

select
    country_name,
    coalesce(corr(percentageofthepopulationinpoverty, grades), 0) as poverty_rate
from grades as g inner join kpi_comparacion_regional as r
    on g.country_code = r.country_code
group by country_name;

--
-- 5. Internet vs notas
--
select
    country_name,
    corr(grades, percentagewithaccesstoschoolswithinternet) as correlation_internet_grades
from grades as g inner join kpi_comparacion_regional as r
    on g.country_code = r.country_code
group by country_name;

--
-- 6. Pobreza y asistencia
--
select
    r.country_name,
    avg(r."grossattendancerateprimaryeducation") as "Average Primary Attendance",
    avg(r."grossattendanceratesecondaryeducation") as "Average Secondary Attendance",
    avg(r."grossattendanceratetertiaryeducation") as "Average Tertiary Attendance",
    avg(percentageofthepopulationinpoverty) * 100 || '%' as "Average Poverty Rate"
from kpi_comparacion_regional as r
group by country_name;