-- =============================================
-- CREAREA DIMENSIUNILOR (VIEWS pentru referinţe)
-- =============================================

-- View pentru angajați: include informații despre coordonator, mentor de dezvoltare și mentor de date
CREATE OR REPLACE VIEW dim_employee AS
SELECT 
    e.employee_id,
    e.name,
    e.location,
    c.name AS coordinator_name,
    d.name AS dev_mentor_name,
    dm.name AS data_mentor_name
FROM davax_employees e
LEFT JOIN coordinators c ON e.coord_id = c.coord_id
LEFT JOIN dev_mentors d ON e.dev_id = d.dev_id
LEFT JOIN data_mentors dm ON e.data_id = dm.data_id;


-- View pentru sesiuni: determină disciplina, data, ora de începere/încetare și numele sesiunii (cu numerotare dinamică)
CREATE OR REPLACE VIEW dim_session_discipline AS
WITH base_sessions AS (
    SELECT 
        TRUNC(first_join) AS session_date,
        SUBSTR(source_file, 1, INSTR(source_file, '_Session') - 1) AS discipline,
        MIN(first_join) AS start_time,
        MAX(last_leave) AS end_time
    FROM stg_attandance
    GROUP BY TRUNC(first_join), SUBSTR(source_file, 1, INSTR(source_file, '_Session') - 1)
),
numbered_sessions AS (
    SELECT 
        ROW_NUMBER() OVER (ORDER BY discipline, session_date) AS session_id,
        discipline,
        session_date,
        'Session' || ROW_NUMBER() OVER (PARTITION BY discipline ORDER BY session_date) AS session_name,
        start_time,
        end_time
    FROM base_sessions
)
SELECT 
    session_id,
    discipline,
    session_date,
    session_name,
    start_time,
    end_time
FROM numbered_sessions;

-- View pentru fapt de prezență: conține durata în minute pentru fiecare sesiune per angajat
CREATE OR REPLACE VIEW fact_attendance AS
SELECT
    e.employee_id,
    s.session_id,
    sa.first_join,
    sa.last_leave,
    EXTRACT(DAY FROM (sa.last_leave - sa.first_join)) * 24 * 60 +
    EXTRACT(HOUR FROM (sa.last_leave - sa.first_join)) * 60 +
    EXTRACT(MINUTE FROM (sa.last_leave - sa.first_join)) AS duration_minutes
FROM stg_attandance sa
JOIN dim_employee e ON LOWER(TRIM(sa.name)) = LOWER(TRIM(e.name))
JOIN dim_session_discipline s
    ON s.discipline = SUBSTR(sa.source_file, 1, INSTR(sa.source_file, '_Session') - 1)
   AND s.session_name = REGEXP_SUBSTR(sa.source_file, 'Session[0-9]+');

-- View pentru absențe: extrage absentele din confluence și le leagă de angajații din tabela principală
CREATE OR REPLACE VIEW fact_absence_with_location AS
SELECT
    de.employee_id,
    de.location AS employee_location,
    ca.reason,
    ca.shour,
    ca.ehour,
    ca.sdate,
    ca.edate
FROM confluence_absence ca
JOIN davax_employees de
    ON LOWER(TRIM(ca.name)) = LOWER(TRIM(de.name));

-- View pentru proiecte
CREATE OR REPLACE VIEW dim_project AS
SELECT 
    project_id,
    description
FROM project;
 
-- View pentru task-uri
CREATE OR REPLACE VIEW dim_task AS
SELECT 
    task_id,
    description
FROM ETL.TASK;
 
-- View pentru date calendaristice (poți adapta sau extinde după nevoie)
CREATE OR REPLACE VIEW dim_date AS
SELECT
    to_date_date AS date_value,
    EXTRACT(YEAR FROM to_date_date) AS year,
    EXTRACT(MONTH FROM to_date_date) AS month,
    EXTRACT(DAY FROM to_date_date) AS day,
    TO_CHAR(to_date_date, 'DAY') AS day_name,
    TO_CHAR(to_date_date, 'IW') AS week_number
FROM (
    SELECT TRUNC(created_at) AS to_date_date FROM ETL.TIMESHEET GROUP BY TRUNC(created_at)
);
 
-- ================================================
-- VIEW TIMESHEET (fact table)
-- ================================================
CREATE OR REPLACE VIEW fact_timesheet AS
SELECT 
    ts.employee_id,
    e.name AS employee_name,
    ts.project_id,
    p.description,
    ts.task_id,
    t.description AS task_description,
    ts.location,
    ts.work_type,
    ts.created_at,
    ts.start_date,
    ts.end_date,
    ts.week_number,
    (ts.end_date - ts.start_date + 1) AS days_worked
FROM ETL.TIMESHEET ts
JOIN dim_employee e ON ts.employee_id = e.employee_id
JOIN dim_project p ON ts.project_id = p.project_id
JOIN dim_task t ON ts.task_id = t.task_id;


-- =============================================
-- RAPOARTE
-- =============================================

-- Raport: Câte sesiuni a frecventat fiecare angajat
SELECT 
    e.employee_id,
    e.name AS employee_name,
    e.location,
    e.coordinator_name,
    COUNT(DISTINCT s.session_id) AS total_sessions_attended
FROM fact_attendance f
JOIN dim_employee e ON f.employee_id = e.employee_id
JOIN dim_session_discipline s ON f.session_id = s.session_id
GROUP BY e.employee_id, e.name, e.location, e.coordinator_name
ORDER BY e.employee_id;

-- Raport: Câte sesiuni posibile există și câte a urmat angajatul (comparație)
SELECT 
    e.employee_id,
    e.name AS employee_name,
    e.location,
    e.coordinator_name,
    COUNT(DISTINCT s.session_id) AS total_sessions_attended,
    (SELECT COUNT(*) FROM dim_session_discipline) AS total_sessions_available
FROM fact_attendance f
JOIN dim_employee e ON f.employee_id = e.employee_id
JOIN dim_session_discipline s ON f.session_id = s.session_id
GROUP BY e.employee_id, e.name, e.location, e.coordinator_name
ORDER BY e.employee_id;

-- Raport: orele de absență per angajat și motivul absenței
SELECT
    f.employee_id,
    d.name AS employee_name,
    d.coordinator_name,
    f.reason,
    ROUND(SUM( 
        (TO_DATE(f.edate || ' ' || f.ehour, 'DD-MON-YY HH24:MI:SS') -
         TO_DATE(f.sdate || ' ' || f.shour, 'DD-MON-YY HH24:MI:SS')) * 24
    ), 2) AS total_hours_absent
FROM fact_absence_with_location f
JOIN dim_employee d ON f.employee_id = d.employee_id
GROUP BY f.employee_id, d.name, d.coordinator_name, f.reason
ORDER BY f.employee_id, f.reason;

-- Raport: total ore de absență per angajat (indiferent de motiv)
SELECT
    f.employee_id,
    d.name AS employee_name,
    d.coordinator_name,
    ROUND(SUM( 
        (TO_DATE(f.edate || ' ' || f.ehour, 'DD-MON-YY HH24:MI:SS') -
         TO_DATE(f.sdate || ' ' || f.shour, 'DD-MON-YY HH24:MI:SS')) * 24
    ), 2) AS total_hours_absent
FROM fact_absence_with_location f
JOIN dim_employee d ON f.employee_id = d.employee_id
GROUP BY f.employee_id, d.name, d.coordinator_name
ORDER BY f.employee_id;

-- Pivot: activități de tip absențe (prezența unui tip de absență pe o perioadă)
SELECT
    e.name AS employee_name,
    MAX(CASE WHEN f.reason = 'EXAM' THEN 'YES' ELSE 'NO' END) AS had_exam,
    MAX(CASE WHEN f.reason = 'FACULTY' THEN 'YES' ELSE 'NO' END) AS had_faculty,
    MAX(CASE WHEN f.reason = 'PROJECT' THEN 'YES' ELSE 'NO' END) AS had_project,
    MAX(CASE WHEN f.reason = 'GRADUATION' THEN 'YES' ELSE 'NO' END) AS had_graduation,
    MAX(CASE WHEN f.reason = 'ANNUAL LEAVE' THEN 'YES' ELSE 'NO' END) AS had_annual_leave
FROM fact_absence_with_location f
JOIN dim_employee e ON f.employee_id = e.employee_id
WHERE f.sdate BETWEEN TO_DATE('24-JUN-25', 'DD-MON-YY') AND TO_DATE('30-JUN-25', 'DD-MON-YY')
GROUP BY e.employee_id, e.name, e.coordinator_name
ORDER BY e.employee_id;

-- Pivot: participarea la sesiuni specifice (ETL, PLSQL etc.)
SELECT 
    e.name AS employee_name,
    -- Exemple de coloane pivotate pe sesiuni
    MAX(CASE WHEN s.discipline = 'ETL' AND s.session_name = 'Session1' THEN 'YES' ELSE 'NO' END) AS ETL_Session1,
    MAX(CASE WHEN s.discipline = 'ETL' AND s.session_name = 'Session2' THEN 'YES' ELSE 'NO' END) AS ETL_Session2,
    MAX(CASE WHEN s.discipline = 'ETL' AND s.session_name = 'Session3' THEN 'YES' ELSE 'NO' END) AS ETL_Session3,

    MAX(CASE WHEN s.discipline = 'PLSQL' AND s.session_name = 'Session1' THEN 'YES' ELSE 'NO' END) AS PLSQL_Session1,
    MAX(CASE WHEN s.discipline = 'PLSQL' AND s.session_name = 'Session2' THEN 'YES' ELSE 'NO' END) AS PLSQL_Session2,
    MAX(CASE WHEN s.discipline = 'PLSQL' AND s.session_name = 'Session3' THEN 'YES' ELSE 'NO' END) AS PLSQL_Session3,
    MAX(CASE WHEN s.discipline = 'PLSQL' AND s.session_name = 'Session4' THEN 'YES' ELSE 'NO' END) AS PLSQL_Session4,
    MAX(CASE WHEN s.discipline = 'PLSQL' AND s.session_name = 'Session5' THEN 'YES' ELSE 'NO' END) AS PLSQL_Session5,
    MAX(CASE WHEN s.discipline = 'PLSQL' AND s.session_name = 'Session6' THEN 'YES' ELSE 'NO' END) AS PLSQL_Session6,
    MAX(CASE WHEN s.discipline = 'PLSQL' AND s.session_name = 'Session7' THEN 'YES' ELSE 'NO' END) AS PLSQL_Session7,

    MAX(CASE WHEN s.discipline = 'RDBMS' AND s.session_name = 'Session1' THEN 'YES' ELSE 'NO' END) AS RDBMS_Session1,
    MAX(CASE WHEN s.discipline = 'RDBMS' AND s.session_name = 'Session2' THEN 'YES' ELSE 'NO' END) AS RDBMS_Session2,

    MAX(CASE WHEN s.discipline = 'SDLC' AND s.session_name = 'Session1' THEN 'YES' ELSE 'NO' END) AS SDLC_Session1,
    MAX(CASE WHEN s.discipline = 'DataModeling' AND s.session_name = 'Session1' THEN 'YES' ELSE 'NO' END) AS DataModeling_Session1,
    MAX(CASE WHEN s.discipline = 'SoftwareArchitecture' AND s.session_name = 'Session1' THEN 'YES' ELSE 'NO' END) AS SoftwareArchitecture_Session1

FROM fact_attendance f
JOIN dim_employee e ON e.employee_id = f.employee_id
JOIN dim_session_discipline s ON s.session_id = f.session_id
GROUP BY e.employee_id, e.name, e.coordinator_name
ORDER BY e.employee_id;

-- Raport total participare per disciplină
SELECT 
    e.name AS employee_name,
    COUNT(CASE WHEN s.discipline = 'ETL' THEN 1 END) AS ETL,
    COUNT(CASE WHEN s.discipline = 'PLSQL' THEN 1 END) AS PLSQL,
    COUNT(CASE WHEN s.discipline = 'RDBMS' THEN 1 END) AS RDBMS,
    COUNT(CASE WHEN s.discipline = 'SDLC' THEN 1 END) AS SDLC,
    COUNT(CASE WHEN s.discipline = 'DataModeling' THEN 1 END) AS DataModeling,
    COUNT(CASE WHEN s.discipline = 'SoftwareArchitecture' THEN 1 END) AS SoftwareArchitecture
FROM fact_attendance f
JOIN dim_employee e ON e.employee_id = f.employee_id
JOIN dim_session_discipline s ON s.session_id = f.session_id
GROUP BY e.name;

--  Total zile lucrate pe angajat
SELECT
    employee_id,
    employee_name,
    SUM(days_worked) AS total_days_worked
FROM fact_timesheet
GROUP BY employee_id, employee_name
ORDER BY employee_id;
 
-- Total zile lucrate pe proiect și angajat
SELECT
    employee_id,
    employee_name,
    project_id,
    description,
    SUM(days_worked) AS total_days_worked
FROM fact_timesheet
GROUP BY employee_id, employee_name, project_id, description
ORDER BY employee_id, project_id;
 
-- Total zile lucrate pe task și angajat
SELECT
    employee_id,
    employee_name,
    task_id,
    task_description,
    SUM(days_worked) AS total_days_worked
FROM fact_timesheet
GROUP BY employee_id, employee_name, task_id, task_description
ORDER BY employee_id, task_id;
 
-- Distribuția zilelor lucrate pe tipul de lucru (work_type) pe angajat
SELECT
    employee_id,
    employee_name,
    work_type,
    SUM(days_worked) AS total_days_worked
FROM fact_timesheet
GROUP BY employee_id, employee_name, work_type
ORDER BY employee_id, work_type;
 
-- Ore / zile lucrate în funcție de locație
SELECT
    employee_id,
    employee_name,
    location,
    SUM(days_worked) AS total_days_worked
FROM fact_timesheet
GROUP BY employee_id, employee_name, location
ORDER BY employee_id, location;
 
 
--  Total zile lucrate pe săptămâni per angajat
SELECT
    employee_id,
    employee_name,
    week_number,
    SUM(days_worked) AS total_days_worked
FROM fact_timesheet
GROUP BY employee_id, employee_name, week_number
ORDER BY employee_id, week_number; 