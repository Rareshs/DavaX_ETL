CREATE OR REPLACE TRIGGER trg_timesheet_dates
BEFORE INSERT OR UPDATE ON timesheet
FOR EACH ROW
DECLARE
    v_start_day NUMBER;
    v_end_day NUMBER;
BEGIN
    -- Verificăm dacă start_date <= end_date
    IF :NEW.start_date > :NEW.end_date THEN
        RAISE_APPLICATION_ERROR(-20001, 'Start_date trebuie să fie mai mic sau egal cu End_date.');
    END IF;

    -- Verificăm dacă start_date și end_date sunt în aceeași zi (ignorăm ora)
    IF TRUNC(:NEW.start_date) != TRUNC(:NEW.end_date) THEN
        RAISE_APPLICATION_ERROR(-20002, 'Start_date și End_date trebuie să fie în aceeași zi.');
    END IF;

    -- Extragem ziua săptămânii (1 = duminică, 7 = sâmbătă)
    v_start_day := TO_CHAR(:NEW.start_date, 'D');
    v_end_day := TO_CHAR(:NEW.end_date, 'D');

    -- Dacă start_date este sâmbătă (7) sau duminică (1) => eroare
    IF v_start_day IN (1,7) THEN
        RAISE_APPLICATION_ERROR(-20003, 'Start_date nu poate fi într-o zi de weekend (sâmbătă sau duminică).');
    END IF;

    -- Dacă end_date este sâmbătă (7) sau duminică (1) => eroare
    IF v_end_day IN (1,7) THEN
        RAISE_APPLICATION_ERROR(-20004, 'End_date nu poate fi într-o zi de weekend (sâmbătă sau duminică).');
    END IF;
END;
/

CREATE OR REPLACE TRIGGER trg_check_project_task
BEFORE INSERT OR UPDATE ON timesheet
FOR EACH ROW
DECLARE
    v_count NUMBER;
BEGIN
    SELECT COUNT(*)
    INTO v_count
    FROM project_task pt
    WHERE pt.project_id = :NEW.project_id
      AND pt.task_id = :NEW.task_id;
    IF v_count = 0 THEN
        RAISE_APPLICATION_ERROR(-20001, 'Invalid combination of project_id and task_id.');
    END IF;
END;
/



--trg de tip SCD type 2 cu istoric pe angajat
CREATE OR REPLACE TRIGGER trg_davax_employee_scd
BEFORE UPDATE ON davax_employees
FOR EACH ROW
DECLARE
    v_change BOOLEAN := FALSE;
BEGIN
    -- Detectăm schimbări pe atributele urmărite
    IF :OLD.name       != :NEW.name OR
       :OLD.location   != :NEW.location OR
       NVL(:OLD.dev_id, -1)    != NVL(:NEW.dev_id, -1) OR
       NVL(:OLD.data_id, -1)   != NVL(:NEW.data_id, -1) OR
       NVL(:OLD.coord_id, -1)  != NVL(:NEW.coord_id, -1) THEN
        v_change := TRUE;
    END IF;

    IF v_change THEN
        -- 1. Marcăm vechiul rând ca fiind închis
        UPDATE historical_davax_employees
        SET current_flag = 'N',
            end_date = SYSDATE
        WHERE employee_id = :OLD.employee_id
          AND current_flag = 'Y';

        -- 2. Inserăm noua versiune
        INSERT INTO historical_davax_employees (
            employee_id, name, location,
            dev_id, data_id, coord_id,
            start_date, end_date, current_flag
        )
        VALUES (
            :NEW.employee_id, :NEW.name, :NEW.location,
            :NEW.dev_id, :NEW.data_id, :NEW.coord_id,
            SYSDATE, NULL, 'Y'
        );
    END IF;
END;
/