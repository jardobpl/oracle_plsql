-- ==================================================
-- SPECYFIKACJA PAKIETU: pkg_antf_load
-- Plik: pkg_antf_load_spec.sql
-- Musi zostać wdrożona PRZED pkg_antf_load_body.sql
-- ==================================================
CREATE OR REPLACE PACKAGE pkg_antf_load IS

    -- Wersja pakietu – widoczna dla narzędzi zewnętrznych i logowana w audycie
    c_package_version CONSTANT VARCHAR2(20) := '3.0.0';
    c_mode_initial_load CONSTANT NUMBER := -1;

    -- -----------------------------------------------
    -- KROK 1: Otwarcie runu + czyszczenie STG
    -- Czyści wszystkie tabele STG, następnie oblicza
    -- i zamraża okno czasowe (extract_from / extract_to)
    -- dla wszystkich batchów. Ustawia status IN_PROGRESS w HWM.
    -- Bezpieczna przy ponownym uruchomieniu po błędzie – nadpisze poprzedni IN_PROGRESS.
    -- -----------------------------------------------
    PROCEDURE prc_start_run ;

    -- -----------------------------------------------
    -- KROK 2a: Ekstrakcja delta – tabele 1–5
    -- Wywołać równolegle z batch_2 i batch_3. Wymaga wcześniejszego prc_start_run .
    -- -----------------------------------------------
    PROCEDURE prc_extract_delta_batch_1;

    -- -----------------------------------------------
    -- KROK 2b: Ekstrakcja delta – tabele 6–10
    -- Wywołać równolegle z batch_1 i batch_3. Wymaga wcześniejszego prc_start_run .
    -- -----------------------------------------------
    PROCEDURE prc_extract_delta_batch_2;

    -- -----------------------------------------------
    -- KROK 2c: Ekstrakcja delta – tabele 11–15
    -- Wywołać równolegle z batch_1 i batch_2. Wymaga wcześniejszego prc_start_run .
    -- -----------------------------------------------
    PROCEDURE prc_extract_delta_batch_3;

    -- -----------------------------------------------
    -- KROK 3: Ładowanie faktów – fct_ubezpieczenia_sprzedaz
    -- Wywołać po zakończeniu wszystkich trzech batchów.
    -- Można uruchamiać równolegle z innymi prc_load_fct_*.
    -- -----------------------------------------------
    PROCEDURE prc_load_fct_policies;

    -- -----------------------------------------------
    -- KROK 4: Zamknięcie runu
    -- Przesuwa HWM (last_success_run := target_run).
    -- Wywołać TYLKO po pomyślnym zakończeniu wszystkich kroków 2 i 3.
    -- -----------------------------------------------
    PROCEDURE prc_end_run ;

END pkg_antf_load;
/

-- ==================================================
-- WYMAGANE ZMIANY DDL PRZED WDROŻENIEM PAKIETU
-- ==================================================
-- 1. etl_audit_log: log_id (identity PK), step_name VARCHAR2(200),
--    extract_from/extract_to DATE.
--
-- 2. etl_hwm_control: last_success_run/target_run DATE, PK na process_name.
--    Wiersz startowy: ('ETL_DELTA_MAIN', <data>, 'COMPLETED', 0, NULL)
--
-- 3. Specyfikacja pakietu (PACKAGE) w pliku pkg_antf_load_spec.sql
--    – wdrożyć PRZED tym plikiem.
-- ==================================================

CREATE OR REPLACE PACKAGE BODY pkg_antf_load IS

    -- ==================================================
    -- KONFIGURACJA PAKIETU
    -- ==================================================
    -- Overlap 15 minut wyrażony jako ułamek dnia (15/1440) – kompatybilne z DATE
    c_overlap_minutes  CONSTANT NUMBER       := 15/1440;
    c_hwm_process_name CONSTANT VARCHAR2(30) := 'ETL_DELTA_MAIN';

    -- Stałe z nazwami batchów – jedno miejsce zmiany konwencji nazewniczej
    c_batch_1_name     CONSTANT VARCHAR2(10) := 'BATCH_1';
    c_batch_2_name     CONSTANT VARCHAR2(10) := 'BATCH_2';
    c_batch_3_name     CONSTANT VARCHAR2(10) := 'BATCH_3';
    c_load_fact_name   CONSTANT VARCHAR2(10) := 'LOAD_FACT';

    -- Ujednolicony status "w trakcie" w etl_audit_log (spójny z etl_hwm_control)
    c_status_running   CONSTANT VARCHAR2(20) := 'IN_PROGRESS';

    -- Wzorzec formatu daty – używany w TO_CHAR w całym pakiecie
    c_date_format      CONSTANT VARCHAR2(30) := 'YYYY-MM-DD HH24:MI:SS';

    -- ==================================================
    -- PRYWATNA PROCEDURA: USTAWIENIE NLS DLA SESJI
    -- Wywoływana na początku każdej publicznej procedury dla
    -- deterministycznego zachowania niezależnie od ustawień sesji.
    -- ==================================================
    PROCEDURE set_session_nls IS
    BEGIN
        EXECUTE IMMEDIATE q'[ALTER SESSION SET NLS_DATE_FORMAT = 'YYYY-MM-DD HH24:MI:SS']';
        --EXECUTE IMMEDIATE q'[ALTER SESSION SET NLS_NUMERIC_CHARACTERS = '.,']';
        --EXECUTE IMMEDIATE q'[ALTER SESSION SET NLS_LANGUAGE = 'AMERICAN']';
        --EXECUTE IMMEDIATE q'[ALTER SESSION SET NLS_TERRITORY = 'AMERICA']';
    END set_session_nls;

    -- ==================================================
    -- FUNKCJA POMOCNICZA: formatowanie błędu z SQLCODE
    -- ==================================================
    FUNCTION format_error RETURN VARCHAR2 IS
    BEGIN
        RETURN 'ORA' || TO_CHAR(SQLCODE) || ': ' || SQLERRM;
    END format_error;

    -- ==================================================
    -- AUTONOMICZNA PROCEDURA AUDYTOWA
    -- ==================================================
    PROCEDURE log_audit(
        p_process      VARCHAR2,
        p_step         VARCHAR2,
        p_rows         NUMBER,
        p_status       VARCHAR2,
        p_err          VARCHAR2 DEFAULT NULL,
        p_extract_from DATE     DEFAULT NULL,
        p_extract_to   DATE     DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO etl_audit_log (
            run_timestamp, process_name, step_name, rows_processed,
            status, error_message, extract_from, extract_to
        )
        VALUES (
            SYSDATE, p_process, p_step, p_rows,
            p_status, SUBSTR(p_err, 1, 4000), p_extract_from, p_extract_to
        );
        COMMIT;
    END log_audit;

    -- ==================================================
    -- PRYWATNA PROCEDURA POMOCNICZA: ODCZYT OKNA RUN
    -- Wywoływana przez każdy batch – jedno miejsce odczytu HWM
    -- ==================================================

    PROCEDURE get_run_window(
    p_extract_from    OUT DATE,
    p_extract_to      OUT DATE,
    p_is_initial_load OUT BOOLEAN
    ) IS
        v_chunk_days NUMBER;
    BEGIN
        SELECT last_success_run - c_overlap_minutes,
            target_run,
            chunk_days
        INTO   p_extract_from, p_extract_to, v_chunk_days
        FROM   etl_hwm_control
        WHERE  process_name   = c_hwm_process_name
        AND  process_status = 'IN_PROGRESS';

        p_is_initial_load := (v_chunk_days = c_mode_initial_load);
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(-20001,
                'BŁĄD: Brak aktywnego runu IN_PROGRESS dla ' || c_hwm_process_name ||
                '. Upewnij się, że prc_start_run został wywołany przed batchami.');
        WHEN TOO_MANY_ROWS THEN
            RAISE_APPLICATION_ERROR(-20004,
                'BŁĄD KRYTYCZNY: Znaleziono wiele rekordów IN_PROGRESS w etl_hwm_control dla ' ||
                c_hwm_process_name || '. Wymagana ręczna interwencja DBA.');
    END get_run_window;

    -- ==================================================
    -- PROCEDURA OTWIERAJĄCA RUN + CZYSZCZENIE STG
    -- Krok 1 sekwensera – wywołać przed batchami ekstrakcji
    -- ==================================================
    PROCEDURE prc_start_run IS
    v_last_run   DATE;
    v_status     VARCHAR2(20);
    v_chunk_days NUMBER;
    v_target_run DATE;
    v_is_initial BOOLEAN;
    BEGIN
        set_session_nls;
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_START_RUN', 'Inicjalizacja');

        log_audit(c_hwm_process_name, 'START RUN', 0, c_status_running,
            'Wersja pakietu: ' || c_package_version);

        -- Odczyt HWM
        BEGIN
            SELECT last_success_run, process_status, chunk_days
            INTO   v_last_run, v_status, v_chunk_days
            FROM   etl_hwm_control
            WHERE  process_name = c_hwm_process_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20002, '...');
            WHEN TOO_MANY_ROWS THEN
                RAISE_APPLICATION_ERROR(-20005, '...');
        END;

        v_is_initial := (v_chunk_days = c_mode_initial_load);

        IF v_status <> 'COMPLETED' THEN
            log_audit(c_hwm_process_name, 'START RUN', 0, 'WARNING',
                'Poprzedni run zakończony statusem: ' || v_status ||
                '. Wznawianie od ostatniego zatwierdzonego HWM.');
        END IF;

        -- -----------------------------------------------
        -- TRYB INITIAL LOAD: pomiń TRUNCATE STG
        -- -----------------------------------------------
        IF v_is_initial THEN
            log_audit(c_hwm_process_name, 'START RUN', 0, 'WARNING',
                'TRYB INITIAL LOAD (chunk_days = ' || c_mode_initial_load ||
                '). STG nie zostanie wyczyszczone, ekstrakcja zostanie pominięta.');
            
            -- Okno "puste" – batche ekstrakcji wyliczą from >= to i nic nie zrobią
            -- (ale guard ORA-20008 nie może się uruchomić – więc ustawiamy ostrożnie)
            v_target_run := v_last_run + NUMTODSINTERVAL(1, 'SECOND');
        ELSE
            -- Normalne czyszczenie STG
            DBMS_APPLICATION_INFO.SET_ACTION('Czyszczenie STG BATCH 1');
            EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_1';
            -- ... pozostałe truncate ...
            log_audit(c_hwm_process_name, 'TRUNCATE STG BATCH 3', 0, 'SUCCESS');

            -- Wyliczenie okna (normalny tryb lub chunk_days > 0)
            IF v_chunk_days = 0 THEN
                v_target_run := SYSDATE;
            ELSE
                v_target_run := v_last_run + v_chunk_days;
                IF v_target_run > SYSDATE THEN
                    v_target_run := SYSDATE;
                END IF;
            END IF;

            IF v_target_run <= v_last_run THEN
                RAISE_APPLICATION_ERROR(-20008, 'zegar cofnięty...');
            END IF;
        END IF;

        -- Zablokowanie HWM
        UPDATE etl_hwm_control
        SET    process_status = 'IN_PROGRESS',
            target_run     = v_target_run
        WHERE  process_name   = c_hwm_process_name;

        COMMIT;

        log_audit(
            c_hwm_process_name, 'START RUN', 0, 'IN_PROGRESS',
            CASE WHEN v_is_initial 
                THEN 'INITIAL LOAD – STG zachowane, ekstrakcja pominięta.' 
                ELSE 'STG wyczyszczone. Okno zatwierdzone dla wszystkich batchów.'
            END,
            v_last_run - c_overlap_minutes,
            v_target_run
        );

        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            log_audit(c_hwm_process_name, 'START RUN', 0, 'FAILED', format_error);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            RAISE;
    END prc_start_run;
    
    
    
    
    PROCEDURE prc_start_run  IS
        v_last_run   DATE;
        v_status     VARCHAR2(20);
        v_chunk_days NUMBER;
        v_target_run DATE;
        v_is_initial BOOLEAN;
    BEGIN
        set_session_nls;
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_OPEN_RUN', 'Inicjalizacja');

        -- Logowanie wersji pakietu – pierwszy wpis każdego runu
        log_audit(c_hwm_process_name, 'START RUN', 0, c_status_running,
            'Wersja pakietu: ' || c_package_version);

        -- -----------------------------------------------
        -- ODCZYT AKTUALNEGO HWM
        -- -----------------------------------------------
        BEGIN
            SELECT last_success_run, process_status, chunk_days
            INTO   v_last_run, v_status, v_chunk_days
            FROM   etl_hwm_control
            WHERE  process_name = c_hwm_process_name;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20002,
                    'BŁĄD: Brak wpisu HWM dla ' || c_hwm_process_name ||
                    '. Wykonaj skrypt inicjalizacyjny przed pierwszym uruchomieniem.');
            WHEN TOO_MANY_ROWS THEN
                RAISE_APPLICATION_ERROR(-20005,
                    'BŁĄD KRYTYCZNY: Wiele wpisów HWM dla ' || c_hwm_process_name ||
                    '. Wymagana ręczna interwencja DBA.');
        END;

        v_is_initial := (v_chunk_days = c_mode_initial_load);
        -- Ostrzeżenie gdy poprzedni run nie zakończył się statusem COMPLETED
        IF v_status <> 'COMPLETED' THEN
            log_audit(c_hwm_process_name, 'OPEN RUN', 0, 'WARNING',
                'Poprzedni run zakończony statusem: ' || v_status ||
                '. Wznawianie od ostatniego zatwierdzonego HWM.');
        END IF;

        IF v_is_initial THEN

        END IF;

        -- -----------------------------------------------
        -- CZYSZCZENIE STG
        -- UWAGA: TRUNCATE = DDL, brak możliwości ROLLBACK
        -- -----------------------------------------------
        DBMS_APPLICATION_INFO.SET_ACTION('Czyszczenie STG BATCH 1');
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_1';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_2';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_3';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_4';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_5';
        log_audit(c_hwm_process_name, 'TRUNCATE STG BATCH 1', 0, 'SUCCESS');

        DBMS_APPLICATION_INFO.SET_ACTION('Czyszczenie STG BATCH 2');
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_6';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_7';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_8';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_9';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_10';
        log_audit(c_hwm_process_name, 'TRUNCATE STG BATCH 2', 0, 'SUCCESS');

        DBMS_APPLICATION_INFO.SET_ACTION('Czyszczenie STG BATCH 3');
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_11';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_12';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_13';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_14';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_15';
        log_audit(c_hwm_process_name, 'TRUNCATE STG BATCH 3', 0, 'SUCCESS');

        -- -----------------------------------------------
        -- WYLICZENIE OKNA CZASOWEGO
        -- -----------------------------------------------
        DBMS_APPLICATION_INFO.SET_ACTION('Wyliczanie okna');

        IF v_chunk_days = 0 THEN
            v_target_run := SYSDATE;                          -- Tryb: zwykła delta nocna
        ELSE
            v_target_run := v_last_run + v_chunk_days;        -- Tryb: zasilenie inicjalne w oknach
            IF v_target_run > SYSDATE THEN
                v_target_run := SYSDATE;                      -- Zabezpieczenie przed wybieganiem w przyszłość
            END IF;
        END IF;

        -- Guard: zegar cofnięty (np. korekta NTP, przejście DST)
        -- Okno musi mieć dodatnią długość, inaczej warunek ">= from AND < to" nie dopasuje niczego
        IF v_target_run <= v_last_run THEN
            RAISE_APPLICATION_ERROR(-20008,
                'BŁĄD: target_run (' || TO_CHAR(v_target_run, c_date_format) ||
                ') <= last_success_run (' || TO_CHAR(v_last_run, c_date_format) ||
                '). Możliwe przyczyny: zegar systemowy cofnięty, przejście DST, ' ||
                'ręczna modyfikacja HWM lub uruchomienie dwa razy w tej samej sekundzie.');
        END IF;

        -- -----------------------------------------------
        -- ZABLOKOWANIE HWM – zamrożenie okna dla batchów
        -- -----------------------------------------------
        UPDATE etl_hwm_control
        SET    process_status = 'IN_PROGRESS',
               target_run     = v_target_run
        WHERE  process_name   = c_hwm_process_name;

        COMMIT;

        log_audit(
            c_hwm_process_name, 'OPEN RUN', 0, 'IN_PROGRESS',
            'STG wyczyszczone. Okno zatwierdzone dla wszystkich batchów.',
            v_last_run - c_overlap_minutes,
            v_target_run
        );

        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            -- TRUNCATE już mógł się wykonać (DDL = brak rollback) –
            -- logujemy błąd i rzucamy dalej; HWM NIE zostaje przestawiony na IN_PROGRESS
            log_audit(c_hwm_process_name, 'OPEN RUN', 0, 'FAILED', format_error);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            RAISE;
    END prc_start_run ;

    -- ==================================================
    -- BATCH 1 (Tabele 1 – 5)
    -- ==================================================
    PROCEDURE prc_extract_delta_batch_1 IS
    v_extract_from   DATE;
    v_extract_to     DATE;
    v_is_initial     BOOLEAN;
    v_current_step   VARCHAR2(200) := 'Inicjalizacja';
    v_rows_processed NUMBER;
    BEGIN
        set_session_nls;
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_BATCH_1', 'Ekstrakcja');
        log_audit(c_batch_1_name, 'START BATCH 1', 0, c_status_running);

        get_run_window(v_extract_from, v_extract_to, v_is_initial);

        -- Tryb INITIAL LOAD – STG zasilone ręcznie, batch pomija swoją pracę
        IF v_is_initial THEN
            log_audit(c_batch_1_name, 'SKIP BATCH 1', 0, 'COMPLETED',
                'Tryb INITIAL LOAD – ekstrakcja pominięta, STG zachowane.',
                v_extract_from, v_extract_to);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            RETURN;
        END IF;

        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        -- ----- TABELA_1 -----
        v_current_step := 'TRUNCATE + INSERT APPEND: stg_tab_1';
        DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);

        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_1';

        INSERT /*+ APPEND PARALLEL(4) */ INTO stg_tab_1 (record_id, extracted_at)
        SELECT DISTINCT id_1, v_extract_to
        FROM (
            SELECT id_1 FROM tabela_1
            WHERE  CZAS_AKTUALIZACJI_REKORDU >= v_extract_from
            AND  CZAS_AKTUALIZACJI_REKORDU <  v_extract_to
            UNION ALL
            SELECT t.id_1 FROM tabela_1 t
            INNER JOIN etl_manual_refresh m ON t.id_1 = m.record_id
            WHERE  m.table_source = 'TABELA_1'
            AND  m.batch_name   = c_batch_1_name
            AND  m.status       = 'PENDING'
        )
        WHERE id_1 IS NOT NULL;

        v_rows_processed := SQL%ROWCOUNT;
        COMMIT;
        log_audit(c_batch_1_name, v_current_step, v_rows_processed, 'SUCCESS',
            NULL, v_extract_from, v_extract_to);

        -- [Tabele 2–5 analogicznie...]

        log_audit(c_batch_1_name, 'KONIEC EKSTRAKCJI BATCH 1', 0, 'COMPLETED',
            NULL, v_extract_from, v_extract_to);
        EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_audit(c_batch_1_name, v_current_step, 0, 'FAILED', format_error);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            BEGIN EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML'; EXCEPTION WHEN OTHERS THEN NULL; END;
            RAISE;
    END prc_extract_delta_batch_1;
    
    
    -- PROCEDURE prc_extract_delta_batch_1 IS
    --     v_extract_from   DATE;
    --     v_extract_to     DATE;
    --     v_current_step   VARCHAR2(200) := 'Inicjalizacja';
    --     v_rows_processed NUMBER;
    -- BEGIN
    --     set_session_nls;
    --     DBMS_APPLICATION_INFO.SET_MODULE('ETL_BATCH_1', 'Ekstrakcja');
    --     log_audit(c_batch_1_name, 'START BATCH 1', 0, c_status_running);

    --     -- Odczyt zamrożonego okna – identyczny dla wszystkich batchów
    --     get_run_window(v_extract_from, v_extract_to, v_is_initial);

    --     -- Tryb INITIAL LOAD – STG zasilone ręcznie, batch pomija swoją pracę
    --     IF v_is_initial THEN
    --         log_audit(c_batch_1_name, 'SKIP BATCH 1', 0, 'COMPLETED',
    --             'Tryb INITIAL LOAD – ekstrakcja pominięta, STG zachowane.',
    --             v_extract_from, v_extract_to);
    --         DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    --         RETURN;
    --     END IF;

    -- EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

    -- -- ----- TABELA_1 -----
    -- v_current_step := 'TRUNCATE + INSERT APPEND: stg_tab_1';
    -- DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);

    -- EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_1';

    -- INSERT /*+ APPEND PARALLEL(4) */ INTO stg_tab_1 (record_id, extracted_at)
    -- SELECT DISTINCT id_1, v_extract_to
    -- FROM (
    --     SELECT id_1 FROM tabela_1
    --     WHERE  CZAS_AKTUALIZACJI_REKORDU >= v_extract_from
    --       AND  CZAS_AKTUALIZACJI_REKORDU <  v_extract_to
    --     UNION ALL
    --     SELECT t.id_1 FROM tabela_1 t
    --     INNER JOIN etl_manual_refresh m ON t.id_1 = m.record_id
    --     WHERE  m.table_source = 'TABELA_1'
    --       AND  m.batch_name   = c_batch_1_name
    --       AND  m.status       = 'PENDING'
    -- )
    -- WHERE id_1 IS NOT NULL;

    -- v_rows_processed := SQL%ROWCOUNT;
    -- COMMIT;

    -- log_audit(c_batch_1_name, v_current_step, v_rows_processed, 'SUCCESS',
    --     NULL, v_extract_from, v_extract_to);

    -- ... kolejne tabele ...



    --     EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

    -- -- TRUNCATE tuż przed INSERT – atomowe z perspektywy batcha
    -- v_current_step := 'TRUNCATE stg_tab_1';
    -- DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    -- EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_1';

    -- v_current_step := 'INSERT APPEND: stg_tab_1';
    -- DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);

    -- INSERT /*+ APPEND PARALLEL(4) */ INTO stg_tab_1 (record_id, extracted_at)
    -- SELECT id_1, v_extract_to FROM (
    -- SELECT id_1 FROM tabela_1
    -- WHERE  CZAS_AKTUALIZACJI_REKORDU >= v_extract_from
    -- AND  CZAS_AKTUALIZACJI_REKORDU <  v_extract_to
    -- UNION
    -- SELECT t.id_1 FROM tabela_1 t
    -- INNER JOIN etl_manual_refresh m ON t.id_1 = m.record_id
    -- WHERE  m.table_source = 'TABELA_1'
    -- AND  m.batch_name   = c_batch_1_name
    -- AND  m.status       = 'PENDING'
    -- );

    -- v_rows_processed := SQL%ROWCOUNT;
    -- COMMIT;  -- MUSI być zaraz po APPEND – bez tego nie można dalej czytać tabeli
    
    -- log_audit(c_batch_1_name, v_current_step, v_rows_processed, 'SUCCESS',
    --     NULL, v_extract_from, v_extract_to);


    --     EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

    --     -- Ekstrakcja: TABELA_1
    --     v_current_step := 'Ekstrakcja: TABELA_1';
    --     DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);

    --     MERGE /*+ PARALLEL(trg) PARALLEL(src) */ INTO stg_tab_1 trg
    --     USING (
    --         -- STRUMIEŃ A: Delta po kolumnie audytowej
    --         SELECT id_1 FROM tabela_1
    --         WHERE  CZAS_AKTUALIZACJI_REKORDU >= v_extract_from
    --           AND  CZAS_AKTUALIZACJI_REKORDU <  v_extract_to
    --         UNION
    --         -- STRUMIEŃ B: Manual Override
    --         SELECT t.id_1 FROM tabela_1 t
    --         INNER JOIN etl_manual_refresh m ON t.id_1 = m.record_id
    --         WHERE  m.table_source = 'TABELA_1'
    --           AND  m.batch_name   = c_batch_1_name
    --           AND  m.status       = 'PENDING'
    --     ) src
    --     ON (trg.record_id = src.id_1)
    --     WHEN MATCHED     THEN UPDATE SET trg.extracted_at = v_extract_to
    --     WHEN NOT MATCHED THEN INSERT (record_id, extracted_at) VALUES (src.id_1, v_extract_to);

    --     v_rows_processed := SQL%ROWCOUNT;
    --     COMMIT;
    --     log_audit(c_batch_1_name, v_current_step, v_rows_processed, 'SUCCESS',
    --         NULL, v_extract_from, v_extract_to);

        -- [Tabele 2–5 analogicznie do TABELA_1...]

        log_audit(c_batch_1_name, 'KONIEC EKSTRAKCJI BATCH 1', 0, 'COMPLETED',
            NULL, v_extract_from, v_extract_to);
        EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_audit(c_batch_1_name, v_current_step, 0, 'FAILED', format_error);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            BEGIN EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML'; EXCEPTION WHEN OTHERS THEN NULL; END;
            RAISE;
    END prc_extract_delta_batch_1;

    -- ==================================================
    -- BATCH 2 (Tabele 6 – 10)
    -- ==================================================
    PROCEDURE prc_extract_delta_batch_2 IS
        v_extract_from   DATE;
        v_extract_to     DATE;
        v_current_step   VARCHAR2(200) := 'Inicjalizacja';
        v_rows_processed NUMBER;
    BEGIN
        set_session_nls;
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_BATCH_2', 'Ekstrakcja');
        log_audit(c_batch_2_name, 'START BATCH 2', 0, c_status_running);

        get_run_window(v_extract_from, v_extract_to);

        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        -- [Ekstrakcja do stg_tab_6 ... stg_tab_10 analogicznie do BATCH_1...]

        log_audit(c_batch_2_name, 'KONIEC EKSTRAKCJI BATCH 2', 0, 'COMPLETED',
            NULL, v_extract_from, v_extract_to);
        EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_audit(c_batch_2_name, v_current_step, 0, 'FAILED', format_error);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            BEGIN EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML'; EXCEPTION WHEN OTHERS THEN NULL; END;
            RAISE;
    END prc_extract_delta_batch_2;

    -- ==================================================
    -- BATCH 3 (Tabele 11 – 15)
    -- ==================================================
    PROCEDURE prc_extract_delta_batch_3 IS
        v_extract_from   DATE;
        v_extract_to     DATE;
        v_current_step   VARCHAR2(200) := 'Inicjalizacja';
        v_rows_processed NUMBER;
    BEGIN
        set_session_nls;
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_BATCH_3', 'Ekstrakcja');
        log_audit(c_batch_3_name, 'START BATCH 3', 0, c_status_running);

        get_run_window(v_extract_from, v_extract_to);

        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        -- [Ekstrakcja do stg_tab_11 ... stg_tab_15 analogicznie do BATCH_1...]

        log_audit(c_batch_3_name, 'KONIEC EKSTRAKCJI BATCH 3', 0, 'COMPLETED',
            NULL, v_extract_from, v_extract_to);
        EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_audit(c_batch_3_name, v_current_step, 0, 'FAILED', format_error);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            BEGIN EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML'; EXCEPTION WHEN OTHERS THEN NULL; END;
            RAISE;
    END prc_extract_delta_batch_3;

    -- ==================================================
    -- ŁADOWANIE FAKTÓW
    -- ==================================================
    PROCEDURE prc_load_fct_policies IS
        v_extract_from   DATE;
        v_extract_to     DATE;
        v_current_step   VARCHAR2(200) := 'Inicjalizacja';
        v_rows_processed NUMBER;
    BEGIN
        set_session_nls;
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_LOAD_FACT', 'fct_policies');
        log_audit(c_load_fact_name, 'START LOAD fct_policies', 0, c_status_running);

        -- Odczyt tego samego okna, którym posługiwały się batche (dla audytu)
        get_run_window(v_extract_from, v_extract_to);

        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        v_current_step := 'Merge: fct_ubezpieczenia_sprzedaz';
        DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);

        MERGE /*+ PARALLEL(trg) PARALLEL(src) */
        INTO fct_ubezpieczenia_sprzedaz trg
        USING (
            SELECT t.id_1          AS id_polisy,
                   t.kwota_skladki,
                   t.data_zawarcia,
                   t.id_agenta,
                   t.kod_produktu,
                   t.status_polisy
            FROM   tabela_1 t
            INNER JOIN stg_tab_1 stg ON t.id_1 = stg.record_id   -- Filtr wydajnościowy przez STG
        ) src
        ON (trg.id_polisy = src.id_polisy)
        WHEN MATCHED THEN
            UPDATE SET
                trg.kwota_skladki     = src.kwota_skladki,
                trg.status_polisy     = src.status_polisy,
                trg.id_agenta         = src.id_agenta,
                trg.data_aktualizacji = SYSDATE
        WHEN NOT MATCHED THEN
            INSERT (id_polisy, kwota_skladki, data_zawarcia, id_agenta, kod_produktu, status_polisy, data_wstawienia)
            VALUES (src.id_polisy, src.kwota_skladki, src.data_zawarcia,
                    src.id_agenta, src.kod_produktu, src.status_polisy, SYSDATE);

        v_rows_processed := SQL%ROWCOUNT;
        COMMIT;
        log_audit(c_load_fact_name, 'KONIEC LOAD fct_policies', v_rows_processed, 'COMPLETED',
            NULL, v_extract_from, v_extract_to);
        EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_audit(c_load_fact_name, v_current_step, 0, 'FAILED', format_error,
                v_extract_from, v_extract_to);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            BEGIN EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML'; EXCEPTION WHEN OTHERS THEN NULL; END;
            RAISE;
    END prc_load_fct_policies;

    -- ==================================================
    -- PROCEDURA ZAMYKAJĄCA RUN
    -- Krok 4 sekwensera – wywołać po zakończeniu wszystkich ładowań
    -- ==================================================
    PROCEDURE prc_end_run  IS
        v_target_run DATE;
    BEGIN
        set_session_nls;
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_CLOSE_RUN', c_hwm_process_name);

        -- Odczyt zatwierdzonego sufitu przed aktualizacją (potrzebny do logu)
        SELECT target_run
        INTO   v_target_run
        FROM   etl_hwm_control
        WHERE  process_name = c_hwm_process_name;

        -- Przesunięcie HWM – tylko gdy status to faktycznie IN_PROGRESS
        UPDATE etl_hwm_control
        SET    last_success_run = target_run,
               process_status   = 'COMPLETED'
        WHERE  process_name     = c_hwm_process_name
          AND  process_status   = 'IN_PROGRESS';

        IF SQL%ROWCOUNT = 0 THEN
            RAISE_APPLICATION_ERROR(-20003,
                'BŁĄD: Próba zamknięcia runu w nieprawidłowym statusie dla: ' || c_hwm_process_name ||
                '. Oczekiwano IN_PROGRESS.');
        END IF;

        -- Zamknięcie TYLKO tych zleceń manualnych, które były obsługiwane w tym runie
        UPDATE etl_manual_refresh
        SET    status = 'COMPLETED'
        WHERE  status = 'PENDING'
          AND  batch_name IN (c_batch_1_name, c_batch_2_name, c_batch_3_name);

        COMMIT;

        log_audit(c_hwm_process_name, 'CLOSE RUN', 0, 'COMPLETED',
            'HWM przesunięty do: ' || TO_CHAR(v_target_run, c_date_format));

        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_audit(c_hwm_process_name, 'CLOSE RUN', 0, 'FAILED', format_error);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            RAISE;
    END prc_end_run ;

END pkg_antf_load;
/

-- ==================================================================
-- KOLEJNOŚĆ WYKONYWANIA PROCEDUR
-- ==================================================================
--   KROK 1:  prc_start_run 
--            → ustawia NLS sesji, loguje wersję pakietu
--            → ostrzega jeśli poprzedni status <> COMPLETED (ale nie blokuje)
--            → czyści STG (stg_tab_1 ... stg_tab_15)
--            → odczyt HWM i wyliczenie okna
--            → guard na cofnięty zegar (target_run <= last_success_run)
--            → ustawienie IN_PROGRESS w etl_hwm_control
--
--   KROK 2:  RÓWNOLEGLE:
--              ├─ prc_extract_delta_batch_1   (tabele 1–5)
--              ├─ prc_extract_delta_batch_2   (tabele 6–10)
--              └─ prc_extract_delta_batch_3   (tabele 11–15)
--
--   KROK 3:  RÓWNOLEGLE:
--              ├─ prc_load_fct_policies
--              └─ prc_load_fct_...            (pozostałe procedury ładowania faktów)
--
--   KROK 4:  prc_end_run 
--            → przesunięcie HWM: last_success_run := target_run
--            → process_status := 'COMPLETED'
--            → zamknięcie obsłużonych zleceń w etl_manual_refresh
--
-- OBSŁUGA BŁĘDÓW:
--   W razie błędu na dowolnym etapie: NIE wywoływać prc_end_run .
--   Po poprawce uruchomić ponownie od kroku 1 – prc_start_run  jest
--   idempotentna i bezpiecznie nadpisze poprzedni status IN_PROGRESS.
-- ==================================================================

- BEGIN
--     pkg_antf_load.prc_truncate_all_stg;
-- END;
-- /

-- BEGIN
--     pkg_antf_load.prc_start_run ;
-- END;
-- /

-- -- Batche normalnie lecą równolegle z DataStage,
-- -- ale można je też wywołać sekwencyjnie do testów:
-- BEGIN
--     pkg_antf_load.prc_extract_delta_batch_1;
-- END;
-- /

-- BEGIN
--     pkg_antf_load.prc_extract_delta_batch_2;
-- END;
-- /

-- BEGIN
--     pkg_antf_load.prc_extract_delta_batch_3;
-- END;
-- /

-- BEGIN
--     pkg_antf_load.prc_load_fct_policies;
-- END;
-- /

-- BEGIN
--     pkg_antf_load.prc_end_run ;
-- END;
-- /

-- UPDATE etl_hwm_control
-- SET    process_status = 'COMPLETED',
--        target_run     = NULL
-- WHERE  process_name   = 'ETL_DELTA_MAIN';
-- COMMIT;


-- ==================================================
-- DDL: etl_hwm_control
-- ==================================================
CREATE TABLE etl_hwm_control (
    process_name     VARCHAR2(30)   NOT NULL,
    last_success_run TIMESTAMP      NOT NULL,
    process_status   VARCHAR2(20)   NOT NULL,
    chunk_days       NUMBER(5)      NOT NULL,
    target_run       TIMESTAMP          NULL,

    CONSTRAINT pk_etl_hwm_control PRIMARY KEY (process_name),
    CONSTRAINT chk_hwm_status CHECK (process_status IN ('COMPLETED', 'IN_PROGRESS', 'FAILED')),
    CONSTRAINT chk_hwm_chunk  CHECK (chunk_days >= 0)
);

COMMENT ON TABLE  etl_hwm_control                  IS 'High Water Mark – kontrola okna ekstrakcji delta ETL';
COMMENT ON COLUMN etl_hwm_control.process_name     IS 'Unikalny identyfikator procesu ETL';
COMMENT ON COLUMN etl_hwm_control.last_success_run IS 'Timestamp ostatniego pomyślnie zamkniętego runu – dolna granica okna';
COMMENT ON COLUMN etl_hwm_control.process_status   IS 'Aktualny status: COMPLETED / IN_PROGRESS / FAILED';
COMMENT ON COLUMN etl_hwm_control.chunk_days       IS 'Rozmiar okna w dniach (tryb inicjalny); 0 = delta nocna do SYSTIMESTAMP';
COMMENT ON COLUMN etl_hwm_control.target_run       IS 'Górna granica okna – ustawiana przez prc_start_run , NULL gdy brak aktywnego runu';

-- ==================================================
-- Przykładowy INSERT – tryb delta nocna (chunk_days = 0)
-- last_success_run: wczoraj o 22:00 (ostatni zamknięty run)
-- ==================================================
INSERT INTO etl_hwm_control (
    process_name,
    last_success_run,
    process_status,
    chunk_days,
    target_run
)
VALUES (
    'ETL_DELTA_MAIN',
    TIMESTAMP '2025-04-15 22:00:00.000000',
    'COMPLETED',
    0,
    NULL
);

COMMIT;

-- ==================================================
-- Przykładowy INSERT – tryb zasilenia inicjalnego (chunk_days > 0)
-- Pakiet będzie przetwarzał dane w oknach 7-dniowych aż dobiegnie do SYSTIMESTAMP
-- ==================================================
-- INSERT INTO etl_hwm_control (
--     process_name,
--     last_success_run,
--     process_status,
--     chunk_days,
--     target_run
-- )
-- VALUES (
--     'ETL_DELTA_MAIN',
--     TIMESTAMP '2020-01-01 00:00:00.000000',  -- punkt startowy historii
--     'COMPLETED',
--     7,


-- ==================================================
-- SPECYFIKACJA PAKIETU: pkg_antf_load
-- Plik: pkg_antf_load_spec.sql
-- Musi zostać wdrożona PRZED pkg_antf_load_body.sql
-- ==================================================
CREATE OR REPLACE PACKAGE pkg_antf_load IS

    -- -----------------------------------------------
    -- KROK 1: Czyszczenie tabel STG
    -- Wywołać jako pierwszy krok sekwencji.
    -- Blokuje wykonanie jeśli aktywny run IN_PROGRESS.
    -- -----------------------------------------------
    PROCEDURE prc_truncate_all_stg;

    -- -----------------------------------------------
    -- KROK 2: Otwarcie runu
    -- Oblicza i zamraża okno czasowe (extract_from / extract_to)
    -- dla wszystkich batchów. Ustawia status IN_PROGRESS w HWM.
    -- -----------------------------------------------
    PROCEDURE prc_start_run ;

    -- -----------------------------------------------
    -- KROK 3a: Ekstrakcja delta – tabele 1–5
    -- Wywołać równolegle z batch_2 i batch_3.
    -- Wymaga wcześniejszego prc_start_run .
    -- -----------------------------------------------
    PROCEDURE prc_extract_delta_batch_1;

    -- -----------------------------------------------
    -- KROK 3b: Ekstrakcja delta – tabele 6–10
    -- Wywołać równolegle z batch_1 i batch_3.
    -- Wymaga wcześniejszego prc_start_run .
    -- -----------------------------------------------
    PROCEDURE prc_extract_delta_batch_2;

    -- -----------------------------------------------
    -- KROK 3c: Ekstrakcja delta – tabele 11–15
    -- Wywołać równolegle z batch_1 i batch_2.
    -- Wymaga wcześniejszego prc_start_run .
    -- -----------------------------------------------
    PROCEDURE prc_extract_delta_batch_3;

    -- -----------------------------------------------
    -- KROK 4: Ładowanie faktów – fct_ubezpieczenia_sprzedaz
    -- Wywołać po zakończeniu wszystkich trzech batchów.
    -- Można uruchamiać równolegle z innymi prc_load_fct_*.
    -- -----------------------------------------------
    PROCEDURE prc_load_fct_policies;

    -- -----------------------------------------------
    -- KROK 5: Zamknięcie runu
    -- Przesuwa HWM (last_success_run := target_run).
    -- Wywołać TYLKO po pomyślnym zakończeniu wszystkich kroków 3 i 4.
    -- -----------------------------------------------
    PROCEDURE prc_end_run ;

END pkg_antf_load;
/

CREATE TABLE etl_audit_log (
    log_id          NUMBER         GENERATED ALWAYS AS IDENTITY NOT NULL,
    run_timestamp   DATE           NOT NULL,
    process_name    VARCHAR2(30)   NOT NULL,
    step_name       VARCHAR2(200)  NOT NULL,
    rows_processed  NUMBER(12)         NULL,
    status          VARCHAR2(20)   NOT NULL,
    error_message   VARCHAR2(4000)     NULL,
    extract_from    DATE               NULL,
    extract_to      DATE               NULL,

    CONSTRAINT pk_etl_audit_log PRIMARY KEY (log_id),
    CONSTRAINT chk_audit_status CHECK (
        status IN ('IN_PROGRESS', 'SUCCESS', 'COMPLETED', 'FAILED', 'WARNING')
    )
);

-- log_id pomijamy – generowany automatycznie przez identity column

INSERT INTO etl_audit_log (run_timestamp, process_name, step_name, rows_processed, status, error_message, extract_from, extract_to)
VALUES (TO_DATE('2026-04-16 01:00:00', 'YYYY-MM-DD HH24:MI:SS'),
        'ETL_DELTA_MAIN', 'OPEN RUN', 0, 'IN_PROGRESS',
        'STG wyczyszczone. Okno zatwierdzone dla wszystkich batchów.',
        TO_DATE('2026-04-15 21:45:00', 'YYYY-MM-DD HH24:MI:SS'),
        TO_DATE('2026-04-16 01:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO etl_audit_log (run_timestamp, process_name, step_name, rows_processed, status, error_message, extract_from, extract_to)
VALUES (TO_DATE('2026-04-16 01:01:00', 'YYYY-MM-DD HH24:MI:SS'),
        'BATCH_1', 'Ekstrakcja: TABELA_1', 1250, 'SUCCESS',
        NULL,
        TO_DATE('2026-04-15 21:45:00', 'YYYY-MM-DD HH24:MI:SS'),
        TO_DATE('2026-04-16 01:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO etl_audit_log (run_timestamp, process_name, step_name, rows_processed, status, error_message, extract_from, extract_to)
VALUES (TO_DATE('2026-04-16 01:05:00', 'YYYY-MM-DD HH24:MI:SS'),
        'BATCH_1', 'KONIEC EKSTRAKCJI BATCH 1', 0, 'COMPLETED',
        NULL,
        TO_DATE('2026-04-15 21:45:00', 'YYYY-MM-DD HH24:MI:SS'),
        TO_DATE('2026-04-16 01:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO etl_audit_log (run_timestamp, process_name, step_name, rows_processed, status, error_message, extract_from, extract_to)
VALUES (TO_DATE('2026-04-16 01:10:00', 'YYYY-MM-DD HH24:MI:SS'),
        'LOAD_FACT', 'KONIEC LOAD fct_policies', 1250, 'COMPLETED',
        NULL,
        TO_DATE('2026-04-15 21:45:00', 'YYYY-MM-DD HH24:MI:SS'),
        TO_DATE('2026-04-16 01:00:00', 'YYYY-MM-DD HH24:MI:SS'));

INSERT INTO etl_audit_log (run_timestamp, process_name, step_name, rows_processed, status, error_message, extract_from, extract_to)
VALUES (TO_DATE('2026-04-16 01:15:00', 'YYYY-MM-DD HH24:MI:SS'),
        'ETL_DELTA_MAIN', 'CLOSE RUN', 0, 'COMPLETED',
        'HWM przesunięty do: 2026-04-16 01:00:00',
        NULL,
        NULL);

COMMIT;


-- KROK 1: Przestaw tryb na initial load
UPDATE etl_hwm_control 
SET    chunk_days = -1
WHERE  process_name = 'ETL_DELTA_MAIN';
COMMIT;

-- KROK 2: Ręcznie wypełnij STG
INSERT INTO stg_tab_1 (record_id, extracted_at)
SELECT id_z_innego_zrodla, SYSDATE
FROM   my_seed_table_1;
-- ... analogicznie stg_tab_2 ... stg_tab_15

COMMIT;

-- KROK 3: Uruchom normalną sekwencję
BEGIN
    pkg_etl_delta_load.prc_start_run;           -- pominie TRUNCATE, STG zachowane
    pkg_etl_delta_load.prc_extract_delta_batch_1;  -- wyloguje SKIP, nic nie zrobi
    pkg_etl_delta_load.prc_extract_delta_batch_2;  -- SKIP
    pkg_etl_delta_load.prc_extract_delta_batch_3;  -- SKIP
    pkg_etl_delta_load.prc_load_fct_policies;    -- normalnie załaduje z STG
    pkg_etl_delta_load.prc_close_run;            -- HWM=SYSDATE, chunk_days=0
END;
/
