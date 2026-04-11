CREATE OR REPLACE PACKAGE pkg_etl_delta_load IS
    -- Główne procedury ekstrakcyjne (Logika czasu i paczek zaszyta w bazie)
    PROCEDURE prc_extract_delta_batch_1;
    PROCEDURE prc_extract_delta_batch_2;
    PROCEDURE prc_extract_delta_batch_3;
    
    -- Procedura zasilająca docelową tabelę faktów (odpalana po ekstrakcji)
    PROCEDURE prc_load_fct_policies;
    
    -- Procedura zamykająca cykl (Handshake z IBM DataStage)
    PROCEDURE prc_close_batch_success(p_batch_name VARCHAR2);

    -- Procedura czyszcząca (Wykonywana na starcie całego okna wsadowego)
    PROCEDURE prc_truncate_all_stg;
END pkg_etl_delta_load;
/

CREATE OR REPLACE PACKAGE BODY pkg_etl_delta_load IS

    -- ==================================================
    -- ZMIENNE GLOBALNE I KONFIGURACJA PAKIETU
    -- ==================================================
    c_overlap_interval CONSTANT INTERVAL DAY TO SECOND := INTERVAL '15' MINUTE;

    -- ==================================================
    -- AUTONOMICZNA PROCEDURA AUDYTOWA
    -- ==================================================
    PROCEDURE log_audit(p_process VARCHAR2, p_step VARCHAR2, p_rows NUMBER, p_status VARCHAR2, p_err VARCHAR2 DEFAULT NULL) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO etl_audit_log (run_timestamp, process_name, step_name, rows_processed, status, error_message)
        VALUES (SYSTIMESTAMP, p_process, p_step, p_rows, p_status, SUBSTR(p_err, 1, 4000));
        COMMIT; 
    END log_audit;

    -- ==================================================
    -- BATCH 1 (Tabele 1 - 5)
    -- ==================================================
    PROCEDURE prc_extract_delta_batch_1 IS
        v_last_run         TIMESTAMP;
        v_status           VARCHAR2(20);
        v_chunk_days       NUMBER;
        v_current_run      TIMESTAMP; 
        v_extract_from     TIMESTAMP;
        v_current_step     VARCHAR2(50);
        v_rows_processed   NUMBER;
    BEGIN
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_BATCH_1', 'Inicjalizacja');
        log_audit('BATCH_1', 'START BATCH 1', 0, 'RUNNING');

        -- 1. Weryfikacja HWM, pobranie konfiguracji i Auto-Naprawa
        v_current_step := 'Weryfikacja HWM';
        BEGIN
            SELECT last_success_run, process_status, chunk_days 
            INTO v_last_run, v_status, v_chunk_days
            FROM etl_hwm_control WHERE process_name = 'BATCH_1'; 
            
            IF v_status <> 'COMPLETED' THEN
                log_audit('BATCH_1', 'AUTO-NAPRAWA', 0, 'WARNING', 'Poprzedni status to ' || v_status || '. Wznawiam bezpiecznie.');
            END IF;
        EXCEPTION
            WHEN NO_DATA_FOUND THEN
                RAISE_APPLICATION_ERROR(-20001, 'BŁĄD: Brak HWM dla BATCH_1.');
        END;

        -- 2. Sterowanie Metadanymi: Wyliczenie okna i sufitu czasowego
        IF v_chunk_days = 0 THEN
            v_current_run := SYSTIMESTAMP; -- Zwykła delta nocna
        ELSE
            v_current_run := v_last_run + v_chunk_days; -- Zasilenie inicjalne w oknach
            IF v_current_run > SYSTIMESTAMP THEN
                v_current_run := SYSTIMESTAMP; -- Zabezpieczenie przed "wybiegnięciem w przyszłość"
            END IF;
        END IF;

        -- 3. Blokada HWM i zamrożenie Target Run
        UPDATE etl_hwm_control 
        SET process_status = 'IN_PROGRESS', target_run = v_current_run 
        WHERE process_name = 'BATCH_1';
        COMMIT;

        -- 4. Aktywacja MPP i wyliczenie czasu startowego
        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';
        v_extract_from := v_last_run - c_overlap_interval;

        -- 5. Ekstrakcja danych (Delta po poprawnej dacie + Zlecenia Biznesowe)
        v_current_step := 'Ekstrakcja: TABELA_1';
        DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
        MERGE /*+ PARALLEL(trg) PARALLEL(src) */ INTO stg_tab_1 trg 
        USING (
            -- STRUMIEŃ A: Złoty Standard ETL (Delta po poprawnej kolumnie)
            SELECT id_1 FROM tabela_1 
            WHERE CZAS_AKTUALIZACJI_REKORDU >= v_extract_from AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
            UNION
            -- STRUMIEŃ B: Manual Override (Wymuszenia z tabeli ręcznej)
            SELECT t.id_1 FROM tabela_1 t
            INNER JOIN etl_manual_refresh m ON t.id_1 = m.record_id
            WHERE m.table_source = 'TABELA_1' AND m.batch_name = 'BATCH_1' AND m.status = 'PENDING'
        ) src
        ON (trg.record_id = src.id_1)
        WHEN MATCHED THEN UPDATE SET trg.extracted_at = v_current_run
        WHEN NOT MATCHED THEN INSERT (record_id, extracted_at) VALUES (src.id_1, v_current_run);
        
        v_rows_processed := SQL%ROWCOUNT;
        COMMIT; log_audit('BATCH_1', v_current_step, v_rows_processed, 'SUCCESS');

        -- [Tutaj zasilenie STG dla tabel 2, 3, 4, 5 działające identycznie...]

        log_audit('BATCH_1', 'KONIEC EKSTRAKCJI BATCH 1', 0, 'COMPLETED');
        EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_audit('BATCH_1', v_current_step, 0, 'FAILED', SQLERRM);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            BEGIN EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML'; EXCEPTION WHEN OTHERS THEN NULL; END;
            RAISE;
    END prc_extract_delta_batch_1;

    -- ==================================================
    -- BATCH 2 (Tabele 6 - 10) [WERSJA SKRÓCONA]
    -- ==================================================
    PROCEDURE prc_extract_delta_batch_2 IS
    BEGIN
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_BATCH_2', 'Ekstrakcja');
        -- Pełna, analogiczna logika odczytu metadanych z chunk_days dla BATCH_2
        -- oraz ekstrakcja do stg_tab_6 ... stg_tab_10
        NULL; 
    END prc_extract_delta_batch_2;

    -- ==================================================
    -- BATCH 3 (Tabele 11 - 15) [WERSJA SKRÓCONA]
    -- ==================================================
    PROCEDURE prc_extract_delta_batch_3 IS
    BEGIN
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_BATCH_3', 'Ekstrakcja');
        -- Pełna, analogiczna logika odczytu metadanych z chunk_days dla BATCH_3
        -- oraz ekstrakcja do stg_tab_11 ... stg_tab_15
        NULL; 
    END prc_extract_delta_batch_3;

    -- ==================================================
    -- ŁADOWANIE FAKTÓW (Zasilenie hurtowni docelowej z filtrem STG)
    -- ==================================================
    PROCEDURE prc_load_fct_policies IS
        v_current_step   VARCHAR2(50);
        v_rows_processed NUMBER;
    BEGIN
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_LOAD_FACT', 'fct_policies');
        log_audit('LOAD_FACT', 'START LOAD fct_policies', 0, 'RUNNING');

        EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

        v_current_step := 'Merge: fct_ubezpieczenia_sprzedaz';
        DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);

        MERGE /*+ PARALLEL(trg) PARALLEL(src) */ 
        INTO fct_ubezpieczenia_sprzedaz trg
        USING (
            SELECT 
                t.id_1 AS id_polisy, t.kwota_skladki, t.data_zawarcia, t.id_agenta, t.kod_produktu, t.status_polisy
            FROM tabela_1 t
            INNER JOIN stg_tab_1 stg ON t.id_1 = stg.record_id -- Filtr wydajnościowy
        ) src
        ON (trg.id_polisy = src.id_polisy)
        WHEN MATCHED THEN 
            UPDATE SET 
                trg.kwota_skladki = src.kwota_skladki,
                trg.status_polisy = src.status_polisy,
                trg.id_agenta     = src.id_agenta,
                trg.data_aktualizacji = SYSDATE
        WHEN NOT MATCHED THEN 
            INSERT (id_polisy, kwota_skladki, data_zawarcia, id_agenta, kod_produktu, status_polisy, data_wstawienia) 
            VALUES (src.id_polisy, src.kwota_skladki, src.data_zawarcia, src.id_agenta, src.kod_produktu, src.status_polisy, SYSDATE);

        v_rows_processed := SQL%ROWCOUNT;
        COMMIT;
        
        log_audit('LOAD_FACT', 'KONIEC LOAD fct_policies', v_rows_processed, 'COMPLETED');
        EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            log_audit('LOAD_FACT', v_current_step, 0, 'FAILED', SQLERRM);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            BEGIN EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML'; EXCEPTION WHEN OTHERS THEN NULL; END;
            RAISE;
    END prc_load_fct_policies;

    -- ==================================================
    -- PROCEDURA ZAMYKAJĄCA (Złoty Klucz uruchamiany na końcu Joba)
    -- ==================================================
    PROCEDURE prc_close_batch_success(p_batch_name VARCHAR2) IS
    BEGIN
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_CLOSE', p_batch_name);
        
        -- 1. Przesunięcie HWM (Zegar startowy dogania zatwierdzony cel)
        UPDATE etl_hwm_control 
        SET last_success_run = target_run, process_status = 'COMPLETED'
        WHERE process_name = p_batch_name;
        
        IF SQL%ROWCOUNT = 0 THEN
            RAISE_APPLICATION_ERROR(-20003, 'BŁĄD: Próba zamknięcia nieistniejącego procesu: ' || p_batch_name);
        END IF;

        -- 2. Zamknięcie zrealizowanych zleceń biznesowych dla tego batcha
        UPDATE etl_manual_refresh
        SET status = 'COMPLETED'
        WHERE batch_name = p_batch_name AND status = 'PENDING';

        COMMIT;
        log_audit(p_batch_name, 'HANDSHAKE Z DATASTAGE', 0, 'COMPLETED', 'Zegar zaktualizowany, zlecenia zamknięte.');
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    END prc_close_batch_success;

    -- ==================================================
    -- PROCEDURA CZYSZCZĄCA (Uruchamiana na samym początku sekwensera)
    -- ==================================================
    PROCEDURE prc_truncate_all_stg IS
        v_current_step VARCHAR2(50);
    BEGIN
        DBMS_APPLICATION_INFO.SET_MODULE('ETL_CLEANUP', 'Czyszczenie STG');
        log_audit('CLEANUP', 'START CZYSZCZENIA STG', 0, 'RUNNING');
        
        v_current_step := 'Czyszczenie STG BATCH 1';
        DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_1';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_2';
        -- [... do 5]

        v_current_step := 'Czyszczenie STG BATCH 2';
        DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
        -- EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_6';
        -- [... do 10]

        v_current_step := 'Czyszczenie STG BATCH 3';
        DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
        -- EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_tab_11';
        -- [... do 15]

        log_audit('CLEANUP', 'KONIEC CZYSZCZENIA STG', 0, 'COMPLETED');
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
    EXCEPTION
        WHEN OTHERS THEN
            log_audit('CLEANUP', v_current_step, 0, 'FAILED', SQLERRM);
            DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
            RAISE;
    END prc_truncate_all_stg;

END pkg_etl_delta_load;
/


-- =====================================================================
-- 1. TWORZENIE TABELI KONTROLNEJ (Metadata & State Management)
-- =====================================================================
CREATE TABLE etl_hwm_control (
    process_name     VARCHAR2(100) NOT NULL,
    last_success_run TIMESTAMP     NOT NULL,               -- Dolna granica okna (od kiedy)
    target_run       TIMESTAMP,                            -- Górna granica okna (do kiedy) - bufor dla Auto-Naprawy
    chunk_days       NUMBER        DEFAULT 0 NOT NULL,     -- Sterowanie: 0 = delta dzienna, >0 = okno historyczne
    process_status   VARCHAR2(20)  DEFAULT 'COMPLETED' NOT NULL,
    updated_at       TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL,
    updated_by       VARCHAR2(50)  DEFAULT SYS_CONTEXT('USERENV', 'OS_USER') NOT NULL,
    
    -- Klucz główny
    CONSTRAINT pk_etl_hwm_control PRIMARY KEY (process_name),
    
    -- Zabezpieczenie integralności statusów
    CONSTRAINT chk_hwm_status CHECK (process_status IN ('COMPLETED', 'IN_PROGRESS', 'FAILED'))
);

-- =====================================================================
-- 2. DOKUMENTACJA SŁOWNIKOWA (Best Practice)
-- =====================================================================
COMMENT ON TABLE etl_hwm_control IS 'Tabela kontrolna HWM sterująca cyklem życia, zakresem czasowym i trybem procesów ETL.';
COMMENT ON COLUMN etl_hwm_control.process_name IS 'PK. Unikalna nazwa procesu/batcha (np. BATCH_1, BATCH_2).';
COMMENT ON COLUMN etl_hwm_control.last_success_run IS 'Data i czas (HWM) będące dolną granicą okna ekstrakcji. Aktualizowane po pełnym sukcesie.';
COMMENT ON COLUMN etl_hwm_control.target_run IS 'Górna granica okna ekstrakcji. Zamrażana w momencie startu ekstrakcji (zabezpieczenie awaryjne).';
COMMENT ON COLUMN etl_hwm_control.chunk_days IS 'Rozmiar paczki w dniach. 0 = przyrost dobowy do SYSTIMESTAMP. Wartość > 0 = zasilenie inicjalne w oknach czasowych.';
COMMENT ON COLUMN etl_hwm_control.process_status IS 'Status procesu: COMPLETED (gotowy do startu), IN_PROGRESS (zablokowany/w trakcie), FAILED.';
COMMENT ON COLUMN etl_hwm_control.updated_at IS 'Techniczny czas ostatniej modyfikacji rekordu.';
COMMENT ON COLUMN etl_hwm_control.updated_by IS 'Użytkownik bazodanowy (lub DataStage), który dokonał modyfikacji rekordu.';

-- =====================================================================
-- 3. TRIGGER AUDYTOWY (Wymuszenie logowania zmian na poziomie silnika)
-- =====================================================================
CREATE OR REPLACE TRIGGER trg_etl_hwm_control_upd
BEFORE UPDATE ON etl_hwm_control
FOR EACH ROW
BEGIN
    -- Niezależnie od tego, co wyśle procedura PL/SQL, baza nadpisze te wartości twardymi danymi z sesji
    :NEW.updated_at := SYSTIMESTAMP;
    :NEW.updated_by := SYS_CONTEXT('USERENV', 'OS_USER');
END;
/

-- =====================================================================
-- 4. INICJALIZACJA DANYCH (Rozwiązanie problemu "Cold Start")
-- =====================================================================
-- Poniżej ustawiona jest bezpieczna data początkowa. 
-- Jeśli zaczynasz od razu od ładowania inicjalnego (np. 4 lata w tył), 
-- zmień last_success_run na '2022-01-01' i chunk_days np. na 180 przed uruchomieniem Jobów.

INSERT INTO etl_hwm_control (process_name, last_success_run, target_run, chunk_days, process_status) 
VALUES ('BATCH_1', TO_TIMESTAMP('2026-04-10 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), NULL, 0, 'COMPLETED');

INSERT INTO etl_hwm_control (process_name, last_success_run, target_run, chunk_days, process_status) 
VALUES ('BATCH_2', TO_TIMESTAMP('2026-04-10 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), NULL, 0, 'COMPLETED');

INSERT INTO etl_hwm_control (process_name, last_success_run, target_run, chunk_days, process_status) 
VALUES ('BATCH_3', TO_TIMESTAMP('2026-04-10 00:00:00', 'YYYY-MM-DD HH24:MI:SS'), NULL, 0, 'COMPLETED');

COMMIT;


-- =====================================================================
-- 1. TWORZENIE TABELI ZLECEŃ RĘCZNYCH (Manual Override)
-- =====================================================================
CREATE TABLE etl_manual_refresh (
    batch_name   VARCHAR2(50)  NOT NULL,
    table_source VARCHAR2(50)  NOT NULL,
    record_id    NUMBER        NOT NULL,
    status       VARCHAR2(20)  DEFAULT 'PENDING' NOT NULL,
    requested_by VARCHAR2(50)  DEFAULT SYS_CONTEXT('USERENV', 'OS_USER') NOT NULL,
    requested_at TIMESTAMP     DEFAULT SYSTIMESTAMP NOT NULL,
    
    -- Klucz główny zapobiega powielaniu tych samych zleceń dla tego samego rekordu
    CONSTRAINT pk_etl_manual_refresh PRIMARY KEY (table_source, record_id),
    
    -- Zabezpieczenie integralności statusów
    CONSTRAINT chk_manual_status CHECK (status IN ('PENDING', 'COMPLETED', 'ERROR'))
);

-- =====================================================================
-- 2. INDEKS OPTYMALIZACYJNY DLA PROCEDURY ZAMYKAJĄCEJ
-- =====================================================================
-- Ten indeks drastycznie przyspieszy operację UPDATE wewnątrz prc_close_batch_success
-- (gdzie szukamy WHERE batch_name = p_batch_name AND status = 'PENDING')
CREATE INDEX idx_etl_manual_status ON etl_manual_refresh(batch_name, status);

-- =====================================================================
-- 3. DOKUMENTACJA SŁOWNIKOWA (Best Practice)
-- =====================================================================
COMMENT ON TABLE etl_manual_refresh IS 'Tabela przechowująca ręczne zlecenia biznesowe na przeładowanie konkretnych identyfikatorów z pominięciem standardowej delty czasowej.';
COMMENT ON COLUMN etl_manual_refresh.batch_name IS 'Nazwa procesu ETL (np. BATCH_1), który ma obsłużyć to zlecenie. Pozwala rozdzielić pracę na procesy równoległe.';
COMMENT ON COLUMN etl_manual_refresh.table_source IS 'Fizyczna nazwa tabeli źródłowej, z której pochodzi rekord (np. TABELA_1). Część klucza głównego.';
COMMENT ON COLUMN etl_manual_refresh.record_id IS 'Identyfikator biznesowy (klucz główny źródła) wymuszony do przeładowania. Część klucza głównego.';
COMMENT ON COLUMN etl_manual_refresh.status IS 'Status zlecenia: PENDING (oczekuje w kolejce do pobrania), COMPLETED (przetworzone poprawnie na wszystkich warstwach), ERROR.';
COMMENT ON COLUMN etl_manual_refresh.requested_by IS 'Użytkownik (lub aplikacja zlecająca) wprowadzający żądanie przeładowania.';
COMMENT ON COLUMN etl_manual_refresh.requested_at IS 'Data i czas wpłynięcia zlecenia do bazy danych.';
