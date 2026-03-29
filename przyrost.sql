CREATE OR REPLACE PROCEDURE prc_extract_delta_ids IS
    v_last_run    TIMESTAMP;
    v_current_run TIMESTAMP := SYSTIMESTAMP;
    
    -- Konfiguracja: Zakładamy, że żadna transakcja OLTP nie wisi bez commitu dłużej niż 15 minut
    v_overlap_interval INTERVAL DAY TO SECOND := INTERVAL '15' MINUTE; 
BEGIN
    BEGIN
        SELECT last_success_run INTO v_last_run
        FROM etl_hwm_control
        WHERE process_name = 'DELTA_EXTRACT';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_last_run := TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD');
    END;

    EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_changed_records';

    -- 3. Zbieranie z "ZAKŁADKĄ"
    -- Cofamy datę ostatniego uruchomienia o nasz bufor bezpieczeństwa (15 minut)
    INSERT /*+ APPEND */ INTO stg_changed_records (table_source, record_id, extracted_at)
    SELECT 'TABELA_A', id, v_current_run
    FROM tabela_a
    -- ZMIANA KRYTYCZNA: odejmujemy interwał od v_last_run
    WHERE modified_at >= (v_last_run - v_overlap_interval)
      AND modified_at < v_current_run;

    MERGE INTO etl_hwm_control trg
    USING (SELECT 'DELTA_EXTRACT' AS process_name, v_current_run AS run_time FROM dual) src
    ON (trg.process_name = src.process_name)
    WHEN MATCHED THEN 
        UPDATE SET trg.last_success_run = src.run_time
    WHEN NOT MATCHED THEN 
        INSERT (process_name, last_success_run) VALUES (src.process_name, src.run_time);

    COMMIT;
END prc_extract_delta_ids;
/


CREATE OR REPLACE PROCEDURE prc_extract_delta_static IS
    v_last_run         TIMESTAMP;
    v_current_run      TIMESTAMP := SYSTIMESTAMP;
    v_overlap_interval INTERVAL DAY TO SECOND := INTERVAL '15' MINUTE; 
    v_current_step     VARCHAR2(50); -- Zmienna pomocnicza do śledzenia błędów
BEGIN
    -- Ustawienie nazwy modułu dla monitorowania w v$session
    DBMS_APPLICATION_INFO.SET_MODULE('ETL_DELTA_LOAD', 'Inicjalizacja');

    v_current_step := 'Odczyt HWM';
    BEGIN
        SELECT last_success_run INTO v_last_run
        FROM etl_hwm_control
        WHERE process_name = 'DELTA_EXTRACT';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_last_run := TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD');
    END;

    -- Wyjątek dozwolony przez architekturę: TRUNCATE
    v_current_step := 'Czyszczenie STG';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    EXECUTE IMMEDIATE 'TRUNCATE TABLE stg_changed_records';

    -- Sekwencja statycznych INSERT-ów z jawnym mapowaniem kolumn kluczy
    
    v_current_step := 'Ekstrakcja: TABELA_1';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    INSERT INTO stg_changed_records (table_source, record_id, extracted_at)
    SELECT 'TABELA_1', id_1, v_current_run
    FROM tabela_1
    WHERE CZAS_AKTUALIZACJI_REKORDU >= (v_last_run - v_overlap_interval)
      AND CZAS_AKTUALIZACJI_REKORDU < v_current_run;

    v_current_step := 'Ekstrakcja: TABELA_2';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    INSERT INTO stg_changed_records (table_source, record_id, extracted_at)
    SELECT 'TABELA_2', idxsds_2, v_current_run
    FROM tabela_2
    WHERE CZAS_AKTUALIZACJI_REKORDU >= (v_last_run - v_overlap_interval)
      AND CZAS_AKTUALIZACJI_REKORDU < v_current_run;

    v_current_step := 'Ekstrakcja: TABELA_3';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    INSERT INTO stg_changed_records (table_source, record_id, extracted_at)
    SELECT 'TABELA_3', id_klienta_3, v_current_run
    FROM tabela_3
    WHERE CZAS_AKTUALIZACJI_REKORDU >= (v_last_run - v_overlap_interval)
      AND CZAS_AKTUALIZACJI_REKORDU < v_current_run;

    -- [... Wstawiasz pozostałe 12 tabel według schematu ...]

    v_current_step := 'Aktualizacja HWM';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    MERGE INTO etl_hwm_control trg
    USING (SELECT 'DELTA_EXTRACT' AS process_name, v_current_run AS run_time FROM dual) src
    ON (trg.process_name = src.process_name)
    WHEN MATCHED THEN 
        UPDATE SET trg.last_success_run = src.run_time
    WHEN NOT MATCHED THEN 
        INSERT (process_name, last_success_run) VALUES (src.process_name, src.run_time);

    -- Zatwierdzenie całej transakcji (od momentu po TRUNCATE do MERGE)
    COMMIT;
    
    -- Czyszczenie metadanych sesji
    DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        -- Zapisujemy krok, w którym wystąpił błąd, co skróci czas diagnozy z godzin do sekund
        DBMS_OUTPUT.PUT_LINE('Błąd krytyczny w kroku: ' || v_current_step);
        DBMS_OUTPUT.PUT_LINE('Komunikat błędu: ' || SQLERRM);
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
        RAISE;
END prc_extract_delta_static;
/


-- =====================================================================
-- 1. TABELA KONTROLNA (High Water Mark)
-- =====================================================================
CREATE TABLE etl_hwm_control (
    process_name     VARCHAR2(100) NOT NULL,
    last_success_run TIMESTAMP NOT NULL,
    CONSTRAINT pk_etl_hwm_control PRIMARY KEY (process_name)
);

-- Komentarze słownikowe (Best Practice)
COMMENT ON TABLE etl_hwm_control IS 'Tabela kontrolna przechowująca czas ostatniego pomyślnego wykonania procesu ETL (High Water Mark).';
COMMENT ON COLUMN etl_hwm_control.process_name IS 'Klucz główny. Unikalna nazwa procesu ETL, np. DELTA_EXTRACT';
COMMENT ON COLUMN etl_hwm_control.last_success_run IS 'Data i czas (Timestamp) zakończenia ostatniego pomyślnego uruchomienia.';

-- =====================================================================
-- 2. TABELA STAGINGOWA (Dane delta)
-- =====================================================================
CREATE TABLE stg_changed_records (
    table_source VARCHAR2(50)  NOT NULL,
    record_id    VARCHAR2(255) NOT NULL, -- Celowo VARCHAR2, aby uniknąć ORA-01722 dla różnych typów kluczy
    extracted_at TIMESTAMP     NOT NULL
) NOLOGGING; -- Wyłączenie logowania Redo na poziomie tabeli dla operacji Bulk/Direct-Path

COMMENT ON TABLE stg_changed_records IS 'Tabela przejściowa (Stage) przechowująca wyciągnięte identyfikatory zmodyfikowanych rekordów z różnych tabel źródłowych.';
COMMENT ON COLUMN stg_changed_records.table_source IS 'Nazwa tabeli źródłowej, z której pochodzi dany rekord (np. TABELA_1).';
COMMENT ON COLUMN stg_changed_records.record_id IS 'Uniwersalny identyfikator rekordu. Typ znakowy dla bezpieczeństwa i uniwersalności.';
COMMENT ON COLUMN stg_changed_records.extracted_at IS 'Data i czas wyciągnięcia rekordu przez proces ETL (równa v_current_run z procedury).';



CREATE OR REPLACE PROCEDURE prc_extract_delta_static IS
    v_last_run         TIMESTAMP;
    v_current_run      TIMESTAMP := SYSTIMESTAMP;
    v_overlap_interval INTERVAL DAY TO SECOND := INTERVAL '15' MINUTE; 
    v_current_step     VARCHAR2(50);
BEGIN
    DBMS_APPLICATION_INFO.SET_MODULE('ETL_DELTA_LOAD', 'Inicjalizacja');

    v_current_step := 'Odczyt HWM';
    BEGIN
        SELECT last_success_run INTO v_last_run
        FROM etl_hwm_control
        WHERE process_name = 'DELTA_EXTRACT';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            v_last_run := TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD');
    END;

    -- UWAGA: USUNIĘTO TRUNCATE! 
    -- Użycie MERGE ma sens tylko wtedy, gdy tabela przechowuje dane między uruchomieniami.

    -- ==========================================
    -- Sekwencja instrukcji MERGE
    -- ==========================================
    
    v_current_step := 'Ekstrakcja: TABELA_1';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    MERGE INTO stg_changed_records trg
    USING (
        SELECT id_1 
        FROM tabela_1
        WHERE CZAS_AKTUALIZACJI_REKORDU >= (v_last_run - v_overlap_interval)
          AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
    ) src
    ON (trg.table_source = 'TABELA_1' AND trg.record_id = TO_CHAR(src.id_1))
    WHEN MATCHED THEN 
        UPDATE SET trg.extracted_at = v_current_run
    WHEN NOT MATCHED THEN 
        INSERT (table_source, record_id, extracted_at) 
        VALUES ('TABELA_1', TO_CHAR(src.id_1), v_current_run);

    v_current_step := 'Ekstrakcja: TABELA_2';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    MERGE INTO stg_changed_records trg
    USING (
        SELECT idxsds_2 
        FROM tabela_2
        WHERE CZAS_AKTUALIZACJI_REKORDU >= (v_last_run - v_overlap_interval)
          AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
    ) src
    ON (trg.table_source = 'TABELA_2' AND trg.record_id = TO_CHAR(src.idxsds_2))
    WHEN MATCHED THEN 
        UPDATE SET trg.extracted_at = v_current_run
    WHEN NOT MATCHED THEN 
        INSERT (table_source, record_id, extracted_at) 
        VALUES ('TABELA_2', TO_CHAR(src.idxsds_2), v_current_run);

    v_current_step := 'Ekstrakcja: TABELA_3';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    MERGE INTO stg_changed_records trg
    USING (
        SELECT id_klienta_3 
        FROM tabela_3
        WHERE CZAS_AKTUALIZACJI_REKORDU >= (v_last_run - v_overlap_interval)
          AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
    ) src
    ON (trg.table_source = 'TABELA_3' AND trg.record_id = TO_CHAR(src.id_klienta_3))
    WHEN MATCHED THEN 
        UPDATE SET trg.extracted_at = v_current_run
    WHEN NOT MATCHED THEN 
        INSERT (table_source, record_id, extracted_at) 
        VALUES ('TABELA_3', TO_CHAR(src.id_klienta_3), v_current_run);

    -- [... Wstawiasz pozostałe 12 tabel według schematu ...]

    v_current_step := 'Aktualizacja HWM';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    MERGE INTO etl_hwm_control trg
    USING (SELECT 'DELTA_EXTRACT' AS process_name, v_current_run AS run_time FROM dual) src
    ON (trg.process_name = src.process_name)
    WHEN MATCHED THEN 
        UPDATE SET trg.last_success_run = src.run_time
    WHEN NOT MATCHED THEN 
        INSERT (process_name, last_success_run) VALUES (src.process_name, src.run_time);

    -- Zatwierdzenie całej transakcji
    COMMIT;
    
    DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Błąd krytyczny w kroku: ' || v_current_step);
        DBMS_OUTPUT.PUT_LINE('Komunikat błędu: ' || SQLERRM);
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
        RAISE;
END prc_extract_delta_static;
/

--------------------------------------------------------------------------
-- rozwiazanie v3

CREATE OR REPLACE PROCEDURE prc_extract_delta_static IS
    v_last_run         TIMESTAMP;
    v_current_run      TIMESTAMP := SYSTIMESTAMP;
    v_overlap_interval INTERVAL DAY TO SECOND := INTERVAL '15' MINUTE; 
    v_current_step     VARCHAR2(50);
BEGIN
    DBMS_APPLICATION_INFO.SET_MODULE('ETL_DELTA_LOAD', 'Inicjalizacja');

    -- v_current_step := 'Odczyt HWM';
    -- BEGIN
    --     SELECT last_success_run INTO v_last_run
    --     FROM etl_hwm_control
    --     WHERE process_name = 'DELTA_EXTRACT';
    -- EXCEPTION
    --     WHEN NO_DATA_FOUND THEN
    --         v_last_run := TO_TIMESTAMP('1900-01-01', 'YYYY-MM-DD');
    -- END;

    v_current_step := 'Odczyt HWM';
    BEGIN
        SELECT last_success_run INTO v_last_run
        FROM etl_hwm_control
        WHERE process_name = 'DELTA_EXTRACT';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            -- Rzucenie niestandardowego wyjątku przerywa działanie procedury
            RAISE_APPLICATION_ERROR(
                -20001, 
                'BŁĄD KRYTYCZNY: W tabeli etl_hwm_control nie znaleziono daty (HWM) dla procesu DELTA_EXTRACT. Zainicjuj tabelę przed uruchomieniem procedury.'
            );
    END;    

    -- ==========================================
    -- Sekwencja instrukcji MERGE (Podział na typy danych)
    -- ==========================================
    
    -- 1. Zrzut ID typu NUMBER
    v_current_step := 'Ekstrakcja: TABELA_1 (NUMBER)';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    MERGE INTO stg_changed_records_n trg
    USING (
        SELECT id_1 
        FROM tabela_1
        WHERE CZAS_AKTUALIZACJI_REKORDU >= (v_last_run - v_overlap_interval)
          AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
    ) src
    ON (trg.table_source = 'TABELA_1' AND trg.record_id = src.id_1) -- Brak TO_CHAR
    WHEN MATCHED THEN 
        UPDATE SET trg.extracted_at = v_current_run
    WHEN NOT MATCHED THEN 
        INSERT (table_source, record_id, extracted_at) 
        VALUES ('TABELA_1', src.id_1, v_current_run);

    -- 2. Zrzut ID typu VARCHAR2
    v_current_step := 'Ekstrakcja: TABELA_2 (VARCHAR2)';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    MERGE INTO stg_changed_records_v trg
    USING (
        SELECT idxsds_2 
        FROM tabela_2
        WHERE CZAS_AKTUALIZACJI_REKORDU >= (v_last_run - v_overlap_interval)
          AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
    ) src
    ON (trg.table_source = 'TABELA_2' AND trg.record_id = src.idxsds_2) -- Brak TO_CHAR
    WHEN MATCHED THEN 
        UPDATE SET trg.extracted_at = v_current_run
    WHEN NOT MATCHED THEN 
        INSERT (table_source, record_id, extracted_at) 
        VALUES ('TABELA_2', src.idxsds_2, v_current_run);

    -- 3. Zrzut ID typu DATE
    v_current_step := 'Ekstrakcja: TABELA_3 (DATE)';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    MERGE INTO stg_changed_records_d trg
    USING (
        SELECT id_klienta_3 
        FROM tabela_3
        WHERE CZAS_AKTUALIZACJI_REKORDU >= (v_last_run - v_overlap_interval)
          AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
    ) src
    ON (trg.table_source = 'TABELA_3' AND trg.record_id = src.id_klienta_3) -- Brak TO_CHAR
    WHEN MATCHED THEN 
        UPDATE SET trg.extracted_at = v_current_run
    WHEN NOT MATCHED THEN 
        INSERT (table_source, record_id, extracted_at) 
        VALUES ('TABELA_3', src.id_klienta_3, v_current_run);

    -- [... Wstawiasz pozostałe 12 tabel, kierując je do odpowiedniego STG w zależności od typu klucza ...]

    v_current_step := 'Aktualizacja HWM';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    MERGE INTO etl_hwm_control trg
    USING (SELECT 'DELTA_EXTRACT' AS process_name, v_current_run AS run_time FROM dual) src
    ON (trg.process_name = src.process_name)
    WHEN MATCHED THEN 
        UPDATE SET trg.last_success_run = src.run_time
    WHEN NOT MATCHED THEN 
        INSERT (process_name, last_success_run) VALUES (src.process_name, src.run_time);

    -- Zatwierdzenie całej transakcji
    COMMIT;
    
    DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK;
        DBMS_OUTPUT.PUT_LINE('Błąd krytyczny w kroku: ' || v_current_step);
        DBMS_OUTPUT.PUT_LINE('Komunikat błędu: ' || SQLERRM);
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
        RAISE;
END prc_extract_delta_static;
/

-- =====================================================================
-- 1. TABELA STG DLA KLUCZY TYPU NUMBER
-- =====================================================================
CREATE TABLE stg_changed_records_n (
    table_source VARCHAR2(50) NOT NULL,
    record_id    NUMBER       NOT NULL,
    extracted_at TIMESTAMP    NOT NULL
) NOLOGGING;

-- Unikalny indeks kompozytowy - KRYTYCZNY DLA WYDAJNOŚCI MERGE
CREATE UNIQUE INDEX idx_stg_rec_n_pk ON stg_changed_records_n(table_source, record_id);

COMMENT ON TABLE stg_changed_records_n IS 'Tabela STG dla identyfikatorów numerycznych z ubezpieczeń majątkowych.';


-- =====================================================================
-- 2. TABELA STG DLA KLUCZY TYPU VARCHAR2
-- =====================================================================
CREATE TABLE stg_changed_records_v (
    table_source VARCHAR2(50)  NOT NULL,
    record_id    VARCHAR2(255) NOT NULL, -- Ograniczony rozmiar optymalizuje indeks B-Tree
    extracted_at TIMESTAMP     NOT NULL
) NOLOGGING;

-- Unikalny indeks kompozytowy - KRYTYCZNY DLA WYDAJNOŚCI MERGE
CREATE UNIQUE INDEX idx_stg_rec_v_pk ON stg_changed_records_v(table_source, record_id);

COMMENT ON TABLE stg_changed_records_v IS 'Tabela STG dla identyfikatorów tekstowych (np. numery polis).';


-- =====================================================================
-- 3. TABELA STG DLA KLUCZY TYPU DATE (Zgodnie z wymogiem, choć architektonicznie odradzane)
-- =====================================================================
CREATE TABLE stg_changed_records_d (
    table_source VARCHAR2(50) NOT NULL,
    record_id    DATE         NOT NULL,
    extracted_at TIMESTAMP    NOT NULL
) NOLOGGING;

-- Unikalny indeks kompozytowy - KRYTYCZNY DLA WYDAJNOŚCI MERGE
CREATE UNIQUE INDEX idx_stg_rec_d_pk ON stg_changed_records_d(table_source, record_id);

COMMENT ON TABLE stg_changed_records_d IS 'Tabela STG dla identyfikatorów opartych o datę (legacy systems).';


-- Skrypt wdrożeniowy (uruchomić RAZ przed startem nocnego batcha ETL)
INSERT INTO etl_hwm_control (process_name, last_success_run) 
VALUES ('DELTA_EXTRACT', TO_TIMESTAMP('2026-03-01 00:00:00', 'YYYY-MM-DD HH24:MI:SS'));
COMMIT;

-- wersja 3


CREATE OR REPLACE PROCEDURE prc_extract_delta_parallel IS
    -- 1. Definicja głównych zmiennych
    v_last_run         TIMESTAMP;
    v_current_run      TIMESTAMP := SYSTIMESTAMP;
    --v_overlap_interval INTERVAL DAY TO SECOND := INTERVAL '15' MINUTE; 
    --v_extract_from     TIMESTAMP;
    v_current_step     VARCHAR2(50);
    v_rows_processed   NUMBER;

    -- 2. Sub-procedura logująca (Niezależna transakcja)
    PROCEDURE log_audit(p_step VARCHAR2, p_rows NUMBER, p_status VARCHAR2, p_err VARCHAR2 DEFAULT NULL) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO etl_audit_log (run_timestamp, process_name, step_name, rows_processed, status, error_message)
        VALUES (v_current_run, 'DELTA_EXTRACT', p_step, p_rows, p_status, SUBSTR(p_err, 1, 4000));
        COMMIT; -- Ten commit dotyczy tylko zapisu logu!
    END log_audit;

BEGIN
    -- Ustawienie metadanych sesji
    DBMS_APPLICATION_INFO.SET_MODULE('ETL_DELTA_LOAD', 'Inicjalizacja');
    log_audit('START PROCESU', 0, 'RUNNING');

    -- WŁĄCZENIE PARALLEL DML DLA SESJI
    EXECUTE IMMEDIATE 'ALTER SESSION ENABLE PARALLEL DML';

    -- ODCZYT HWM (Fail-Fast)
    v_current_step := 'Odczyt HWM';
    BEGIN
        SELECT last_success_run INTO v_last_run
        FROM etl_hwm_control
        WHERE process_name = 'DELTA_EXTRACT';
    EXCEPTION
        WHEN NO_DATA_FOUND THEN
            RAISE_APPLICATION_ERROR(
                -20001, 
                'BŁĄD KRYTYCZNY: Brak daty HWM w tabeli etl_hwm_control. Zainicjuj tabelę.'
            );
    END;

    -- Wyliczenie daty okna (żeby nie liczyć w SQL)
    --v_extract_from := v_last_run - v_overlap_interval;

    -- ==========================================
    -- GŁÓWNY BLOK EKSTRAKCJI (MERGE)
    -- ==========================================
    
    -- --- TABELA 1: Typ NUMBER ---
    v_current_step := 'Ekstrakcja: TABELA_1 (NUMBER)';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    
    MERGE /*+ PARALLEL(trg, 4) PARALLEL(src, 4) */ 
    INTO stg_changed_records_n trg
    USING (
        SELECT id_1 
        FROM tabela_1
        --WHERE CZAS_AKTUALIZACJI_REKORDU >= v_extract_from
        WHERE CZAS_AKTUALIZACJI_REKORDU >= v_last_run
          AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
    ) src
    ON (trg.table_source = 'TABELA_1' AND trg.record_id = src.id_1)
    WHEN MATCHED THEN 
        UPDATE SET trg.extracted_at = v_current_run
    WHEN NOT MATCHED THEN 
        INSERT (table_source, record_id, extracted_at) 
        VALUES ('TABELA_1', src.id_1, v_current_run);

    v_rows_processed := SQL%ROWCOUNT; -- Pobranie liczby zmodyfikowanych wierszy
    COMMIT; -- Zwolnienie blokady Parallel DML (ORA-12838)
    log_audit(v_current_step, v_rows_processed, 'SUCCESS');

    -- --- TABELA 2: Typ VARCHAR2 ---
    v_current_step := 'Ekstrakcja: TABELA_2 (VARCHAR2)';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    
    MERGE /*+ PARALLEL(trg, 4) PARALLEL(src, 4) */ 
    INTO stg_changed_records_v trg
    USING (
        SELECT idxsds_2 
        FROM tabela_2
        WHERE CZAS_AKTUALIZACJI_REKORDU >= v_extract_from
          AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
    ) src
    ON (trg.table_source = 'TABELA_2' AND trg.record_id = src.idxsds_2)
    WHEN MATCHED THEN 
        UPDATE SET trg.extracted_at = v_current_run
    WHEN NOT MATCHED THEN 
        INSERT (table_source, record_id, extracted_at) 
        VALUES ('TABELA_2', src.idxsds_2, v_current_run);

    v_rows_processed := SQL%ROWCOUNT;
    COMMIT;
    log_audit(v_current_step, v_rows_processed, 'SUCCESS');

    -- --- TABELA 3: Typ DATE ---
    v_current_step := 'Ekstrakcja: TABELA_3 (DATE)';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    
    MERGE /*+ PARALLEL(trg, 4) PARALLEL(src, 4) */ 
    INTO stg_changed_records_d trg
    USING (
        SELECT id_klienta_3 
        FROM tabela_3
        WHERE CZAS_AKTUALIZACJI_REKORDU >= v_extract_from
          AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
    ) src
    ON (trg.table_source = 'TABELA_3' AND trg.record_id = src.id_klienta_3)
    WHEN MATCHED THEN 
        UPDATE SET trg.extracted_at = v_current_run
    WHEN NOT MATCHED THEN 
        INSERT (table_source, record_id, extracted_at) 
        VALUES ('TABELA_3', src.id_klienta_3, v_current_run);

    v_rows_processed := SQL%ROWCOUNT;
    COMMIT;
    log_audit(v_current_step, v_rows_processed, 'SUCCESS');

    -- [... Tutaj dodajesz bloki dla pozostałych 12 tabel według tego samego wzorca ...]

    -- ==========================================
    -- AKTUALIZACJA KONTROLNA HWM
    -- ==========================================
    v_current_step := 'Aktualizacja HWM';
    DBMS_APPLICATION_INFO.SET_ACTION(v_current_step);
    MERGE INTO etl_hwm_control trg
    USING (SELECT 'DELTA_EXTRACT' AS process_name, v_current_run AS run_time FROM dual) src
    ON (trg.process_name = src.process_name)
    WHEN MATCHED THEN 
        UPDATE SET trg.last_success_run = src.run_time
    WHEN NOT MATCHED THEN 
        INSERT (process_name, last_success_run) VALUES (src.process_name, src.run_time);

    COMMIT;
    log_audit(v_current_step, 1, 'SUCCESS');
    log_audit('KONIEC PROCESU', 0, 'COMPLETED');
    
    -- Sprzątanie sesji
    EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML';
    DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);

EXCEPTION
    WHEN OTHERS THEN
        ROLLBACK; -- Cofa tylko to, co nie dostało COMMIT (np. HWM jeśli awaria była na końcu)
        
        -- Autonomiczny zapis błędu (nie zostanie cofnięty przez ROLLBACK wyżej!)
        log_audit(v_current_step, 0, 'FAILED', SQLERRM);
        
        DBMS_OUTPUT.PUT_LINE('Błąd krytyczny w kroku: ' || v_current_step);
        DBMS_OUTPUT.PUT_LINE('Komunikat błędu: ' || SQLERRM);
        
        -- Sprzątanie po awarii
        DBMS_APPLICATION_INFO.SET_MODULE(NULL, NULL);
        BEGIN EXECUTE IMMEDIATE 'ALTER SESSION DISABLE PARALLEL DML'; EXCEPTION WHEN OTHERS THEN NULL; END;
        
        RAISE; -- Wyrzucenie błędu do warstwy DataStage
END prc_extract_delta_parallel;
/


--to też jest ok
MERGE /*+ PARALLEL(trg, 4) PARALLEL(src, 4) */ 
    INTO stg_changed_records_v trg
    USING (
        -- 1. Pobranie IDków, gdy zmodyfikowano samego rodzica
        SELECT idxsds_2 
        FROM tabela_2
        WHERE CZAS_AKTUALIZACJI_REKORDU >= v_extract_from
          AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
          
        UNION -- UNION jest tu KRYTYCZNY: deduplikuje IDki, by nie wywołać błędu ORA-30926 w MERGE
        
        -- 2. Pobranie IDków rodzica, gdy zmodyfikowano TYLKO dziecko
        -- ZOPTYMALIZOWANO: Usunięto INNER JOIN. Czytamy wprost z klucza obcego tabeli dziecka.
        SELECT fk_idxsds_2 AS idxsds_2
        FROM tabela_4a 
        WHERE CZAS_AKTUALIZACJI_REKORDU >= v_extract_from
          AND CZAS_AKTUALIZACJI_REKORDU < v_current_run
        UNION
        SELECT idxsds_2 
        FROM tabela_2 t2
        inner join tabela_4a 4a
        on t2.idxsds_2 = 4a.fk_idxsds_2
        WHERE 4a.CZAS_AKTUALIZACJI_REKORDU >= v_extract_from
          AND 4a.CZAS_AKTUALIZACJI_REKORDU < v_current_run
    ) src
    ON (trg.table_source = 'TABELA_2' AND trg.record_id = src.idxsds_2)
    WHEN MATCHED THEN 
        UPDATE SET trg.extracted_at = v_current_run
    WHEN NOT MATCHED THEN 
        INSERT (table_source, record_id, extracted_at) 
        VALUES ('TABELA_2', src.idxsds_2, v_current_run);
