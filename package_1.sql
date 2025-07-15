-- Poprawiona SPECYFIKACJA PAKIETU
CREATE OR REPLACE PACKAGE pkg_data_loader AS

    /**
     * Uruchamia proces zasilania.
     * @param p_target_table Opcjonalna nazwa tabeli do zasilenia. Jeśli NULL, zasila wszystkie.
     */
    PROCEDURE run_load(p_target_table IN VARCHAR2 DEFAULT NULL);

END pkg_data_loader;
/

-- Poprawione CIAŁO PAKIETU
CREATE OR REPLACE PACKAGE BODY pkg_data_loader AS

    -- Prywatna zmienna globalna przechowująca ID bieżącego uruchomienia.
    g_run_id NUMBER;

    -- Prywatna procedura do zapisu logów.
    PROCEDURE p_log(
        p_target_table  IN VARCHAR2,
        p_status        IN VARCHAR2,
        p_rows_affected IN NUMBER   DEFAULT NULL,
        p_error_message IN VARCHAR2 DEFAULT NULL
    ) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        INSERT INTO data_load_log (run_id, target_table, status, rows_affected, error_message, log_timestamp)
        VALUES (g_run_id, p_target_table, p_status, p_rows_affected, p_error_message, SYSTIMESTAMP);
        COMMIT;
    END p_log;

    -- Prywatna funkcja sprawdzająca warunki wstępne.
    FUNCTION f_are_conditions_met RETURN BOOLEAN IS
        v_condition_ok BOOLEAN := TRUE;
        -- TODO: Dodać logikę sprawdzającą, np. dostępność systemów źródłowych, istnienie plików etc.
        -- Przykład:
        -- SELECT COUNT(*) INTO v_count FROM some_external_system@dblink;
        -- IF v_count = 0 THEN v_condition_ok := FALSE; END IF;
        RETURN v_condition_ok;
    END f_are_conditions_met;

    -- Prywatne, dedykowane procedury do zasilania każdej z tabel.
    PROCEDURE p_load_customers IS
        v_rows_inserted NUMBER;
    BEGIN
        INSERT INTO target_customers (customer_id, full_name)
        SELECT src.id, src.name || ' ' || src.surname FROM source_customers src;

        v_rows_inserted := SQL%ROWCOUNT;
        p_log('TARGET_CUSTOMERS', 'SUCCESS', v_rows_inserted);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            -- Sugestia: Użycie FORMAT_ERROR_STACK dla pełniejszej informacji o błędzie
            p_log('TARGET_CUSTOMERS', 'FAILURE', p_error_message => DBMS_UTILITY.FORMAT_ERROR_STACK);
            -- Jeśli błąd tej procedury ma zatrzymać cały proces, odkomentuj poniższą linię:
            -- RAISE;
    END p_load_customers;

    PROCEDURE p_load_products IS
        v_rows_inserted NUMBER;
    BEGIN
        INSERT INTO target_products (product_sku, product_name)
        SELECT sku, name FROM source_products;

        v_rows_inserted := SQL%ROWCOUNT;
        p_log('TARGET_PRODUCTS', 'SUCCESS', v_rows_inserted);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_log('TARGET_PRODUCTS', 'FAILURE', p_error_message => DBMS_UTILITY.FORMAT_ERROR_STACK);
            -- RAISE;
    END p_load_products;

    PROCEDURE p_load_orders IS
        v_rows_inserted NUMBER;
    BEGIN
        -- TODO: Uzupełnić logikę INSERT dla zamówień...
        -- PRZYKŁAD:
        -- INSERT INTO target_orders (order_id, customer_id, order_date)
        -- SELECT id, customer_id, creation_date FROM source_orders;
        -- v_rows_inserted := SQL%ROWCOUNT;

        v_rows_inserted := 0; -- Tymczasowo, do czasu implementacji
        p_log('TARGET_ORDERS', 'SUCCESS', v_rows_inserted);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_log('TARGET_ORDERS', 'FAILURE', p_error_message => DBMS_UTILITY.FORMAT_ERROR_STACK);
            -- RAISE;
    END p_load_orders;

    -- Implementacja publicznej procedury
    PROCEDURE run_load(p_target_table IN VARCHAR2 DEFAULT NULL) IS
    BEGIN
        -- 1. Ustawienie ID dla całego uruchomienia.
        g_run_id := TO_NUMBER(TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF6')); -- Dodano FF6 dla większej precyzji

        p_log('N/A', 'INFO', p_error_message => 'Proces zasilania rozpoczęty.');

        -- 2. Sprawdzenie warunków wstępnych
        IF NOT f_are_conditions_met THEN
            p_log('N/A', 'FAILURE', p_error_message => 'Warunki wstępne nie zostały spełnione.');
            RETURN;
        END IF;

        -- 3. Wykonanie zasilania
        IF p_target_table IS NULL THEN
            -- Uruchomienie pełnego zasilania
            p_load_customers;
            p_load_products;
            p_load_orders;
        ELSE
            -- Uruchomienie dla pojedynczej tabeli
            CASE UPPER(p_target_table)
                WHEN 'TARGET_CUSTOMERS' THEN p_load_customers;
                WHEN 'TARGET_PRODUCTS'  THEN p_load_products;
                WHEN 'TARGET_ORDERS'    THEN p_load_orders; -- POPRAWKA: Dodano brakującą opcję
                ELSE
                    p_log(p_target_table, 'FAILURE', p_error_message => 'Nie zdefiniowano procedury zasilającej dla podanej tabeli.');
            END CASE;
        END IF;

        p_log('N/A', 'INFO', p_error_message => 'Proces zasilania zakończony.');

    EXCEPTION
        WHEN OTHERS THEN
            -- Logowanie nieoczekiwanego błędu w głównej procedurze
            p_log('FATAL', 'FAILURE', p_error_message => 'Niespodziewany błąd w głównej procedurze: ' || DBMS_UTILITY.FORMAT_ERROR_STACK);
            RAISE; -- Przekaż wyjątek dalej, aby proces wywołujący (np. scheduler) wiedział o awarii
    END run_load;

END pkg_data_loader;
/