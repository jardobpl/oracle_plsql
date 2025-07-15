CREATE OR REPLACE PACKAGE pkg_data_loader AS

    /**
     * Uruchamia pełny proces zasilania wszystkich zdefiniowanych tabel.
     * Wykonuje sprawdzanie warunków wstępnych, a następnie po kolei
     * uruchamia procedury zasilające.
     */
    PROCEDURE run_load;

END pkg_data_loader;
/

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
        INSERT INTO data_load_log (run_id, target_table, status, rows_affected, error_message)
        VALUES (g_run_id, p_target_table, p_status, p_rows_affected, p_error_message);
        COMMIT;
    END p_log;

    -- Prywatna funkcja sprawdzająca warunki wstępne.
    FUNCTION f_are_conditions_met RETURN BOOLEAN IS
        v_condition_ok BOOLEAN := TRUE;
        -- ... tutaj logika sprawdzająca, np. dostępność systemów źródłowych
        RETURN v_condition_ok;
    END f_are_conditions_met;

    -- Prywatne, dedykowane procedury do zasilania każdej z tabel.
    PROCEDURE p_load_customers IS
    BEGIN
        -- Logika INSERT dla klientów...
        p_log('TARGET_CUSTOMERS', 'SUCCESS', SQL%ROWCOUNT);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_log('TARGET_CUSTOMERS', 'FAILURE', p_error_message => SQLERRM);
    END p_load_customers;

    PROCEDURE p_load_customers IS
        v_rows_inserted NUMBER;
    BEGIN
        INSERT INTO target_customers (customer_id, full_name)
        SELECT src.id, src.name || ' ' || src.surname FROM source_customers src;
        
        v_rows_inserted := SQL%ROWCOUNT;
        -- Uproszczone wywołanie p_log
        p_log('TARGET_CUSTOMERS', 'SUCCESS', v_rows_inserted);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_log('TARGET_CUSTOMERS', 'FAILURE', p_error_message => SQLERRM);
    END p_load_customers;

    PROCEDURE p_load_products IS
        v_rows_inserted NUMBER;
    BEGIN
        INSERT INTO target_products (product_sku, product_name)
        SELECT sku, name FROM source_products;

        v_rows_inserted := SQL%ROWCOUNT;
        -- Uproszczone wywołanie p_log
        p_log('TARGET_PRODUCTS', 'SUCCESS', v_rows_inserted);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_log('TARGET_PRODUCTS', 'FAILURE', p_error_message => SQLERRM);
    END p_load_products;

    PROCEDURE p_load_orders IS
    BEGIN
        -- Logika INSERT dla zamówień...
        p_log('TARGET_ORDERS', 'SUCCESS', SQL%ROWCOUNT);
        COMMIT;
    EXCEPTION
        WHEN OTHERS THEN
            ROLLBACK;
            p_log('TARGET_ORDERS', 'FAILURE', p_error_message => SQLERRM);
    END p_load_orders;

    PROCEDURE run_load(p_target_table IN VARCHAR2 DEFAULT NULL) IS
    BEGIN
        -- 1. Ustawienie ID dla całego uruchomienia. To jedyne miejsce, gdzie jest generowane.
        g_run_id := TO_NUMBER(TO_CHAR(SYSTIMESTAMP, 'YYYYMMDDHH24MISSFF'));
        
        p_log('N/A', 'INFO', p_error_message => 'Proces zasilania rozpoczęty.');

        -- 2. Sprawdzenie warunków wstępnych
        IF NOT f_are_conditions_met THEN
            p_log('N/A', 'FAILURE', p_error_message => 'Warunki wstępne nie zostały spełnione.');
            RETURN;
        END IF;

        -- 3. Wykonanie zasilania
        IF p_target_table IS NULL THEN
            p_load_customers;
            p_load_products;
            p_load_orders;
        ELSE
            CASE UPPER(p_target_table)
                WHEN 'TARGET_CUSTOMERS' THEN p_load_customers
                WHEN 'TARGET_PRODUCTS'  THEN p_load_products
                ELSE
                    p_log(p_target_table, 'FAILURE', p_error_message => 'Nie zdefiniowano procedury zasilającej.');
            END CASE;
        END IF;
        
        p_log('N/A', 'INFO', p_error_message => 'Proces zasilania zakończony.');

    EXCEPTION
        WHEN OTHERS THEN
            p_log('FATAL', 'FAILURE', p_error_message => 'Niespodziewany błąd w głównej procedurze: ' || SQLERRM);
            RAISE;
    END run_load;

END pkg_data_loader;
/