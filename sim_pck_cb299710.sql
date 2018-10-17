CREATE OR REPLACE PACKAGE BODY sim_pck_cb299710 AS
    /*
      Modificado   : Rolphy Quintero - Asesoftware
      Fecha        : Mayo 9 de 2018
      Descripción  : Se crea paquete de CB299710 para el manejo de
                     jobs en las optimizaciones.
    */

    vg_usuario            VARCHAR2(8) := 'OPS$PUMA';
    vg_job_uno            VARCHAR2(30) := 'JOB_CB299710_UNO';
    vg_job_dos            VARCHAR2(30) := 'JOB_CB299710_DOS';
    vg_job_tres           VARCHAR2(30) := 'JOB_CB299710_TRES';
    vg_job_cuatro         VARCHAR2(30) := 'JOB_CB299710_CUATRO';
    vg_job_cinco          VARCHAR2(30) := 'JOB_CB299710_CINCO';
    vg_nombre_cobol       VARCHAR2(30) := 'CBXXXYYY_CB299710.pco';
    vg_muestra_1          NUMBER(1) := 1;
    vg_muestra_2          NUMBER(1) := 2;
    vg_muestra_3          NUMBER(1) := 3;
    vg_muestra_4          NUMBER(1) := 4;
    vg_muestra_5          NUMBER(1) := 5;
    vg_blanco             VARCHAR2(1) := ' ';
    vg_id_session_defecto NUMBER(3) := 777;
    vg_nombre_job_defecto sim_pck_cb299710.vd_job_name%TYPE := 'JOB_CB299710_ERROR';
    --
    --=================================================================================================
    --
    PROCEDURE prc_borra_auditoria_cb299710(p_id_session sim_auditoria_cb299710.id_session%TYPE
                                          ,p_nombre_job sim_auditoria_cb299710.nombre_job%TYPE) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        IF (p_id_session IS NOT NULL AND p_nombre_job IS NOT NULL) THEN
            DELETE /*+APPEND*/
            sim_auditoria_cb299710
             WHERE id_session = p_id_session
               AND nombre_job = p_nombre_job;
            COMMIT;
        END IF;
    END prc_borra_auditoria_cb299710;
    --
    --=================================================================================================
    --
    PROCEDURE prc_borra_auditoria_tiempos(p_id_session sim_auditoria_cb299710_tiempo.id_session%TYPE
                                         ,p_nombre_job sim_auditoria_cb299710_tiempo.nombre_job%TYPE) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        IF (p_id_session IS NOT NULL AND p_nombre_job IS NOT NULL) THEN
            DELETE /*+APPEND*/
            sim_auditoria_cb299710_tiempo
             WHERE id_session = p_id_session
               AND nombre_job = p_nombre_job;
            COMMIT;
        END IF;
    END prc_borra_auditoria_tiempos;
    --
    --=================================================================================================
    --
    PROCEDURE prc_auditoria_cb299710(p_id_session IN OUT NOCOPY sim_auditoria_cb299710.id_session%TYPE
                                    ,p_nombre_job IN OUT NOCOPY sim_auditoria_cb299710.nombre_job%TYPE
                                    ,p_mensaje    IN VARCHAR2
                                    ,p_escribir   IN sim_pck_cb299710.vd_marca%TYPE DEFAULT 'S') IS
        PRAGMA AUTONOMOUS_TRANSACTION;
    BEGIN
        IF (p_escribir = 'S' AND p_id_session IS NOT NULL AND p_nombre_job IS NOT NULL) THEN
            INSERT /*+APPEND*/
            INTO sim_auditoria_cb299710
                (secuencia
                ,id_session
                ,nombre_job
                ,mensaje)
            VALUES
                (seq_auditoria_cb299710.nextval
                ,p_id_session
                ,p_nombre_job
                ,substr(p_mensaje, 1, 2000));
            COMMIT;
        END IF;
    END prc_auditoria_cb299710;
    --
    --=================================================================================================
    --
    PROCEDURE prc_auditoria_tiempos(p_id_session IN OUT NOCOPY sim_auditoria_cb299710_tiempo.id_session%TYPE
                                   ,p_nombre_job IN OUT NOCOPY sim_auditoria_cb299710_tiempo.nombre_job%TYPE
                                   ,p_consulta   IN OUT NOCOPY sim_auditoria_cb299710_tiempo.consulta_y_o_proceso%TYPE
                                   ,p_fecha_ini  IN OUT NOCOPY sim_pck_cb299710.vd_fecha_milisegundos%TYPE
                                   ,p_fecha_fin  IN OUT NOCOPY sim_pck_cb299710.vd_fecha_milisegundos%TYPE
                                   ,p_escribir   IN OUT NOCOPY sim_pck_cb299710.vd_marca%TYPE) IS
        PRAGMA AUTONOMOUS_TRANSACTION;
        vl_cantidad                  sim_auditoria_cb299710_tiempo.cantidad%TYPE;
        vl_tiempo_total_milisegundos sim_auditoria_cb299710_tiempo.tiempo_total_milisegundos%TYPE;
        vl_promedio_ejecucion        sim_auditoria_cb299710_tiempo.promedio_ejecucion%TYPE;
        vl_diferencia                sim_auditoria_cb299710_tiempo.tiempo_total_milisegundos%TYPE;
    BEGIN
        IF (p_consulta IS NOT NULL AND p_fecha_ini IS NOT NULL AND p_fecha_fin IS NOT NULL AND p_id_session IS NOT NULL AND p_escribir = 'S') THEN
            BEGIN
                vl_diferencia := sim_pck_cb299710.fun_fecha_dif(p_fecha_ini, p_fecha_fin);
                SELECT a.cantidad
                      ,a.tiempo_total_milisegundos
                      ,a.promedio_ejecucion
                  INTO vl_cantidad
                      ,vl_tiempo_total_milisegundos
                      ,vl_promedio_ejecucion
                  FROM sim_auditoria_cb299710_tiempo a
                 WHERE a.id_session = p_id_session
                   AND a.nombre_job = p_nombre_job
                   AND a.consulta_y_o_proceso = p_consulta
                   AND rownum <= 1;
                vl_cantidad                  := vl_cantidad + 1;
                vl_tiempo_total_milisegundos := vl_tiempo_total_milisegundos + vl_diferencia;
                vl_promedio_ejecucion        := vl_tiempo_total_milisegundos / vl_cantidad;
                UPDATE /*+APPEND*/ sim_auditoria_cb299710_tiempo a
                   SET a.cantidad                  = vl_cantidad
                      ,a.tiempo_total_milisegundos = vl_tiempo_total_milisegundos
                      ,a.promedio_ejecucion        = vl_promedio_ejecucion
                 WHERE a.id_session = p_id_session
                   AND a.nombre_job = p_nombre_job
                   AND a.consulta_y_o_proceso = p_consulta;
            EXCEPTION
                WHEN no_data_found THEN
                    INSERT /*+APPEND*/
                    INTO sim_auditoria_cb299710_tiempo
                        (id_session
                        ,nombre_job
                        ,consulta_y_o_proceso
                        ,cantidad
                        ,tiempo_total_milisegundos
                        ,promedio_ejecucion)
                    VALUES
                        (p_id_session
                        ,p_nombre_job
                        ,p_consulta
                        ,1
                        ,vl_diferencia
                        ,vl_diferencia);
            END;
            COMMIT;
        END IF;
    END prc_auditoria_tiempos;
    --
    --=================================================================================================
    --
    FUNCTION fun_fecha_dif(p_fecha_ini sim_pck_cb299710.vd_fecha_milisegundos%TYPE
                          ,p_fecha_fin sim_pck_cb299710.vd_fecha_milisegundos%TYPE) RETURN sim_pck_cb299710.vd_fecha_milisegundos%TYPE IS
        vl_salida           sim_pck_cb299710.vd_fecha_milisegundos%TYPE := 0;
        vl_dif_segundos_gen sim_pck_cb299710.vd_fecha_milisegundos%TYPE := 0;
        vl_dif_milisegundos sim_pck_cb299710.vd_fecha_milisegundos%TYPE := 0;
    BEGIN
        IF (p_fecha_ini IS NOT NULL AND p_fecha_fin IS NOT NULL) THEN
            IF substr(p_fecha_fin, 1, 8) = substr(p_fecha_ini, 1, 8) THEN
                -- Mismo dia
                IF substr(p_fecha_fin, 9, 4) != substr(p_fecha_ini, 9, 4) THEN
                    -- Diferente hora y minutos
                    vl_dif_segundos_gen := (to_date(substr(p_fecha_fin, 1, 14), 'YYYYMMDDHH24MISS') - to_date(substr(p_fecha_ini, 1, 14), 'YYYYMMDDHH24MISS')) * 24 * 60 * 60;
                    vl_dif_segundos_gen := vl_dif_segundos_gen * 1000000;
                    vl_dif_milisegundos := CASE
                                               WHEN substr(p_fecha_ini, 15) = substr(p_fecha_fin, 15) THEN
                                                0
                                               WHEN substr(p_fecha_fin, 15) > substr(p_fecha_ini, 15) THEN
                                                substr(p_fecha_fin, 15) - substr(p_fecha_ini, 15)
                                               WHEN substr(p_fecha_ini, 15) > substr(p_fecha_fin, 15) THEN
                                                1000000 - substr(p_fecha_ini, 15) + substr(p_fecha_fin, 15)
                                               ELSE
                                                0
                                           END;
                    vl_salida           := vl_dif_segundos_gen + vl_dif_milisegundos;
                ELSE
                    -- Misma hora diferencias en milisegundos
                    vl_salida := p_fecha_fin - p_fecha_ini;
                END IF;
            ELSE
                vl_salida := 0;
            END IF;
        END IF;
        RETURN vl_salida;
    END fun_fecha_dif;
    --
    --=================================================================================================
    --
    FUNCTION fun_fecha_formato(p_fecha DATE) RETURN sim_pck_cb299710.vd_fecha_formato%TYPE IS
    BEGIN
        RETURN CASE WHEN p_fecha IS NOT NULL THEN to_char(p_fecha, 'DD-MON-YYYY') ELSE NULL END;
    END fun_fecha_formato;
    --
    --=================================================================================================
    --
    PROCEDURE prc_ejecutar_estadisticas(p_usuario all_tables.owner%TYPE
                                       ,p_tabla   all_tables.table_name%TYPE) IS
        vl_sql     VARCHAR2(500);
        vl_slq_err VARCHAR2(32767);
    BEGIN
        vl_sql := 'BEGIN DBMS_STATS.GATHER_TABLE_STATS(ownname => ''' || p_usuario || ''', tabname => ''' || p_tabla || '''); END;';
        EXECUTE IMMEDIATE vl_sql;
    EXCEPTION
        WHEN OTHERS THEN
            vl_slq_err := substr(SQLERRM, 32767);
            raise_application_error(-20001, 'Error ejecutando estadísticas del usuario: ' || p_usuario || ', tabla: ' || p_tabla || ', error: ' || vl_slq_err);
    END prc_ejecutar_estadisticas;
    --
    --=================================================================================================
    --

    /*
      Modificacion: Luis Carlos Castaneda Oviedo
      Fecha       : Octubre 16 de 2018
      Descripcion : Procedimientos para la optimizacion de los perform
                    y consultas de del cobol CB299710
    */

    /*******************************************************************************************************************************/
    -- Procedimiento que obtiene la muestra de polizas que se van a procesar para gargarla en la tabla 'SIM_MAESTRO_EXPUESTOS_MUESTRA'
    --
    -- %param ip_cod_cia                 IN NUMBER                            codigo de la compania a procesar
    -- %param ip_arr_cod_secc            IN VARCHAR2                          varchar separado por comas con las secciones a procesar (opcional)
    -- %param ip_fec_ini                 IN DATE                              fecha de inicio de vigencia de las polizas que se van a procesar
    -- %param ip_fec_fin                 IN DATE                              fecha de fin de vigencia de las polizas que se van a procesar
    -- %param ip_opcion                  IN NUMBER                            opcion a que ingresa el usuario: 1 o 2
    -- %param op_resultado               OUT  NUMBER                          Resultado (0: OK, -1: Error, 1: Warning).
    -- %param op_arrerrores              OUT  sim_typ_array_error             Array de Errores.
    -- %version 1.0
    --
    -- Control de cambios
    -- DATE            AUTHOR - email                                 DESCRIPTION
    -- ----------      -----------------------------------------      ------------------------------------
    -- 10/10/2018      Luis Carlos Castaneda Oviedo - lcastaneda@asesoftware.com          1. creacion del procedimiento
    PROCEDURE prc_procesar_job(ip_cod_cia      IN NUMBER
                              ,ip_arr_cod_secc IN VARCHAR2
                              ,ip_fec_ini      IN DATE
                              ,ip_fec_fin      IN DATE
                              ,ip_opcion       IN NUMBER
                              ,op_resultado    OUT NUMBER
                              ,op_arrerrores   OUT sim_typ_array_error) IS
    
        CURSOR c_polizas_endoso_exp(p_cod_secc sim_maestro_expuestos_muestra.cod_cia%TYPE
                                   ,p_fec_ini  DATE
                                   ,p_fec_fin  DATE) IS
            SELECT a.fecha_venc_pol
                  ,a.cod_secc AS cod_ram_emi
                  ,a.num_secu_pol AS num_secu_pol
                  ,a.num_pol1 AS num_pol
                  ,a.cod_cia
                  ,a.cod_ramo AS cod_prod
                  ,decode(nvl((SELECT b.num_secu_pol
                                FROM referidos b
                               WHERE b.num_secu_pol = a.num_secu_pol)
                             ,1)
                         ,1
                         ,'N'
                         ,'S') AS mca_ref
                  ,a.nro_documto AS num_doc_tom
                  ,a.tdoc_tercero AS tip_doc_tom
                  ,a.num_end
                  ,nvl(a.tipo_end, '  ') AS tip_end
                  ,a.cod_end
                  ,a.sub_cod_end
                  ,a.fecha_vig_end AS fec_ini_end
                  ,nvl(a.fecha_venc_end, add_months(a.fecha_vig_end, 12)) AS fec_fin_end
                  ,nvl(a.mca_anu_pol, 'N') AS mca_anu_pol
              FROM a2000030 a
             WHERE CASE
                       WHEN a.fecha_venc_pol >= p_fec_ini THEN
                        1
                       WHEN a.fecha_venc_pol IS NULL THEN
                        1
                       ELSE
                        0
                   END = 1
               AND a.cod_secc = p_cod_secc
               AND a.cod_cia = ip_cod_cia
               AND a.num_pol1 IS NOT NULL
               AND CASE
                       WHEN a.mca_provisorio IS NULL THEN
                        'N'
                       ELSE
                        a.mca_provisorio
                   END = 'N'
               AND a.fecha_vig_pol <= p_fec_fin
               AND a.fecha_equipo <= p_fec_fin
               AND a.fecha_vig_end <= CASE
                       WHEN a.fecha_venc_end IS NULL THEN
                        add_months(a.fecha_vig_end, 12)
                       ELSE
                        a.fecha_venc_end
                   END;
    
        CURSOR c_riesgo(p_numsecupol  NUMBER
                       ,p_numend      NUMBER
                       ,p_mca_anu_pol VARCHAR2) IS
            SELECT DISTINCT a.cod_ries
                           ,CASE
                                WHEN (decode(p_mca_anu_pol, 'S', 1, 0) + decode(a.mca_baja_ries, 'S', 1, 0)) > 0 THEN
                                 'S'
                                ELSE
                                 'N'
                            END AS mca_anu
              FROM a2000040 a
             WHERE a.num_secu_pol = p_numsecupol
               AND a.num_end = p_numend
               AND a.tipo_reg = 'T';
    
        TYPE arr_secciones IS TABLE OF NUMBER INDEX BY PLS_INTEGER;
    
        l_con_caracter CONSTANT VARCHAR2(1) := ',';
    
        d_secciones  arr_secciones;
        v_cadena_aux VARCHAR2(200);
        v_pos1       NUMBER(3);
        v_contador   NUMBER(3);
        v_error      sim_typ_error;
    
    BEGIN
        EXECUTE IMMEDIATE 'ALTER SESSION SET NLS_DATE_FORMAT=''DD/MM/YYYY HH24:MI:SS''';
    
        -- se inicializa el contador para llenar el array de secciones
        v_contador := 1;
    
        BEGIN
        
            -- si el varchar 'ip_arr_cod_secc' no es nulo se separa por comas, se crea un array de 'arr_secciones' y se agrega cada seccion a este array
            IF ip_arr_cod_secc IS NOT NULL THEN
                BEGIN
                    v_cadena_aux := ip_arr_cod_secc;
                    LOOP
                        v_pos1 := instr(v_cadena_aux, l_con_caracter);
                    
                        IF v_pos1 = 0 THEN
                            d_secciones(v_contador) := to_char(v_cadena_aux);
                            EXIT;
                        ELSE
                            d_secciones(v_contador) := to_char(substr(v_cadena_aux, 1, v_pos1 - 1));
                            v_cadena_aux := substr(v_cadena_aux, v_pos1 + 1);
                            v_contador := v_contador + 1;
                        END IF;
                    END LOOP;
                END;
            ELSE
            
                -- se el varchar 'ip_arr_cod_secc' esta nulo selecciona las secciones a procesar con el siguiente query
                FOR c_secc IN (SELECT cod_secc
                                     ,a.nom_secc
                                 FROM a1000200 a
                                WHERE cod_cia = ip_cod_cia
                                  AND cod_secc NOT IN (45, 335, 13, 38, 69, 801, 805, 806, 807, 808, 809, 810, 811, 812, 888, 901, 912, 914, 915, 923, 966, 997, 998, 999)
                                ORDER BY 1)
                LOOP
                    d_secciones(v_contador) := c_secc.cod_secc;
                    v_contador := v_contador + 1;
                END LOOP;
            END IF;
        
            -- trunca las tablas de muestra
            prc_truncar_tablas_muestra;
        
            -- se recorre el array de secciones
            v_contador := 1;
            FOR i IN d_secciones.first() .. d_secciones.last()
            LOOP
            
                -- con cada seccion se usa el cursor para obtener la informacion que se va a grabar en la tabla 'SIM_MAESTRO_EXPUESTOS_MUESTRA'
                FOR reg IN c_polizas_endoso_exp(d_secciones(i), ip_fec_ini, ip_fec_fin)
                LOOP
                    --dbms_output.put_line(1);
                
                    FOR reg2 IN c_riesgo(reg.num_secu_pol, reg.num_end, reg.mca_anu_pol)
                    LOOP
                        --dbms_output.put_line(2);
                        --                        dbms_output.put_line(reg.num_secu_pol || '+' || lpad(reg2.cod_ries, 5, '0') || '+' || lpad(reg.cod_prod, 4, '0'));
                    
                    END LOOP;
                
                END LOOP;
            
            END LOOP;
        
        EXCEPTION
            WHEN OTHERS THEN
                op_resultado := -1;
            
                op_arrerrores := NEW sim_typ_array_error();
                op_arrerrores.extend();
            
                v_error           := NEW sim_typ_error();
                v_error.msg_error := SQLERRM;
            
                op_arrerrores(op_arrerrores.count()) := v_error;
        END;
    
    END prc_procesar_job;

    /*******************************************************************************************************************************/
    -- Procedimiento que trunca las tablas 'SIM_MAESTRO_EXPUESTOS_MUESTRA', 'SIM_MAESTRO_EXPUESTOS_TMP1', 'SIM_MAESTRO_EXPUESTOS_TMP2',
    --'SIM_MAESTRO_EXPUESTOS_TMP3', 'SIM_MAESTRO_EXPUESTOS_TMP4' y 'SIM_MAESTRO_EXPUESTOS_TMP5'
    --
    -- %version 1.0
    --
    -- Control de cambios
    -- DATE            AUTHOR - email                                 DESCRIPTION
    -- ----------      -----------------------------------------      ------------------------------------
    -- 10/10/2018      Luis Carlos Castaneda Oviedo - lcastaneda@asesoftware.com          1. creacion del procedimiento
    PROCEDURE prc_truncar_tablas_muestra IS
    BEGIN
    
        EXECUTE IMMEDIATE 'TRUNCATE TABLE SIM_MAESTRO_EXPUESTOS_MUESTRA';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE SIM_MAESTRO_EXPUESTOS_TMP1';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE SIM_MAESTRO_EXPUESTOS_TMP2';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE SIM_MAESTRO_EXPUESTOS_TMP3';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE SIM_MAESTRO_EXPUESTOS_TMP4';
        EXECUTE IMMEDIATE 'TRUNCATE TABLE SIM_MAESTRO_EXPUESTOS_TMP5';
    
    END prc_truncar_tablas_muestra;

END sim_pck_cb299710;
