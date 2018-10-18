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
    PROCEDURE prc_carga_muestra(ip_cod_cia      IN NUMBER
                               ,ip_arr_cod_secc IN VARCHAR2
                               ,ip_fec_ini      IN DATE
                               ,ip_fec_fin      IN DATE
                               ,ip_opcion       IN NUMBER
                               ,op_resultado    OUT NUMBER
                               ,op_arrerrores   OUT sim_typ_array_error) IS
    
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
    
        v_registro sim_maestro_expuestos_muestra%ROWTYPE;
    
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
            FOR i IN d_secciones.first() .. d_secciones.last()
            LOOP
            
                -- inserta en la tabla 'sim_maestro_expuestos_muestra' los datos para la opcion 1
                IF ip_opcion = 1 THEN
                    INSERT INTO sim_maestro_expuestos_muestra
                        SELECT a.num_pol1 || '+' || (SELECT DISTINCT lpad(b.cod_ries, 5, '0')
                                                       FROM a2000040 b
                                                      WHERE b.num_secu_pol = a.num_secu_pol
                                                        AND b.num_end = a.num_end
                                                        AND b.tipo_reg = 'T') || '+'
                               
                               || lpad(a.cod_ramo, 4, '0')
                              ,NULL -- anio
                              ,NULL -- mes
                              ,a.num_secu_pol
                              ,a.num_end
                              ,NULL -- fec_ini
                              ,NULL -- fec_fin
                              ,NULL -- sem
                              ,NULL -- tri
                              ,a.cod_cia
                              ,a.cod_secc
                              ,a.cod_ramo
                              ,NULL -- cod_sub_prod
                              ,a.num_pol1
                              ,decode(nvl((SELECT b.num_secu_pol
                                            FROM referidos b
                                           WHERE b.num_secu_pol = a.num_secu_pol)
                                         ,1)
                                     ,1
                                     ,'N'
                                     ,'S')
                              ,a.nro_documto
                              ,a.tdoc_tercero
                              ,NULL -- num_doc_ase
                              ,NULL -- tip_doc_ase
                              ,NULL -- cod_ries
                              ,NULL -- expo
                              ,CASE
                                   WHEN a.mca_anu_pol IS NULL THEN
                                    'N'
                                   ELSE
                                    a.mca_anu_pol
                               END
                              ,'MAE_EXP_' || lpad(d_secciones(i), 3, '0') || '_' || lpad(ip_opcion, 2, 0) || '_' || to_char(ip_fec_ini, 'YYYYMMDD') || '_' ||
                               to_char(ip_fec_fin, 'YYYYMMDD') || '_' || to_char(SYSDATE, 'YYYYMMDD') || '_' || to_char(SYSDATE, 'HH:MI')
                              ,SYSDATE
                              ,'INSERT MUESTRA'
                              ,NULL -- numero_muestra
                          FROM a2000030 a
                         WHERE CASE
                                   WHEN a.fecha_venc_pol >= ip_fec_ini THEN
                                    1
                                   WHEN a.fecha_venc_pol IS NULL THEN
                                    1
                                   ELSE
                                    0
                               END = 1
                           AND a.cod_secc = d_secciones(i)
                           AND a.cod_cia = ip_cod_cia
                           AND a.num_pol1 IS NOT NULL
                           AND CASE
                                   WHEN a.mca_provisorio IS NULL THEN
                                    'N'
                                   ELSE
                                    a.mca_provisorio
                               END = 'N'
                           AND a.fecha_vig_pol <= ip_fec_fin
                           AND a.fecha_equipo <= ip_fec_fin
                           AND a.fecha_vig_end <= CASE
                                   WHEN a.fecha_venc_end IS NULL THEN
                                    add_months(a.fecha_vig_end, 12)
                                   ELSE
                                    a.fecha_venc_end
                               END;
                END IF;
            
                -- inserta en la tabla 'sim_maestro_expuestos_muestra' los datos para la opcion 2
                IF ip_opcion = 2 THEN
                    dbms_output.put_line('');
                END IF;
            
                COMMIT;
            
            /*-- con cada seccion se usa el cursor para obtener la informacion que se va a grabar en la tabla 'SIM_MAESTRO_EXPUESTOS_MUESTRA'
                                                                                                                                                                                                                                                                                        FOR reg IN c_polizas_endoso_exp(d_secciones(i), ip_fec_ini, ip_fec_fin)
                                                                                                                                                                                                                                                                                        LOOP
                                                                                                                                                                                                                                                                                        
                                                                                                                                                                                                                                                                                            FOR reg2 IN c_riesgo(reg.num_secu_pol, reg.num_end, reg.mca_anu_pol)
                                                                                                                                                                                                                                                                                            LOOP
                                                                                                                                                                                                                                                                                                --dbms_output.put_line(2);
                                                                                                                                                                                                                                                                                                --dbms_output.put_line(reg.num_secu_pol || '+' || lpad(reg2.cod_ries, 5, '0') || '+' || lpad(reg.cod_prod, 4, '0'));
                                                                                                                                                                                                                                                                                                v_registro.llave := reg.num_secu_pol || '+' || lpad(reg2.cod_ries, 5, '0') || '+' || lpad(reg.cod_prod, 4, '0');
                                                                                                                                                                                                                                                                                                --v_registro.ano          := 1;
                                                                                                                                                                                                                                                                                                --v_registro.mes          := 1;
                                                                                                                                                                                                                                                                                                v_registro.num_secu_pol := reg.num_secu_pol;
                                                                                                                                                                                                                                                                                                v_registro.num_end      := reg.num_end;
                                                                                                                                                                                                                                                                                                --v_registro.fec_ini := ;
                                                                                                                                                                                                                                                                                                --v_registro.fec_fin :=;
                                                                                                                                                                                                                                                                                                --v_registro.sem :=;
                                                                                                                                                                                                                                                                                                --v_registro.tri :=;
                                                                                                                                                                                                                                                                                                v_registro.cod_cia     := ip_cod_cia;
                                                                                                                                                                                                                                                                                                v_registro.cod_ram_emi := reg.cod_ram_emi;
                                                                                                                                                                                                                                                                                                v_registro.cod_prod    := reg.cod_prod;
                                                                                                                                                                                                                                                                                                --v_registro.cod_sub_prod :=;
                                                                                                                                                                                                                                                                                                v_registro.num_pol     := reg.num_pol;
                                                                                                                                                                                                                                                                                                v_registro.mca_ref     := reg.mca_ref;
                                                                                                                                                                                                                                                                                                v_registro.num_doc_tom := reg.num_doc_tom;
                                                                                                                                                                                                                                                                                                v_registro.tip_doc_tom := reg.tip_doc_tom;
                                                                                                                                                                                                                                                                                                v_registro.cod_rie     := reg2.cod_ries;
                                                                                                                                                                                                                                                                                                --v_registro.expo :=;
                                                                                                                                                                                                                                                                                                v_registro.mca_anu := reg2.mca_anu;
                                                                                                                                                                                                                                                                                                --v_registro.nom_pro :=;
                                                                                                                                                                                                                                                                                                --v_registro.fec_pro :=;
                                                                                                                                                                                                                                                                                                v_registro.log            := '';
                                                                                                                                                                                                                                                                                                v_registro.numero_muestra := 0;
                                                                                                                                                                                                                                                                                            
                                                                                                                                                                                                                                                                                                prc_escribe_tabla_muestra(v_registro, op_resultado, op_arrerrores);
                                                                                                                                                                                                                                                                                                COMMIT;
                                                                                                                                                                                                                                                                                            
                                                                                                                                                                                                                                                                                            END LOOP;
                                                                                                                                                                                                                                                                                        
                                                                                                                                                                                                                                                                                        END LOOP;*/
            
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
    
    END prc_carga_muestra;

    PROCEDURE prc_procesar(p_nom_proc     VARCHAR2
                          ,p_cod_cia      NUMBER
                          ,p_cod_ram_emi  NUMBER
                          ,p_cod_prod     NUMBER
                          ,p_num_secu_pol NUMBER
                          ,p_num_pol      NUMBER
                          ,p_num_end      NUMBER
                          ,p_tip_end      VARCHAR2
                          ,p_cod_end      NUMBER
                          ,p_sub_cod_end  NUMBER
                          ,p_fec_ini_end  DATE
                          ,p_fec_fin_end  DATE
                          ,p_mca_anu_pol  VARCHAR2
                          ,p_mca_ref      VARCHAR2
                          ,p_num_doc_tom  NUMBER
                          ,p_tip_doc_tom  VARCHAR2) IS
    
        idrow_mae VARCHAR2(20);
    
        flag        NUMBER;
        ind         NUMBER;
        p_llave     VARCHAR2(24);
        p_llave_aux VARCHAR2(24);
    
        mes_con NUMBER;
    
        fec_ini_mes DATE;
        fec_fin_mes DATE;
    
        fec_end_ini DATE;
        fec_end_fin DATE;
    
        fec_min DATE;
        fec_max DATE;
    
        fec_max_llave     DATE;
        fec_min_llave     DATE;
        fec_max_llave_end DATE;
        fec_min_llave_end DATE;
        fec_max_llave_mes DATE;
        fec_min_llave_rie DATE;
    
        p_ano         NUMBER;
        p_ano_fin_end NUMBER;
        p_ano_end     NUMBER;
        con_end       NUMBER;
    
        sem           NUMBER;
        tri           NUMBER;
        p_mes         NUMBER;
        p_mes_end     NUMBER;
        p_mes_fin_end NUMBER;
    
        cod_sub_prod NUMBER;
    
        num_doc_ase NUMBER;
        tip_doc_ase VARCHAR2(5);
    
        CURSOR cur_rie(p_numsecupol  NUMBER
                      ,p_numend      NUMBER
                      ,p_mca_anu_pol VARCHAR2) IS
            SELECT DISTINCT a.cod_ries AS cod_rie
                           ,CASE
                                WHEN (decode(p_mca_anu_pol, 'S', 1, 0) + decode(a.mca_baja_ries, 'S', 1, 0)) > 0 THEN
                                 'S'
                                ELSE
                                 'N'
                            END AS mca_anu
              FROM a2000040 a
             WHERE a.num_secu_pol = p_numsecupol
               AND a.num_end = p_numend
               AND a.tipo_reg = 'T'
            --  AND A.COD_RIES     = 1
             ORDER BY cod_ries;
    
    BEGIN
        BEGIN
            SELECT /*+ index (B I_A2000020) */
             to_number(b.valor_campo)
              INTO cod_sub_prod
              FROM a2000020 b
             WHERE b.num_secu_pol = p_num_secu_pol
               AND b.cod_ries IS NULL
               AND b.valor_campo IS NOT NULL
               AND b.cod_campo = 'PRODUCTOS'
               AND b.mca_vigente = 'S';
        
        EXCEPTION
            WHEN OTHERS THEN
                cod_sub_prod := 9999;
        END;
    
        p_llave_aux := 'INICIO';
    
        FOR r IN cur_rie(p_num_secu_pol, p_num_end, p_mca_anu_pol)
        LOOP
        
            p_llave := p_num_pol || '+' || lpad(r.cod_rie, 5, '0') || '+' || lpad(p_cod_prod, 4, '0');
        
            IF (p_llave_aux != p_llave) THEN
                fec_min_llave_rie := p_fec_ini_end + 1;
                p_llave_aux       := p_llave;
            END IF;
        
            BEGIN
            
                SELECT MIN(CASE
                               WHEN y.cod_campo IN ('COD_ASEG', 'COD_ASEG1', 'COD_BENE', 'COD_AFIANZADO') THEN
                                y.valor_campo
                           END) AS cdoc
                      ,MIN(CASE
                               WHEN y.cod_campo IN ('TIPO_DOC_ASEG', 'TIPO_DOC_ASE2', 'COD_DOCUM', 'TIPO_DOC_AFI') THEN
                                y.valor_campo
                           END) AS tdoc
                  INTO num_doc_ase
                      ,tip_doc_ase
                  FROM a2000020 y
                 WHERE y.num_secu_pol = p_num_secu_pol
                   AND y.num_end =
                       (SELECT MAX(z.num_end)
                          FROM a2000020 z
                         WHERE z.num_secu_pol = p_num_secu_pol
                           AND z.num_end <= p_num_end
                           AND z.cod_ries = r.cod_rie
                           AND z.cod_campo IN ('COD_ASEG', 'COD_ASEG1', 'TIPO_DOC_ASEG', 'TIPO_DOC_ASE2', 'COD_BENE', 'COD_DOCUM', 'COD_AFIANZADO', 'TIPO_DOC_AFI'))
                   AND y.cod_ries = r.cod_rie
                   AND y.cod_campo IN ('COD_ASEG', 'COD_ASEG1', 'TIPO_DOC_ASEG', 'TIPO_DOC_ASE2', 'COD_BENE', 'COD_DOCUM', 'COD_AFIANZADO', 'TIPO_DOC_AFI');
            
            EXCEPTION
                WHEN OTHERS THEN
                
                    num_doc_ase := '99999999';
                    tip_doc_ase := 'XX';
                
            END;
        
            BEGIN
            
                p_ano_fin_end := extract(YEAR FROM p_fec_fin_end);
                p_mes_fin_end := extract(MONTH FROM p_fec_fin_end);
                fec_end_fin   := to_char(p_fec_fin_end, 'DD/MM/YYYY');
                p_ano_end     := extract(YEAR FROM(p_fec_ini_end));
                p_mes_end     := extract(MONTH FROM(p_fec_ini_end));
                flag          := 5; -- Se reinicia la varable FLAG
                ind           := 0;
            
                IF (fec_max > p_fec_fin_end) THEN
                    fec_max := p_fec_fin_end;
                END IF;
            
                SELECT MAX(fec_fin) AS fec_max_llave
                      ,MIN(fec_ini) AS fec_min_llave
                  INTO fec_max_llave
                      ,fec_min_llave
                  FROM sim_maestro_expuestos
                 WHERE llave = p_llave;
            
                --- Si el endoso inicial es menor a la fecha minima de inicio y fecha fin de endoso es igual a la fecha maxima fin, recostruyo el registro 
                IF ((p_fec_ini_end + 1) < fec_min_llave_rie) AND (p_fec_fin_end = fec_max_llave) THEN
                
                    DELETE FROM sim_maestro_expuestos
                     WHERE llave = p_llave;
                    fec_min_llave := NULL;
                    --COMMIT; --+ 1;  
                END IF;
            
                --- Si la fecha mas fin de la LLAVE es menor a la fecha fin del endoso, borro solo el registro del mes para reconstruirlo
                -- 1070561199701 Riesgo 51
                IF (fec_max_llave < fec_end_fin) AND (p_tip_end != 'MV') AND (fec_end_ini != fec_max_llave) THEN
                
                    DELETE FROM sim_maestro_expuestos
                     WHERE llave = p_llave
                       AND ano = extract(YEAR FROM(fec_end_fin))
                       AND mes = extract(MONTH FROM(fec_end_fin));
                    --COMMIT;
                END IF;
            
                IF fec_min_llave IS NOT NULL THEN
                    -- Si la variable FEC_MIN_LLAVE es nula quiere decir que no existen registros de esa llave
                    --- Si la mecha minia es igual a la fecha d einicio del endoso + 1 dia, entoces se obtiens
                    --- el año y el mes del la la fecha de inicio de vigencia + 1 dia
                
                    SELECT MAX(fec_ini) AS fec_max_llave
                          ,MIN(fec_ini) AS fec_min_llave
                          ,MAX(fec_fin) AS fec_max_llave_mes
                          ,COUNT(llave) AS con_end
                      INTO fec_max_llave_end
                          ,fec_min_llave_end
                          ,fec_max_llave_mes
                          ,con_end
                      FROM sim_maestro_expuestos
                     WHERE llave = p_llave
                       AND ano = p_ano_end
                       AND mes = p_mes_end;
                
                    IF p_cod_ram_emi != 310 AND (fec_min_llave = (p_fec_ini_end + 1)) THEN
                        p_ano       := extract(YEAR FROM(p_fec_ini_end + 1));
                        p_mes       := extract(MONTH FROM(p_fec_ini_end + 1));
                        fec_end_ini := p_fec_ini_end + 1;
                        fec_min     := fec_end_ini;
                        fec_max     := last_day(add_months(fec_end_ini, 0));
                    
                    ELSE
                        --- En algunos casos, la fecha inicio es el ultimo dia del mes. Para esos casos, la fecha maxima (FEC_MAX) y  fecha minima (FEC_MIN)
                        --- es igual a la fecha de inicio del endoso + 1 dia
                        IF (last_day(add_months(p_fec_ini_end, 0)) = p_fec_ini_end) AND (p_fec_ini_end != p_fec_fin_end) THEN
                            fec_min := to_char((last_day(add_months(p_fec_ini_end + 1, -1)) + 1), 'DD/MM/YYYY');
                            fec_max := to_char(last_day(add_months(p_fec_ini_end + 1, 0)), 'DD/MM/YYYY');
                        
                            IF fec_min_llave = p_fec_ini_end THEN
                                fec_end_ini := p_fec_ini_end;
                            ELSE
                                fec_end_ini := (p_fec_ini_end + 1);
                            END IF;
                        
                            p_ano := extract(YEAR FROM fec_end_ini);
                            p_mes := extract(MONTH FROM fec_end_ini);
                        
                        ELSE
                            fec_min := p_fec_ini_end;
                            fec_max := last_day(add_months(p_fec_ini_end, 0));
                        
                            fec_end_ini := p_fec_ini_end;
                            p_ano       := extract(YEAR FROM fec_end_ini);
                            p_mes       := extract(MONTH FROM fec_end_ini);
                        
                        END IF;
                    END IF;
                
                    -- INICIO : Endosos que acortan o alargan la vigencia
                
                    --IF (((P_FEC_FIN_END < FEC_MAX_LLAVE) OR  (P_FEC_FIN_END > FEC_MAX_LLAVE)) AND (P_TIP_END = 'MV')) THEN
                    IF (p_tip_end = 'MV' AND fec_end_ini != fec_max_llave) THEN
                    
                        -- FEC_INI_MES     := TO_CHAR((LAST_DAY(ADD_MONTHS(FEC_MAX_LLAVE,-1))+1),'DD/MM/YYYY');
                    
                        DELETE FROM sim_maestro_expuestos
                         WHERE llave = p_llave
                           AND fec_ini >= p_fec_ini_end; -- 1000488841601+00077+0250 Endoso 11  
                        --AND FEC_INI >= P_FEC_INI_END ;
                        --FLAG := 4;   
                        --COMMIT; --+ 1;           
                        -- Se vuelve a sacar la fecha maxima y minima dado que se elimino por movimiento de vigencia
                        SELECT MAX(fec_fin) AS fec_max_llave
                              ,MIN(fec_ini) AS fec_min_llave
                          INTO fec_max_llave
                              ,fec_min_llave
                          FROM sim_maestro_expuestos
                         WHERE llave = p_llave;
                    
                    END IF;
                
                    -- FIN : Endosos que acortan la vigencia
                
                    -- Si la fecha 'FEC_MAX_LLAVE' es ingual al inicio del endoso entonces se deben insertar los nuevos registros
                    IF (fec_max_llave = fec_end_ini) AND (p_tip_end = 'MV') THEN
                        flag := 4;
                        IF fec_min < p_fec_ini_end THEN
                            fec_min := p_fec_ini_end;
                        END IF;
                    END IF;
                ELSE
                    flag := 4;
                END IF;
            
                mes_con := 0;
                IF (flag = 4) THEN
                    GOTO primer_end; --- Insertar los registros correspondiente al endoso(0) inicial                         
                END IF;
            
                ---  Cuando hay mas de tres registros en el mismo mes debo buscar el que este mas cerca al de inicio del endoso
                IF (con_end > 1) THEN
                    IF ((fec_max_llave_end < fec_end_ini) AND (fec_min != fec_max_llave_end) AND (p_tip_end != 'MV')) THEN
                    
                        -- Cuando la fecha de mi nuevo endoso es mayor en un dia a la fecha maxima (FEC_MAX_LLAVE_END)
                        -- Significa que debod insertar los nuevos registros desde la fecha de inicio del nuevo endoso
                        -- NUM_POL = 1000286067801 COD_SECC = 4 ENDOSO = 8 Y 11
                        IF (fec_end_ini - fec_max_llave_end) = 1 THEN
                            ind := 0;
                        ELSE
                            fec_end_ini := fec_max_llave_end;
                            fec_min     := fec_max_llave_end;
                            ind         := 1;
                        END IF;
                    
                    END IF;
                    -- Algunas polizas hay endosos que son menores a la ultima fecha del endoso en ese mes. Por lo tanto
                    -- se toma la fecha minima para en ese mes para actualizar desde ese me todos los regiistros
                    -- 2000290375301 Endoso 7 02/10/12
                    -- 01/10/12  04/10/12
                    -- 05/10/12  07/10/12
                    -- 08/10/12  31/10/12
                
                    IF ((fec_max_llave_end > (fec_end_ini + 1)) AND (fec_min != fec_max_llave_end) AND (p_tip_end != 'MV')) THEN
                        fec_end_ini := fec_min_llave_end;
                        fec_min     := fec_min_llave_end;
                        ind         := 2;
                    END IF;
                END IF;
            
                prc_inicializa_fechas(p_llave, p_ano, p_mes, fec_end_ini, fec_end_fin, fec_min, fec_max, idrow_mae, flag, fec_ini_mes, fec_fin_mes);
            
                IF ((fec_end_ini = fec_max_llave_end) AND ind = 1) THEN
                
                    -- Debo cerciorarme que no existe el registro antes de hacer los cambios
                    -- 1000486837812+00176+0250 Endoso 12
                    BEGIN
                        SELECT 1
                          INTO flag
                          FROM sim_maestro_expuestos
                         WHERE llave = p_llave
                           AND fec_ini = (p_fec_ini_end + 1);
                    EXCEPTION
                        WHEN OTHERS THEN
                            flag := 0;
                    END;
                
                END IF;
            
                --Si la fecha del endoso - 1 es igual la fecha max del endoso en ese mes entonces solo debe actualizar la fecha de inicio
                -- 1010495849201+00025+0250 Endoso 22
                IF (ind = 1) AND ((p_fec_ini_end - 1) = fec_max_llave_end) THEN
                    flag := 1;
                END IF;
            
                -- Cuando la fecha de mi nuevo endoso es mayor en un dia a la fecha maxima (FEC_MAX_LLAVE_END)
                -- Significa que debod insertar los nuevos registros desde la fecha de inicio del nuevo endoso
                -- NUM_POL = 1000286067801 COD_SECC = 4 ENDOSO = 8 Y 11
            
                IF (ind = 0) AND ((p_fec_ini_end - 1) = fec_max_llave_end) THEN
                    flag          := 4;
                    fec_min_llave := NULL;
                END IF;
            
                mes_con := 0;
                IF (flag = 4) THEN
                    GOTO primer_end; --- Insertar los registros correspondiente al endoso(0) inicial                         
                END IF;
            
                WHILE last_day(add_months(p_fec_ini_end, mes_con)) <= last_day(add_months(p_fec_fin_end, 0))
                LOOP
                
                    IF mes_con = 0 THEN
                    
                        fec_min := to_char(p_fec_ini_end, 'DD/MM/YYYY');
                        fec_max := to_char(last_day(add_months(p_fec_ini_end, mes_con)), 'DD/MM/YYYY');
                    
                        p_ano := extract(YEAR FROM p_fec_ini_end);
                        p_mes := extract(MONTH FROM fec_min);
                        sem := CASE
                                   WHEN (extract(MONTH FROM fec_min)) <= 6 THEN
                                    1
                                   ELSE
                                    2
                               END;
                        tri := CASE
                                   WHEN (extract(MONTH FROM fec_min)) BETWEEN 1 AND 3 THEN
                                    1
                                   WHEN (extract(MONTH FROM fec_min)) BETWEEN 4 AND 6 THEN
                                    2
                                   WHEN (extract(MONTH FROM fec_min)) BETWEEN 7 AND 9 THEN
                                    3
                                   WHEN (extract(MONTH FROM fec_min)) BETWEEN 10 AND 12 THEN
                                    4
                               END;
                    
                        /* FLAG = 0 : La FEC_INI_END no coincide con la fecha inicio del registro (FEC_INI) o hay varios registros para el mismo mes */
                        IF flag = 0 THEN
                            /*Si la fecha FEC_INI_END es mayor a la fecha inicio del mes  (FEC_INI_MES) 
                            Se modifica la FEC_FIN y la exposicion. No se modifica el endoso, ni la mca_anu debido a que el endoso no esta
                            modificando lo viejo*/
                            IF (p_fec_ini_end > fec_ini_mes) THEN
                            
                                BEGIN
                                    UPDATE sim_maestro_expuestos
                                       SET fec_fin = CASE
                                                         WHEN fec_ini = p_fec_ini_end THEN
                                                          p_fec_ini_end
                                                         ELSE
                                                          (p_fec_ini_end - 1)
                                                     END
                                           --   1563235139602+00001+0250
                                           --  ,MCA_ANU  = R.MCA_ANU
                                          ,expo = CASE
                                                      WHEN mca_anu = 'S' THEN
                                                       0
                                                      ELSE
                                                       ((((p_fec_ini_end - 1) - fec_ini) + 1) / (365.2425))
                                                  END
                                          ,fec_pro = SYSDATE
                                          ,"LOG"   = 'UPDATE : 1'
                                           ,nom_pro = p_nom_proc
                                     WHERE ROWID = idrow_mae;
                                    --COMMIT; --+ 1;                                                  
                                EXCEPTION
                                    WHEN OTHERS THEN
                                    
                                        INSERT INTO log_table
                                        VALUES
                                            (SYSDATE
                                            ,'ERROR UPDATE : 1'
                                            ,SYSDATE
                                            ,to_char(SYSDATE, 'HH:MI:SS')
                                            ,SYSDATE
                                            ,'0'
                                            ,0
                                            ,0);
                                        --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                                
                                END;
                            
                            ELSE
                            
                                /*Si la fecha FEC_INI_END es IGUAL a la fecha inicio del mes  (FEC_INI_MES) 
                                Se modifica la EXPO, NUM_END y MCA_ANU*/
                                IF (abs((p_fec_fin_end - p_fec_ini_end)) > 1) THEN
                                    --- No se tienen encuenta endoso de un dia  
                                
                                    BEGIN
                                        UPDATE sim_maestro_expuestos
                                           SET num_end = p_num_end
                                              ,mca_anu = r.mca_anu
                                              ,expo = CASE
                                                          WHEN (r.mca_anu = 'S') THEN
                                                           0
                                                          ELSE
                                                           (((fec_fin - fec_ini) + 1) / (365.2425))
                                                      END
                                              ,fec_pro = SYSDATE
                                              ,"LOG"   = 'UPDATE : 2'
                                               ,nom_pro = p_nom_proc
                                         WHERE ROWID = idrow_mae;
                                        --COMMIT; --+ 1;  
                                    
                                    EXCEPTION
                                        WHEN OTHERS THEN
                                            INSERT INTO log_table
                                            VALUES
                                                (SYSDATE
                                                ,'ERROR UPDATE : 2'
                                                ,SYSDATE
                                                ,to_char(SYSDATE, 'HH:MI:SS')
                                                ,SYSDATE
                                                ,'0'
                                                ,0
                                                ,0);
                                            --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                                    END;
                                
                                END IF;
                                /*Se valida que la difrencia entre las dos fecha se mayor a 1 dia.
                                Esto teniendo encuenta que al inicio de vigencia se le suma 1*/
                                IF abs((fec_ini_mes - p_fec_ini_end)) > 1 THEN
                                    IF fec_ini_mes = fec_min THEN
                                        fec_min := fec_min + 1;
                                    END IF;
                                    IF fec_fin_mes = fec_max THEN
                                        fec_max := fec_ini_mes - 1;
                                    END IF;
                                END IF;
                            END IF;
                        
                            BEGIN
                            
                                -- Si la fecha de inicio del endoso es menor a la del registro. Se debe actualizar la fecha de incio de resgitro actual
                                --- 5132551792001
                                ---Endoso 1 16/05/11
                                ---Endoso 2 11/05/11
                                IF (fec_max_llave_end > fec_end_ini) THEN
                                
                                    BEGIN
                                        UPDATE sim_maestro_expuestos
                                           SET fec_ini = fec_end_ini
                                              ,mca_anu = r.mca_anu
                                              ,num_end = p_num_end
                                              ,expo = CASE
                                                          WHEN r.mca_anu = 'S' THEN
                                                           0
                                                          ELSE
                                                           (((fec_fin - fec_end_ini) + 1) / (365.2425))
                                                      END
                                              ,fec_pro = SYSDATE
                                              ,"LOG"   = 'UPDATE : 3'
                                               ,nom_pro = p_nom_proc
                                         WHERE llave = p_llave
                                           AND fec_ini = fec_max_llave_end;
                                        --COMMIT; --+ 1;  
                                    
                                    EXCEPTION
                                        WHEN OTHERS THEN
                                        
                                            INSERT INTO log_table
                                            VALUES
                                                (SYSDATE
                                                ,'ERROR UPDATE : 3'
                                                ,SYSDATE
                                                ,to_char(SYSDATE, 'HH:MI:SS')
                                                ,SYSDATE
                                                ,'0'
                                                ,0
                                                ,0);
                                            --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                                    
                                    END;
                                
                                ELSE
                                    -- La fecha minima nunca debe ser menor a la fecha inicio de endoso
                                    IF fec_min < p_fec_ini_end THEN
                                        fec_min := p_fec_ini_end;
                                    END IF;
                                    -- La fecha maxima nunca debe ser mayor a la fecha inicio de endoso       
                                    IF (fec_max > p_fec_fin_end) THEN
                                        fec_max := p_fec_fin_end;
                                    END IF;
                                
                                    --IF (FEC_MAX <=  P_FEC_FIN_END) AND ((P_FEC_INI_END+1) != FEC_MIN_LLAVE ) THEN   
                                    IF ((p_fec_ini_end + 1) != fec_min_llave) THEN
                                        /* Se inserta el nuevo resgitro desde donde se modifico la vigencia */
                                        INSERT INTO sim_maestro_expuestos
                                            (nom_pro
                                            ,fec_pro
                                            ,llave
                                            ,cod_cia
                                            ,cod_ram_emi
                                            ,cod_prod
                                            ,cod_sub_prod
                                            ,num_pol
                                            ,mca_ref
                                            ,num_doc_tom
                                            ,tip_doc_tom
                                            ,num_doc_ase
                                            ,tip_doc_ase
                                            ,cod_rie
                                            ,num_end
                                            ,fec_ini
                                            ,fec_fin
                                            ,ano
                                            ,sem
                                            ,tri
                                            ,mes
                                            ,expo
                                            ,mca_anu
                                            ,"LOG")
                                        VALUES
                                            (p_nom_proc
                                            ,SYSDATE
                                            ,p_llave
                                            ,p_cod_cia
                                            ,p_cod_ram_emi
                                            ,p_cod_prod
                                            ,cod_sub_prod
                                            ,p_num_pol
                                            ,p_mca_ref
                                            ,p_num_doc_tom
                                            ,p_tip_doc_tom
                                            ,num_doc_ase
                                            ,tip_doc_ase
                                            ,r.cod_rie
                                            ,p_num_end
                                            ,CASE WHEN(fec_min = fec_ini_mes) THEN p_fec_ini_end + 1 ELSE fec_min END
                                            ,CASE WHEN(p_fec_fin_end < fec_max) THEN p_fec_fin_end ELSE fec_max END
                                            ,p_ano
                                            ,sem
                                            ,tri
                                            ,p_mes
                                            ,CASE WHEN(r.mca_anu = 'S') THEN 0
                                             ELSE((((CASE WHEN(p_fec_fin_end < fec_max) THEN p_fec_fin_end ELSE fec_max END) -
                                                  (CASE WHEN(fec_min = fec_ini_mes) THEN p_fec_ini_end + 1 ELSE fec_min END)) + 1) / (365.2425)) END
                                            ,r.mca_anu
                                            ,'INSERT : 1');
                                        --COMMIT; --+ 1;  
                                    END IF;
                                END IF;
                            
                            EXCEPTION
                                WHEN OTHERS THEN
                                    INSERT INTO log_table
                                    VALUES
                                        (SYSDATE
                                        ,'ERROR INSERT 1'
                                        ,SYSDATE
                                        ,to_char(SYSDATE, 'HH:MI:SS')
                                        ,SYSDATE
                                        ,'0'
                                        ,0
                                        ,0);
                                    --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                            END;
                        
                            /* Inicio del endoso es igual a fecha de incio en la tabla */
                            /* Se modifica la MCA_ANU y la Exposicion solo a un registro*/
                            /* FLAG = 1 : La FEC_INI_END coincide con la fecha inicio del registro (FEC_INI) */
                        ELSIF flag = 1 THEN
                        
                            --Si la fecha del endoso - 1 es igual la fecha max del endoso en ese mes entonces solo debe actualizar la fecha de inicio
                            -- 1010495849201+00025+0250 Endoso 22                                               
                            IF (ind = 1) AND ((p_fec_ini_end - 1) = fec_max_llave_end) THEN
                            
                                BEGIN
                                
                                    UPDATE sim_maestro_expuestos
                                       SET fec_ini = p_fec_ini_end
                                          ,num_end = p_num_end
                                          ,expo = CASE
                                                      WHEN (r.mca_anu = 'S') THEN
                                                       0
                                                      ELSE
                                                       (((p_fec_ini_end - fec_ini) + 1) / (365.2425))
                                                  END
                                          ,mca_anu = r.mca_anu
                                          ,fec_pro = SYSDATE
                                          ,"LOG"   = 'UPDATE : 4'
                                           ,nom_pro = p_nom_proc
                                     WHERE ROWID = idrow_mae;
                                    --COMMIT; --+ 1;  
                                
                                EXCEPTION
                                    WHEN OTHERS THEN
                                        INSERT INTO log_table
                                        VALUES
                                            (SYSDATE
                                            ,'ERROR UPDATE : 4'
                                            ,SYSDATE
                                            ,to_char(SYSDATE, 'HH:MI:SS')
                                            ,SYSDATE
                                            ,'0'
                                            ,0
                                            ,0);
                                        --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                                END;
                            
                            ELSE
                                -- Algunas polizas hay endosos que son menores a la ultima fecha del endoso en ese mes. Por lo tanto
                                -- se toma la fecha minima para en ese mes para actualizar desde ese me todos los regiistros
                                -- 2000290375301 Endoso 7 02/10/12
                                -- 01/10/12  04/10/12
                                -- 05/10/12  07/10/12
                                -- 08/10/12  31/10/12
                                BEGIN
                                    IF ind = 2 THEN
                                    
                                        UPDATE sim_maestro_expuestos
                                           SET num_end = CASE
                                                             WHEN num_end > p_num_end THEN
                                                              num_end
                                                             ELSE
                                                              p_num_end
                                                         END
                                              ,expo = CASE
                                                          WHEN (r.mca_anu = 'S') THEN
                                                           0
                                                          ELSE
                                                           ((fec_fin - fec_ini) + 1) / (365.2425)
                                                      END
                                              ,mca_anu = r.mca_anu
                                              ,fec_pro = SYSDATE
                                              ,"LOG"   = 'UPDATE : 5'
                                               ,nom_pro = p_nom_proc
                                         WHERE llave = p_llave
                                           AND ano = p_ano_end
                                           AND mes = p_mes_end
                                           AND fec_ini >= fec_end_ini;
                                        --COMMIT; --+ 1;  
                                    
                                    ELSE
                                        UPDATE sim_maestro_expuestos
                                           SET num_end = p_num_end
                                              ,expo = CASE
                                                          WHEN (r.mca_anu = 'S') THEN
                                                           0
                                                          ELSE
                                                           (((fec_fin - fec_ini) + 1) / (365.2425))
                                                      END
                                              ,mca_anu = r.mca_anu
                                              ,fec_pro = SYSDATE
                                              ,"LOG"   = 'UPDATE : 6'
                                               ,nom_pro = p_nom_proc
                                         WHERE ROWID = idrow_mae;
                                        --COMMIT; --+ 1;  
                                    
                                    END IF;
                                
                                EXCEPTION
                                    WHEN OTHERS THEN
                                        INSERT INTO log_table
                                        VALUES
                                            (SYSDATE
                                            ,'ERROR UPDATE : 5 + 6'
                                            ,SYSDATE
                                            ,to_char(SYSDATE, 'HH:MI:SS')
                                            ,SYSDATE
                                            ,'0'
                                            ,0
                                            ,0);
                                        --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                                END;
                            
                            END IF;
                            /* Hay varios registros para el mismo mes */
                            /* Se modifica la MCA_ANU y la Exposicion para todos los registros*/
                        ELSIF flag = 2 THEN
                        
                            BEGIN
                            
                                UPDATE sim_maestro_expuestos
                                   SET num_end = CASE
                                                     WHEN num_end > p_num_end THEN
                                                      num_end
                                                     ELSE
                                                      p_num_end
                                                 END
                                      ,expo = CASE
                                                  WHEN (r.mca_anu = 'S') THEN
                                                   0
                                                  ELSE
                                                   ((fec_fin - fec_ini) + 1) / (365.2425)
                                              END
                                      ,mca_anu = r.mca_anu
                                      ,fec_pro = SYSDATE
                                      ,"LOG"   = 'UPDATE : 7'
                                       ,nom_pro = p_nom_proc
                                 WHERE llave = p_llave
                                   AND ano = p_ano
                                   AND mes = p_mes
                                   AND fec_ini >= p_fec_ini_end;
                                --COMMIT; --+ 1;  
                            
                            EXCEPTION
                                WHEN OTHERS THEN
                                    INSERT INTO log_table
                                    VALUES
                                        (SYSDATE
                                        ,'ERROR UPDATE : 7'
                                        ,SYSDATE
                                        ,to_char(SYSDATE, 'HH:MI:SS')
                                        ,SYSDATE
                                        ,'0'
                                        ,0
                                        ,0);
                                    --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                            END;
                        
                            /* Se modifica la MCA_ANU y la Exposicion y la fecha */
                        
                        ELSIF flag = 3 THEN
                        
                            BEGIN
                                UPDATE sim_maestro_expuestos
                                   SET fec_fin = p_fec_ini_end - 1
                                      ,num_end = p_num_end
                                      ,expo = CASE
                                                  WHEN (r.mca_anu = 'S') THEN
                                                   0
                                                  ELSE
                                                   (((((p_fec_ini_end - 1) - fec_min) + 1) / (365.2425)))
                                              END
                                      ,mca_anu = r.mca_anu
                                      ,fec_pro = SYSDATE
                                      ,"LOG"   = 'UPDATE : 8'
                                       ,nom_pro = p_nom_proc
                                 WHERE ROWID = idrow_mae;
                                --COMMIT; --+ 1;             
                            
                                UPDATE sim_maestro_expuestos
                                   SET num_end = p_num_end
                                      ,expo = CASE
                                                  WHEN (r.mca_anu = 'S') THEN
                                                   0
                                                  ELSE
                                                   (((((p_fec_ini_end - 1) - fec_min) + 1) / (365.2425)))
                                              END
                                      ,mca_anu = r.mca_anu
                                      ,fec_pro = SYSDATE
                                      ,"LOG"   = 'UPDATE : 9'
                                       ,nom_pro = p_nom_proc
                                 WHERE llave = p_llave
                                   AND ano = p_ano
                                   AND mes = p_mes
                                   AND fec_ini = fec_end_ini
                                   AND fec_fin = fec_max;
                                --COMMIT; --+ 1;   
                            
                            EXCEPTION
                                WHEN OTHERS THEN
                                
                                    INSERT INTO log_table
                                    VALUES
                                        (SYSDATE
                                        ,'ERROR UPDATE : 8 + 9'
                                        ,SYSDATE
                                        ,to_char(SYSDATE, 'HH:MI:SS')
                                        ,SYSDATE
                                        ,'0'
                                        ,0
                                        ,0);
                                    --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                            
                            END;
                        
                        END IF;
                    
                    ELSE
                    
                        fec_min := to_char((last_day(add_months(p_fec_ini_end, mes_con - 1)) + 1), 'DD/MM/YYYY');
                    
                        IF p_fec_fin_end < last_day(add_months(p_fec_ini_end, mes_con)) THEN
                            fec_max := to_char(p_fec_fin_end, 'DD/MM/YYYY');
                        ELSE
                            fec_max := to_char(last_day(add_months(p_fec_ini_end, mes_con)), 'DD/MM/YYYY');
                        END IF;
                    
                        p_ano := extract(YEAR FROM(last_day(add_months(p_fec_ini_end, mes_con - 1)) + 1));
                        p_mes := extract(MONTH FROM fec_min);
                    
                        sem := CASE
                                   WHEN (extract(MONTH FROM fec_min)) <= 6 THEN
                                    1
                                   ELSE
                                    2
                               END;
                        tri := CASE
                                   WHEN (extract(MONTH FROM fec_min)) BETWEEN 1 AND 3 THEN
                                    1
                                   WHEN (extract(MONTH FROM fec_min)) BETWEEN 4 AND 6 THEN
                                    2
                                   WHEN (extract(MONTH FROM fec_min)) BETWEEN 7 AND 9 THEN
                                    3
                                   WHEN (extract(MONTH FROM fec_min)) BETWEEN 10 AND 12 THEN
                                    4
                               END;
                    
                        BEGIN
                        
                            UPDATE sim_maestro_expuestos
                               SET num_end = p_num_end
                                  ,expo = CASE
                                              WHEN (r.mca_anu = 'S') THEN
                                               0
                                              ELSE
                                               ((fec_fin - fec_ini) + 1) / (365.2425)
                                          END
                                  ,mca_anu = r.mca_anu
                                  ,fec_pro = SYSDATE
                                  ,"LOG"   = 'UPDATE : 10'
                                   ,nom_pro = p_nom_proc
                             WHERE llave = p_llave
                               AND ano = p_ano
                               AND mes = p_mes
                                  --AND  FEC_INI >= FEC_INI_ROW 
                               AND fec_fin <= p_fec_fin_end;
                            --  --COMMIT; --+ 1;  
                        
                        EXCEPTION
                            WHEN OTHERS THEN
                                INSERT INTO log_table
                                VALUES
                                    (SYSDATE
                                    ,'ERROR UPDATE : 10'
                                    ,SYSDATE
                                    ,to_char(SYSDATE, 'HH:MI:SS')
                                    ,SYSDATE
                                    ,'0'
                                    ,0
                                    ,0);
                                --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                        END;
                    
                        IF (SQL%ROWCOUNT = 0) /*AND (P_FEC_FIN_END  <= FEC_MAX_LLAVE) */
                         THEN
                        
                            IF flag != 4 THEN
                            
                                BEGIN
                                
                                    UPDATE sim_maestro_expuestos
                                       SET fec_ini = CASE
                                                         WHEN fec_ini > p_fec_fin_end THEN
                                                          fec_ini
                                                         ELSE
                                                          p_fec_fin_end
                                                     END
                                          ,expo = CASE
                                                      WHEN mca_anu = 'S' THEN
                                                       0
                                                      ELSE
                                                       ((((fec_fin - p_fec_fin_end) + 1) / (365.2425)))
                                                  END
                                          ,fec_pro = SYSDATE
                                          ,"LOG"   = 'UPDATE : 11'
                                           ,nom_pro = p_nom_proc
                                     WHERE llave = p_llave
                                       AND ano = p_ano
                                       AND mes = p_mes;
                                    --COMMIT; --+ 1 ;  
                                
                                EXCEPTION
                                    WHEN OTHERS THEN
                                        INSERT INTO log_table
                                        VALUES
                                            (SYSDATE
                                            ,'ERROR UPDATE : 11'
                                            ,SYSDATE
                                            ,to_char(SYSDATE, 'HH:MI:SS')
                                            ,SYSDATE
                                            ,'0'
                                            ,0
                                            ,0);
                                        --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                                END;
                            
                                /*Si no actualiza significa que son registros nuevos*/
                                IF SQL%ROWCOUNT > 0 THEN
                                    fec_max := p_fec_fin_end - 1;
                                END IF;
                            
                                /* Se inserta el nuevo resgitro desde donde se modifico la vigencia */
                                /* 1000487510005-168     Endoso : 62*/
                                IF fec_max >= fec_min THEN
                                    -- Se valida que la fecha sea fin (FEC_MAX) que la fecha de inicio (FEC_MIN) -> 1563231018112+00001+0250, Endoso 1 
                                
                                    BEGIN
                                    
                                        INSERT INTO sim_maestro_expuestos
                                            (nom_pro
                                            ,fec_pro
                                            ,llave
                                            ,cod_cia
                                            ,cod_ram_emi
                                            ,cod_prod
                                            ,cod_sub_prod
                                            ,num_pol
                                            ,mca_ref
                                            ,num_doc_tom
                                            ,tip_doc_tom
                                            ,num_doc_ase
                                            ,tip_doc_ase
                                            ,cod_rie
                                            ,num_end
                                            ,fec_ini
                                            ,fec_fin
                                            ,ano
                                            ,sem
                                            ,tri
                                            ,mes
                                            ,expo
                                            ,mca_anu
                                            ,"LOG")
                                        VALUES
                                            (p_nom_proc
                                            ,SYSDATE
                                            ,p_llave
                                            ,p_cod_cia
                                            ,p_cod_ram_emi
                                            ,p_cod_prod
                                            ,cod_sub_prod
                                            ,p_num_pol
                                            ,p_mca_ref
                                            ,p_num_doc_tom
                                            ,p_tip_doc_tom
                                            ,num_doc_ase
                                            ,tip_doc_ase
                                            ,r.cod_rie
                                            ,p_num_end
                                            ,fec_min
                                            ,fec_max
                                            ,p_ano
                                            ,sem
                                            ,tri
                                            ,p_mes
                                            ,CASE WHEN(r.mca_anu = 'S') THEN 0 ELSE(((fec_max - fec_min) + 1) / (365.2425)) END
                                            ,r.mca_anu
                                            ,'INSERT : 2');
                                        --COMMIT; --+ 1;  
                                    
                                    EXCEPTION
                                        WHEN OTHERS THEN
                                            INSERT INTO log_table
                                            VALUES
                                                (SYSDATE
                                                ,'ERROR INSERT : 2'
                                                ,SYSDATE
                                                ,to_char(SYSDATE, 'HH:MI:SS')
                                                ,SYSDATE
                                                ,'0'
                                                ,0
                                                ,0);
                                            --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                                    END;
                                
                                END IF;
                            
                            ELSE
                                GOTO primer_end;
                            END IF;
                        
                        END IF;
                        --COMMIT; --+ 1;  
                        /*  END IF;*/
                    
                    END IF;
                
                    mes_con := mes_con + 1;
                END LOOP;
            
                <<primer_end>>
            
                IF flag = 4 THEN
                    BEGIN
                    
                        -- MES_CON := 0;  
                        WHILE last_day(add_months(p_fec_ini_end, mes_con)) <= last_day(add_months(p_fec_fin_end, 0))
                        LOOP
                        
                            IF mes_con = 0 THEN
                            
                                /*El primer dia de vigencia no hay cobertura*/
                                /* Se valida que no sean endoso de un solo dia */
                            
                                IF abs((p_fec_fin_end - p_fec_ini_end)) >= 1 THEN
                                
                                    IF (p_cod_ram_emi != 310 AND p_tip_end != 'MV' AND fec_min_llave IS NULL) THEN
                                        --1000488437702 Endoso 4. Se pregunta por el endoso MV, debido a que si modifican la vigencia, debe comzar desde el inicio de la vigencia
                                        --  IF P.COD_RAM_EMI != 310  THEN 
                                        IF ((p_fec_ini_end < fec_max_llave) AND (extract(MONTH FROM p_fec_ini_end) = extract(MONTH FROM fec_max_llave)) AND
                                           (extract(YEAR FROM p_fec_ini_end) = extract(YEAR FROM fec_max_llave)) AND ((fec_max_llave + 1) < p_fec_fin_end)) THEN
                                            fec_min := fec_max_llave + 1;
                                            fec_max := to_char(last_day(add_months(fec_max_llave + 1, mes_con)), 'DD/MM/YYYY');
                                        ELSE
                                            fec_min := p_fec_ini_end + 1;
                                            fec_max := to_char(last_day(add_months(p_fec_ini_end + 1, mes_con)), 'DD/MM/YYYY');
                                        END IF;
                                    
                                    ELSE
                                    
                                        IF ((fec_min_llave IS NULL) OR (fec_max_llave = p_fec_ini_end) OR (fec_min_llave = (p_fec_ini_end + 1))) AND p_tip_end = 'MV' THEN
                                            fec_min := p_fec_ini_end + 1;
                                            fec_max := to_char(last_day(add_months(p_fec_ini_end + 1, mes_con)), 'DD/MM/YYYY');
                                        ELSE
                                            fec_min := p_fec_ini_end;
                                            fec_max := to_char(last_day(add_months(p_fec_ini_end, mes_con)), 'DD/MM/YYYY');
                                        END IF;
                                    
                                    END IF;
                                
                                END IF;
                            
                            ELSE
                                fec_min := to_char((last_day(add_months(p_fec_ini_end, mes_con - 1)) + 1), 'DD/MM/YYYY');
                            
                                IF fec_min < p_fec_ini_end THEN
                                    fec_min := p_fec_ini_end;
                                END IF;
                            
                                IF p_fec_fin_end < last_day(add_months(p_fec_ini_end, mes_con)) THEN
                                    fec_max := to_char(p_fec_fin_end, 'DD/MM/YYYY');
                                ELSE
                                    fec_max := to_char(last_day(add_months(p_fec_ini_end, mes_con)), 'DD/MM/YYYY');
                                END IF;
                            
                            END IF;
                        
                            -- dbms_output.put_line( 'FEC_MIN : ' || FEC_MIN ||' FEC : ' || LAST_DAY(ADD_MONTHS(P_FEC_INI_END,MES_CON)) );
                            -- dbms_output.put_line( 'FEC_MAX : ' || FEC_MAX ||' FEC : ' || (LAST_DAY(ADD_MONTHS(P_FEC_INI_END,(MES_CON-1)))+1));
                        
                            mes_con := mes_con + 1;
                        
                            p_ano := extract(YEAR FROM fec_min);
                            sem := CASE
                                       WHEN (extract(MONTH FROM fec_min)) <= 6 THEN
                                        1
                                       ELSE
                                        2
                                   END;
                            tri := CASE
                                       WHEN (extract(MONTH FROM fec_min)) BETWEEN 1 AND 3 THEN
                                        1
                                       WHEN (extract(MONTH FROM fec_min)) BETWEEN 4 AND 6 THEN
                                        2
                                       WHEN (extract(MONTH FROM fec_min)) BETWEEN 7 AND 9 THEN
                                        3
                                       WHEN (extract(MONTH FROM fec_min)) BETWEEN 10 AND 12 THEN
                                        4
                                   END;
                            p_mes := extract(MONTH FROM fec_min);
                        
                            BEGIN
                            
                                /*En algunos casos el primer endoso comienza el ultimo dia del mes, al sumar un dia, este seria mayor que 
                                la fecha fin (FEC_MAX). Ejm : 30/09/2006 (FEC_INI) + 1 Dia = 01/10/2006 y la fecha fin es igual a 30/09/2006 (FEC_MAX)
                                Poliza 1010495484901*/
                                /* Lo anterior porque se le suma un dia al incio de vigencia debido a que le 1 dia no hay cobertura */
                                /* Se debe validar la anterior situacion y no insertar el registro */
                            
                                IF (fec_min <= fec_max) AND (abs((p_fec_fin_end - p_fec_ini_end)) >= 1) THEN
                                
                                    -- La fecha minima nunca debe ser menor a la fecha inicio de endoso
                                    IF fec_min < p_fec_ini_end THEN
                                        fec_min := p_fec_ini_end;
                                    END IF;
                                
                                    -- Laa fecha minima no debe ser menor a la fecha max del fin del mes
                                    -- 1563134349601+00001+0150 Endoso 8 : 
                                    -- Fecha Fi Mes        : 27/01/2015
                                    -- Fecha incio endoso  : 02/02/2015
                                
                                    IF (fec_min <= fec_max_llave_mes) AND (p_tip_end != 'MV') THEN
                                        fec_min := fec_max_llave_mes + 1;
                                        IF (fec_min > p_fec_fin_end) OR (fec_min > fec_max) THEN
                                            fec_min := p_fec_ini_end;
                                        END IF;
                                    END IF;
                                
                                    -- La fecha maxima nunca debe ser mayor a la fecha inicio de endoso       
                                    IF (fec_max > p_fec_fin_end) THEN
                                        fec_max := p_fec_fin_end;
                                    END IF;
                                
                                    INSERT INTO sim_maestro_expuestos
                                        (nom_pro
                                        ,fec_pro
                                        ,llave
                                        ,cod_cia
                                        ,cod_ram_emi
                                        ,cod_prod
                                        ,cod_sub_prod
                                        ,num_pol
                                        ,mca_ref
                                        ,num_doc_tom
                                        ,tip_doc_tom
                                        ,num_doc_ase
                                        ,tip_doc_ase
                                        ,cod_rie
                                        ,num_end
                                        ,fec_ini
                                        ,fec_fin
                                        ,ano
                                        ,sem
                                        ,tri
                                        ,mes
                                        ,expo
                                        ,mca_anu
                                        ,"LOG")
                                    VALUES
                                        (p_nom_proc
                                        ,SYSDATE
                                        ,p_llave
                                        ,p_cod_cia
                                        ,p_cod_ram_emi
                                        ,p_cod_prod
                                        ,cod_sub_prod
                                        ,p_num_pol
                                        ,p_mca_ref
                                        ,p_num_doc_tom
                                        ,p_tip_doc_tom
                                        ,num_doc_ase
                                        ,tip_doc_ase
                                        ,r.cod_rie
                                        ,p_num_end
                                        ,fec_min
                                        ,fec_max
                                        ,p_ano
                                        ,sem
                                        ,tri
                                        ,p_mes
                                        ,CASE WHEN(r.mca_anu = 'S') THEN 0 ELSE(((fec_max - fec_min) + 1) / (365.2425)) END
                                        ,r.mca_anu
                                        ,'INSERT : 3');
                                    --COMMIT; --+ 1;  
                                END IF;
                            EXCEPTION
                                WHEN OTHERS THEN
                                
                                    BEGIN
                                    
                                        UPDATE sim_maestro_expuestos
                                           SET num_end = p_num_end
                                              ,expo = CASE
                                                          WHEN (r.mca_anu = 'S') THEN
                                                           0
                                                          ELSE
                                                           ((fec_fin - fec_ini) + 1) / (365.2425)
                                                      END
                                              ,mca_anu = r.mca_anu
                                              ,fec_pro = SYSDATE
                                              ,"LOG"   = 'UPDATE : 12'
                                               ,nom_pro = p_nom_proc
                                         WHERE llave = p_llave
                                           AND ano = p_ano
                                           AND mes = p_mes;
                                        --COMMIT; --+ 1;  
                                    
                                    EXCEPTION
                                        WHEN OTHERS THEN
                                            INSERT INTO log_table
                                            VALUES
                                                (SYSDATE
                                                ,'ERROR UPDATE : 12'
                                                ,SYSDATE
                                                ,to_char(SYSDATE, 'HH:MI:SS')
                                                ,SYSDATE
                                                ,'0'
                                                ,0
                                                ,0);
                                            --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                                    END;
                                
                                    BEGIN
                                    
                                        SELECT MIN(fec_ini) AS fec_fin_mes
                                          INTO fec_fin_mes
                                          FROM sim_maestro_expuestos a
                                         WHERE a.llave = p_llave
                                           AND a.ano = p_ano
                                           AND a.mes = p_mes
                                           AND a.fec_fin <= fec_max;
                                    
                                    EXCEPTION
                                        WHEN OTHERS THEN
                                        
                                            INSERT INTO log_table
                                            VALUES
                                                (SYSDATE
                                                ,'ERROR SELECT'
                                                ,SYSDATE
                                                ,to_char(SYSDATE, 'HH:MI:SS')
                                                ,SYSDATE
                                                ,'0'
                                                ,0
                                                ,0);
                                            --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                                    
                                    END;
                                
                                    IF fec_min <= (fec_fin_mes - 1) THEN
                                        --- CORRIGE PEDAZOS QUE HALLAN QUEDADO MAL (HUECOS)
                                        --- EJEM  2030285166208+00168+0250
                                        --- 01/04/08  01/04/08  (Este es un hueco)
                                        --- 02/04/08  30/04/0
                                        BEGIN
                                        
                                            INSERT INTO sim_maestro_expuestos
                                                (nom_pro
                                                ,fec_pro
                                                ,llave
                                                ,cod_cia
                                                ,cod_ram_emi
                                                ,cod_prod
                                                ,cod_sub_prod
                                                ,num_pol
                                                ,mca_ref
                                                ,num_doc_tom
                                                ,tip_doc_tom
                                                ,num_doc_ase
                                                ,tip_doc_ase
                                                ,cod_rie
                                                ,num_end
                                                ,fec_ini
                                                ,fec_fin
                                                ,ano
                                                ,sem
                                                ,tri
                                                ,mes
                                                ,expo
                                                ,mca_anu
                                                ,"LOG")
                                            VALUES
                                                (p_nom_proc
                                                ,SYSDATE
                                                ,p_llave
                                                ,p_cod_cia
                                                ,p_cod_ram_emi
                                                ,p_cod_prod
                                                ,cod_sub_prod
                                                ,p_num_pol
                                                ,p_mca_ref
                                                ,p_num_doc_tom
                                                ,p_tip_doc_tom
                                                ,num_doc_ase
                                                ,tip_doc_ase
                                                ,r.cod_rie
                                                ,p_num_end
                                                ,fec_min
                                                ,fec_fin_mes - 1
                                                ,p_ano
                                                ,sem
                                                ,tri
                                                ,p_mes
                                                ,CASE WHEN(r.mca_anu = 'S') THEN 0 ELSE((((fec_fin_mes - 1) - fec_min) + 1) / (365.2425)) END
                                                ,r.mca_anu
                                                ,'INSERT : 4');
                                            --COMMIT; --+ 1;                                                                            
                                        
                                        EXCEPTION
                                            WHEN OTHERS THEN
                                                INSERT INTO log_table
                                                VALUES
                                                    (SYSDATE
                                                    ,'ERROR INSERT : 4'
                                                    ,SYSDATE
                                                    ,to_char(SYSDATE, 'HH:MI:SS')
                                                    ,SYSDATE
                                                    ,'0'
                                                    ,0
                                                    ,0);
                                                --,p_llave || '+' || p_ano || '+' || p_mes || '+' || fec_min || '+' || fec_max);
                                        END;
                                    
                                    END IF;
                            END;
                        END LOOP;
                    
                    END;
                END IF;
            END;
        
        END LOOP;
    
    END prc_procesar;

    PROCEDURE prc_inicializa_fechas(p_llave     IN VARCHAR2
                                   ,p_ano       IN NUMBER
                                   ,p_mes       IN NUMBER
                                   ,p_fec_ini   IN DATE
                                   ,p_fec_fin   IN DATE
                                   ,p_fec_min   IN DATE
                                   ,p_fec_max   IN DATE
                                   ,p_idrow_mae OUT VARCHAR2
                                   ,p_flag      OUT NUMBER
                                   ,fec_ini_mes OUT DATE
                                   ,fec_fin_mes OUT DATE) IS
    
    BEGIN
    
        BEGIN
            SELECT ROWID
                  ,CASE
                       WHEN (a.fec_ini = p_fec_ini OR a.fec_ini = (p_fec_ini + 1)) THEN
                        1
                       ELSE
                        0
                   END AS flag
                  ,fec_ini
                  ,fec_fin
              INTO p_idrow_mae
                  ,p_flag
                  ,fec_ini_mes
                  ,fec_fin_mes
              FROM sim_maestro_expuestos a
             WHERE a.llave = p_llave
                  -- AND A.ANO      = P_ANO 
                  -- AND A.MES      = P_MES
               AND (a.fec_ini = p_fec_min OR a.fec_ini = p_fec_ini OR a.fec_ini = (p_fec_ini + 1))
               AND a.fec_fin <= p_fec_max; -- OR A.FEC_FIN = P_FEC_FIN);
        
        EXCEPTION
            WHEN no_data_found THEN
                BEGIN
                
                    SELECT ROWID
                          ,0 AS flag
                          ,fec_ini
                          ,fec_fin
                      INTO p_idrow_mae
                          ,p_flag
                          ,fec_ini_mes
                          ,fec_fin_mes
                      FROM sim_maestro_expuestos a
                     WHERE a.llave = p_llave
                       AND a.ano = p_ano
                       AND a.mes = p_mes;
                
                EXCEPTION
                    WHEN too_many_rows THEN
                        p_idrow_mae := NULL;
                        p_flag      := 2;
                    WHEN no_data_found THEN
                        p_idrow_mae := NULL;
                        p_flag      := 4;
                END;
            
            WHEN too_many_rows THEN
                BEGIN
                
                    SELECT ROWID
                          ,3 AS flag
                          ,fec_ini
                          ,fec_fin
                      INTO p_idrow_mae
                          ,p_flag
                          ,fec_ini_mes
                          ,fec_fin_mes
                      FROM sim_maestro_expuestos a
                     WHERE a.llave = p_llave
                       AND a.ano = p_ano
                       AND a.mes = p_mes
                       AND a.fec_ini = p_fec_min
                       AND a.fec_fin = p_fec_max;
                EXCEPTION
                    WHEN OTHERS THEN
                        p_idrow_mae := NULL;
                        p_flag      := 2;
                END;
            
        END;
    
    END prc_inicializa_fechas;

    /*******************************************************************************************************************************/
    -- Procedimiento que que inserta en la tabla 'SIM_MAESTRO_EXPUESTOS_MUESTRA'
    --
    -- %param ip_data                    IN OUT NOCOPY                        registro sim_maestro_expuestos_muestra que se va a insertar
    -- %param op_resultado               OUT  NUMBER                          Resultado (0: OK, -1: Error, 1: Warning).
    -- %param op_arrerrores              OUT  sim_typ_array_error             Array de Errores.
    -- %version 1.0
    --
    -- Control de cambios
    -- DATE            AUTHOR - email                                 DESCRIPTION
    -- ----------      -----------------------------------------      ------------------------------------
    -- 10/10/2018      Luis Carlos Castaneda Oviedo - lcastaneda@asesoftware.com          1. creacion del procedimiento
    PROCEDURE prc_escribe_tabla_muestra(ip_data       IN OUT NOCOPY sim_maestro_expuestos_muestra%ROWTYPE
                                       ,op_resultado  OUT NUMBER
                                       ,op_arrerrores OUT sim_typ_array_error) IS
    BEGIN
        INSERT INTO sim_maestro_expuestos_muestra
        VALUES ip_data;
    END prc_escribe_tabla_muestra;

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

----------------------------------------------------------------

END sim_pck_cb299710;
