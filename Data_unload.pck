CREATE OR REPLACE PACKAGE Data_unload IS
  --取出表行数最大要求
  MAXDATA CONSTANT NUMBER := 3000000;

  TYPE DEF IS RECORD(
    V_ROWID VARCHAR2(640),
    V_NUM   NUMBER);
  TYPE DEF1 IS TABLE OF DEF INDEX BY PLS_INTEGER;

  --表数据卸载程序执行入口
  PROCEDURE MAIN;
  --表数据卸载执行程序
  FUNCTION PURGETABLE(I_TABLE VARCHAR2,
                      O_ERROR OUT CLOB,
                      O_FILE  OUT VARCHAR2) RETURN BOOLEAN;
  --数据卸载抽取数据
  PROCEDURE RUNUNLOAD;
  --测试使用
  PROCEDURE TEMP_INSERT;
  --获取表是否历史数据
  PROCEDURE HISTDATA(O_ERROR OUT VARCHAR2);
  --游标引入
  FUNCTION TEMPORARY_TABLE(I_TABLE VARCHAR2) RETURN SYS_REFCURSOR;
  --数据补救
  PROCEDURE SAVE_TO(I_TABLE VARCHAR2, I_SIDE_TABLES VARCHAR2);
  --多线程执行结果
  PROCEDURE SCHEDULER_REN_DETAILS(I_TABLE VARCHAR2);
END;
/
CREATE OR REPLACE PACKAGE BODY DATA_UNLOAD IS
  YYYY       CONSTANT VARCHAR2(4) := '2018';
  V_HEADNAME CONSTANT VARCHAR2(200) := 'TEMP_IMPORTTABLENAME';
  L_LIMIT    CONSTANT NUMBER := 10000;
  V_CPU      CONSTANT INT := 15; --服务器CPU几核量

  --测试使用过程
  PROCEDURE TEMP_INSERT IS
    CURSOR TAB IS
      SELECT * FROM SM_LOG_OPERATELOG;
    TYPE A IS TABLE OF TAB%ROWTYPE;
    V_TABLE     A;
    V_INT       INT;
    V_TABLENAME VARCHAR2(254) := 'TMP_SM_LOG_OPERATELOG';
    V_SQL       VARCHAR2(2000);
  BEGIN
  
    SELECT COUNT(1)
      INTO V_INT
      FROM TMP_SM_LOG_OPERATELOG
     WHERE ROWNUM < = 1;
  
    IF V_INT = 0 THEN
    
      OPEN TAB;
      FETCH TAB BULK COLLECT
        INTO V_TABLE;
      CLOSE TAB;
      IF V_TABLE.COUNT > 0 THEN
        FORALL I IN INDICES OF V_TABLE SAVE EXCEPTIONS
          INSERT INTO TMP_SM_LOG_OPERATELOG VALUES V_TABLE (I);
      END IF;
    END IF;
  EXCEPTION
    WHEN OTHERS THEN
      BEGIN
        FOR I IN 1 .. SQL%BULK_EXCEPTIONS.COUNT LOOP
        
          DBMS_OUTPUT.PUT_LINE(SQLERRM(- (SQL%BULK_EXCEPTIONS(I).ERROR_CODE)));
        
        END LOOP;
      
      EXCEPTION
        WHEN OTHERS THEN
          RAISE_APPLICATION_ERROR(-20999, SQLERRM);
      END;
  END;
  PROCEDURE SAVE_TO(I_TABLE VARCHAR2, I_SIDE_TABLES VARCHAR2) AS
    L_FIELD CLOB;
    L_SQL   CLOB;
  BEGIN
    SELECT LISTAGG(T.COLUMN_NAME, ',') WITHIN GROUP(ORDER BY T.COLUMN_NAME)
      INTO L_FIELD
      FROM USER_TAB_COLUMNS T
     WHERE T.TABLE_NAME = I_TABLE
     ORDER BY T.COLUMN_ID ASC;
  
    L_SQL := 'INSERT INTO ' || I_TABLE || ' SELECT ' || L_FIELD || ' FROM ' ||
             I_SIDE_TABLES || ' WHERE 1=1';
    EXECUTE IMMEDIATE L_SQL;
    EXECUTE IMMEDIATE 'DROP TABLE ' || I_SIDE_TABLES || ' PURGE';
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      ROLLBACK;
      RAISE_APPLICATION_ERROR(-2009, SQLERRM);
  END;
  PROCEDURE SCHEDULER_REN_DETAILS(I_TABLE VARCHAR2) AS
    CURSOR SCH(C_TOPIC VARCHAR2) IS
      SELECT T.LOG_ID, T.ADDITIONAL_INFO, T.STATUS
        FROM USER_SCHEDULER_JOB_RUN_DETAILS T
       WHERE T.JOB_NAME = C_TOPIC
         AND ROWNUM <= 1
       ORDER BY TO_NUMBER(TO_CHAR(T.LOG_DATE, 'YYYYMMDDHH24MISS')) DESC;
    V_LOG_ID          NUMBER;
    V_ADDITIONAL_INFO VARCHAR2(500);
    V_STATUS          VARCHAR2(600);
  
  BEGIN
    FOR DB IN (SELECT *
                 FROM UNLOAD_SCHEDULER_JOB
                WHERE FLAG = 2
                  AND SUBSTR(TOPIC, 1, INSTR(TOPIC, '_', -1) - 1) = I_TABLE) LOOP
      OPEN SCH(DB.TOPIC);
      FETCH SCH
        INTO V_LOG_ID, V_ADDITIONAL_INFO, V_STATUS;
      CLOSE SCH;
      IF V_LOG_ID IS NOT NULL THEN
        UPDATE UNLOAD_SCHEDULER_JOB T
           SET T.LOG_ID          = V_LOG_ID,
               T.ADDITIONAL_INFO = NVL(V_ADDITIONAL_INFO, V_STATUS),
               FLAG              = DECODE(V_STATUS, 'SUCCEEDED', 100, 4)
         WHERE T.TOPIC = DB.TOPIC
           AND T.LNUM = DB.LNUM
           AND FLAG = 2;
      ELSE
        UPDATE UNLOAD_SCHEDULER_JOB T
           SET FLAG = 99, ADDITIONAL_INFO = 'JOBS线程未找到相关数据'
         WHERE T.TOPIC = DB.TOPIC
           AND T.LNUM = DB.LNUM
           AND FLAG = 2;
      END IF;
    END LOOP;
    COMMIT;
  END;
  FUNCTION SCHEDULER_JOB(I_SCHEDULER_NAME VARCHAR2,
                         I_ROWNUM         NUMBER,
                         I_DATE           DATE DEFAULT SYSDATE,
                         O_ERROR          OUT VARCHAR2) RETURN BOOLEAN IS
    V_COUNT      INT := V_CPU + 1;
    V_JOB_ACTION CLOB;
  BEGIN
    WHILE V_COUNT > V_CPU LOOP
      BEGIN
        SELECT COUNT(*)
          INTO V_COUNT
          FROM USER_SCHEDULER_JOBS T
         WHERE SUBSTR(T.JOB_NAME, 1, INSTR(T.JOB_NAME, '_') - 1) =
               SUBSTR(I_SCHEDULER_NAME, 1, INSTR(I_SCHEDULER_NAME, '_') - 1);
        IF V_COUNT > V_CPU + V_CPU THEN
          DBMS_LOCK.SLEEP(1);
        END IF;
      EXCEPTION
        WHEN OTHERS THEN
          V_COUNT := 0;
      END;
    END LOOP;
    V_JOB_ACTION := ' BEGIN
      FOR DB IN (SELECT * 
                   FROM UNLOAD_SCHEDULER_JOB T
                  WHERE T.TOPIC =' || CHR(39) ||
                    I_SCHEDULER_NAME || CHR(39) || ' 
                    AND FLAG = 0
                    AND T.LNUM = ' || I_ROWNUM ||
                    ' ) LOOP
        EXECUTE IMMEDIATE DB.JOB_ACTION;
          EXECUTE IMMEDIATE ''UPDATE UNLOAD_SCHEDULER_JOB SET FLAG=2,LASTDATETIME=SYSDATE WHERE TOPIC=:1AND LNUM=:2'' USING DB.TOPIC,DB.LNUM;
      END LOOP;
    END;';
    DBMS_SCHEDULER.CREATE_JOB(JOB_NAME        => I_SCHEDULER_NAME,
                              JOB_TYPE        => 'PLSQL_BLOCK',
                              JOB_ACTION      => V_JOB_ACTION,
                              ENABLED         => TRUE,
                              START_DATE      => SYSTIMESTAMP,
                              REPEAT_INTERVAL => NULL,
                              COMMENTS        => I_DATE);
    -- DBMS_SCHEDULER.RUN_JOB(I_SCHEDULER_NAME);
    RETURN TRUE;
  EXCEPTION
    WHEN OTHERS THEN
      O_ERROR := 'DATA_UNLOAD.SCHEDULER_JOB: ' || SQLERRM;
      RETURN FALSE;
  END;

  FUNCTION TEMPORARY_TABLE(I_TABLE VARCHAR2) RETURN SYS_REFCURSOR AS
    V_SQL CLOB;
    A     SYS_REFCURSOR;
  BEGIN
    V_SQL := 'OPEN  A FOR SELECT * FROM ' || I_TABLE;
  
    EXECUTE IMMEDIATE V_SQL;
    RETURN A;
  END;
  FUNCTION PUT_DATA(I_TABLENALE    VARCHAR2,
                    TEMP_TABLENAME VARCHAR2,
                    O_ERROR        OUT VARCHAR2) RETURN BOOLEAN AS
    V_SQL       CLOB;
    V_INT       INT := 1;
    V_TABLE     SYS_REFCURSOR;
    V_COUNT     INT := 1;
    V_TABLENAME VARCHAR2(64);
    V_STRLIST   CLOB;
    V_VATLIST   CLOB;
    L_EXCEPTION EXCEPTION;
    V_TOPIC VARCHAR2(640);
  BEGIN
  
    IF I_TABLENALE IS NOT NULL AND TEMP_TABLENAME IS NOT NULL THEN
    
      V_SQL := 'SELECT COUNT(1) FROM ' || TEMP_TABLENAME || ' WHERE 1=1';
    
      EXECUTE IMMEDIATE V_SQL
        INTO V_COUNT;
    
      V_COUNT := CEIL(V_COUNT / L_LIMIT);
      IF V_COUNT > 0 THEN
        --V_GET(I) 批量导入数组
        SELECT LISTAGG(T.COLUMN_NAME, ',') WITHIN GROUP(ORDER BY T.COLUMN_NAME),
               LISTAGG('V_GET(I).' || T.COLUMN_NAME, ',') WITHIN GROUP(ORDER BY 'V_GET(I).' || T.COLUMN_NAME)
          INTO V_STRLIST, V_VATLIST
          FROM USER_TAB_COLUMNS T
         WHERE T.TABLE_NAME = I_TABLENALE
         ORDER BY T.COLUMN_ID ASC;
        -- 删除原表数据，在重新导入，释放原表空间缓存
        V_SQL := 'TRUNCATE TABLE ' || I_TABLENALE;
        EXECUTE IMMEDIATE V_SQL;
      
        FOR I IN 1 .. V_COUNT LOOP
        
          V_SQL   := 'DECLARE
                         CURSOR GET(I_STR INT) IS
                            SELECT :B0 FROM :B1 T WHERE T.LROWID BETWEEN :B3 AND I_STR;
                            TYPE A IS TABLE OF GET%ROWTYPE INDEX BY PLS_INTEGER;
                             V_GET A;
                        BEGIN
                              OPEN GET(:B2);
                               FETCH GET BULK COLLECT
                                  INTO V_GET LIMIT ' ||
                     L_LIMIT || ';
                              CLOSE GET;

                               IF V_GET.COUNT > 0 THEN
  
                                  FORALL I IN V_GET.FIRST .. V_GET.LAST
                                   INSERT /*+ APPEND */  INTO ' ||
                     I_TABLENALE || '(:B0) VALUES (:B4);
                                END IF;
                         END;
';
          V_SQL   := REPLACE(REPLACE(V_SQL, ':B3', V_INT), ':B0', V_STRLIST);
          V_SQL   := REPLACE(REPLACE(V_SQL, ':B1', TEMP_TABLENAME),
                             ':B2',
                             L_LIMIT * I);
          V_SQL   := REPLACE(V_SQL, ':B4', V_VATLIST);
          V_INT   := V_INT + L_LIMIT;
          V_TOPIC := I_TABLENALE || '_' || I;
          --线程池记录数据
          BEGIN
            INSERT INTO UNLOAD_SCHEDULER_JOB
              (TOPIC, JOB_ACTION, LNUM)
            VALUES
              (V_TOPIC, V_SQL, I);
          EXCEPTION
            WHEN OTHERS THEN
              O_ERROR := 'DATA_UNLOAD.PUT_DATA 线程池记录数据已经存在，不允许重复执行 :' ||
                         V_TOPIC;
              RAISE L_EXCEPTION;
          END;
          -- EXECUTE IMMEDIATE V_SQL;
          -- 多线程序执行程序
          IF SCHEDULER_JOB(V_TOPIC, I, SYSDATE, O_ERROR) = FALSE THEN
            O_ERROR := '多程序执行失败:--->' || O_ERROR;
            RAISE L_EXCEPTION;
          ELSE
            /* DELETE FROM TEMP_TMP_SM_LOG_OPERATELOG T
            WHERE MOD(V_COUNT, V_COUNT) = T.LROWID;*/
            --测试数据
            NULL;
          END IF;
        
        END LOOP;
        COMMIT;
      END IF;
    ELSE
      ROLLBACK;
      RETURN FALSE;
    END IF;
    COMMIT;
    RETURN TRUE;
  
  EXCEPTION
    WHEN L_EXCEPTION THEN
      ROLLBACK;
      RETURN FALSE;
  END;
  PROCEDURE HISTDATA(O_ERROR OUT VARCHAR2) AS
    V_SQL   VARCHAR2(2000);
    V_COUNT INT;
  BEGIN
    FOR DB IN (SELECT * FROM UNLOAD_TABLENAME WHERE FLAG = 0) LOOP
      V_SQL := 'SELECT 1 FROM ' || DB.TABLE_NAME ||
               ' WHERE ROWNUM <=1 AND SUBSTR(TS,1,4) < ''' || YYYY || '' ||
               CHR(39);
      BEGIN
        EXECUTE IMMEDIATE V_SQL
          INTO V_COUNT;
      EXCEPTION
        WHEN OTHERS THEN
          V_COUNT := 0;
      END;
    
      IF V_COUNT = 0 THEN
      
        V_SQL := ' UPDATE  UNLOAD_TABLENAME SET FLAG=3,MESS=' || CHR(39) || YYYY ||
                 '年之前未发现数据' || CHR(39) || ' WHERE FLAG=0 AND TABLE_NAME=''' ||
                 DB.TABLE_NAME || CHR(39);
      
        EXECUTE IMMEDIATE V_SQL;
      END IF;
    END LOOP;
    COMMIT;
  EXCEPTION
    WHEN OTHERS THEN
      O_ERROR := SQLERRM;
  END;
  PROCEDURE TEMP_TABLE(I_TABLE VARCHAR2, O_TABLE OUT VARCHAR2) AS
  
  BEGIN
    O_TABLE := 'TEMP_' || I_TABLE;
  
    BEGIN
      EXECUTE IMMEDIATE 'CREATE TABLE ' || O_TABLE ||
                        ' AS SELECT /*+ PARALLEL */  T.*,ROWNUM AS LROWID FROM ' ||
                        I_TABLE || ' T WHERE SUBSTR(TS,1,4)>=' || CHR(39) || YYYY ||
                        CHR(39);
    
    EXCEPTION
      WHEN OTHERS THEN
        EXECUTE IMMEDIATE 'DROP TABLE ' || O_TABLE || ' PURGE';
        EXECUTE IMMEDIATE 'CREATE TABLE ' || O_TABLE ||
                          ' AS SELECT /*+ PARALLEL */ T.*,ROWNUM AS LROWID FROM ' ||
                          I_TABLE || ' T WHERE SUBSTR(TS,1,4)>=' || CHR(39) || YYYY ||
                          CHR(39);
    END;
  END;

  FUNCTION PURGETABLE(I_TABLE VARCHAR2,
                      O_ERROR OUT CLOB,
                      O_FILE  OUT VARCHAR2) RETURN BOOLEAN AS
    V_PURGE     CLOB;
    V_TABLENAME VARCHAR2(64);
    V_INT       INT;
    V_SQL       VARCHAR2(2000);
    V_CLOB      CLOB;
    V_ROWCOUNT  CLOB;
    V_NUM       INT;
    V_EXCEPTION EXCEPTION;
    V_INT1    INT := 500000;
    V_NUMLIST VARCHAR2(2000);
  BEGIN
  
    TEMP_TABLE(I_TABLE, V_TABLENAME);
    IF V_TABLENAME IS NOT NULL THEN
    
      V_SQL := 'SELECT COUNT(*)  FROM ' || V_TABLENAME ||
               ' WHERE  SUBSTR(TS,1,4)>=' || CHR(39) || YYYY || CHR(39);
    
      EXECUTE IMMEDIATE V_SQL
        INTO V_INT;
    
    ELSE
      O_ERROR := 'DATA_UNLOAD.TEMP_TABLE 备份文件创建失败';
      RAISE V_EXCEPTION;
    
    END IF;
    V_ROWCOUNT := 'SELECT COUNT(*) FROM ' || I_TABLE ||
                  ' WHERE  SUBSTR(TS,1,4)>=' || CHR(39) || YYYY || CHR(39);
    EXECUTE IMMEDIATE V_ROWCOUNT
      INTO V_NUM;
    --检查备份数据与正式表数据是否一致
    IF V_INT = V_NUM THEN
    
      IF PUT_DATA(I_TABLE, V_TABLENAME, O_ERROR) = FALSE THEN
        O_ERROR := 'DATA_UNLOAD.PUT_DATA 数据载入失败--->' || O_ERROR;
        RAISE V_EXCEPTION;
      END IF;
    ELSE
      O_ERROR := 'DATA_UNLOAD.PURGETABLE 主数据与备份数据量不一致' || V_INT || '--' ||
                 V_NUM;
      RAISE V_EXCEPTION;
    END IF;
    O_FILE := V_TABLENAME;
    RETURN TRUE;
  EXCEPTION
    WHEN V_EXCEPTION THEN
      IF O_ERROR IS NULL THEN
        O_ERROR := SQLERRM;
      END IF;
      ROLLBACK;
      RETURN FALSE;
  END;
  PROCEDURE RUNUNLOAD IS
    CURSOR GETTBA IS
      SELECT T.TABLE_NAME FROM UNLOAD_TABLENAME T WHERE T.FLAG = 1;
  
    TYPE GET IS TABLE OF GETTBA%ROWTYPE INDEX BY PLS_INTEGER;
    V_GET GET;
    CURSOR USER_SEGMENTS(I_TABLE VARCHAR2) IS
      SELECT T.NUM_ROWS,
             ROUND(NVL(T1.BYTES, 0) / 1024 / 1024, 2) || 'M' BLOCKS
        FROM USER_TABLES T, USER_SEGMENTS T1
       WHERE T.TABLE_NAME = I_TABLE
         AND T.TABLE_NAME = T1.SEGMENT_NAME;
    V_NUM          NUMBER;
    V_BLOCKS       VARCHAR2(64);
    V_TABLEBOOLEAN BOOLEAN;
    V_MESS         CLOB;
    V_FILE         VARCHAR2(640);
    V_SQL          VARCHAR2(3000);
  BEGIN
    --测试使用数据
    TEMP_INSERT;
  
    OPEN GETTBA;
    FETCH GETTBA BULK COLLECT
      INTO V_GET;
    CLOSE GETTBA;
    IF V_GET.COUNT > 0 THEN
      FOR DB IN V_GET.FIRST .. V_GET.LAST LOOP
        /*        V_SQL := 'SELECT * FROM ' || V_GET(DB).TABLE_NAME ||
                 ' FOR UPDATE NOWAIT';
        EXECUTE IMMEDIATE V_SQL;*/
        IF PURGETABLE(V_GET(DB).TABLE_NAME, V_MESS, V_FILE) = FALSE THEN
          UPDATE UNLOAD_TABLENAME T
             SET FLAG = 3, T.LAST_DATATIME = SYSDATE, T.MESS = V_MESS
           WHERE TABLE_NAME = V_GET(DB).TABLE_NAME
             AND FLAG = 1;
          EXIT;
        ELSE
          DBMS_STATS.GATHER_TABLE_STATS(USER, V_GET(DB).TABLE_NAME);
          OPEN USER_SEGMENTS(V_GET(DB).TABLE_NAME);
          FETCH USER_SEGMENTS
            INTO V_NUM, V_BLOCKS;
          CLOSE USER_SEGMENTS;
        
          UPDATE UNLOAD_TABLENAME
             SET FLAG           = 2,
                 REMAINING_SIZE = V_NUM,
                 USAGE          = V_BLOCKS,
                 LAST_DATATIME  = SYSDATE,
                 BACKUP_FILE    = V_FILE
           WHERE TABLE_NAME = V_GET(DB).TABLE_NAME;
        
          -- 多线程执行结果
          SCHEDULER_REN_DETAILS(V_GET(DB).TABLE_NAME);
        END IF;
      END LOOP;
    END IF;
    BEGIN
      SELECT 1 INTO V_NUM FROM UNLOAD_TABLENAME WHERE FLAG = 1;
    EXCEPTION
      WHEN OTHERS THEN
        V_NUM := 0;
    END;
    IF V_NUM = 0 THEN
      UPDATE UNLOAD_TABLENAME
         SET FLAG = 1
       WHERE ROWNUM < = 1
         AND FLAG = 0;
    END IF;
    COMMIT;
  END;
  PROCEDURE MAIN IS
    CURSOR C_MAIN IS
      SELECT T.TABLE_NAME,
             T.NUM_ROWS,
             ROUND(NVL(T1.BYTES, 0) / 1024 / 1024, 2) || 'M' BLOCKS,
             MD.DISPLAYNAME
        FROM USER_TABLES T, USER_SEGMENTS T1, MD_TABLE MD
       WHERE T.NUM_ROWS >= MAXDATA
         AND T.TABLE_NAME = T1.SEGMENT_NAME
         AND T.TABLE_NAME = UPPER(MD.NAME(+));
    TYPE I_TABLE IS TABLE OF C_MAIN%ROWTYPE INDEX BY PLS_INTEGER;
    V_TABLE I_TABLE;
    V_SQL   VARCHAR2(500) := 'INSERT INTO UNLOAD_TABLENAME(TABLE_NAME,NUM_ROWS,BLOCKS,TABLENAME) VALUES(:1,:2,:3,:4)';
    V_ERROR VARCHAR2(5000);
    V_EXCEPTION EXCEPTION;
  BEGIN
    OPEN C_MAIN;
    FETCH C_MAIN BULK COLLECT
      INTO V_TABLE;
    CLOSE C_MAIN;
    IF V_TABLE.COUNT > 0 THEN
    
      EXECUTE IMMEDIATE 'TRUNCATE TABLE UNLOAD_TABLENAME';
    
      FORALL I IN V_TABLE.FIRST .. V_TABLE.LAST EXECUTE IMMEDIATE V_SQL
                                   USING V_TABLE(I).TABLE_NAME, V_TABLE(I)
                                  .NUM_ROWS, V_TABLE(I).BLOCKS, V_TABLE(I)
                                  .DISPLAYNAME
        ;
    
      V_SQL := 'UPDATE   UNLOAD_TABLENAME T  SET FLAG = 4 WHERE NOT EXISTS(SELECT 1 FROM USER_TAB_COLS TB WHERE TB.COLUMN_NAME =''TS'' AND T.TABLE_NAME=TB.TABLE_NAME)';
    
      EXECUTE IMMEDIATE V_SQL;
    
      HISTDATA(V_ERROR);
      IF V_ERROR IS NOT NULL THEN
        RAISE V_EXCEPTION;
      END IF;
    END IF;
  
    COMMIT;
  EXCEPTION
    WHEN V_EXCEPTION THEN
      RAISE_APPLICATION_ERROR(-299999, V_ERROR);
  END;

END;
/
