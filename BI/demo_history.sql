-- Importação histórica acadêmica: cadastro dos ciclos/pedidos diretamente,
-- fatos, classificações, pontos e auditoria pelas rotinas oficiais.
-- Não simula faturamento: estes ciclos são fixtures, sem comprovação de pagamento.
SELECT pg_temp.dl_actor('SYSTEM');
CREATE TEMP TABLE bi_clock AS
SELECT date_trunc('month', CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date AS month,
       (CURRENT_TIMESTAMP AT TIME ZONE 'America/Sao_Paulo')::date AS today;
INSERT INTO establishment_subscription(id,establishment_id,status,activated_at,created_at)
SELECT pg_temp.dl_id('subscription',i),pg_temp.dl_id('est',i),'ACTIVE',
       ((month - interval '12 months')::timestamp AT TIME ZONE 'America/Sao_Paulo'),
       ((month - interval '12 months')::timestamp AT TIME ZONE 'America/Sao_Paulo')
FROM generate_series(1,30) n(i),bi_clock;

INSERT INTO subscription_cycle(id,subscription_id,establishment_id,cycle_number,starts_at,ends_at,
  anchor_day,anchor_local_time,anchor_timezone,plan_id,plan_name,monthly_price,volume_limit_liters,collection_limit)
SELECT pg_temp.dl_id('bi_cycle',m*30+i),pg_temp.dl_id('subscription',i),pg_temp.dl_id('est',i),m+1,
       (month + make_interval(months=>m-12))::timestamp AT TIME ZONE 'America/Sao_Paulo',
       (month + make_interval(months=>m-11))::timestamp AT TIME ZONE 'America/Sao_Paulo',
       1,'00:00','America/Sao_Paulo',p.id,p.name,p.monthly_price,p.volume_limit_liters,p.collection_limit
FROM generate_series(0,12) months(m),generate_series(1,30) est(i),bi_clock,subscription_plan p
WHERE p.id=pg_temp.dl_id('plan',3);

DO $$
DECLARE m INTEGER; i INTEGER; k INTEGER; j INTEGER:=0; dt TIMESTAMPTZ; req UUID; event UUID;
        est UUID; drv UUID; vol NUMERIC; vmonth DATE; today DATE; n INTEGER; validator UUID; citizen UUID;
BEGIN
  SELECT month,bi_clock.today INTO vmonth,today FROM bi_clock;
  FOR m IN 0..12 LOOP
    FOR i IN 1..30 LOOP
      est:=pg_temp.dl_id('est',i); drv:=pg_temp.dl_id('driver',1+i%5);
      FOR k IN 1..2 LOOP
        j:=j+1;
        -- No mês atual usa o dia 1; nunca cria fato físico futuro.
        dt:=((vmonth+make_interval(months=>m-12))::date
              + CASE WHEN m=12 THEN 0 ELSE 3+k*7+(i%5) END + time '00:00')
              AT TIME ZONE 'America/Sao_Paulo';
        vol:=round((8+(i*7+k*3+m*5)%65 + m*0.37)::numeric,2);
        INSERT INTO collection_request(id,estimated_volume_liters,status,request_at,establishment_id,
          subscription_cycle_id,idempotency_key,approved_by,approved_at,scheduled_at,
          arrived_at,arrival_recorded_at,arrival_driver_id,service_accepted_at,
          service_acceptance_recorded_at,service_accepted_by,observation)
        VALUES(pg_temp.dl_id('bi_request',j),vol,'APPROVED',dt,est,
          pg_temp.dl_id('bi_cycle',m*30+i),pg_temp.dl_id('bi_request_key',j),
          pg_temp.dl_id('admin',1),dt,dt,dt,dt,drv,dt,dt,est,'[DEMO BI] Importação histórica')
        RETURNING id INTO req;
        PERFORM pg_temp.dl_actor('DRIVER_FORM',NULL,drv);
        IF j%9=0 THEN
          CALL record_collection(req,drv,0,0,'NOT_ASSESSED',FALSE,
            ARRAY['VOLUME_OUT_OF_TOLERANCE'],'[DEMO BI] Sem óleo na visita',dt,event);
        ELSE
          CALL record_collection(req,drv,vol,vol,'ACCEPTABLE',FALSE,
            ARRAY[]::text[],'[DEMO BI] Coleta histórica',dt,event);
        END IF;
      END LOOP;
    END LOOP;
    FOR n IN 1..40 LOOP
      i:=1+(n%8); citizen:=pg_temp.dl_id('citizen',20+n);
      validator:=CASE WHEN i<=4 THEN pg_temp.dl_id('citizen',i) ELSE pg_temp.dl_id('est',i-4) END;
      dt:=((vmonth+make_interval(months=>m-12))::date
           + CASE WHEN m=12 THEN 0 ELSE 1+n%25 END + time '00:00') AT TIME ZONE 'America/Sao_Paulo';
      PERFORM pg_temp.dl_actor('USER',validator);
      CALL record_pev_delivery(pg_temp.dl_id('bi_delivery_key',m*40+n),citizen,pg_temp.dl_id('pev',i),
        validator,round((0.5+n%12+m*0.19)::numeric,2),dt,event);
    END LOOP;
  END LOOP;
  -- Quinze pendentes, dez para hoje, dez para o fim do mês, cinco rejeitadas,
  -- cinco canceladas. O dia final pode coincidir com hoje: categorias se sobrepõem.
  PERFORM pg_temp.dl_actor('SYSTEM');
  FOR n IN 1..45 LOOP
    i:=1+(n-1)%30; est:=pg_temp.dl_id('est',i);
    dt:=(today::timestamp AT TIME ZONE 'America/Sao_Paulo');
    INSERT INTO collection_request(id,estimated_volume_liters,status,request_at,establishment_id,
      subscription_cycle_id,idempotency_key)
    VALUES(pg_temp.dl_id('bi_open_request',n),10,'PENDING',dt,est,
      pg_temp.dl_id('bi_cycle',12*30+i),pg_temp.dl_id('bi_open_key',n)) RETURNING id INTO req;
    IF n BETWEEN 16 AND 35 OR n>=41 THEN
      PERFORM pg_temp.dl_actor('USER',pg_temp.dl_id('admin',1));
      CALL schedule_collection_request(req,
        CASE WHEN n<=25 THEN dt+interval '18 hours'
             ELSE ((vmonth+interval '1 month'-interval '1 day')::date + time '18:00')
                  AT TIME ZONE 'America/Sao_Paulo' END,
        clock_timestamp(),pg_temp.dl_id('admin',1),'[DEMO BI] Agenda de testes');
      IF n>=41 THEN
        CALL cancel_collection_request(req,pg_temp.dl_id('admin',1),'OPERATION','[DEMO BI] Cancelamento operacional');
      END IF;
    ELSIF n BETWEEN 36 AND 40 THEN
      PERFORM reject_collection_request(req,pg_temp.dl_id('admin',1),'[DEMO BI] Rejeitada');
    END IF;
  END LOOP;
END $$;
SELECT pg_temp.dl_assert((SELECT count(*) FROM collection)=780,'780 coletas esperadas');
SELECT pg_temp.dl_assert((SELECT count(*) FROM delivery_pev)=520,'520 entregas esperadas');
SELECT pg_temp.dl_assert(NOT EXISTS(SELECT 1 FROM subscription_cycle c
  CROSS JOIN LATERAL get_cycle_availability(c.id) a
  WHERE a.reserved_collection_slots+a.consumed_collection_slots+a.forfeited_collection_slots>c.collection_limit
     OR a.reserved_volume_liters+a.consumed_volume_liters+a.forfeited_volume_liters>c.volume_limit_liters),
  'Consumo deve respeitar os limites dos ciclos');
