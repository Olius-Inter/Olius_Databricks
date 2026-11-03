-- Carga ADICIONAL: preserva os registros existentes. Executar o arquivo inteiro.
BEGIN ISOLATION LEVEL REPEATABLE READ;
SET LOCAL search_path=public,pg_temp;
-- Helpers temporários da carga: não acrescentam tabelas ou rotinas à aplicação.
CREATE TEMP TABLE dl_config ON COMMIT DROP AS SELECT '{"source": "Contexto do Projeto - OLIUS.docx / Regras de Negocio - OLIUS.docx, pasta 01_Sobre/Técnico", "plans": [{"name": "[DEMO BI CLOUD] Plano Básico", "monthly_price": "19.90", "volume_limit_liters": "50.00", "collection_limit": 2}, {"name": "[DEMO BI CLOUD] Plano Profissional", "monthly_price": "39.90", "volume_limit_liters": "220.00", "collection_limit": 4}, {"name": "[DEMO BI CLOUD] Plano Empresa", "monthly_price": "79.90", "volume_limit_liters": "500.00", "collection_limit": 8}], "certificate_levels": []}'::JSONB AS value;
CREATE OR REPLACE FUNCTION pg_temp.dl_id(kind TEXT, number INTEGER) RETURNS UUID
LANGUAGE SQL IMMUTABLE AS $$ SELECT md5('olius-bi-cloud-v1:' || kind || ':' || number)::UUID $$;
CREATE OR REPLACE FUNCTION pg_temp.dl_assert(condition BOOLEAN, explanation TEXT) RETURNS VOID
LANGUAGE plpgsql AS $$ BEGIN
    IF condition IS DISTINCT FROM TRUE THEN RAISE EXCEPTION 'Data Load: %', explanation; END IF;
END $$;
CREATE OR REPLACE FUNCTION pg_temp.dl_actor(actor TEXT, user_id UUID DEFAULT NULL, driver_id UUID DEFAULT NULL)
RETURNS VOID LANGUAGE plpgsql AS $$ BEGIN
    PERFORM set_config('app.audit_actor', actor, TRUE);
    PERFORM set_config('app.current_user_id', COALESCE(user_id::TEXT, ''), TRUE);
    PERFORM set_config('app.operational_driver_id', COALESCE(driver_id::TEXT, ''), TRUE);
    PERFORM set_config('app.audit_reason', 'Massa artificial OLIUS Data Load v1', TRUE);
END $$;
-- CPF/CNPJ com dígitos verificadores para dados sintéticos; não comprovam titularidade.
CREATE OR REPLACE FUNCTION pg_temp.dl_document(number INTEGER, cnpj BOOLEAN DEFAULT FALSE) RETURNS TEXT
LANGUAGE plpgsql AS $$
DECLARE digits TEXT := lpad((820000000 + number)::TEXT, CASE WHEN cnpj THEN 12 ELSE 9 END, '0');
        weights INTEGER[]; total INTEGER; digit INTEGER; i INTEGER; pass INTEGER;
BEGIN
    FOR pass IN 1..2 LOOP
        weights := CASE WHEN cnpj AND pass=1 THEN ARRAY[5,4,3,2,9,8,7,6,5,4,3,2]
                        WHEN cnpj THEN ARRAY[6,5,4,3,2,9,8,7,6,5,4,3,2] END;
        total := 0;
        FOR i IN 1..length(digits) LOOP
            total := total + substring(digits,i,1)::INTEGER *
                CASE WHEN cnpj THEN weights[i] ELSE length(digits)+2-i END;
        END LOOP;
        digit := CASE WHEN total % 11 < 2 THEN 0 ELSE 11-total%11 END;
        digits := digits || digit::TEXT;
    END LOOP;
    RETURN digits;
END $$;

CREATE TEMP TABLE bi_cloud_run(loaded BOOLEAN NOT NULL) ON COMMIT DROP;
INSERT INTO bi_cloud_run VALUES(FALSE);
DO $bi_gate$
DECLARE users_count INTEGER; collections_count INTEGER; deliveries_count INTEGER; requests_count INTEGER;
BEGIN
  IF NOT pg_try_advisory_xact_lock(71007, 2) THEN RAISE EXCEPTION 'Carga em andamento'; END IF;
  SELECT count(*) INTO users_count FROM users WHERE id IN (
    SELECT pg_temp.dl_id(kind,i) FROM (VALUES ('admin',2),('citizen',80),('est',30)) own(kind,quantity)
    CROSS JOIN LATERAL generate_series(1,quantity) n(i));
  IF users_count<>0 THEN
    SELECT count(*) INTO collections_count FROM collection WHERE establishment_id IN (
      SELECT pg_temp.dl_id('est',i) FROM generate_series(1,30) n(i));
    SELECT count(*) INTO deliveries_count FROM delivery_pev WHERE citizen_id IN (
      SELECT pg_temp.dl_id('citizen',i) FROM generate_series(1,80) n(i));
    SELECT count(*) INTO requests_count FROM collection_request WHERE establishment_id IN (
      SELECT pg_temp.dl_id('est',i) FROM generate_series(1,30) n(i));
    IF users_count=112 AND collections_count=780 AND deliveries_count=520 AND requests_count=825 THEN
      RAISE NOTICE 'Massa BI CLOUD já carregada; nenhuma operação será duplicada.';
      RETURN;
    END IF;
    RAISE EXCEPTION 'Massa BI CLOUD parcial ou alterada; nenhuma linha foi modificada. Revise os registros de demonstração.';
  END IF;
  EXECUTE $bi_mass$
SELECT pg_temp.dl_actor('SYSTEM');

INSERT INTO subscription_plan(id,name,description,monthly_price,volume_limit_liters,collection_limit)
SELECT pg_temp.dl_id('plan',ordinality::INTEGER), p->>'name',
       'Oferta oficial documentada; registro instalado para a demonstração.',
       (p->>'monthly_price')::NUMERIC, (p->>'volume_limit_liters')::NUMERIC,
       (p->>'collection_limit')::INTEGER
FROM dl_config, jsonb_array_elements(value->'plans') WITH ORDINALITY AS x(p,ordinality);
-- Metas não definidas ficam vazias; jamais inferir metas a partir dos pontos.
INSERT INTO certificate_level(id,name,required_liters)
SELECT pg_temp.dl_id('level',ordinality::INTEGER), p->>'name', (p->>'required_liters')::NUMERIC
FROM dl_config, jsonb_array_elements(value->'certificate_levels') WITH ORDINALITY AS x(p,ordinality);

INSERT INTO establishment_type(id,name,description) VALUES
(pg_temp.dl_id('type',1),'[DEMO BI CLOUD] Restaurante','Categoria usada pelos estabelecimentos fictícios.'),
(pg_temp.dl_id('type',2),'[DEMO BI CLOUD] Lanchonete','Categoria usada pelos estabelecimentos fictícios.'),
(pg_temp.dl_id('type',3),'[DEMO BI CLOUD] Padaria','Categoria usada pelos estabelecimentos fictícios.');
INSERT INTO collection_failure_reason(code,name) VALUES
('VOLUME_OUT_OF_TOLERANCE','Volume fora da faixa'),
('OIL_UNACCEPTABLE','Óleo inadequado'),
('COMPROMISING_OCCURRENCE','Ocorrência comprometedora') ON CONFLICT (code) DO NOTHING;
SELECT pg_temp.dl_assert((SELECT count(*) FROM collection_failure_reason WHERE code IN ('VOLUME_OUT_OF_TOLERANCE','OIL_UNACCEPTABLE','COMPROMISING_OCCURRENCE') AND status='ACTIVE')=3,'Os três motivos oficiais de falha devem estar ativos');

INSERT INTO users(id,name,email,password_hash,user_type)
SELECT pg_temp.dl_id(kind,i),
       CASE kind WHEN 'admin' THEN '[DEMO BI CLOUD] Administrador ' || i
            WHEN 'est' THEN '[DEMO BI CLOUD] Estabelecimento ' || lpad(i::TEXT,3,'0')
            ELSE '[DEMO BI CLOUD] ' || (ARRAY['Ana','Bruno','Carla','Daniel','Elisa','Felipe','Gabriela','Henrique','Isabela','João'])[1+((i-1)%10)]
                 || ' ' || (ARRAY['Almeida','Barbosa','Costa','Dias','Ferreira','Gomes','Lima','Moreira'])[1+((i-1)/10)] END,
       kind || '.' || i || '@bi-cloud.olius.example.invalid',
       '!DEMO_ACCOUNT_NO_LOGIN!', profile::user_type_t
FROM (VALUES ('admin','ADMIN',2),('citizen','CITIZENS',80),('est','ESTABLISHMENT',30)) x(kind,profile,quantity)
CROSS JOIN LATERAL generate_series(1,quantity) n(i);
-- Estas contas não servem para autenticação: nenhum hash de senha utilizável é distribuído.
INSERT INTO user_qr_code(user_id,qr_token,user_type)
SELECT id, md5(id::TEXT || ':demonstration-qr'), user_type FROM users WHERE user_type<>'ADMIN' AND id IN (
      SELECT pg_temp.dl_id(kind,i)
      FROM (VALUES ('est',30),('citizen',80)) own(kind,quantity)
      CROSS JOIN LATERAL generate_series(1,quantity) n(i));
INSERT INTO citizens(id,cpf,qr_token)
SELECT pg_temp.dl_id('citizen',i),pg_temp.dl_document(i),md5(pg_temp.dl_id('citizen',i)::TEXT || ':demonstration-qr')
FROM generate_series(1,80) n(i);

INSERT INTO addresses(id,owner_kind,state,city,neighborhood,street,number,cep) VALUES
(pg_temp.dl_id('est_address',1),'ESTABLISHMENT','SP','São Paulo','Pinheiros','Avenida Rebouças','3970','05402600'),
(pg_temp.dl_id('est_address',2),'ESTABLISHMENT','SP','São Paulo','Jardim das Acácias','Avenida Roque Petroni Júnior','1089','04707900'),
(pg_temp.dl_id('est_address',3),'ESTABLISHMENT','SP','São Paulo','Moema','Avenida Ibirapuera','3103','04029902'),
(pg_temp.dl_id('est_address',4),'ESTABLISHMENT','SP','São Paulo','Jardim Universidade Pinheiros','Avenida Dra. Ruth Cardoso','4777','05477000'),
(pg_temp.dl_id('est_address',5),'ESTABLISHMENT','SP','São Paulo','Tatuapé','Avenida Regente Feijó','1739','03342900'),
(pg_temp.dl_id('est_address',6),'ESTABLISHMENT','SP','São Paulo','Pinheiros','Avenida Rebouças','3970','05402600'),
(pg_temp.dl_id('est_address',7),'ESTABLISHMENT','SP','São Paulo','Jardim das Acácias','Avenida Roque Petroni Júnior','1089','04707900'),
(pg_temp.dl_id('est_address',8),'ESTABLISHMENT','SP','São Paulo','Moema','Avenida Ibirapuera','3103','04029902'),
(pg_temp.dl_id('est_address',9),'ESTABLISHMENT','SP','São Paulo','Jardim Universidade Pinheiros','Avenida Dra. Ruth Cardoso','4777','05477000'),
(pg_temp.dl_id('est_address',10),'ESTABLISHMENT','SP','São Paulo','Tatuapé','Avenida Regente Feijó','1739','03342900'),
(pg_temp.dl_id('est_address',11),'ESTABLISHMENT','SP','São Paulo','Pinheiros','Avenida Rebouças','3970','05402600'),
(pg_temp.dl_id('est_address',12),'ESTABLISHMENT','SP','São Paulo','Jardim das Acácias','Avenida Roque Petroni Júnior','1089','04707900'),
(pg_temp.dl_id('est_address',13),'ESTABLISHMENT','SP','São Paulo','Moema','Avenida Ibirapuera','3103','04029902'),
(pg_temp.dl_id('est_address',14),'ESTABLISHMENT','SP','São Paulo','Jardim Universidade Pinheiros','Avenida Dra. Ruth Cardoso','4777','05477000'),
(pg_temp.dl_id('est_address',15),'ESTABLISHMENT','SP','São Paulo','Tatuapé','Avenida Regente Feijó','1739','03342900'),
(pg_temp.dl_id('est_address',16),'ESTABLISHMENT','SP','São Paulo','Pinheiros','Avenida Rebouças','3970','05402600'),
(pg_temp.dl_id('est_address',17),'ESTABLISHMENT','SP','São Paulo','Jardim das Acácias','Avenida Roque Petroni Júnior','1089','04707900'),
(pg_temp.dl_id('est_address',18),'ESTABLISHMENT','SP','São Paulo','Moema','Avenida Ibirapuera','3103','04029902'),
(pg_temp.dl_id('est_address',19),'ESTABLISHMENT','SP','São Paulo','Jardim Universidade Pinheiros','Avenida Dra. Ruth Cardoso','4777','05477000'),
(pg_temp.dl_id('est_address',20),'ESTABLISHMENT','SP','São Paulo','Tatuapé','Avenida Regente Feijó','1739','03342900'),
(pg_temp.dl_id('est_address',21),'ESTABLISHMENT','SP','São Paulo','Pinheiros','Avenida Rebouças','3970','05402600'),
(pg_temp.dl_id('est_address',22),'ESTABLISHMENT','SP','São Paulo','Jardim das Acácias','Avenida Roque Petroni Júnior','1089','04707900'),
(pg_temp.dl_id('est_address',23),'ESTABLISHMENT','SP','São Paulo','Moema','Avenida Ibirapuera','3103','04029902'),
(pg_temp.dl_id('est_address',24),'ESTABLISHMENT','SP','São Paulo','Jardim Universidade Pinheiros','Avenida Dra. Ruth Cardoso','4777','05477000'),
(pg_temp.dl_id('est_address',25),'ESTABLISHMENT','SP','São Paulo','Tatuapé','Avenida Regente Feijó','1739','03342900'),
(pg_temp.dl_id('est_address',26),'ESTABLISHMENT','SP','São Paulo','Pinheiros','Avenida Rebouças','3970','05402600'),
(pg_temp.dl_id('est_address',27),'ESTABLISHMENT','SP','São Paulo','Jardim das Acácias','Avenida Roque Petroni Júnior','1089','04707900'),
(pg_temp.dl_id('est_address',28),'ESTABLISHMENT','SP','São Paulo','Moema','Avenida Ibirapuera','3103','04029902'),
(pg_temp.dl_id('est_address',29),'ESTABLISHMENT','SP','São Paulo','Jardim Universidade Pinheiros','Avenida Dra. Ruth Cardoso','4777','05477000'),
(pg_temp.dl_id('est_address',30),'ESTABLISHMENT','SP','São Paulo','Tatuapé','Avenida Regente Feijó','1739','03342900'),
(pg_temp.dl_id('pev_address',1),'PEV','SP','São Paulo','Pinheiros','Avenida Rebouças','3970','05402600'),
(pg_temp.dl_id('pev_address',2),'PEV','SP','São Paulo','Jardim das Acácias','Avenida Roque Petroni Júnior','1089','04707900'),
(pg_temp.dl_id('pev_address',3),'PEV','SP','São Paulo','Moema','Avenida Ibirapuera','3103','04029902'),
(pg_temp.dl_id('pev_address',4),'PEV','SP','São Paulo','Jardim Universidade Pinheiros','Avenida Dra. Ruth Cardoso','4777','05477000'),
(pg_temp.dl_id('pev_address',5),'PEV','SP','São Paulo','Tatuapé','Avenida Regente Feijó','1739','03342900'),
(pg_temp.dl_id('pev_address',6),'PEV','SP','São Paulo','Pinheiros','Avenida Rebouças','3970','05402600'),
(pg_temp.dl_id('pev_address',7),'PEV','SP','São Paulo','Jardim das Acácias','Avenida Roque Petroni Júnior','1089','04707900'),
(pg_temp.dl_id('pev_address',8),'PEV','SP','São Paulo','Moema','Avenida Ibirapuera','3103','04029902');
INSERT INTO establishment(id,cnpj,qr_token,type_id,address_id,description)
SELECT pg_temp.dl_id('est',i),pg_temp.dl_document(i,TRUE),md5(pg_temp.dl_id('est',i)::TEXT || ':demonstration-qr'),
       pg_temp.dl_id('type',1+(i%3)),pg_temp.dl_id('est_address',i),'Estabelecimento artificial para testes de volume.'
FROM generate_series(1,30) n(i);
INSERT INTO telephone(telephone,user_id)
SELECT '119' || lpad(n::TEXT,8,'0'),id FROM (
    SELECT id,row_number() OVER(ORDER BY id) AS n FROM users WHERE user_type<>'ADMIN' AND id IN (
      SELECT pg_temp.dl_id(kind,i)
      FROM (VALUES ('est',30),('citizen',80)) own(kind,quantity)
      CROSS JOIN LATERAL generate_series(1,quantity) n(i))
) x;
INSERT INTO driver(id,name,cpf,cnh)
SELECT pg_temp.dl_id('driver',i),'[DEMO BI CLOUD] Motorista ' || i,pg_temp.dl_document(100+i),
       lpad((920000000+i)::TEXT,11,'0') FROM generate_series(1,5) n(i);

SELECT pg_temp.dl_actor('USER',pg_temp.dl_id('admin',1));
INSERT INTO pev(id,citizen_id,establishment_id,address_id,status,approved_by,approved_at)
SELECT pg_temp.dl_id('pev',i),CASE WHEN i<=4 THEN pg_temp.dl_id('citizen',i) END,
       CASE WHEN i>4 THEN pg_temp.dl_id('est',i-4) END,pg_temp.dl_id('pev_address',i),
       'APPROVED',pg_temp.dl_id('admin',1),clock_timestamp()
FROM generate_series(1,8) n(i);
-- Importação histórica acadêmica: cadastro dos ciclos/pedidos diretamente,
-- fatos, classificações, pontos e auditoria pelas rotinas oficiais.
-- Não simula faturamento: estes ciclos são fixtures, sem comprovação de pagamento.
SELECT pg_temp.dl_actor('SYSTEM');
CREATE TEMP TABLE bi_clock ON COMMIT DROP AS
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
SELECT pg_temp.dl_assert((SELECT count(*) FROM collection WHERE establishment_id IN (SELECT pg_temp.dl_id('est',i) FROM generate_series(1,30) n(i)))=780,'780 coletas esperadas');
SELECT pg_temp.dl_assert((SELECT count(*) FROM delivery_pev WHERE citizen_id IN (SELECT pg_temp.dl_id('citizen',i) FROM generate_series(1,80) n(i)))=520,'520 entregas esperadas');
SELECT pg_temp.dl_assert(NOT EXISTS(SELECT 1 FROM subscription_cycle c
  CROSS JOIN LATERAL get_cycle_availability(c.id) a
  WHERE c.establishment_id IN (SELECT pg_temp.dl_id('est',i) FROM generate_series(1,30) n(i)) AND (a.reserved_collection_slots+a.consumed_collection_slots+a.forfeited_collection_slots>c.collection_limit
     OR a.reserved_volume_liters+a.consumed_volume_liters+a.forfeited_volume_liters>c.volume_limit_liters)),
  'Consumo deve respeitar os limites dos ciclos');

$bi_mass$;
  UPDATE bi_cloud_run SET loaded=TRUE;
  CALL dm.refresh_data_mart();
END;
$bi_gate$;
SELECT CASE WHEN loaded THEN 'Carga adicional concluída: 780 coletas, 520 entregas, 825 solicitações; dm atualizado.'
       ELSE 'Carga BI CLOUD já existente; sem duplicações.' END AS resultado
FROM bi_cloud_run;
COMMIT;
