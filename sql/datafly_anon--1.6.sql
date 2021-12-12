CREATE OR REPLACE FUNCTION return_attribute_with_most_distinct_values (qi_attributes character varying[], target_schema_name varchar, target_table_name varchar)
RETURNS varchar
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE 
		attribute_with_most_distinct_values varchar;
		temp_num integer;
		test integer;
		BEGIN 
		temp_num:=0;
		for counter in 1..array_length(qi_attributes,1) loop
			if (return_attr_distinct_count(qi_attributes[counter], target_schema_name, target_table_name)>temp_num) then
				temp_num:=return_attr_distinct_count(qi_attributes[counter], target_schema_name,target_table_name);
				attribute_with_most_distinct_values:=qi_attributes[counter];
			end if;		
		end loop;
		return attribute_with_most_distinct_values;
		END;
	$$;
	
CREATE OR REPLACE FUNCTION return_k_parameter_from_current_dataset(qi_attributes varchar[], target_schema_name varchar, target_table_name varchar)
RETURNS integer
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE
		k_param integer;
		qi varchar;
		BEGIN
		qi:= array_to_string(qi_attributes, ',', '*');
			EXECUTE 'SELECT min(counter) 
					from (select '|| qi ||', count(*) AS counter
					from '||target_schema_name||'.'||target_table_name ||' b 
					group by '|| qi ||')sub'
			INTO k_param
			USING qi, target_schema_name, target_table_name;
 	 		raise notice '%',k_param;
		return k_param;
		END;
  	$$;  
CREATE OR REPLACE FUNCTION return_attr_distinct_count(attribute_name varchar, target_schema_name varchar, target_table_name varchar)
RETURNS integer
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE 
		distinct_count integer;
		BEGIN
		EXECUTE 'SELECT COUNT(DISTINCT ' || attribute_name ||') FROM '||target_schema_name||'.'||target_table_name INTO distinct_count
		USING distinct_count;
		raise notice '% %', attribute_name, distinct_count; 
		RETURN distinct_count;	
		END;
	$$;
-- 	DROP FUNCTION init_datafly(integer,character varying,character varying,character varying,character varying,boolean,boolean)
CREATE OR REPLACE FUNCTION init_datafly(k integer, sch_name varchar, tbl_name varchar, target_sch_name varchar, target_view varchar, test_mode bool,  is_triggered bool default false)
RETURNS void
language plpgsql
	AS $$
		DECLARE
		qi_attributes varchar[];
		counter integer;
		BEGIN
		IF (does_table_exist(sch_name, tbl_name) IS FALSE) THEN 
			RAISE EXCEPTION 'Table % in schema % does not exist', sch_name, tbl_name;
		END IF;
		IF (SELECT EXISTS (select 1 FROM generalization_config gc INNER JOIN anonymized_tables an ON gc.original_and_anonymized_objects_id=an.id
							   where table_name = tbl_name and schema_name=sch_name and target_view_name=target_view and target_schema_name=target_sch_name) IS FALSE) THEN
			RAISE EXCEPTION 'Specified tables are not configured';							   
		END IF;							   
		qi_attributes:= array (select distinct attr FROM generalization_config gc INNER JOIN anonymized_tables an ON gc.original_and_anonymized_objects_id=an.id
							   where table_name = tbl_name and schema_name=sch_name and target_view_name=target_view and target_schema_name=target_sch_name);
		raise notice '%',qi_attributes;
		FOR	counter IN 1..array_length(qi_attributes,1) LOOP
			PERFORM does_column_exist_in_table(qi_attributes[counter],sch_name, tbl_name);
		END LOOP;
		target_view:= generate_init_view( sch_name , tbl_name, target_sch_name,target_view, test_mode, is_triggered);
		counter:=return_k_parameter_from_current_dataset(qi_attributes, target_sch_name, target_view);	
		raise notice '%', k;
 		WHILE counter<k LOOP
			  PERFORM generalize(return_attribute_with_most_distinct_values(qi_attributes, target_sch_name, target_view),target_sch_name,target_view, sch_name, tbl_name);
			  counter:=return_k_parameter_from_current_dataset(qi_attributes, target_sch_name, target_view);
 		END LOOP;
		IF (is_triggered is false) then
			PERFORM generate_triggers(k,sch_name, tbl_name,target_sch_name,target_view );
		END IF;
		UPDATE anonymized_tables at set k_param = $1 WHERE at.schema_name=sch_name and at.table_name=tbl_name and at.target_schema_name=target_sch_name and at.target_view_name=target_view;	  
		END;
	$$;
-- select generate_init_view('public','patients_information','secured','patients_information_anon', true,false);
CREATE OR REPLACE FUNCTION generate_init_view(sch_name varchar, tbl_name varchar, target_sch_name varchar, target_view varchar, test_mode bool, is_triggered bool)
RETURNS varchar
language plpgsql
	AS $$ 
	DECLARE 
	exist bool;
	final_view_name varchar;
	BEGIN
	final_view_name := target_view;
	IF(test_mode) THEN
		FOR counter in 1..100000000 loop
				IF does_table_exist(target_sch_name, final_view_name) is false then 
					EXECUTE 'CREATE OR REPLACE VIEW '||target_sch_name||'.'||final_view_name||' AS SELECT * FROM '||sch_name||'.'||tbl_name; 
					exit;
				ELSE 
					final_view_name:=target_view||'_'||counter;
				END IF;
		END LOOP;
		IF (final_view_name <> target_view) THEN

			INSERT INTO anonymized_tables(schema_name,table_name,target_schema_name,target_view_name) 
			SELECT
			schema_name, table_name, target_schema_name, final_view_name 
		    FROM anonymized_tables at
			WHERE at.schema_name = sch_name and at.table_name=tbl_name and at.target_schema_name = target_sch_name and at.target_view_name=target_view;		
		 	
			INSERT INTO generalization_config (lvl,original_and_anonymized_objects_id, attr, generalization_rule, is_active, current_function)
			SELECT 
			lvl, (SELECT id FROM anonymized_tables 
				  WHERE schema_name = sch_name and 
				  table_name = tbl_name and target_schema_name = target_sch_name and 
				  target_view_name = final_view_name), attr, generalization_rule, is_active, current_function
			FROM generalization_config gc INNER JOIN anonymized_tables at ON gc.original_and_anonymized_objects_id=at.id
			WHERE schema_name = sch_name and tbl_name=table_name and target_sch_name = target_schema_name and target_view_name = target_view;
			
			UPDATE generalization_config SET is_active = false 
			WHERE original_and_anonymized_objects_id=(SELECT id FROM anonymized_tables 
				  WHERE schema_name = sch_name and 
				  table_name = tbl_name and target_schema_name = target_sch_name and 
				  target_view_name = final_view_name) and lvl <> 0;		
		END IF;
		return final_view_name;
	ELSE 
		IF (is_triggered IS FALSE) THEN
			IF (does_table_exist(target_sch_name,final_view_name) IS TRUE) THEN 
			EXECUTE 'DROP VIEW '|| target_sch_name||'.'||target_view;
			UPDATE generalization_config SET is_active = false 
			WHERE original_and_anonymized_objects_id=(SELECT id FROM anonymized_tables 
				  WHERE schema_name = sch_name and 
				  table_name = tbl_name and target_schema_name = target_sch_name and 
				  target_view_name = final_view_name) and lvl <> 0;		
			END IF;
		EXECUTE 'CREATE OR REPLACE VIEW '||target_sch_name||'.'||target_view||' AS SELECT * FROM '||sch_name||'.'||tbl_name;
		END IF;
		return final_view_name;
	END IF;
	END;
	$$;


CREATE OR REPLACE FUNCTION init_datafly_tg() RETURNS TRIGGER
 LANGUAGE plpgsql
 AS
$$
   BEGIN
     PERFORM init_datafly( CAST (TG_ARGV[0] AS INTEGER),TG_ARGV[1],TG_ARGV[2],TG_ARGV[3],TG_ARGV[4], false, true);
	 RETURN NEW;
   END;
$$;
-- DROP FUNCTION generalization_changer_and_view_executor(character varying,character varying,integer,character varying,character varying,character varying,character varying)
--  select generate_triggers (3,'public','patients_information','secured','patients_information_anon_1')
CREATE OR REPLACE FUNCTION generate_triggers (k_param integer,schema_name varchar, table_name varchar, target_schema_name varchar, target_table_name varchar)
returns void 
language plpgsql 
	AS $$
		DECLARE 
		BEGIN 
		EXECUTE 'DROP TRIGGER IF EXISTS '||target_schema_name||'_'||target_table_name ||'_insert_trigger ON '||schema_name||'.'||table_name;
		EXECUTE 'DROP TRIGGER IF EXISTS '||target_schema_name||'_'||target_table_name ||'_update_trigger ON '||schema_name||'.'||table_name;
		EXECUTE 'DROP TRIGGER IF EXISTS '||target_schema_name||'_'||target_table_name ||'_delete_trigger ON '||schema_name||'.'||table_name;
		EXECUTE 'CREATE TRIGGER '||target_schema_name||'_'||target_table_name ||'_insert_trigger AFTER INSERT ON '||schema_name||'.'||table_name ||
		' EXECUTE FUNCTION init_datafly_tg('||k_param||','||schema_name||','||table_name||','||target_schema_name||','||target_table_name||')';
		EXECUTE 'CREATE TRIGGER '||target_schema_name||'_'||target_table_name ||'_delete_trigger AFTER DELETE ON '||schema_name||'.'||table_name ||
		' EXECUTE FUNCTION init_datafly_tg('||k_param||','||schema_name||','||table_name||','||target_schema_name||','||target_table_name||')';
		EXECUTE 'CREATE TRIGGER '||target_schema_name||'_'||target_table_name ||'_update_trigger AFTER UPDATE ON '||schema_name||'.'||table_name ||
		' EXECUTE FUNCTION init_datafly_tg('||k_param||','||schema_name||','||table_name||','||target_schema_name||','||target_table_name||')';		
		END;
	$$;
	
CREATE OR REPLACE FUNCTION generalize(attribute_name varchar, target_sch_name varchar, target_view varchar, sch_name varchar, tbl_name varchar)
returns void
language plpgsql
	AS $$
		DECLARE 
		generalization_rule varchar;
		new_level integer;
		BEGIN 
			EXECUTE 'SELECT generalization_rule, lvl 
					from generalization_config gc inner join anonymized_tables an
					ON gc.original_and_anonymized_objects_id=an.id
					where gc.attr='''|| attribute_name ||'''
					and lvl=(select min(lvl) 
			   				from generalization_config 
			   				where attr='''||attribute_name||'''
			   				and is_active=false
							and schema_name = '''||sch_name||''' 
							and table_name='''||tbl_name||''' 
							and target_schema_name = '''||target_sch_name||''' 
							and target_view_name = '''||target_view||'''
							)'
				INTO generalization_rule,new_level;
			RAISE WARNING 'Column ''%'' is being generalized with rule %',
                    attribute_name, generalization_rule;
			raise notice '%',target_view;
			IF new_level IS NULL THEN
				RAISE EXCEPTION 'Not enough generalization levels for column ''%'' % % % % % % ',
						attribute_name, generalization_rule, new_level, target_sch_name , target_view , sch_name , tbl_name  ;
			END IF;
			PERFORM generalization_changer_and_view_executor(attribute_name, generalization_rule, new_level,target_sch_name, target_view, sch_name, tbl_name);
			UPDATE generalization_config
			SET is_active = true
			WHERE generalization_config.lvl=new_level and attr=$1 and original_and_anonymized_objects_id=(SELECT id FROM anonymized_tables 
				  WHERE schema_name = sch_name and 
				  table_name = tbl_name and target_schema_name = target_sch_name and 
				  target_view_name = target_view) ;	
		END;
	$$;
-- select init_datafly(2,'public','patients_information','secured','patients_information_anon', true);	
CREATE OR REPLACE FUNCTION generalization_changer_and_view_executor(attribute_name varchar,generalize_rule varchar, new_level integer, target_sch_name varchar, target_view varchar, sch_name varchar,tbl_name varchar)
RETURNS void
LANGUAGE plpgsql STRICT
	AS $$ 
		DECLARE 
		view_definition varchar;
		current_data_type_id varchar;
		generalization_function varchar;
		caster varchar;
		previous_function_definition varchar;
		final_function_with_parameters varchar;
		previous_generalization_rule varchar;
		BEGIN 
		caster:='';
		current_data_type_id := (SELECT atttypid 
						  FROM pg_class c
						  JOIN pg_attribute a
						  ON c.oid=a.attrelid
						  WHERE relname=tbl_name AND attname=attribute_name);
						  
		CASE 
			WHEN current_data_type_id IN('701','3906','23')
			THEN caster:='::numeric';
			ELSE caster:='';
		END CASE;
		view_definition := REPLACE((select pg_get_viewdef(target_sch_name||'.'||target_view, true)),'::text','');
 		view_definition := REPLACE(view_definition,'::character varying','');
		
		SELECT current_function, generalization_rule
		INTO previous_function_definition,previous_generalization_rule
		from generalization_config 
		where attr=$1 and lvl=$3-1 and original_and_anonymized_objects_id=(SELECT id FROM anonymized_tables 
									   WHERE schema_name = sch_name and 
									   table_name = tbl_name and target_schema_name = target_sch_name and 
									   target_view_name = target_view);						
		generalization_function := (SELECT current_function 
									   FROM generalization_config 
									   WHERE attr=$1 and lvl=$3 and original_and_anonymized_objects_id=(SELECT id FROM anonymized_tables 
									   WHERE schema_name = sch_name and 
									   table_name = tbl_name and target_schema_name = target_sch_name and 
									   target_view_name = target_view));
		IF new_level <> 1 THEN 
		previous_function_definition:=  previous_function_definition || 
										'('||tbl_name||'.'|| attribute_name || caster ||', '''|| previous_generalization_rule ||''') AS ' ||attribute_name;
		END IF;
		final_function_with_parameters := generalization_function ||
								   '('||tbl_name||'.'||attribute_name || caster ||', '''||generalize_rule ||''') AS '||attribute_name;	
		view_definition := REPLACE (view_definition, previous_function_definition,final_function_with_parameters);	
		EXECUTE 'DROP VIEW '|| target_sch_name||'.'||target_view;
		EXECUTE 'CREATE VIEW '|| target_sch_name||'.'||target_view||' AS' || view_definition::TEXT;
		END;
	$$;
-- DROP FUNCTION add_level_generalization(character varying,character varying,character varying,character varying,integer,character varying,character varying,character varying)
-- select add_level_generalization ('public','svoris','patients_information','200',18,'generalize_numrange','secured','patients_information_anon',true)
CREATE OR REPLACE FUNCTION add_level_generalization(sch_name varchar, attribute_name varchar,tbl_name varchar,generalization_rule varchar, new_level integer,current_function varchar, target_sch_name varchar, target_view varchar, re_init_anon bool)
RETURNS void
LANGUAGE plpgsql STRICT
	AS $$  
         DECLARE
		 k integer;
		 BEGIN 
 		 IF (does_table_exist(sch_name, tbl_name) IS false) THEN 
			RAISE EXCEPTION 'Table % in schema % does not exist', tbl_name, sch_name;
		 END IF;
		 IF (does_column_exist_in_table(attribute_name, sch_name, tbl_name) IS FALSE) THEN 
			RAISE EXCEPTION 'Attribute % in table % does not exist', attribute_name, tbl_name;
		 END IF;
		 IF (check_if_generalization_rule_exists(attribute_name, sch_name, tbl_name, generalization_rule, target_sch_name, target_view) IS TRUE) THEN 
			RAISE EXCEPTION 'Rule % in anonymized view % for attribute % already exists', generalization_rule, target_view, attribute_name;
		 END IF;
		 IF (check_if_level_exists(attribute_name, sch_name, tbl_name, new_level, target_sch_name, target_view) IS TRUE) THEN
		 	RAISE 'Level % already exist in anonymized view % for attribute %', new_level, target_view, attribute_name;
		 END IF;
	     IF (SELECT EXISTS
			 	(SELECT 1 from generalization_config where attr=attribute_name) IS FALSE) THEN
		 	EXECUTE 'INSERT INTO generalization_config(lvl,original_and_anonymized_objects_id, attr,generalization_rule,is_active,current_function)
						VALUES (0,(SELECT id FROM anonymized_tables WHERE schema_name = $2 and table_name = $3 and target_schema_name = $4 and target_view_name = $5),
						  $6,NULL,TRUE,'''||tbl_name ||'.'||attribute_name||''')'
						USING new_level, sch_name, tbl_name, target_sch_name, target_view, attribute_name, generalization_rule, current_function;
		 END IF;
		 EXECUTE 'INSERT INTO generalization_config(lvl,original_and_anonymized_objects_id, attr,generalization_rule,is_active,current_function) 
		 		  VALUES ($1,(SELECT id FROM anonymized_tables WHERE schema_name = $2 and table_name = $3 and target_schema_name = $4 and target_view_name = $5),
						  $6,$7,FALSE,$8)'
				  USING new_level, sch_name, tbl_name, target_sch_name, target_view, attribute_name, generalization_rule, current_function;
		 
		 IF (re_init_anon IS TRUE) THEN
			k := (SELECT k_param from anonymized_tables where schema_name = $1 and table_name = $3 and target_schema_name = $7 and target_view_name = $8);
			EXECUTE 'UPDATE generalization_config SET is_active = FALSE
				  WHERE original_and_anonymized_objects_id=(SELECT id FROM anonymized_tables WHERE schema_name = $1 and table_name = $2 and target_schema_name = $3 and target_view_name = $4)
				  and lvl <> 0'
			USING sch_name,tbl_name, target_sch_name, target_view;
			IF(k is null) THEN
				RAISE EXCEPTION 'There are no previous executions for specified original and target objects. You cannot re-execute datafly if it was not executed initially, please use init_datafly function';
			ELSE 
				PERFORM init_datafly(k, sch_name, tbl_name, target_sch_name, target_view, FALSE);
		    END IF;
		  END IF;
		 END;
	$$;
CREATE OR REPLACE FUNCTION check_if_level_exists(attribute_name varchar, schema_name varchar,table_name varchar, new_level integer, target_schema_name varchar, target_table_name varchar)
RETURNS boolean
LANGUAGE plpgsql STRICT
	AS $$
	BEGIN 
	IF (SELECT EXISTS (SELECT 1 FROM generalization_config gc 
					   INNER JOIN anonymized_tables at ON gc.original_and_anonymized_objects_id=at.id 
					   WHERE gc.attr=$1 and at.schema_name=$2 AND at.table_name = $3 and gc.lvl=$4
					   AND at.target_schema_name=$5 AND at.target_view_name=$6) 
					   IS TRUE) THEN
		RETURN TRUE;
	ELSE 
		RETURN FALSE;
	END IF;
	END;
	$$;
CREATE OR REPLACE FUNCTION check_if_generalization_rule_exists (attribute_name varchar, schema_name varchar, table_name varchar, rule varchar, target_schema_name varchar, target_table_name varchar)
RETURNS boolean
LANGUAGE plpgsql STRICT 
	AS $$ 
	BEGIN 
		IF (SELECT EXISTS (SELECT 1 FROM generalization_config gc 
						   INNER JOIN anonymized_tables at on gc.original_and_anonymized_objects_id=at.id  
						   WHERE gc.attr=$1 and at.schema_name =$2 and at.table_name=$3 and at.target_schema_name=$5 
						   and at.target_view_name=$6 and gc.generalization_rule=$4) 
						   IS TRUE) THEN 
			RETURN TRUE;
		ELSE 
			RETURN FALSE;
		END IF;
	END;
	$$;

CREATE OR REPLACE FUNCTION does_column_exist_in_table(attribute_name varchar, sch_name varchar,tbl_name varchar)
returns boolean
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE 
		colname varchar;
		BEGIN 
  			SELECT column_name INTO colname
  			FROM information_schema.columns
  			WHERE table_name=tbl_name::TEXT
			AND table_schema=sch_name::TEXT
  			AND column_name=attribute_name::TEXT;
  			IF colname IS NULL THEN
    		  RETURN FALSE;
			ELSE 
			 RETURN TRUE;			
  			END IF;
		END;
	$$;	
CREATE OR REPLACE FUNCTION does_table_exist(schema_name varchar, tbl_name varchar)
RETURNS boolean
LANGUAGE plpgsql STRICT
	AS $$ 
		BEGIN
		IF (SELECT EXISTS (
			   SELECT FROM information_schema.tables 
			   WHERE  table_schema = schema_name
			   AND    table_name   =  tbl_name
			   ) IS FALSE) THEN
			return false;
		ELSE
			return true;
		END IF;
		END;
	$$;
-- 	select update_level_generalization('svoris','public','patients_information','secured','patients_information_anon','190','17',false)
CREATE OR REPLACE FUNCTION update_level_generalization(attribute_name varchar, sch_name varchar, tbl_name varchar, target_sch_name varchar, target_view varchar, generalization_rule varchar, generalization_lvl integer, re_init_anon bool default false)
RETURNS void
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE
		if_exist boolean;
		level_num integer;
		k integer;
		BEGIN 
		 IF (check_if_generalization_rule_exists(attribute_name, sch_name, tbl_name, generalization_rule, target_sch_name, target_view) IS TRUE) THEN 
			RAISE EXCEPTION 'Rule % in anonymized view % for attribute % already exists', generalization_rule, target_table_name, attribute_name;
		 END IF;
		 IF (check_if_level_exists(attribute_name, sch_name, tbl_name, generalization_lvl, target_sch_name, target_view) IS FALSE) THEN
		 	RAISE EXCEPTION 'Level % does not exist in anonymized view % for attribute % or table is not configured for anonymization', generalization_lvl, target_table_name, attribute_name;
		 END IF;
		 EXECUTE 'UPDATE generalization_config SET generalization_rule = $1
				  WHERE attr= $2 and lvl= $3 and original_and_anonymized_objects_id=(SELECT id FROM anonymized_tables WHERE schema_name = $4 and table_name = $5 and target_schema_name = $6 and target_view_name = $7) '
		 USING generalization_rule, attribute_name, generalization_lvl,sch_name,tbl_name, target_sch_name, target_view;
		 IF (re_init_anon IS TRUE) THEN
			k := (SELECT k_param from anonymized_tables where schema_name = $2 and table_name = $3 and target_schema_name = $4 and target_view_name = $5);
			EXECUTE 'UPDATE generalization_config SET is_active = FALSE
				  WHERE original_and_anonymized_objects_id=(SELECT id FROM anonymized_tables WHERE schema_name = $1 and table_name = $2 and target_schema_name = $3 and target_view_name = $4)
				  and lvl <> 0'
			USING sch_name,tbl_name, target_sch_name, target_view;
			IF(k is null) THEN
				RAISE EXCEPTION 'There are no previous executions for specified original and target objects. You cannot re-execute datafly if it was not executed initially, please use init_datafly function';
			ELSE 
				PERFORM init_datafly(k, sch_name, tbl_name, target_sch_name, target_view, FALSE);
		    END IF;
		  END IF;
		 END;
	$$;
-- 	select remove_level_generalization('ugis','patients_information','public','secured','patients_information_anon',17,false) 
CREATE OR REPLACE FUNCTION remove_level_generalization(attribute_name varchar, tbl_name varchar, sch_name varchar,  target_sch_name varchar, target_view varchar, generalization_lvl integer,re_init_anon bool)
RETURNS void
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE
		k integer;
		BEGIN 
		  IF (SELECT EXISTS (SELECT 1 FROM generalization_config WHERE attr=$1 and original_and_anonymized_objects_id=(SELECT id FROM anonymized_tables WHERE schema_name = $3 and table_name = $2 and target_schema_name = $4 and target_view_name = $5) and lvl=$6) IS TRUE) THEN
				EXECUTE 'DELETE FROM generalization_config
							WHERE attr= $1 and lvl= $2 and original_and_anonymized_objects_id=(SELECT id FROM anonymized_tables WHERE schema_name = $3 and table_name = $4 and target_schema_name = $6 and target_view_name = $5)'
							USING attribute_name::TEXT, generalization_lvl, sch_name, tbl_name, target_view, target_sch_name;
				ELSE
					RAISE EXCEPTION 'Generalization level % does not exist or table generalization is not configured',generalization_lvl;
		 END IF;		 
		 IF (re_init_anon IS TRUE) THEN
			k := (SELECT k_param from anonymized_tables where schema_name = $3 and table_name = $2 and target_schema_name = $4 and target_view_name = $5);
			EXECUTE 'UPDATE generalization_config SET is_active = FALSE
				  WHERE original_and_anonymized_objects_id=(SELECT id FROM anonymized_tables WHERE schema_name = $1 and table_name = $2 and target_schema_name = $3 and target_view_name = $4)
				  and lvl <> 0'
			USING sch_name,tbl_name, target_sch_name, target_view;
			IF(k is null) THEN
				RAISE EXCEPTION 'There are no previous executions for specified original and target objects. You cannot re-execute datafly if it was not executed initially, please use init_datafly function';
			ELSE 
				PERFORM init_datafly(k, sch_name, tbl_name, target_sch_name, target_view, FALSE);
		    END IF;
		  END IF;
		 
		END;
	$$;	

CREATE OR REPLACE FUNCTION configure_plugin(json_config jsonb)
RETURNS void
LANGUAGE plpgsql STRICT
 	AS $$
	DECLARE 
		sch_name varchar;
		tbl_name varchar;
		target_sch_name varchar;
		target_tbl_name varchar;
		quasi_identifiers json;
		quasi_identifiers_info json;
		quasi_identifiers_generalization json;
		BEGIN 
		sch_name := json_config ->>'schName';
		tbl_name := json_config ->>'tblName';
		target_sch_name := json_config ->>'targetSchemaName';
		target_tbl_name := json_config ->>'targetTableName';
 		quasi_identifiers := json_config->'quasiIdentifiers';
		IF(does_table_exist(sch_name,'anonymized_tables') IS FALSE) THEN
			EXECUTE 'CREATE TABLE ' || sch_name || '.anonymized_tables
				(id SERIAL PRIMARY KEY,
				schema_name varchar,
				table_name varchar,
				target_schema_name varchar,
				target_view_name varchar,
				k_param integer,
				PRIMARY KEY (schema_name,table_name, target_schema_name, target_view_name))';
		END IF;	
		IF(does_table_exist(sch_name,'generalization_config') IS FALSE) THEN 	
			 EXECUTE 'create table '|| sch_name ||'.generalization_config 
				(lvl integer,
			     original_and_anonymized_objects_id integer, 
				 attr varchar,
				generalization_rule VARCHAR,
				is_active boolean,
				 current_function varchar,
				PRIMARY KEY (lvl,attr,original_and_anonymized_objects_id),
				CONSTRAINT fk_target_and_source_objects
				FOREIGN KEY(original_and_anonymized_objects_id)
				REFERENCES anonymized_tables(id))';
		 END IF;
							
		IF(sch_name is null or tbl_name is null or target_sch_name is null or target_tbl_name is null or quasi_identifiers is null) THEN 
			RAISE EXCEPTION 'Json is not set correctly';
		END IF;
		IF(does_table_exist(sch_name,tbl_name) IS FALSE) THEN
			RAISE EXCEPTION 'Table % does not exist in schema %', tbl_name, sch_name;
		END IF;
		INSERT INTO anonymized_tables(schema_name,table_name,target_schema_name,target_view_name) values (sch_name,tbl_name,target_sch_name,target_tbl_name);
		for counter in 0 .. json_array_length(quasi_identifiers)-1 loop
			quasi_identifiers_info := quasi_identifiers->>counter;
			quasi_identifiers_generalization := quasi_identifiers_info ->>'generalizationConfiguraion';
			for counter in 0 ..json_array_length(quasi_identifiers_generalization)-1 loop 
				IF (quasi_identifiers_info->>'attrName' is null or quasi_identifiers_generalization->counter->>'generalizationRule' is null or quasi_identifiers_generalization->counter->>'level' is null or quasi_identifiers_generalization->counter->>'generalizationFunction' is null or quasi_identifiers_generalization->counter->>'generalizationFunction' not in ('generalize_numrange', 'generalize_daterange') ) then
					RAISE EXCEPTION 'Json is not set correctly';
				END IF;
				perform add_level_generalization(sch_name,quasi_identifiers_info->>'attrName'::VARCHAR,tbl_name, quasi_identifiers_generalization->counter->>'generalizationRule'::VARCHAR, CAST(quasi_identifiers_generalization->counter->>'level' AS INTEGER), quasi_identifiers_generalization->counter->>'generalizationFunction'::VARCHAR,target_sch_name,target_tbl_name);		
				end loop;
		end loop;
		END;
	$$;

	CREATE OR REPLACE FUNCTION generalize_numrange(
  val NUMERIC,
  step VARCHAR 
)
RETURNS NUMRANGE
AS $$
WITH i AS (
  SELECT int4range(
    val::INTEGER / step::INTEGER * step::INTEGER,
    ((val::INTEGER / step::INTEGER)+1) * step::INTEGER
  ) as r
)
SELECT numrange(
    lower(i.r)::NUMERIC,
    upper(i.r)::NUMERIC
  )
FROM i
;
$$
LANGUAGE SQL IMMUTABLE SECURITY INVOKER;

CREATE OR REPLACE FUNCTION generalize_daterange(
  val DATE,
  step TEXT DEFAULT 'decade'
)
RETURNS DATERANGE
AS $$
SELECT daterange(
    date_trunc(step, val)::DATE,
    (date_trunc(step, val) + ('1 '|| step)::INTERVAL)::DATE
  );
$$
LANGUAGE SQL IMMUTABLE SECURITY INVOKER;



CREATE OR REPLACE FUNCTION generalize_numrange_fractional(
  val NUMERIC,
  step VARCHAR 
)
RETURNS NUMRANGE
AS $$

SELECT numrange(
	
	trim(trailing '00' FROM (val / CAST(step AS NUMERIC) * CAST(step AS NUMERIC))::varchar)::numeric,
	trim(trailing '00' FROM (((val / CAST(step AS NUMERIC))+1) * CAST(step AS NUMERIC))::varchar)::numeric
)

;
$$
LANGUAGE SQL IMMUTABLE SECURITY INVOKER;