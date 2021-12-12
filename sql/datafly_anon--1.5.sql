CREATE OR REPLACE FUNCTION return_attribute_with_most_distinct_values (qi_attributes character varying[], target_schema_name varchar, target_table_name varchar)
RETURNS varchar
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE 
		attribute_with_most_distinct_values varchar;
		temp_num integer;
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
		RETURN distinct_count;	
		END;
	$$;
CREATE OR REPLACE FUNCTION init_datafly(k_param integer, schema_name varchar, table_name varchar, target_schema_name varchar, target_table_name varchar)
RETURNS void
language plpgsql
	AS $$
		DECLARE
		qi_attributes varchar[];
		counter integer;
		BEGIN
		IF (does_table_exist(schema_name, table_name) IS FALSE) THEN 
			RAISE EXCEPTION 'Table % in schema % does not exist', table_name, schema_name;
		END IF;
		qi_attributes:= array (select distinct attr from vgh where tbl = table_name and sch_name=schema_name and target_tbl_name=target_table_name and target_sch_name=target_schema_name);
		raise notice '%',qi_attributes;
		FOR	counter IN 1..array_length(qi_attributes,1) LOOP
			PERFORM does_column_exist_in_table(qi_attributes[counter],table_name);
		END LOOP;
		target_table_name:= generate_init_view( schema_name , table_name, target_schema_name,target_table_name);
		counter:=return_k_parameter_from_current_dataset(qi_attributes, target_schema_name, target_table_name);	
 		WHILE counter<k_param LOOP
			  PERFORM generalize(return_attribute_with_most_distinct_values(qi_attributes, target_schema_name, target_table_name),target_schema_name,target_table_name, schema_name, table_name);
			  counter:=return_k_parameter_from_current_dataset(qi_attributes, target_schema_name, target_table_name);
 			  END LOOP;
		END;
	$$;
CREATE OR REPLACE FUNCTION generate_init_view(schema_name varchar, table_name varchar, target_schema_name varchar, target_table_name varchar)
RETURNS varchar
language plpgsql
	AS $$ 
	DECLARE 
	exist bool;
	final_table_name varchar;
	BEGIN
	final_table_name := target_table_name;
		FOR counter in 1..100000000 loop
				IF does_table_exist(target_schema_name, final_table_name) is false then 
					EXECUTE 'CREATE OR REPLACE VIEW '||target_schema_name||'.'||final_table_name||' AS SELECT * FROM '||schema_name||'.'||table_name; 
					return final_table_name;
					exit;
				ELSE 
					final_table_name:=target_table_name||'_'||counter;
				END IF;
		END LOOP;
	END;
	$$;
CREATE OR REPLACE FUNCTION generalize(attribute_name varchar, target_schema_name varchar, target_table_name varchar, schema_name varchar, table_name varchar)
returns void
language plpgsql
	AS $$
		DECLARE 
		generalization_rule varchar;
		new_level integer;
		BEGIN 
			EXECUTE 'SELECT generalization_rule, lvl 
					from vgh 
					where attr='''|| attribute_name ||'''
					and lvl=(select min(lvl) 
			   				from vgh 
			   				where attr='''||attribute_name||'''
			   				and is_active=false
							and tbl='''|| table_name||'''
							and sch_name='''||schema_name||'''
							and target_tbl_name='''|| target_table_name||'''
							and target_sch_name='''||target_schema_name||'''
							)'
				INTO generalization_rule,new_level
				USING attribute_name, target_table_name,target_schema_name;
			RAISE WARNING 'Column ''%'' is being generalized',
                    attribute_name;
			IF new_level IS NULL THEN
				RAISE EXCEPTION 'Not enough generalization levels for column ''%'' % % % % % % ',
						attribute_name, generalization_rule, new_level, target_schema_name , target_table_name , schema_name , table_name  ;
			END IF;
			PERFORM generalization_changer_and_view_executor(attribute_name, generalization_rule, new_level,target_schema_name, target_table_name, schema_name, table_name);
			UPDATE vgh
			SET is_active = true
			WHERE vgh.lvl=new_level and attr=$1 and tbl=$5 and sch_name=$4 and target_tbl_name =$3 and target_sch_name = $2;	
		END;
	$$;
CREATE OR REPLACE FUNCTION generalization_changer_and_view_executor(attribute_name varchar,generalize_rule varchar, new_level integer, target_schema_name varchar, target_table_name varchar, schema_name varchar,table_name varchar)
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
						  WHERE relname=table_name AND attname=attribute_name);
						  
		CASE 
			WHEN current_data_type_id IN('701','3906','23')
			THEN caster:='::numeric';
			ELSE caster:='';
		END CASE;
		view_definition := REPLACE((select pg_get_viewdef(target_schema_name||'.'||target_table_name, true)),'::text','');
 		view_definition := REPLACE(view_definition,'::character varying','');
		
		SELECT current_function, generalization_rule
		INTO previous_function_definition,previous_generalization_rule
		from vgh 
		where attr=$1 and lvl=$3-1 and tbl=$7 and sch_name=$6;						
		generalization_function := (SELECT current_function 
									   from vgh 
									   where attr=$1 and lvl=$3 and tbl=$7 and sch_name = $6 and target_tbl_name=$5 and target_sch_name=$4);
		IF new_level <> 1 THEN 
		previous_function_definition:=  previous_function_definition || 
										'('||table_name||'.'|| attribute_name || caster ||', '''|| previous_generalization_rule ||''') AS ' ||attribute_name;
		END IF;
		final_function_with_parameters := generalization_function ||
								   '('||table_name||'.'||attribute_name || caster ||', '''||generalize_rule ||''') AS '||attribute_name;	
		view_definition := REPLACE (view_definition, previous_function_definition,final_function_with_parameters);	
		RAISE notice 'View %, %',
						final_function_with_parameters, previous_function_definition;
		EXECUTE 'DROP VIEW '|| target_schema_name||'.'||target_table_name;
		EXECUTE 'CREATE VIEW '|| target_schema_name||'.'||target_table_name||' AS' || view_definition::TEXT;
		END;
	$$;
	

CREATE OR REPLACE FUNCTION add_level_generalization(schema_name varchar, attribute_name varchar,tbl_name varchar,generalization_rule varchar, new_level integer,current_function varchar, target_sch_name varchar, target_tbl_name varchar)
RETURNS void
LANGUAGE plpgsql STRICT
	AS $$  
         DECLARE
		 BEGIN 
		 		 IF(SELECT EXISTS (
			   SELECT FROM information_schema.tables 
			   WHERE  table_schema = schema_name
			   AND    table_name   = 'vgh'
			   ) IS FALSE) THEN 	
			 create table vgh 
				(lvl integer,
				 sch_name varchar,
				 tbl varchar,
				 target_sch_name varchar,
				 target_tbl_name varchar,
				 attr varchar,
				generalization_rule VARCHAR,
				is_active boolean,
				 current_function varchar,
				PRIMARY KEY (lvl,attr,sch_name,tbl,target_sch_name,target_tbl_name));
		 END IF;
-- 		 PERFORM does_table_exist(schema_name,table_name);
-- 		 PERFORM does_column_exist_in_table(attribute_name, table_name);
-- 		 PERFORM check_if_level_for_function_already_exists (attribute_name, table_name, current_function, new_level);
-- 		 PERFORM check_if_generalization_rule_exists (attribute_name, table_name, current_function, generalization_rule);
		 IF (SELECT EXISTS
			 	(SELECT 1 from vgh where attr=attribute_name) IS FALSE) THEN
		 	EXECUTE 'INSERT INTO VGH(lvl,sch_name,tbl,target_sch_name, target_tbl_name, attr, generalization_rule, is_active,current_function)
						VALUES (0,'''||schema_name||''','''|| tbl_name ||''','''||target_sch_name||''','''||target_tbl_name||''','''|| attribute_name ||''',NULL,TRUE,'''||tbl_name ||'.'||attribute_name||''')'
						USING attribute_name,tbl_name, schema_name;
		 END IF;
		 EXECUTE 'INSERT INTO VGH(lvl,sch_name,tbl,target_sch_name, target_tbl_name, attr,generalization_rule,is_active,current_function) 
		 		  VALUES ($1,$6,$5,$7,$8, $2, $3, FALSE,$4)'
				  USING new_level, attribute_name, generalization_rule,current_function,tbl_name,schema_name, target_sch_name, target_tbl_name;
		 END;
	$$;
CREATE OR REPLACE FUNCTION check_if_level_for_function_already_exists(attribute_name varchar, table_name varchar, current_function varchar, new_level integer)
RETURNS void
LANGUAGE plpgsql STRICT
	AS $$
	BEGIN 
	IF (SELECT EXISTS (SELECT 1 FROM vgh WHERE attr=$1 and tbl = $2 and current_function=$3 and lvl=$4) IS TRUE) THEN
		RAISE EXCEPTION ' ''%'' column ''%'' already has generalization function with level %',table_name, attribute_name, new_level
		USING HINT = 'If you want to update current level of the column use function update_level_generalization';
	END IF;
	END;
	$$;
CREATE OR REPLACE FUNCTION check_if_generalization_rule_exists (attribute_name varchar, table_name varchar, current_function varchar, generalization_rule varchar)
RETURNS void
LANGUAGE plpgsql STRICT 
	AS $$ 
	BEGIN 
	IF (SELECT EXISTS (SELECT 1 FROM vgh WHERE attr=$1 and tbl =$2 and current_function=$3 and generalization_rule=$4) IS TRUE) THEN 
		RAISE EXCEPTION ' ''%'' column ''%'' already has generalization function rule %',table_name, attribute_name, generalization_rule;
	END IF;
	END;
	$$;
CREATE OR REPLACE FUNCTION does_column_exist_in_table(attribute_name varchar, tbl_name varchar)
returns boolean
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE 
		colname varchar;
		BEGIN 
  			SELECT column_name INTO colname
  			FROM information_schema.columns
  			WHERE table_name=tbl_name::TEXT
  			AND column_name=attribute_name::TEXT;
  			IF colname IS NULL THEN
    		  RAISE EXCEPTION 'Column ''%'' is not present in table ''%''.',
                    attribute_name,
                    tbl_name;
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

CREATE OR REPLACE FUNCTION update_level_generalization(attribute_name varchar, table_name varchar, generalization_rule varchar, generalization_lvl integer)
RETURNS boolean
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE
		if_exist boolean;
		level_num integer;
		BEGIN 
		 PERFORM does_table_exist(table_name);
		 PERFORM does_column_exist_in_table(attribute_name, table_name);
		 PERFORM check_if_generalization_rule_exists (attribute_name, table_name, current_function, generalization_rule);
				IF (SELECT EXISTS (
				   SELECT lvl
					from vgh 
					where lvl = generalization_lvl
					and attr = attribute_name::TEXT
					and tbl = table_name
				   ) IS FALSE) THEN 
					RAISE EXCEPTION 'Level ''%'' is not present in column ''%''.',
                    generalization_lvl,
                    attribute_name;
    		  		RETURN FALSE;
				ELSE
					EXECUTE 'UPDATE vgh SET generalization_rule = $1
							WHERE attribute_name= $2 and lvl= $3'
							USING generalization_rule, attribute_name, generalization_lvl;
					RETURN TRUE;
				END IF;
		END;
	$$;
CREATE OR REPLACE FUNCTION removeLevelGeneralization(attribute_name varchar, table_name varchar, generalization_lvl integer)
RETURNS boolean
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE
		level_num integer;
		BEGIN 
		PERFORM does_table_exist(table_name);
		PERFORM does_column_exist_in_table(attribute_name, table_name);
		SELECT lvl into level_num 
				from vgh 
				where lvl = generalization_lvl
				and attr = attribute_name::TEXT;
				IF level_num IS NULL THEN 
					RAISE EXCEPTION 'Level ''%'' is not present in column ''%''.',
                    generalization_lvl,
                    attribute_name;
    		  		RETURN FALSE;
				ELSE
					EXECUTE 'DELETE FROM vgh
							WHERE tbl=$3, attr= $1 and lvl= $2'
							USING attribute_name::TEXT, generalization_lvl, table_name;
					RETURN TRUE;
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
		for counter in 0 .. json_array_length(quasi_identifiers)-1 loop
			quasi_identifiers_info := quasi_identifiers->>counter;
			quasi_identifiers_generalization := quasi_identifiers_info ->>'generalizationConfiguraion';
			for counter in 0 ..json_array_length(quasi_identifiers_generalization)-1 loop
				perform add_level_generalization(sch_name,quasi_identifiers_info->>'attrName'::VARCHAR,tbl_name, quasi_identifiers_generalization->counter->>'generalizationRule'::VARCHAR, CAST(quasi_identifiers_generalization->counter->>'level' AS INTEGER), quasi_identifiers_generalization->counter->>'generalizationFunction'::VARCHAR,target_sch_name,target_tbl_name);
				raise notice 'values: %, %, %, %', quasi_identifiers_generalization->counter->>'level', quasi_identifiers_generalization->counter->>'generalizationRule',quasi_identifiers_generalization->counter->>'generalizationFunction',sch_name;				
				end loop;
		end loop;
		END;
	$$;
CREATE OR REPLACE FUNCTION generalize_int4range(
  val INTEGER,
  step INTEGER default 10
)
RETURNS INT4RANGE
AS $$
SELECT int4range(
    val / step * step,
    ((val / step)+1) * step
  );
$$
LANGUAGE SQL IMMUTABLE SECURITY INVOKER;	
	
	CREATE OR REPLACE FUNCTION generalize_numrange(
  val NUMERIC,
  step VARCHAR 
)
RETURNS NUMRANGE
AS $$
WITH i AS (
  SELECT generalize_int4range(val::INTEGER,CAST(step AS INTEGER)) as r
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