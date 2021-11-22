
CREATE OR REPLACE FUNCTION returnBiggestFrequentAttribute (quasiindentifyingattributes character varying[], tableName varchar)
RETURNS varchar
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE 
		biggestDistinctAttribute varchar;
		tempNum integer;
		BEGIN 
		tempNum:=0;
		for counter in 1..array_length(quasiIndentifyingAttributes,1) loop
			if (returnSepQIFreq(quasiIndentifyingAttributes[counter], tableName)>tempNum) then
				tempNum:=returnSepQIFreq(quasiIndentifyingAttributes[counter], tableName);
				biggestDistinctAttribute:=quasiIndentifyingAttributes[counter];
			end if;		
		end loop;
		return biggestDistinctAttribute;
		END;
	$$;
	
CREATE OR REPLACE FUNCTION returnMinimalSetOfQIsFrequency(quasiIdentifiers varchar[], tableName varchar)
RETURNS integer
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE
		minFreq integer;
		qiInVar varchar;
		BEGIN
		qiInVar:= array_to_string(quasiIdentifiers, ',', '*');
			EXECUTE 'SELECT min(counter) 
					from (select '|| qiInVar ||', count(*) AS counter
					from '||tableName||'_anon b 
					group by '|| qiInVar ||')sub'
			INTO minFreq
			USING qiInVar, tableName;
		return minFreq;
		END;
  	$$;  
CREATE OR REPLACE FUNCTION returnSepQIFreq(columnName varchar, tableName varchar)
RETURNS integer
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE 
		distinctCounter integer;
		BEGIN
		EXECUTE 'SELECT COUNT(DISTINCT ' || columnName ||') FROM '||tableName||'_anon' INTO distinctCounter
		USING distinctCounter;
		RETURN distinctCounter;	
		END;
	$$;

CREATE OR REPLACE FUNCTION anon(k_param integer, quasiIdentifiers varchar[], tableName varchar)
RETURNS void
language plpgsql
	AS $$
		DECLARE
		allAttributes varchar[];
		counter integer;
		BEGIN
		PERFORM doesTableExist(tableName);
		FOR	counter IN 1..array_length(quasiIdentifiers,1) LOOP
			PERFORM doesColumnExistInTable(quasiIdentifiers[counter],tableName);
		END LOOP;
		PERFORM generateViewForAnonimization(tableName);
		counter:=returnMinimalSetOfQIsFrequency(quasiIdentifiers,tableName);
 		WHILE counter<k_param LOOP
			  PERFORM generalize(returnBiggestFrequentAttribute(quasiIdentifiers, tableName),tableName);
			  counter:=returnMinimalSetOfQIsFrequency(quasiIdentifiers, tableName);
			  RAISE WARNING ' Min Number ''%'' grouped',
              counter;
 			  END LOOP;
		END;
	$$;

CREATE OR REPLACE FUNCTION generateViewForAnonimization(tableName varchar)
RETURNS void
language plpgsql
	AS $$ 
	BEGIN
	IF (SELECT EXISTS (
			   SELECT FROM information_schema.tables 
			   WHERE  table_schema = 'public'
			   AND    table_name   = tableName ||'_anon'
			   ) IS false ) THEN
		EXECUTE 'CREATE OR REPLACE VIEW '||tableName||'_anon AS SELECT * FROM '||tableName;
		raise warning 'lmao';
	END IF;
	END;
	$$;

CREATE OR REPLACE FUNCTION generalize(columnName varchar, tableName varchar)
returns void
language plpgsql
	AS $$
		DECLARE 
		generalizationRule varchar;
		newLevel integer;
		BEGIN 
			EXECUTE 'SELECT generalization_rule, lvl 
					from vgh 
					where attr='''|| columnName ||'''
					and lvl=(select min(lvl) 
			   				from vgh 
			   				where attr='''||columnName||'''
			   				and is_active=false
							and tbl='''|| tableName||''')'
				INTO generalizationRule,newLevel
				USING columnName, tableName;
			RAISE WARNING 'Column ''%'' is being generalized',
                    columnName;
			IF newLevel IS NULL THEN
				RAISE EXCEPTION 'Not enough generalization levels for column ''%''',
						columnName;
				EXECUTE 'DROP VIEW '||tableName||'_anon';
			END IF;
			PERFORM dataTypeChangerAndDefinitionExecutor(columnName, generalizationRule,newLevel,tableName);
			UPDATE vgh
			SET is_active = true
			WHERE vgh.lvl=newLevel and attr=$1 and tbl=$2;	
		END;
	$$;
CREATE OR REPLACE FUNCTION dataTypeChangerAndDefinitionExecutor(columnName varchar,generalizationRule varchar, newLevel integer, tableName varchar)
RETURNS void
LANGUAGE plpgsql STRICT
	AS $$ 
		DECLARE 
		viewDefinition varchar;
		currentDataTypeId varchar;
		generalizationFunctionName varchar;
		caster varchar;
		previousFunctionDefinition varchar;
		finalFunctionWithParameters varchar;
		previousGeneralizationRule varchar;
		BEGIN 
		caster:='';
		currentDataTypeId := (SELECT atttypid 
						  FROM pg_class c
						  JOIN pg_attribute a
						  ON c.oid=a.attrelid
						  WHERE relname=tableName AND attname=columnName);
						  
		CASE 
 			WHEN currentDataTypeId IN('1082')
 			THEN
			WHEN currentDataTypeId IN('701','3906','23')
			THEN caster:='::numeric';
		END CASE;
		viewDefinition := REPLACE((select pg_get_viewdef(tableName||'_anon', true)),'::text','');
 		viewDefinition := REPLACE(viewDefinition,'::character varying','');
		SELECT current_function, generalization_rule
		INTO previousFunctionDefinition,previousGeneralizationRule
		from vgh 
		where attr=$1 and lvl=$3-1 and tbl=$4;						
		generalizationFunctionName := (SELECT current_function 
									   from vgh 
									   where attr=$1 and lvl=$3 and tbl=$4);
		IF newLevel <> 1 THEN 
		previousFunctionDefinition:=  previousFunctionDefinition || 
										'('||tableName||'.'|| columnName || caster ||', '''|| previousGeneralizationRule ||''') AS ' ||columnName;
		END IF;
		finalFunctionWithParameters := generalizationFunctionName ||
								   '('||tableName||'.'||columnName || caster ||', '''||generalizationRule ||''') AS '||columnName;	
		viewDefinition := REPLACE (viewDefinition, previousFunctionDefinition,finalFunctionWithParameters);					   
		EXECUTE 'DROP VIEW '||tableName||'_anon';
		EXECUTE 'CREATE VIEW '||tableName||'_anon AS' || viewDefinition::TEXT;
		END;
	$$;

CREATE OR REPLACE FUNCTION addLevelGeneralization(columnName varchar,tableName varchar,generalizationRule varchar, newLevel integer,currentFunction varchar)
RETURNS void
LANGUAGE plpgsql STRICT
	AS $$  
         DECLARE
		 BEGIN 
		 PERFORM doesTableExist(tableName);
		 PERFORM doesColumnExistInTable(columnName, tableName);
		 PERFORM checkIfLevelForFunctionAlreadyExists (columnName, tableName, currentFunction, newLevel);
		 PERFORM checkIfGeneralizationRuleAlreadyExists (columnName, tableName, currentFunction, generalizationRule);
		 IF(SELECT EXISTS (
			   SELECT FROM information_schema.tables 
			   WHERE  table_schema = 'public'
			   AND    table_name   = 'vgh'
			   ) IS FALSE) THEN 	
			 create table vgh 
				(lvl integer,
				 tbl varchar,
				 attr varchar,
				generalization_rule VARCHAR,
				is_active boolean,
				 current_function varchar,
				PRIMARY KEY (lvl,attr));
		 END IF;
		 IF (SELECT EXISTS
			 	(SELECT 1 from vgh where attr=columnName) IS FALSE) THEN
		 	EXECUTE 'INSERT INTO VGH(lvl,tbl,attr, generalization_rule, is_active,current_function)
						VALUES (0,'''|| tableName ||''','''|| columnName ||''',NULL,TRUE,'''||tableName ||'.'||columnName||''')'
						USING columnName,tableName;
		 END IF;
		 EXECUTE 'INSERT INTO VGH(lvl,tbl,attr,generalization_rule,is_active,current_function) 
		 		  VALUES ($1,$5, $2, $3, FALSE,$4)'
				  USING newLevel, columnName, generalizationRule,currentFunction,tableName;
		 END;
	$$;
CREATE OR REPLACE FUNCTION checkIfLevelForFunctionAlreadyExists(columnName varchar, tableName varchar, currentFunction varchar, newLevel integer)
RETURNS void
LANGUAGE plpgsql STRICT
	AS $$
	BEGIN 
	IF (SELECT EXISTS (SELECT 1 FROM vgh WHERE attr=$1 and tbl = $2 and current_function=$5 and lvl=$4) IS TRUE) THEN
		RAISE EXCEPTION ' ''%'' column ''%'' already has generalization function with level %',tableName, columnName, newLevel
		USING HINT = 'If you want to update current level of the column use function updateLevelGeneralization';
	END IF;
	END;
	$$;
CREATE OR REPLACE FUNCTION checkIfGeneralizationRuleAlreadyExists (columnName varchar, tableName varchar, currentFunction varchar, generalizationRule varchar)
RETURNS void
LANGUAGE plpgsql STRICT 
	AS $$ 
	BEGIN 
	IF (SELECT EXISTS (SELECT 1 FROM vgh WHERE attr=$1 and tbl =$2 and current_function=$5 and generalizationRule=$3) IS TRUE) THEN 
		RAISE EXCEPTION ' ''%'' column ''%'' already has generalization function rule %',tableName, columnName, generalizationRule;
	END IF;
	END;
	$$;
CREATE OR REPLACE FUNCTION doesColumnExistInTable(columnName varchar, tableName varchar)
returns boolean
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE 
		colname varchar;
		BEGIN 
  			SELECT column_name INTO colname
  			FROM information_schema.columns
  			WHERE table_name=tableName::TEXT
  			AND column_name=columnName::TEXT;
  			IF colname IS NULL THEN
    		  RAISE EXCEPTION 'Column ''%'' is not present in table ''%''.',
                    columnName,
                    tableName;
    		  RETURN FALSE;
			ELSE 
				RETURN TRUE;			
  			END IF;
		END;
	$$;

CREATE OR REPLACE FUNCTION doesTableExist(tableName varchar)
RETURNS void
LANGUAGE plpgsql STRICT
	AS $$ 
		BEGIN
		IF (SELECT EXISTS (
			   SELECT FROM information_schema.tables 
			   WHERE  table_schema = 'public'
			   AND    table_name   =  tableName
			   ) IS FALSE) THEN
			RAISE EXCEPTION 'Table ''%'' does not exist',
				tableName;
		END IF;
		END;
	$$;


CREATE OR REPLACE FUNCTION updateLevelGeneralization(columnName varchar, tableName varchar, generalizationRule varchar, generalizationLvl integer)
RETURNS boolean
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE
		ifExist boolean;
		levelNum integer;
		BEGIN 
		 PERFORM doesTableExist(tableName);
		 PERFORM doesColumnExistInTable(columnName, tableName);
		 PERFORM checkIfGeneralizationRuleAlreadyExists (columnName, tableName, currentFunction, generalizationRule);
				IF (SELECT EXISTS (
				   SELECT lvl
					from vgh 
					where lvl = generalizationLvl
					and attr = columnName::TEXT
					and tbl = tableName
				   ) IS FALSE) THEN 
					RAISE EXCEPTION 'Level ''%'' is not present in column ''%''.',
                    generalizationLvl,
                    columnName;
    		  		RETURN FALSE;
				ELSE
					EXECUTE 'UPDATE vgh SET generalizationRule = $1
							WHERE columnName= $2 and lvl= $3'
							USING generalizationRule, columnName, generalizationLvl;
					RETURN TRUE;
				END IF;
		END;
	$$;
CREATE OR REPLACE FUNCTION removeLevelGeneralization(columnName varchar, tableName varchar, generalizationLvl integer)
RETURNS boolean
LANGUAGE plpgsql STRICT
	AS $$
		DECLARE
		levelNum integer;
		BEGIN 
		PERFORM doesTableExist(tableName);
		PERFORM doesColumnExistInTable(columnName, tableName);
		SELECT lvl into levelNum 
				from vgh 
				where lvl = generalizationLvl
				and attr = columnName::TEXT;
				IF levelNum IS NULL THEN 
					RAISE EXCEPTION 'Level ''%'' is not present in column ''%''.',
                    generalizationLvl,
                    columnName;
    		  		RETURN FALSE;
				ELSE
					EXECUTE 'DELETE FROM vgh
							WHERE tbl=$3, attr= $1 and lvl= $2'
							USING columnName::TEXT, generalizationLvl, tableName;
					RETURN TRUE;
				END IF;	
		END;
	$$;	
