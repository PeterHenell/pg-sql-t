-- Copyright 2014 Peter Henell
-- 
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
-- 
--     http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.



drop schema IF EXISTS pgSQLt cascade;

create schema pgSQLt;

create table pgsqlt.test_class(
	class_id serial primary key,
	name text
);

create table pgSQLt.test_session(
	test_session_id serial primary key,
	started timestamptz not null default(now()),
	ended timestamptz null,
	is_active boolean  not null default(true)
);


create function pgSQLt.new_test_class(className text)
returns void
as
$$ 
DECLARE
	sql text;
begin

	sql := 'DROP SCHEMA IF EXISTS ' || className || ' CASCADE';
	EXECUTE (sql);
	sql := 'CREATE SCHEMA ' || className;
	EXECUTE (sql);
	insert into pgsqlt.test_class (name) values(className);
	
end
$$ language plpgsql;


CREATE FUNCTION pgSQLt.private_start_test_session()
RETURNS INT
AS
$$ 
DECLARE
	inserted_id INT;
BEGIN	
	INSERT INTO pgSQLt.test_session DEFAULT VALUES
	RETURNING test_session_id INTO inserted_id;

	RETURN inserted_id;	
END
$$ LANGUAGE plpgsql;


CREATE FUNCTION pgSQLt.private_end_test_session(session_id int)
RETURNS VOID
AS
$$ 
BEGIN	
	UPDATE pgSQLt.test_session 
		SET 
			is_active = false,
			ended = now()
	where test_session_id = session_id;
	
END
$$ LANGUAGE plpgsql;


create type pgSQLt.test_execution_result as ENUM ('OK', 'FAIL', 'ERROR');
create type pgSQLt.test_report as (test_name text, message text, result pgSQLt.test_execution_result);



create function pgSQLt.private_split_object_name(objectName text, out schema_name text , out object_name text )
returns record AS
$$
begin	
	select 
		case when strpos(objectName, '.') = 0 then 'public'
		     else split_part(objectName, '.', 1)
		end, 
		case when strpos(objectName, '.') = 0 then objectName
		     else split_part(objectName, '.', 2) 
		end
		
	into 
		schema_name, object_name; 
end
$$ language plpgsql;


CREATE FUNCTION pgSQLt.private_signal_test(report pgSQLt.test_report, session_id int = null, final_test_in_session boolean = true)
RETURNS pgSQLt.test_report AS 
$$
BEGIN
	if final_test_in_session then 
		perform pgSQLt.private_end_test_session(session_id);
	end if;
	return report;
end
$$ language plpgsql;


CREATE FUNCTION pgSQLt.Run(testName text, session_id int = null, final_test_in_session boolean = true) 
RETURNS pgSQLt.test_report AS 
$$
DECLARE 
	tc text;
	exceptionText text;
	report pgSQLt.test_report;
BEGIN 	
	select schema_name into tc from pgSQLt.private_split_object_name(testName);
	if not exists (select 1 FROM information_schema.schemata WHERE schema_name = lower(tc)) THEN
		raise exception 'Test class [%] does not exist, to add it run PERFORM pgSQLt.NewTestClass (''%'');', tc, tc;
	end if;

	if session_id is null then
		select pgSQLt.private_start_test_session() into session_id; 
	end if;
	
	raise notice 'Setting up test class [%]', tc;
	execute 'select ' || tc || '.setup();';
	
 	raise notice 'Running test [%]' ,testname;
	execute 'SELECT ' || testName || '();';

	raise exception using
            errcode='ALLOK',
            message='Test case successfull.',
            hint='This exception is only ment to rollback any changes made by the test.';

EXCEPTION 
	when sqlstate 'ALLOK' then
		return pgSQLt.private_signal_test(ROW(testName, 'Test succeded', 'OK')::pgSQLt.test_report);
	when sqlstate 'ASSRT' then
		GET STACKED DIAGNOSTICS 
			exceptionText = MESSAGE_TEXT;	
		return pgSQLt.private_signal_test(ROW(testName, 'Test FAILED to due assertion error [' || exceptionText || ']', 'FAIL')::pgSQLt.test_report);
	when others then
		GET STACKED DIAGNOSTICS 
			exceptionText = MESSAGE_TEXT;
		return pgSQLt.private_signal_test(ROW(testName, 'Test failed in ERROR due to [' || exceptionText ||']' , 'ERROR')::pgSQLt.test_report);	
END 
$$ LANGUAGE plpgsql;


CREATE FUNCTION pgSQLt.run_class(class_name text, out report pgSQLt.test_report) 
RETURNS setof pgSQLt.test_report AS 
$$
DECLARE 
	test_method RECORD;
	session_id int;
BEGIN

	select pgSQLt.private_start_test_session() into session_id;
	
	FOR test_method IN select routine_name as test, class_name as test_class from information_schema.routines 
		where routine_schema = lower(class_name) 
		and lower(routine_name) != 'setup' 
	LOOP
		select * into report from pgSQLt.run(format('%s.%s', test_method.test_class, test_method.test), session_id, false);
		return next;
	END LOOP;

	perform pgSQLt.private_end_test_session(session_id);
	
END
$$ LANGUAGE plpgsql;




create function pgsqlt.private_raise_assert_exception(message text) 
returns void
as
$$
BEGIN 
	raise exception using
            errcode='ASSRT',
            message=message,
            hint='This test failed due to assertion exception';
END 
$$ LANGUAGE plpgsql;


create function pgsqlt.private_raise_error_exception(message text) 
returns void
as
$$
BEGIN 
	raise exception using
            errcode='ERROR',
            message=message,
            hint='This test failed due to ERROR';
END 
$$ LANGUAGE plpgsql;


create function pgSQLt.private_assert_test_session_active() 
returns void AS 
$$
BEGIN 
	if not exists(select 1 from pgSQLt.test_session where is_active = true) then
		perform pgSQLt.private_raise_error_exception('Test session is not started, use pgSQLt.run or pgSQLt.run_class to execute test methods. Do not execute them directly.');
	end if;
		
END 
$$ LANGUAGE plpgsql;

create function pgSQLt.assert_equal_strings(expected text, actual text) 
returns void AS 
$$
BEGIN 
     perform pgSQLt.private_assert_test_session_active();

     if expected != actual then
	perform pgSQLt.private_raise_assert_exception(format('assert_equal_strings: Expected [%s] but got [%s]', expected, actual));
     end if;
END 
$$ LANGUAGE plpgsql;


CREATE FUNCTION pgSQLt.assert_empty_table(table_name text) RETURNS VOID AS 
$$
DECLARE
	a int;
BEGIN 
	perform pgSQLt.private_assert_test_session_active();
	
	EXECUTE 'SELECT 1 FROM ' || table_name || ' LIMIT 1'  INTO a ;
	IF a IS NOT NULL
	THEN
		perform pgSQLt.private_raise_assert_exception(format('assert_empty_table: Table [%s] was NOT empty',  table_name ));
	END IF;
END 
$$ LANGUAGE PLPGSQL;



CREATE FUNCTION pgSQLt.assert_not_empty_table(table_name text) RETURNS VOID AS 
$$
DECLARE
	a int;
BEGIN 
	perform pgSQLt.private_assert_test_session_active();
	
	EXECUTE 'SELECT 1 FROM ' || table_name || ' LIMIT 1'  INTO a ;
	IF a IS NULL
	THEN
		perform pgSQLt.private_raise_assert_exception(format('assert_not_empty_table: Table [%s] WAS empty',  table_name ));
	END IF;
END 
$$ LANGUAGE PLPGSQL;


CREATE FUNCTION pgSQLt.fail(message text) RETURNS VOID AS 
$$
DECLARE
	a int;
BEGIN 
	perform pgSQLt.private_assert_test_session_active();
	
	perform pgSQLt.private_raise_assert_exception(message);	
END 
$$ LANGUAGE PLPGSQL;


-- create a new table that have no constraints but same columns and datatypes
-- the old table should be renamed and then during test completion will be rolled back into normal state
CREATE FUNCTION pgSQLt.fake_table(table_name text) RETURNS VOID AS 
$$
DECLARE 
	obj_name text;
	sch_name text;
BEGIN 
	perform pgSQLt.private_assert_test_session_active();

	select object_name, schema_name into obj_name, sch_name from pgSQLt.private_split_object_name(table_name);

	execute format('create table %1$s.temporary_clone 
	as select * from %1$s.%2$s where 1 = 0;

	alter table %1$s.%2$s RENAME TO %2$s_renamed;
	alter table %1$s.temporary_clone RENAME TO %2$s;', sch_name, obj_name);	     
END 
$$ LANGUAGE PLPGSQL;


 
CREATE FUNCTION pgSQLt.assert_equals(expected anyelement, actual anyelement) RETURNS VOID AS 
$$
BEGIN 
	PERFORM pgSQLt.private_assert_test_session_active();

	if actual is distinct from expected then 
		perform pgSQLt.private_raise_assert_exception(format('assert_equals: Expected [%s] but got [%s]', expected::text, actual::text));
	end if;
END 
$$ LANGUAGE PLPGSQL;


CREATE FUNCTION pgSQLt.assert_tables_equal(expected text, actual text) RETURNS VOID AS 
$$
declare
	r int;
BEGIN 
	PERFORM pgSQLt.private_assert_test_session_active();

	execute format('
select count(*) from 
(
	select * from
	(
	 select * from %1$s 
	 except
	 select * from %2$s
	) as a
	union all
	( 
	 select * from %2$s 
	 except
	 select * from %1$s
	) 
) as diff', expected, actual) into r;
	if r > 0 then
		perform pgSQLt.private_raise_assert_exception(format('assert_tables_equal: Expected table [%s] was not same as actual table [%s]', expected::text, actual::text));
	end if;
	
END 
$$ LANGUAGE PLPGSQL;


CREATE FUNCTION pgSQLt.assert_tables_not_equal(expected text, actual text) RETURNS VOID AS 
$$
declare
	r int;
BEGIN 
	PERFORM pgSQLt.private_assert_test_session_active();

	execute format('
select count(*) from 
(
	select * from
	(
	 select * from %1$s 
	 except
	 select * from %2$s
	) as a
	union all
	( 
	 select * from %2$s 
	 except
	 select * from %1$s
	) 
) as diff', expected, actual) into r;
	if r = 0 then
		perform pgSQLt.private_raise_assert_exception(format('assert_tables_not_equal: Expected table [%s] WAS same as actual table [%s]', expected::text, actual::text));
	end if;
	
END 
$$ LANGUAGE PLPGSQL;

-- CREATE TABLE pgSQLt.CaptureOutputLog (
--   Id SERIAL PRIMARY KEY ,
--   OutputText text
-- );

-- 
-- CREATE VIEW pgSQLt.TestClasses
-- AS
--   -- SELECT s.name AS Name, s.schema_id AS SchemaId
-- --     FROM sys.extended_properties ep
-- --     JOIN sys.schemas s
-- --       ON ep.major_id = s.schema_id
-- --    WHERE ep.name = N'pgSQLt.TestClass';
-- select 1
-- ;
-- 
-- CREATE VIEW pgSQLt.Tests
-- AS
-- --   SELECT classes.SchemaId, classes.Name AS TestClassName, 
-- --          procs.object_id AS ObjectId, procs.name AS Name
-- --     FROM pgSQLt.TestClasses classes
-- --     JOIN sys.procedures procs ON classes.SchemaId = procs.schema_id
-- --    WHERE LOWER(procs.name) LIKE 'test%';
-- select 1
-- ;


-- CREATE TABLE pgSQLt.TestResult(
--     Id SERIAL PRIMARY KEY ,
--     Class text NOT NULL,
--     TestCase text NOT NULL,
--     TranName text NOT NULL,
--     Result text NULL,
--     Msg text NULL
-- );

-- CREATE TABLE pgSQLt.TestMessage(
--     Msg text
-- );
-- ;
-- CREATE TABLE pgSQLt.Run_LastExecution(
--     TestName text,
--     SessionId INT,
--     LoginTime timestamp
-- );

-- 


-- 
-- create function pgSQLt.AssertObjectExists() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.AssertResultSetsHaveSameMetaData() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.ResultSetFilter() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.AssertEqualsTable() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.AssertLike() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.AssertNotEquals() returns void AS 
-- $$	
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 

-- 
-- create function pgSQLt.Fail() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.ExpectNoException() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.ExpectException() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.FakeFunction() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 

-- 
-- create function pgSQLt.RenameClass() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.RemoveObject() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.ApplyTrigger() returns void AS 
-- $$
--  BEGIN 
-- perform pgSQLt.private_assert_test_session_active();
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.SpyProcedure() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.Info() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.TableToText() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.RunAll() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.RunTest() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.RunTestClass() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.RunWithNullResults() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.RunWithXmlResults() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.TestCaseSummary() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.XmlResultFormatter() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.DefaultResultFormatter() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.GetTestResultFormatter() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.SetTestResultFormatter() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.LogCapturedOutput() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.SuppressOutput() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.CaptureOutput() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.NewConnection() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.Uninstall() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.DropClass() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.SetFakeViewOff() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- create function pgSQLt.SetFakeViewOn() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.F_Num() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 
-- create function pgSQLt.GetNewTranName() returns void AS 
-- $$
--  BEGIN 
--      RAISE EXCEPTION 'NOT IMPLEMENTED'; 
-- END 
-- $$ LANGUAGE plpgsql;
-- 
-- 

