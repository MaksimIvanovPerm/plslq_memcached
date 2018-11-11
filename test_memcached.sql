declare
 CURSOR c1 IS
 SELECT *
 FROM all_objects t
 WHERE t.object_type='PACKAGE BODY'
   AND t.owner='SOME_USER';

 v_key        VARCHAR2(257);
 v_value      CLOB;
 v_buffer     varchar2(32767);
 tt           FLOAT;
 v_module     varchar2(30) := 'plsql_block';
 v_str    varchar2(30);

 function get_from_db(p_owner IN VARCHAR2, p_name IN varchar2)
 RETURN clob
 IS
  l_clob  CLOB;
  CURSOR c2(p_owner VARCHAR2, p_name varchar2) IS
  SELECT *
  FROM all_source t
  WHERE t.type='PACKAGE BODY'
    AND t.name=Upper(p_name)
    AND t.owner=Upper(p_owner)
  ORDER BY t.line asc;
 BEGIN
   l_clob:='';
   FOR i IN c2(p_owner, p_name)
   LOOP
    l_clob:=l_clob||i.text;
   END LOOP;
   RETURN l_clob;
 END get_from_db;

begin
 memcached.set_logfile('memcached.log');
 memcached.open_logfile;
 tt := dbms_utility.get_time;
 FOR i IN c1
 LOOP
  v_key:=i.owner||'.'||i.object_name;
  v_buffer:='';
  v_value:=excellent.memcached.gat_value(the_key=>v_key, the_exptime=>3600);
  IF v_value='NOT_FOUND'
  THEN
    v_value:=get_from_db(i.owner, i.object_name);
    v_buffer:=excellent.memcached.set_value( the_key=>v_key,the_value=>v_value,the_exptime=>3600,p_noreply=>'');
  END IF;
  --v_buffer:=memcached.delete_value(the_key=>v_key,p_noreply=>'');
 END LOOP; --by c1 cursor

 tt := dbms_utility.get_time-tt;
 memcached.logtofile(to_char(systimestamp,'yyyy.mm.dd hh24:mi:ss.ff3')||' '||v_module||' '||tt);
 memcached.close_logfile;
end;
/
