CREATE OR REPLACE package BODY excellent.memcached
IS
 empty_key                 exception;
 wrong_noreply             exception;
 e_not_connected           exception;
 empty_the_connection_list exception;
 v_connection              utl_tcp.connection;
 t1                        TIMESTAMP;
 v_elatime                 NUMBER;

type t_connection is record (
 	host varchar2( 256 ),
 	port number( 5 )
 	);
type t_connection_list is table of t_connection index by binary_integer;
the_connection_list t_connection_list;

 PRAGMA EXCEPTION_INIT(e_not_connected, -29260);
/*-----------------------------------------------------------------*/
-- Misc routines
function show_logfile_name
RETURN VARCHAR2
IS
BEGIN
 RETURN v_logfile;
END show_logfile_name;

PROCEDURE set_logfile(p_file_name IN varchar2)
IS
 e_name_too_long    EXCEPTION;
 v_module           VARCHAR2(30):='set_logfile';
BEGIN
 IF Length(p_file_name)>30
 THEN
  RAISE e_name_too_long;
 END IF;

 v_logfile:=p_file_name;
EXCEPTION
 WHEN e_name_too_long THEN RAISE_APPLICATION_ERROR(-20005, v_module||' length of file name have to be <=30 symb; logfile name is: '||v_logfile);
END;

function get_file_name RETURN VARCHAR2
IS
 v_str     VARCHAR2(30):='';
BEGIN
 SELECT DISTINCT sid INTO v_str FROM v$mystat;
 RETURN 'memcached_'||v_str||'.log';
END get_file_name;

function timestamp_diff(a timestamp, b timestamp)
return number
is
begin
  return extract (day from (a-b))*24*60*60+extract(HOUR from (a-b))*60*60+extract (minute from (a-b))*60+extract (second from (a-b));
END timestamp_diff;

procedure open_logfile(p_open_mode IN VARCHAR2 DEFAULT 'a', p_linesize IN NUMBER DEFAULT 1024)
--RETURN Utl_File.file_type
IS
 e_empty_dir   EXCEPTION;
 e_empty_file  EXCEPTION;
 v_module      VARCHAR2(30) := 'open_logfile';
BEGIN
 IF v_directory IS NULL
 THEN
  RAISE e_empty_dir;
 END IF;

 IF v_logfile IS NULL
 THEN
  RAISE e_empty_file;
 END IF;
 --Dbms_Output.put_line('try to open2: '||v_logfile);
 memcached.v_fh:=Utl_File.fopen(v_directory, v_logfile, p_open_mode, p_linesize);

EXCEPTION
  WHEN e_empty_dir THEN RAISE_APPLICATION_ERROR(-20003, v_module||' you have to set non-empty value on p_dir parameter');
  WHEN e_empty_file THEN RAISE_APPLICATION_ERROR(-20004, v_module||' you have to set non-empty value on p_file parameter');
END open_logfile;

PROCEDURE logtofile(p_line IN varchar2)
IS
 --v_x     BOOLEAN;
BEGIN
 IF v_logging=1
 THEN
   Utl_File.put_line(memcached.v_fh, p_line);
   UTL_FILE.FFLUSH(memcached.v_fh);
 END IF;
END logtofile;

PROCEDURE close_logfile
IS
 --v_x     BOOLEAN;
BEGIN
 UTL_FILE.FCLOSE_ALL;
END close_logfile;
/*-----------------------------------------------------------------*/
function read_from_connection
return clob
is
 v_output       clob;
 v_next_line    varchar2(32766) := '';

 e_data_size    EXCEPTION;
 v_ch_recvd     PLS_INTEGER;
 v_data_size    NUMBER;
 l_pos          NUMBER;
 l_amount       NUMBER;
 v_module       VARCHAR2(30) := 'read_from_connection';
begin

 /*
 according to the doc: https://github.com/memcached/memcached/blob/master/doc/protocol.txt
 The retrieval commands "get" and "gets" operate like this:

 get <key>*\r\n
 gets <key>*\r\n

- <key>* means one or more key strings separated by whitespace.
After this command, the client expects zero or more items, each of
which is received as a text line followed by a data block. After all
the items have been transmitted, the server sends the string

"END\r\n"

to indicate the end of response.
 */

 v_next_line:=Utl_Tcp.get_line(v_connection, remove_crlf=>true);
 --Dbms_Output.put_line(v_module||'>>'||v_next_line||'<<');
 --So it case there is not kv-pair with fiven key: the server answers by sinle line with one word: "END\r\n"
 IF regexp_like(v_next_line,'END','c')
 THEN
  v_output:='NOT_FOUND';
  RETURN v_output;
 END IF;
 --And, in the all, other cases (kv-pair is there): the first line alwas will be VALUE <key> <flags> <bytes> [<cas unique>]\r\n
 v_data_size:=instr(v_next_line,' ',-1,1);
 v_data_size := To_Number( regexp_replace(SubStr(v_next_line,v_data_size), '\D','') );
 --Dbms_Output.put_line('q1: '||v_next_line||'->'||v_data_size);
 IF v_data_size IS NULL
 THEN
   --v_output:=v_next_line;
   --RETURN v_output;
   --Dbms_Output.put_line('q2: '||v_next_line||'->'||v_data_size);
   RAISE e_data_size;
 END IF;

 IF v_data_size <= 32766
 THEN
  v_output:=Utl_Tcp.get_text(v_connection, v_data_size, false);
 ELSE
  l_amount:=32766;
  v_output:='';
  l_pos:=1;
  LOOP
   --Dbms_Output.put_line(l_pos||' '||v_data_size||' '||l_amount);
   v_next_line:=Utl_Tcp.get_text(v_connection, l_amount, false);
   v_output:=v_output||v_next_line;
   v_data_size:=v_data_size-l_amount;
   IF v_data_size<32766
   THEN
    l_amount:=v_data_size;
   ELSE
    l_amount:=32766;
   END IF;
   l_pos:=l_pos+1;
  EXIT WHEN v_data_size=0;
  END LOOP;
 END IF;

 RETURN v_output;
end read_from_connection;

function set_value( the_key varchar2,
                    the_value clob DEFAULT '',
                    the_flags binary_integer default 0,
                    the_exptime binary_integer default 0,
                    p_bytes NUMBER DEFAULT null,
                    p_noreply VARCHAR2 DEFAULT '' ) return VARCHAR2
IS
 v_clob          CLOB;
 l_result        pls_integer;
 l_clob_len      number;
 l_pos           number := 1;
 l_amount        binary_integer := 32766;
 v_buffer        varchar2(32766);
 v_noreply       VARCHAR2(8);
 connection_id   binary_integer;
 v_module        VARCHAR2(30) := 'set_value';
BEGIN
 if the_key is null
 then
  raise empty_key;
 end if;

 if p_noreply is not null and lower(p_noreply) NOT IN ('noreply','reply')
 then
  raise wrong_noreply;
 end if;

 if lower(p_noreply) = 'noreply'
 then
  v_noreply:='noreply';
 ELSE
  v_noreply:='';
 end if;

  l_pos := DBMS_LOB.GETLENGTH(the_value);
  v_clob:='set '||the_key||' '||nvl( the_flags, 0 )||' '||nvl( the_exptime, 0 )||' '||l_pos||' '||v_noreply||chr(13)||chr(10)||the_value||chr(13)||chr(10);

  l_clob_len:=DBMS_LOB.GETLENGTH(v_clob);
  l_pos:=1;
  connection_id:=get_connection_id(the_key);
  v_connection := utl_tcp.open_connection(remote_host=>the_connection_list(connection_id).host, remote_port=>the_connection_list(connection_id).port, tx_timeout=>null);

  t1 := systimestamp;
  while l_pos <= l_clob_len
  loop
   DBMS_LOB.READ(v_clob, l_amount, l_pos, v_buffer);
 	 l_result := utl_tcp.write_text( v_connection, v_buffer);
 	 utl_tcp.flush( v_connection );
   l_pos := l_pos+l_amount;
  end loop;
  utl_tcp.flush( v_connection );

  if v_noreply='noreply'
  THEN
   v_buffer:=null;
  ELSE
   v_buffer:=utl_tcp.get_line( v_connection, true );
  end if;

  v_elatime:=timestamp_diff(systimestamp,t1);
  utl_tcp.close_connection(v_connection);
  logtofile(to_char(systimestamp,'yyyy.mm.dd hh24:mi:ss.ff3')||' '||v_module||' '||the_connection_list(connection_id).host||' '||the_key||' '||the_exptime||' '||l_clob_len||' '||v_buffer||' '||v_elatime);
  RETURN v_buffer;

exception
 when empty_key then RAISE_APPLICATION_ERROR(-20000, v_module||' you have to provide call of the proc with non-empty p_key parameter');
 when wrong_noreply then RAISE_APPLICATION_ERROR(-20001, v_module||' possible value for p_noreply are: reply|noreply');
 WHEN OTHERS THEN utl_tcp.close_all_connections; RAISE;
END set_value;

function delete_value(the_key VARCHAR2, p_noreply VARCHAR2 DEFAULT '') RETURN varchar2
IS
 l_result        pls_integer;
 v_buffer        varchar2(32766);
 v_noreply       VARCHAR2(8);
 connection_id   binary_integer;
 v_module        VARCHAR2(30) := 'delete_value';
BEGIN
 IF the_key IS NULL
 THEN
  RAISE empty_key;
 END IF;

 if p_noreply is not null and lower(p_noreply) NOT IN ('noreply','reply')
 then
  raise wrong_noreply;
 end if;

 if lower(p_noreply) = 'noreply'
 then
  v_noreply:='noreply';
 ELSE
  v_noreply:='';
 end if;

 v_buffer:='delete '||the_key||' '||v_noreply||chr(13)||chr(10);
 connection_id:=get_connection_id(the_key);
 v_connection := utl_tcp.open_connection(remote_host=>the_connection_list(connection_id).host, remote_port=>the_connection_list(connection_id).port, tx_timeout=>null);
 t1 := systimestamp;

 l_result := utl_tcp.write_text( v_connection, v_buffer);
 utl_tcp.flush( v_connection );
 if v_noreply='noreply'
 THEN
  v_buffer:=null;
 ELSE
  v_buffer:=utl_tcp.get_line( v_connection, true );
 end if;

 v_elatime:=timestamp_diff(systimestamp,t1);
 utl_tcp.close_connection(v_connection);
 logtofile(to_char(systimestamp,'yyyy.mm.dd hh24:mi:ss.ff3')||' '||the_connection_list(connection_id).host||' '||v_module||' '||the_key||' '||v_buffer||' '||v_elatime);
 RETURN v_buffer;

exception
 when empty_key then RAISE_APPLICATION_ERROR(-20000, v_module||' you have to provide call of the proc with non-empty p_key parameter');
 when wrong_noreply then RAISE_APPLICATION_ERROR(-20001, v_module||' possible value for p_noreply are: reply|noreply');
 WHEN OTHERS THEN utl_tcp.close_all_connections; RAISE;
END delete_value;

function get_value(the_key VARCHAR2) RETURN CLOB
IS
l_result      NUMBER;
v_clob        CLOB;
v_module      VARCHAR2(30) := 'get_value';
connection_id binary_integer;
l_clob_len    number;
BEGIN

 IF the_key IS NULL
 THEN
  RAISE empty_key;
 END IF;

 connection_id:=get_connection_id(the_key);
 v_connection := utl_tcp.open_connection(remote_host=>the_connection_list(connection_id).host, remote_port=>the_connection_list(connection_id).port, tx_timeout=>null);
 t1 := systimestamp;

 l_result := utl_tcp.write_text( v_connection, 'get '||the_key||chr(13)||chr(10));
 utl_tcp.flush( v_connection );
 v_clob:=read_from_connection;

 v_elatime:=timestamp_diff(systimestamp,t1);
 utl_tcp.close_connection(v_connection);
 l_clob_len:=DBMS_LOB.GETLENGTH(v_clob);
 logtofile(to_char(systimestamp,'yyyy.mm.dd hh24:mi:ss.ff3')||' '||v_module||' '||the_connection_list(connection_id).host||' '||the_key||' '||l_clob_len||' '||SubStr(v_clob,1,16)||' '||v_elatime);
 RETURN v_clob;


EXCEPTION
 WHEN empty_key THEN RAISE_APPLICATION_ERROR(-20000, v_module||' you have to provide call of the proc with non-empty p_key parameter');
 WHEN OTHERS THEN utl_tcp.close_all_connections; RAISE;
END get_value;

function gat_value(the_key VARCHAR2, the_exptime binary_integer default 0) RETURN CLOB
IS
l_result      NUMBER;
v_clob        CLOB;
v_module      VARCHAR2(30) := 'gat_value';
connection_id binary_integer;
l_clob_len    number;
BEGIN
IF the_key IS NULL
 THEN
  RAISE empty_key;
 END IF;

 connection_id:=get_connection_id(the_key);
 v_connection := utl_tcp.open_connection(remote_host=>the_connection_list(connection_id).host, remote_port=>the_connection_list(connection_id).port, tx_timeout=>null);
 t1 := systimestamp;

 l_result := utl_tcp.write_text( v_connection, 'gat '||Nvl(the_exptime, 0)||' '||the_key||chr(13)||chr(10));
 utl_tcp.flush( v_connection );
 v_clob:=read_from_connection;

 v_elatime:=timestamp_diff(systimestamp,t1);
 utl_tcp.close_connection(v_connection);
 l_clob_len:=DBMS_LOB.GETLENGTH(v_clob);
 logtofile(to_char(systimestamp,'yyyy.mm.dd hh24:mi:ss.ff3')||' '||v_module||' '||the_connection_list(connection_id).host||' '||the_key||' '||l_clob_len||' '||SubStr(v_clob,1,16)||' '||v_elatime);
 RETURN v_clob;


EXCEPTION
 WHEN empty_key THEN RAISE_APPLICATION_ERROR(-20000, v_module||' you have to provide call of the proc with non-empty p_key parameter');
 WHEN OTHERS THEN utl_tcp.close_all_connections; RAISE;
END gat_value;

FUNCTION get_connection_id(the_key IN VARCHAR2)
RETURN BINARY_INTEGER
IS
v_module      VARCHAR2(30) := 'get_connection_id';
v_number      number;
v_y           binary_integer;
BEGIN
 IF the_key IS NULL
 THEN
  RAISE empty_key;
 END IF;

 IF the_connection_list.Count=0
 THEN
  RAISE empty_the_connection_list;
 END IF;

 IF the_connection_list.Count=1
 THEN
  v_y:=the_connection_list.first;
 ELSE
  v_y:=the_connection_list.first;
  v_number:=mod(Dbms_Utility.get_hash_value(the_key,1,1024), the_connection_list.Count);
  IF v_number>0
  THEN
   FOR i IN 1..v_number
   LOOP
    v_y:=the_connection_list.NEXT(v_y);
   END LOOP;
  END IF;
 END IF;

 RETURN v_y;

EXCEPTION
WHEN empty_key THEN RAISE_APPLICATION_ERROR(-20000, v_module||' you have to provide call of the proc with non-empty p_key parameter');
WHEN empty_the_connection_list THEN RAISE_APPLICATION_ERROR(-20002, v_module||' connection-list is empty');
END get_connection_id;

PROCEDURE prepare_connection_list
IS
 	the_connection t_connection;
 	connection_id binary_integer;
BEGIN
 	connection_id := the_connection_list.count + 1;
 	the_connection := null;
 	the_connection.host := '10.101.21.229'; 
 	the_connection.port := 11211;
  the_connection_list( connection_id ) := the_connection;


  connection_id := the_connection_list.count + 1;
 	the_connection := null;
 	the_connection.host := '10.101.21.227'; 
 	the_connection.port := 11211;
 	the_connection_list( connection_id ) := the_connection;

END prepare_connection_list;

-- one-time section
BEGIN
  prepare_connection_list;
  v_logfile:=get_file_name;
end;
/
