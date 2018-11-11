CREATE OR REPLACE package excellent.memcached
IS
v_logging    NUMBER := 1; --1 logging on; 0 logging off;
v_directory  VARCHAR2(30) := 'PLSHPROF_DIR'; -- Where log file will be placed on;   --
v_logfile    VARCHAR2(30) := 'memcached.log';
v_fh         Utl_File.file_type;

FUNCTION get_connection_id(the_key IN VARCHAR2) RETURN BINARY_INTEGER;
function get_file_name RETURN VARCHAR2;
function show_logfile_name RETURN VARCHAR2;
PROCEDURE set_logfile(p_file_name IN varchar2);
function timestamp_diff(a timestamp, b timestamp) return NUMBER;
procedure open_logfile(p_open_mode IN VARCHAR2 DEFAULT 'a', p_linesize IN NUMBER DEFAULT 1024);
PROCEDURE logtofile(p_line IN varchar2);
PROCEDURE close_logfile;


function get_value(the_key VARCHAR2) RETURN CLOB;
function gat_value(the_key VARCHAR2, the_exptime binary_integer default 0) RETURN CLOB;
function delete_value(the_key VARCHAR2, p_noreply VARCHAR2 DEFAULT '') RETURN varchar2;
function set_value( the_key varchar2,
                    the_value clob DEFAULT '',
                    the_flags binary_integer default 0,
                    the_exptime binary_integer default 0,
                    p_bytes NUMBER DEFAULT null,
                    p_noreply VARCHAR2 DEFAULT '' ) return varchar2;

end;
/
