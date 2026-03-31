"""
GMP Agent — Merged MQTT listener + DB Setup + DB Get.
Listens on gmp/device/{MAC}/command for action payloads.
When action = "db_setup", generates SQL from the JSON and
executes it directly against the local MySQL database.
"""
import os
import sys
import time
import signal
import logging
import json
import subprocess
import socket
import platform
import base64
import paho.mqtt.client as mqtt
import pymysql
import pymysql.cursors
# -------------------------------------------------------------------
# Configuration
# -------------------------------------------------------------------
MQTT_BROKER  = "b-a7ea48df-8e78-4841-bbb3-8364bc748d37-1.mq.us-east-1.amazonaws.com"
MQTT_PORT    = 8883
MQTT_USER    = "access-analytics-mq45"
MQTT_PASS    = "Tk7TvUZQGvf5hlV"
# MySQL — adjust DB_NAME and DB_PASS to match your environment
DB_HOST = "localhost"
DB_PORT = 3306
DB_USER = "root"
DB_PASS = "root"
DB_NAME = "access_online"       # <-- Change to your actual database name
LOGFILE = "/var/log/gmp_agent.log"
# -------------------------------------------------------------------
# Logging Setup
# -------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOGFILE),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger("gmp_agent")
# -------------------------------------------------------------------
# DB Helper
# -------------------------------------------------------------------
def get_db_connection():
    """Creates and returns a MySQL connection."""
    return pymysql.connect(
        host=DB_HOST,
        port=DB_PORT,
        user=DB_USER,
        password=DB_PASS,
        database=DB_NAME,
        cursorclass=pymysql.cursors.DictCursor
    )
def execute_sql_transaction(sql_statements: list[str]):
    """
    Executes a list of SQL statements inside a single transaction.
    Rolls back everything if any statement fails.
    Returns (success: bool, error_message: str | None)
    """
    conn = None
    try:
        conn = get_db_connection()
        conn.begin()
        with conn.cursor() as cursor:
            for sql in sql_statements:
                sql = sql.strip()
                if sql:
                    logger.info(f"Executing SQL:\n{sql}")
                    cursor.execute(sql)
        conn.commit()
        logger.info("All SQL statements committed successfully.")
        return True, None
    except pymysql.Error as e:
        if conn:
            conn.rollback()
        logger.error(f"DB error — rolled back transaction: {e}")
        return False, str(e)
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Unexpected error — rolled back transaction: {e}")
        return False, str(e)
    finally:
        if conn:
            conn.close()
# -------------------------------------------------------------------
# SQL Generators
# (All functions now RETURN SQL strings instead of printing them)
# -------------------------------------------------------------------
def generate_enable_config_sql(data: dict) -> str:
    row = data['enable_online_config']
    defaults = {
        'default_frontend_url': 'https://0.0.0.0:4000/',
        'default_variant_name': 'ONLINE',
        'default_plugins': '',
        'default_payment_type': 'POSTPAY',
        'default_kill_system': 'ACTIVE',
        'default_is_heartbeat_enabled': 1,
        'default_heartbeat_interval': 60,
        'default_nuc_device_id': 1,
        'default_is_pending_event_retry': 0,
        'default_remove_spclchar_anpr_read': 1,
        'default_car_park_full_time_out_seconds': 1,
        'default_heartbeat_pin_ping_interval': 30,
        'default_controller_hci_heartbeat_diff_threshold': 60,
        'default_is_boombarrier_sensor_enabled': 1,
        'default_is_sla_logs_enabled': 0,
        'default_controller_fallback_to_rest_enabled': 0,
        'default_mosquitto_broker_ip': '127.0.0.1',
        'default_process_hci_entry_exit_msg': 0,
        'default_suppress_counting_hci_manual_entry_exit': 0,
        'default_is_open_all_garage_door_enabled': 0,
        'default_is_early_response_to_reverse_req': 1,
        'default_cp_stp_hw_enabled': 0
    }
    return f"""INSERT INTO enable_config 
( cloud_parking_id, cloud_company_id, cloud_location_id, frontend_url, varient_name, plugins, company_name, tenant_name, parking_name, payment_type, kill_system, created_at, updated_at, is_heartbeat_enabled, heartbeat_interval, nuc_device_id, nuc_device_name, timezone, is_pending_event_retry, geo_hash, remove_spclchar_anpr_read, car_park_full_time_out_seconds, heartbeat_pin_ping_interval, controller_hci_heartbeat_diff_threshold, is_boombarrier_sensor_enabled, is_sla_logs_enabled, controller_fallback_to_rest_enabled, mosquitto_broker_ip, process_hci_entry_exit_msg, suppress_counting_hci_manual_entry_exit, is_open_all_garage_door_enabled, is_early_response_to_reverse_req, country_code, attach_not_found_lpr, cp_stp_hw_enabled, open_barrier_on_sl_untrigger, boom_barrier_async_trigger, analytics_enabled, parking_system_name, levenshtein_for_duplicate_check, place_of_issue_source, duplicate_request_expiry_time_sec, lane_logic_version, tag_lp_to_image_enabled ) 
VALUES 
( {row['cloud_parking_id']}, {row['cloud_company_id']}, '{row['cloud_location_id']}', '{defaults['default_frontend_url']}', '{defaults['default_variant_name']}', '{defaults['default_plugins']}', '{row['company_name']}', '{row['tenant_name']}', '{row['parking_name']}', '{defaults['default_payment_type']}', '{defaults['default_kill_system']}', now(), now(), {defaults['default_is_heartbeat_enabled']}, {defaults['default_heartbeat_interval']}, {defaults['default_nuc_device_id']}, '{row['nuc_device_name']}', '{row['timezone']}', {defaults['default_is_pending_event_retry']}, '{row['geohash']}', '{defaults['default_remove_spclchar_anpr_read']}', {defaults['default_car_park_full_time_out_seconds']}, {defaults['default_heartbeat_pin_ping_interval']}, {defaults['default_controller_hci_heartbeat_diff_threshold']}, {defaults['default_is_boombarrier_sensor_enabled']}, {defaults['default_is_sla_logs_enabled']}, {defaults['default_controller_fallback_to_rest_enabled']}, '{defaults['default_mosquitto_broker_ip']}', {defaults['default_process_hci_entry_exit_msg']}, '{defaults['default_suppress_counting_hci_manual_entry_exit']}', {defaults['default_is_open_all_garage_door_enabled']}, {defaults['default_is_early_response_to_reverse_req']}, '{row['country_code']}', {row['attach_not_found_lpr']}, {defaults['default_cp_stp_hw_enabled']}, {row['open_barrier_on_sl_untrigger']}, {row['boom_barrier_async_trigger']}, {row['analytics_enabled']}, '{row['parking_system_name']}', {row['levenshtein_for_duplicate_check']}, '{row['place_of_issue_source']}', {row['duplicate_request_expiry_time_sec']}, {row['lane_logic_version']}, {row['tag_lp_to_image_enabled']});"""
def generate_online_config_sql(data: dict) -> str:
    row = data['enable_online_config']
    defaults = {
        'default_cloud_base_url': 'https://api.parkingglobalserver.com',
        'default_universal_secret_key': 'yyyyyyyyyyyyyyy',
        'default_universal_hmac_username': 'gmpxxxxxxxxxx',
        'default_universal_currency': 'USD',
        'default_hmac_username': 'enable',
        'default_hmac_password': 'ZW5hYmxlMTIzIQ==',
        'default_mqtt_broker_address': 'mqtt.xxxxxxxxx',
        'default_mqtt_port': '8883',
        'default_mqtt_user': 'enable',
        'default_mqtt_password': 'xxxxxxxxxxx',
        'default_image_remove_cron_duration': 1,
        'default_image_to_be_removed_before': 47,
        'default_send_loop_data_interval': 180,
        'default_track_loop_status_on_grafana': 0
    }
    return f"""INSERT INTO online_config 
( cloud_parking_id, cloud_base_url, universal_secret_key, universal_hmac_username, universal_currency, hmac_username, hmac_password, mqtt_broker_address, mqtt_port, mqtt_user, mqtt_password, image_upload_enabled, image_remove_cron_duration, image_to_be_removed_before, created_at, updated_at, send_loop_data_interval, track_loop_status_on_grafana )
VALUES 
( {row['cloud_parking_id']}, '{defaults['default_cloud_base_url']}', '{defaults['default_universal_secret_key']}', '{defaults['default_universal_hmac_username']}', '{defaults['default_universal_currency']}', '{defaults['default_hmac_username']}', '{defaults['default_hmac_password']}', '{defaults['default_mqtt_broker_address']}', '{defaults['default_mqtt_port']}', '{defaults['default_mqtt_user']}', '{defaults['default_mqtt_password']}', 1, {defaults['default_image_remove_cron_duration']}, {defaults['default_image_to_be_removed_before']}, now(), now(), {defaults['default_send_loop_data_interval']}, {defaults['default_track_loop_status_on_grafana']});"""
def generate_enable_controllers_sql(data: dict) -> str:
    rows = data['lane_controller_hardware']
    default_mqtt_port                   = 1883
    default_response_type               = 'JSON'
    default_firmware_version            = 'A1.16 B02'
    default_controller_state            = 'ACTIVE'
    default_timer_bw_pin_high_low       = 2
    default_is_wise                     = 1
    default_is_lane_independent_controller = 0
    seen_controllers = set()
    values = []
    for row in rows:
        if row['controller_id'] in seen_controllers:
            continue
        seen_controllers.add(row['controller_id'])
        mqtt_user_password = f"{row['MQTT_user']}:{row['MQTT_password']}"
        header_key = base64.b64encode(mqtt_user_password.encode()).decode()
        values.append(
            f"({row['controller_id']}, '{row['controller_name']}', '{header_key}', "
            f"'{row['controller_ip']}', {default_mqtt_port}, '{row['MQTT_user']}', "
            f"'{row['MQTT_password']}', '{default_response_type}', "
            f"'{default_firmware_version}', '{row['controller_device_name']}', "
            f"'{default_controller_state}', {default_timer_bw_pin_high_low}, "
            f"{default_is_wise}, now(), now(), {default_is_lane_independent_controller})"
        )
    return (
        "INSERT INTO enable_controllers \n"
        "( controller_id, controller_name, header_key, ip, mqtt_port, MQTT_user, MQTT_password, "
        "response_type, firmware_version, controller_device_name, controller_state, "
        "timer_bw_pin_high_low, is_wise, created_at, updated_at, is_lane_independent_controller )\n"
        "VALUES \n"
        + ',\n'.join(values) + ';'
    )
def generate_lanes_sql(data: dict) -> str:
    rows = data['lane_controller_hardware']
    defaults = {
        'default_vehicle_type': 'CAR',
        'default_is_ticket_dispenser_active': 0,
        'default_ticket_dispenser_suppressed_value_true': 1,
        'default_timer_bw_loops': 0,
        'default_online_media_type': 'LPR',
        'default_lane_state': 'ACTIVE',
        'default_max_time_between_loops': 0,
        'default_is_deep_health_check': 0,
        'default_is_lane_disabled': 0,
        'default_lane_pair_id': 'NULL',
        'default_bb_sensor_wait_timeout': 30,
        'default_capture_lp_seconds': 5
    }
    values = []
    for row in rows:
        prefix = f"{row['lane_type']}_{row['cloud_lane_id']}"
        values.append(
            f"({row['controller_id']}, {row['cloud_lane_id']}, '{defaults['default_vehicle_type']}', "
            f"'{prefix}', '{row['loop_logic']}', "
            f"{row['loop1_interval']}, "
            f"{row['loop2_interval']}, "
            f"{row['boom_barrier_interval']}, "
            f"{row['is_boombarrier_active']}, "
            f"{defaults['default_is_ticket_dispenser_active']}, {defaults['default_ticket_dispenser_suppressed_value_true']}, "
            f"'{row['lane_type']}', {defaults['default_timer_bw_loops']}, "
            f"'{defaults['default_online_media_type']}', now(), now(), '{defaults['default_lane_state']}', "
            f"'{row['controller_device_name']}', {defaults['default_max_time_between_loops']}, "
            f"{defaults['default_is_deep_health_check']}, {row['controller_id']}, "
            f"{row.get('pos_reset_trigger', 0)}, "
            f"{row.get('pos_trigger_enabled', 0)}, "
            f"{defaults['default_is_lane_disabled']}, {defaults['default_lane_pair_id']}, "
            f"{defaults['default_bb_sensor_wait_timeout']}, {defaults['default_capture_lp_seconds']})"
        )
    return (
        "INSERT INTO lanes \n"
        "( id, cloud_lane_id, vehicle_type, prefix, loop_logic, loop1_interval, loop2_interval, "
        "boom_barrier_interval, is_boombarrier_active, is_ticket_dispenser_active, "
        "ticket_dispenser_suppressed_value_true, lane_type, timer_bw_loops, online_media_type, "
        "created_at, updated_at, lane_state, lane_name, max_time_between_loops, is_deep_health_check, "
        "controller_id, pos_reset_trigger, pos_trigger_enabled, is_lane_disabled, lane_pair_id, "
        "bb_sensor_wait_timeout, capture_lp_seconds)\n"
        "VALUES \n"
        + ',\n'.join(values) + ';'
    )
def generate_hardware_sql(data: dict) -> str:
    rows = data['lane_controller_hardware']
    port                         = "'1883'"
    url                          = '0.0.0.0'
    response_type                = 'JSON'
    rest_request_json_high_value = '{"DOVal": [{"Ch": 0,"Md": 0,"Val": 1}]}'
    max_connections_allowed      = 1
    hardware_username            = 'admin'
    hardware_password            = 'secret'
    status                       = 'ACTIVE'
    is_stp                       = 1
    name_map = {
        'PRESENCE_LOOP': 'PL', 'SAFETY_LOOP': 'SL', 'BOOMBARRIER': 'BB',
        'BOOMBARRIER_SENSOR_WAIT': 'BSW', 'CERRADA_LPR': 'LPR', 'POS': 'POS', 'KIOSK_APP': 'KIOSK'
    }
    sql_query_values = []
    hardware_counter = 1
    for row in rows:
        for hw_type in [h.strip() for h in row['hardware_type'].split(',')]:
            hw_name      = f"{row['controller_device_name']}_{name_map.get(hw_type, 'HW')}"
            rest_endpoint = (
                f"http://{row['controller_ip']}/do_value/slot_0"
                if hw_type == 'BOOMBARRIER'
                else f"http://{row['controller_ip']}/di_value/slot_0"
            )
            # Access URL
            if hw_type in ['PRESENCE_LOOP', 'SAFETY_LOOP', 'BOOMBARRIER_SENSOR_WAIT']:
                access_url = f"Advantech/{row['controller_mac']}/data"
            elif hw_type == 'BOOMBARRIER':
                access_url = f"Advantech/{row['controller_mac']}/ctl/do{row['boom_barrier_pin']}"
            elif hw_type == 'CERRADA_LPR':
                access_url = row['camera_ip']
            elif hw_type in ['POS', 'KIOSK_APP']:
                access_url = row['pos_ip']
            else:
                access_url = ''
            conn_type = "'FTP'" if hw_type in ['CERRADA_LPR', 'POS'] else "'MQTT_SERVER'"
            # Pin
            if hw_type == 'PRESENCE_LOOP':
                pin = row['presence_loop_pin']
            elif hw_type == 'SAFETY_LOOP':
                pin = row['safety_loop_pin']
            elif hw_type == 'BOOMBARRIER_SENSOR_WAIT':
                pin = row['boombarrier_sensor_pin']
            elif hw_type == 'BOOMBARRIER':
                pin = row['boom_barrier_pin']
            else:
                pin = ''
            rest_json = rest_request_json_high_value if hw_type == 'BOOMBARRIER' else ''
            usr       = hardware_username if hw_type == 'CERRADA_LPR' else ''
            pwd       = hardware_password if hw_type == 'CERRADA_LPR' else ''
            sql_query_values.append(
                f"({hardware_counter}, {conn_type}, '{hw_type}', {port}, '{url}', '{pin}', "
                f"'{response_type}', '{rest_endpoint}', '{rest_json}', '{access_url}', "
                f"{max_connections_allowed}, '{hw_name}', '{usr}', '{pwd}', '', '', '', "
                f"'{status}', now(), now(), {row['controller_id']}, {row['controller_id']}, {is_stp})"
            )
            hardware_counter += 1
    return (
        "INSERT INTO hardware_devices \n"
        "(hardware_device_id, connection_type, type, port, url, input_type, response_type, "
        "rest_endpoint, rest_request_json_high_value, device_access_url, max_connections_allowed, "
        "hardware_device_name, hardware_username, hardware_password, ftp_username, ftp_password, "
        "external_identifier, status, created_at, updated_at, controller_id, lane_id, is_stp) \n"
        "VALUES \n"
        + ',\n'.join(sql_query_values) + ';'
    )
def generate_extra_configs_sql(data: dict) -> list[str]:
    a = data['analytics_config']
    k = data['kiosk_config']
    return [
        f"INSERT INTO `analytics_config` (`mqtt_broker_address`, `mqtt_port`, `mqtt_user`, `mqtt_password`) "
        f"VALUES ('{a['mqtt_broker_address']}', '{a['mqtt_port']}', '{a['mqtt_user']}', '{a['mqtt_password']}');",
        f"INSERT INTO `kiosk_config` (`mqtt_broker_address`, `mqtt_port`, `mqtt_user`, `mqtt_password`, `created_at`, `updated_at`) "
        f"VALUES ('{k['mqtt_broker_address']}', '{k['mqtt_port']}', '{k['mqtt_user']}', '{k['mqtt_password']}', now(), now());"
    ]
# -------------------------------------------------------------------
# Helper Functions
# -------------------------------------------------------------------
def get_mac_address(strip_colons=False):
    """Fetches MAC. Strips colons for the MQTT topic, keeps them for the status payload."""
    try:
        mac = subprocess.check_output(
            "ip link show | awk '/ether/ {print $2; exit}'",
            shell=True
        ).decode().strip()
        if mac:
            return mac.replace(':', '').lower() if strip_colons else mac.lower()
    except Exception as e:
        logger.error(f"Failed to fetch MAC address: {e}")
    sys.exit(1)
def get_os_info():
    """Parses /etc/os-release to get the exact OS name and version."""
    name, version = "Unknown", "Unknown"
    try:
        with open("/etc/os-release") as f:
            data = dict(line.strip().split("=", 1) for line in f if "=" in line)
            name    = data.get("NAME", "").strip('"')
            version = data.get("VERSION", "").strip('"')
    except Exception:
        pass
    return {"name": name, "version": version}
def publish_response(client, topic, action, status, data, error=None):
    """Formats and sends the response back to the MQTT broker."""
    payload = {
        "action":    action,
        "status":    status,
        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "data":      data,
        "error":     error
    }
    client.publish(topic, json.dumps(payload))
    logger.info(f"Published response for '{action}' to {topic}")
def run_shell(cmd):
    """Executes a shell command and returns the output."""
    try:
        result = subprocess.run(
            cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True
        )
        return result.stdout.strip() if result.returncode == 0 else f"Error: {result.stderr.strip()}"
    except Exception as e:
        return str(e)
# -------------------------------------------------------------------
# Task Handlers
# -------------------------------------------------------------------
def handle_get_status(client, response_topic, payload):
    logger.info("Executing: get_status")
    hostname    = socket.gethostname()
    mac_address = get_mac_address(strip_colons=False)
    tv_id       = run_shell("grep -oP '(?<=ClientID = ).*' /etc/teamviewer/global.conf || echo 'N/A'")
    cpu_model = run_shell("lscpu | grep 'Model name' | awk -F: '{print $2}'")
    cpu_cores = run_shell("nproc")
    cpu       = f"{cpu_model if cpu_model else platform.machine()} ({cpu_cores} cores)"
    memory = run_shell("free -h | awk '/Mem:/ {print $2}'")
    disk   = run_shell("lsblk -o SIZE -dn | head -n1")
    uptime = run_shell("uptime -p")
    ip        = run_shell("hostname -I | awk '{print $1}'")
    gateway   = run_shell("ip route | awk '/default/ {print $3}'")
    subnet    = run_shell("ip -o -f inet addr show | awk '/scope global/ {print $4}'")
    interface = run_shell("ip route | awk '/default/ {print $5}'")
    services = ["docker", "mysql", "mosquitto", "teamviewerd"]
    service_status = {svc: run_shell(f"systemctl is-active {svc}") for svc in services}
    service_status["dbeaver"] = "installed" if "bin/dbeaver" in run_shell("which dbeaver") else "not installed"
    docker_images = run_shell("docker images --format '{{.Repository}}:{{.Tag}}'").split('\n')
    mysql_dbs     = run_shell(f"mysql -uroot -p'{DB_PASS}' -e 'SHOW DATABASES;' | grep -v Database").split('\n')
    data = {
        "hostname": hostname,
        "os":       get_os_info(),
        "cpu":      cpu,
        "memory":   memory,
        "disk":     disk,
        "uptime":   uptime,
        "mac_address":                mac_address,
        "teamviewer_id":              tv_id,
        "teamviewer_password_status": "Encrypted in OS",
        "network": {
            "ip":        ip,
            "subnet":    subnet,
            "gateway":   gateway,
            "interface": interface
        },
        "services":        service_status,
        "docker_images":   [img for img in docker_images if img],
        "mysql_databases": [db for db in mysql_dbs if db]
    }
    publish_response(client, response_topic, "get_status", "success", data)
def handle_network_scan(client, response_topic, payload):
    logger.info("Executing: network_scan")
    subnet = run_shell("ip -o -f inet addr show | awk '/scope global/ {print $4}' | head -n1")
    if not subnet or "Error" in subnet:
        publish_response(client, response_topic, "network_scan", "failed", {},
                         "Could not determine local subnet")
        return
    scan_result = run_shell(f"nmap -sn {subnet}")
    publish_response(client, response_topic, "network_scan", "success", {"raw_output": scan_result})
def handle_cmd(client, response_topic, payload):
    logger.info("Executing: cmd")
    command = payload.get("command")
    if not command:
        publish_response(client, response_topic, "cmd", "failed", {},
                         "No 'command' provided in payload")
        return
    logger.warning(f"SECURITY ALERT: Running remote command: {command}")
    output = run_shell(command)
    publish_response(client, response_topic, "cmd", "success",
                     {"command": command, "output": output})
def handle_db_query(client, response_topic, payload):
    """Run a raw SQL query and return the results."""
    logger.info("Executing: db_query")
    sql = payload.get("sql")
    if not sql:
        publish_response(client, response_topic, "db_query", "failed", {},
                         "No 'sql' key provided in payload")
        return
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
        conn.close()
        publish_response(client, response_topic, "db_query", "success", {"rows": rows})
    except Exception as e:
        publish_response(client, response_topic, "db_query", "failed", {}, str(e))
def handle_docker_update(client, response_topic, payload):
    logger.info("Executing: docker_update - STUBBED")
    publish_response(client, response_topic, "docker_update", "pending",
                     {"message": "Feature under development"})
def handle_db_setup(client, response_topic, payload):
    """
    Receives a DB provisioning JSON, generates all SQL statements,
    and executes them in a single MySQL transaction.
    Expected MQTT payload:
    {
        "action": "db_setup",
        "data": {
            "enable_online_config": { ... },
            "lane_controller_hardware": [ ... ],
            "analytics_config": { ... },
            "kiosk_config": { ... }
        }
    }
    """
    logger.info("Executing: db_setup")
    db_json = payload.get("data")
    if not db_json:
        publish_response(client, response_topic, "db_setup", "failed", {},
                         "No 'data' key found in payload")
        return
    # Validate required top-level keys
    required_keys = ["enable_online_config", "lane_controller_hardware",
                     "analytics_config", "kiosk_config"]
    missing = [k for k in required_keys if k not in db_json]
    if missing:
        publish_response(client, response_topic, "db_setup", "failed", {},
                         f"Missing required keys in data: {missing}")
        return
    # --- Generate all SQL statements ---
    try:
        sql_statements = [
            generate_enable_config_sql(db_json),
            generate_online_config_sql(db_json),
            generate_enable_controllers_sql(db_json),
            generate_lanes_sql(db_json),
            generate_hardware_sql(db_json),
            *generate_extra_configs_sql(db_json),   # returns a list of 2 statements
        ]
    except KeyError as e:
        publish_response(client, response_topic, "db_setup", "failed", {},
                         f"Missing field in JSON: {e}")
        return
    except Exception as e:
        publish_response(client, response_topic, "db_setup", "failed", {},
                         f"SQL generation error: {e}")
        return
    # Log all generated SQL for audit trail
    for i, sql in enumerate(sql_statements, 1):
        logger.info(f"[db_setup] Statement {i}:\n{sql}")
    # --- Execute in one transaction ---
    success, error = execute_sql_transaction(sql_statements)
    if success:
        publish_response(
            client, response_topic, "db_setup", "success",
            {"message": "All DB tables populated successfully.",
             "statements_executed": len(sql_statements)}
        )
    else:
        publish_response(client, response_topic, "db_setup", "failed", {}, error)
def handle_db_get(client, response_topic, payload):
    """
    Reads the current DB state and publishes it back in the exact same
    JSON format that db_setup expects — so it can be round-tripped.
    Reconstructs:
      - enable_online_config  <- enable_config table
      - lane_controller_hardware <- lanes + enable_controllers + hardware_devices
      - analytics_config      <- analytics_config table
      - kiosk_config          <- kiosk_config table
    """
    logger.info("Executing: db_get")
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 1. enable_config (single row)
            cursor.execute(
                "SELECT cloud_parking_id, cloud_company_id, cloud_location_id, "
                "company_name, tenant_name, parking_name, nuc_device_name, timezone, "
                "geo_hash, country_code, parking_system_name, attach_not_found_lpr, "
                "open_barrier_on_sl_untrigger, boom_barrier_async_trigger, analytics_enabled, "
                "levenshtein_for_duplicate_check, place_of_issue_source, "
                "duplicate_request_expiry_time_sec, lane_logic_version, tag_lp_to_image_enabled "
                "FROM enable_config LIMIT 1"
            )
            ec = cursor.fetchone()
            if not ec:
                publish_response(client, response_topic, "db_get", "failed", {},
                                 "No data found in enable_config table")
                conn.close()
                return
            # 2. enable_controllers — keyed by controller_id
            cursor.execute(
                "SELECT controller_id, controller_name, ip, MQTT_user, MQTT_password, "
                "controller_device_name FROM enable_controllers"
            )
            controllers = {row['controller_id']: row for row in cursor.fetchall()}
            # 3. lanes
            cursor.execute(
                "SELECT id, cloud_lane_id, lane_type, lane_name, loop_logic, "
                "loop1_interval, loop2_interval, boom_barrier_interval, "
                "is_boombarrier_active, pos_reset_trigger, pos_trigger_enabled, "
                "controller_id FROM lanes"
            )
            lanes = cursor.fetchall()
            # 4. hardware_devices — grouped by lane_id
            cursor.execute(
                "SELECT lane_id, type, input_type, device_access_url "
                "FROM hardware_devices ORDER BY hardware_device_id"
            )
            hw_by_lane = {}
            for hw in cursor.fetchall():
                lid = hw['lane_id']
                hw_by_lane.setdefault(lid, []).append(hw)
            # 5. analytics_config
            cursor.execute(
                "SELECT mqtt_broker_address, mqtt_port, mqtt_user, mqtt_password "
                "FROM analytics_config LIMIT 1"
            )
            analytics = cursor.fetchone()
            # 6. kiosk_config
            cursor.execute(
                "SELECT mqtt_broker_address, mqtt_port, mqtt_user, mqtt_password "
                "FROM kiosk_config LIMIT 1"
            )
            kiosk = cursor.fetchone()
        conn.close()
        # --- Reconstruct lane_controller_hardware ---
        lane_controller_hardware = []
        for lane in lanes:
            lane_id = lane['id']
            ctrl    = controllers.get(lane['controller_id'], {})
            hw_list = hw_by_lane.get(lane_id, [])
            # Map each hw type to its row for easy field extraction
            hw_by_type = {hw['type']: hw for hw in hw_list}
            # hardware_type: preserve original insertion order
            hardware_type = ','.join(hw['type'] for hw in hw_list)
            # Extract individual pins
            presence_loop_pin      = hw_by_type.get('PRESENCE_LOOP',         {}).get('input_type', '')
            safety_loop_pin        = hw_by_type.get('SAFETY_LOOP',            {}).get('input_type', '')
            boombarrier_sensor_pin = hw_by_type.get('BOOMBARRIER_SENSOR_WAIT',{}).get('input_type', '')
            boom_barrier_pin       = hw_by_type.get('BOOMBARRIER',            {}).get('input_type', '')
            # Camera / POS IPs
            camera_ip = hw_by_type.get('CERRADA_LPR', {}).get('device_access_url', '')
            pos_ip    = (hw_by_type.get('POS') or hw_by_type.get('KIOSK_APP') or {}).get('device_access_url', '')
            # Recover controller_mac from PRESENCE_LOOP device_access_url
            # Format stored: "Advantech/{mac}/data"
            pl_url = hw_by_type.get('PRESENCE_LOOP', {}).get('device_access_url', '')
            try:
                controller_mac = pl_url.split('/')[1]   # index 1 = mac segment
            except (IndexError, AttributeError):
                controller_mac = ''
            lane_controller_hardware.append({
                "controller_id":          lane['controller_id'],
                "controller_name":        ctrl.get('controller_name', ''),
                "cloud_lane_id":          lane['cloud_lane_id'],
                "controller_device_name": lane['lane_name'],
                "lane_type":              lane['lane_type'],
                "controller_ip":          ctrl.get('ip', ''),
                "controller_mac":         controller_mac,
                "MQTT_user":              ctrl.get('MQTT_user', ''),
                "MQTT_password":          ctrl.get('MQTT_password', ''),
                "loop_logic":             lane['loop_logic'],
                "loop1_interval":         lane['loop1_interval'],
                "loop2_interval":         lane['loop2_interval'],
                "boom_barrier_interval":  lane['boom_barrier_interval'],
                "is_boombarrier_active":  lane['is_boombarrier_active'],
                "pos_reset_trigger":      lane.get('pos_reset_trigger', 0),
                "pos_trigger_enabled":    lane.get('pos_trigger_enabled', 0),
                "hardware_type":          hardware_type,
                "camera_ip":              camera_ip,
                "pos_ip":                 pos_ip,
                "presence_loop_pin":      presence_loop_pin,
                "safety_loop_pin":        safety_loop_pin,
                "boombarrier_sensor_pin": boombarrier_sensor_pin,
                "boom_barrier_pin":       boom_barrier_pin,
            })
        # --- Build final payload ---
        result = {
            "enable_online_config": {
                "cloud_parking_id":               ec['cloud_parking_id'],
                "cloud_company_id":               ec['cloud_company_id'],
                "cloud_location_id":              ec['cloud_location_id'],
                "company_name":                   ec['company_name'],
                "tenant_name":                    ec['tenant_name'],
                "parking_name":                   ec['parking_name'],
                "nuc_device_name":                ec['nuc_device_name'],
                "timezone":                       ec['timezone'],
                "geohash":                        ec['geo_hash'],          # DB col is geo_hash
                "country_code":                   ec['country_code'],
                "parking_system_name":            ec['parking_system_name'],
                "attach_not_found_lpr":           ec['attach_not_found_lpr'],
                "open_barrier_on_sl_untrigger":   ec['open_barrier_on_sl_untrigger'],
                "boom_barrier_async_trigger":     ec['boom_barrier_async_trigger'],
                "analytics_enabled":              ec['analytics_enabled'],
                "levenshtein_for_duplicate_check":ec['levenshtein_for_duplicate_check'],
                "place_of_issue_source":          ec['place_of_issue_source'],
                "duplicate_request_expiry_time_sec": ec['duplicate_request_expiry_time_sec'],
                "lane_logic_version":             ec['lane_logic_version'],
                "tag_lp_to_image_enabled":        ec['tag_lp_to_image_enabled'],
            },
            "lane_controller_hardware": lane_controller_hardware,
            "analytics_config": {
                "mqtt_broker_address": analytics['mqtt_broker_address'],
                "mqtt_port":           analytics['mqtt_port'],
                "mqtt_user":           analytics['mqtt_user'],
                "mqtt_password":       analytics['mqtt_password'],
            } if analytics else {},
            "kiosk_config": {
                "mqtt_broker_address": kiosk['mqtt_broker_address'],
                "mqtt_port":           kiosk['mqtt_port'],
                "mqtt_user":           kiosk['mqtt_user'],
                "mqtt_password":       kiosk['mqtt_password'],
            } if kiosk else {},
        }
        logger.info(f"db_get: returning config for parking_id={ec['cloud_parking_id']} "
                    f"with {len(lane_controller_hardware)} lanes")
        publish_response(client, response_topic, "db_get", "success", result)
    except Exception as e:
        logger.error(f"db_get failed: {e}")
        publish_response(client, response_topic, "db_get", "failed", {}, str(e))
# -------------------------------------------------------------------
# The Command Router
# -------------------------------------------------------------------
ACTION_ROUTER = {
    "get_status":   handle_get_status,
    "network_scan": handle_network_scan,
    "cmd":          handle_cmd,
    "db_query":     handle_db_query,
    "docker_update":handle_docker_update,
    "db_setup":     handle_db_setup,
    "db_get":       handle_db_get,         # <-- NEW
}
def on_message(client, userdata, msg):
    logger.info(f"Received message on {msg.topic}")
    try:
        payload        = json.loads(msg.payload.decode('utf-8'))
        action         = payload.get("action")
        response_topic = userdata["response_topic"]
        if action in ACTION_ROUTER:
            ACTION_ROUTER[action](client, response_topic, payload)
        else:
            logger.warning(f"Unknown action requested: {action}")
            publish_response(client, response_topic, action or "unknown", "failed", {},
                             "Unknown action")
    except json.JSONDecodeError:
        logger.error("Received non-JSON payload, ignoring.")
    except Exception as e:
        logger.error(f"Error processing message: {e}")
# -------------------------------------------------------------------
# Main Service Loop
# -------------------------------------------------------------------
def on_connect(client, userdata, flags, rc):
    if rc == 0:
        logger.info(f"Connected to MQTT Broker. Subscribing to: {userdata['command_topic']}")
        client.subscribe(userdata['command_topic'])
if __name__ == "__main__":
    logger.info("Starting GMP Agent Service...")
    signal.signal(signal.SIGINT,  lambda s, f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))
    DEVICE_MAC_ID  = get_mac_address(strip_colons=True)
    COMMAND_TOPIC  = f"gmp/device/{DEVICE_MAC_ID}/command"
    RESPONSE_TOPIC = f"gmp/device/{DEVICE_MAC_ID}/response"
    logger.info(f"Agent ID: {DEVICE_MAC_ID}")
    client = mqtt.Client(
        client_id=f"gmp_agent_{DEVICE_MAC_ID}",
        clean_session=False,
        userdata={
            "command_topic":  COMMAND_TOPIC,
            "response_topic": RESPONSE_TOPIC
        }
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.tls_set()
    client.on_connect = on_connect
    client.on_message = on_message
    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
        client.loop_forever()
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        sys.exit(1)