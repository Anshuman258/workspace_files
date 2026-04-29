"""
GMP Agent — Merged MQTT listener + DB Setup + DB Update + Garage Door + PROD + Scheduler
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
import threading
import datetime
import urllib.parse
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
# Scheduled DB Update — Firebase Configuration
# -------------------------------------------------------------------
FIREBASE_BUCKET            = "gmp-pos-877ee.firebasestorage.app"
FIREBASE_SA_KEY_PATH       = "/etc/gmp/firebase_service_account.json"
SCHEDULE_POLL_INTERVAL     = 30 * 60          # seconds between Firebase polls (30 min)
MAX_LATE_EXECUTION_HOURS   = 24               # skip if scheduled_at is older than this
LAST_SCHEDULE_STATE_FILE   = "/var/lib/gmp/last_schedule_state.json"
# Docker restart after successful db_update
# Set RESTART_DOCKER_ON_UPDATE = True to enable.
# Add more container names to DOCKER_CONTAINERS as needed.
RESTART_DOCKER_ON_UPDATE   = False
DOCKER_CONTAINERS          = ["access-online"]  # list of containers to restart
# Thread-safety lock — prevents simultaneous MQTT + scheduler db_update
_db_update_lock = threading.Lock()
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
        'default_universal_secret_key': 'YkYxf6znXrx4zrSP',
        'default_universal_hmac_username': 'gmpiot',
        'default_universal_currency': 'USD',
        'default_hmac_username': 'enable',
        'default_hmac_password': 'ZW5hYmxlMTIzIQ==',
        'default_mqtt_broker_address': 'mqtt.parkingglobalserver.com',
        'default_mqtt_port': '8883',
        'default_mqtt_user': 'enable',
        'default_mqtt_password': 'pabMQpMQ1zkjGMK7',
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


def generate_garage_door_sql(data: dict, existing_controller_ids: set) -> list[str]:
    """
    Generates SQL for GARAGE_DOOR hardware entries.
    - For each entry in garage_door[], inserts a hardware_devices row with lane_id = NULL.
    - If the controller_id is NOT already in existing_controller_ids (lane controllers),
      also inserts a new enable_controllers row.
    existing_controller_ids: set of controller_ids already inserted by generate_enable_controllers_sql.
    Returns a list of SQL strings.
    """
    gd_rows = data.get('garage_door', [])
    if not gd_rows:
        return []
    sqls = []
    rest_json = '{"DOVal": [{"Ch": 0,"Md": 0,"Val": 1}]}'
    seen_ctrl = set()
    hw_counter_base = 9000  # GD hw IDs start high to avoid collision with lane hw
    for i, gd in enumerate(gd_rows):
        cid = gd['controller_id']
        # Insert controller if not already covered by lane_controller_hardware
        if cid not in existing_controller_ids and cid not in seen_ctrl:
            hk = base64.b64encode(f"{gd['MQTT_user']}:{gd['MQTT_password']}".encode()).decode()
            sqls.append(
                f"INSERT INTO enable_controllers "
                f"(controller_id, controller_name, header_key, ip, mqtt_port, MQTT_user, MQTT_password, "
                f"response_type, firmware_version, controller_device_name, controller_state, "
                f"timer_bw_pin_high_low, is_wise, created_at, updated_at, is_lane_independent_controller) "
                f"VALUES ({cid}, 'ADAM_HIGH', '{hk}', '{gd['controller_ip']}', "
                f"1883, '{gd['MQTT_user']}', '{gd['MQTT_password']}', 'JSON', 'A1.16 B02', "
                f"'{gd['garage_door_name']}', 'ACTIVE', 2, 1, now(), now(), 0);"
            )
        seen_ctrl.add(cid)
        # Build hardware_devices row
        pin      = str(gd['garage_door_pin'])
        mac      = gd['controller_mac']
        access_url = f"Advantech/{mac}/ctl/do{pin}"
        rest_ep  = f"http://{gd['controller_ip']}/do_value/slot_0"
        ext_id   = str(gd['external_identifier'])
        hw_id    = hw_counter_base + i
        sqls.append(
            f"INSERT INTO hardware_devices "
            f"(hardware_device_id, connection_type, type, port, url, input_type, response_type, "
            f"rest_endpoint, rest_request_json_high_value, device_access_url, max_connections_allowed, "
            f"hardware_device_name, hardware_username, hardware_password, ftp_username, ftp_password, "
            f"external_identifier, status, created_at, updated_at, controller_id, lane_id, is_stp) "
            f"VALUES ({hw_id}, 'MQTT_SERVER', 'GARAGE_DOOR', '1883', '0.0.0.0', '{pin}', 'JSON', "
            f"'{rest_ep}', '{rest_json}', '{access_url}', 1, '{gd['garage_door_name']}', "
            f"'', '', '', '', '{ext_id}', 'ACTIVE', now(), now(), {cid}, NULL, 1);"
        )
    return sqls
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
            "kiosk_config": { ... },
            "garage_door": [ ... ]    # optional
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
        # Track which controller_ids are covered by lane_controller_hardware
        lane_ctrl_ids = {row['controller_id'] for row in db_json.get('lane_controller_hardware', [])}
        sql_statements = [
            generate_enable_config_sql(db_json),
            generate_online_config_sql(db_json),
            generate_enable_controllers_sql(db_json),
            generate_lanes_sql(db_json),
            generate_hardware_sql(db_json),
            *generate_extra_configs_sql(db_json),
            *generate_garage_door_sql(db_json, lane_ctrl_ids),   # optional; empty list if absent
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
# -------------------------------------------------------------------
# DB Update Helpers
# -------------------------------------------------------------------
def _expected_hw_fields(row: dict, hw_type: str) -> dict:
    """
    Given a lane_controller_hardware JSON row and a hardware type,
    returns a dict of the expected DB field values for that hardware device.
    Used for diffing against current DB state.
    """
    name_map = {
        'PRESENCE_LOOP': 'PL', 'SAFETY_LOOP': 'SL', 'BOOMBARRIER': 'BB',
        'BOOMBARRIER_SENSOR_WAIT': 'BSW', 'CERRADA_LPR': 'LPR',
        'POS': 'POS', 'KIOSK_APP': 'KIOSK'
    }
    hw_name = f"{row['controller_device_name']}_{name_map.get(hw_type, 'HW')}"
    rest_endpoint = (
        f"http://{row['controller_ip']}/do_value/slot_0"
        if hw_type == 'BOOMBARRIER'
        else f"http://{row['controller_ip']}/di_value/slot_0"
    )
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
    if hw_type == 'PRESENCE_LOOP':
        pin = str(row['presence_loop_pin'])
    elif hw_type == 'SAFETY_LOOP':
        pin = str(row['safety_loop_pin'])
    elif hw_type == 'BOOMBARRIER_SENSOR_WAIT':
        pin = str(row['boombarrier_sensor_pin'])
    elif hw_type == 'BOOMBARRIER':
        pin = str(row['boom_barrier_pin'])
    else:
        pin = ''
    return {
        'hardware_device_name':         hw_name,
        'rest_endpoint':                rest_endpoint,
        'device_access_url':            access_url,
        'input_type':                   pin,
        'rest_request_json_high_value': '{"DOVal": [{"Ch": 0,"Md": 0,"Val": 1}]}' if hw_type == 'BOOMBARRIER' else '',
        'hardware_username':            'admin'  if hw_type == 'CERRADA_LPR' else '',
        'hardware_password':            'secret' if hw_type == 'CERRADA_LPR' else '',
    }


def _expected_gd_fields(gd: dict) -> dict:
    """
    Returns the expected DB field values for a GARAGE_DOOR hardware row.
    Used for diffing in db_update.
    """
    pin        = str(gd['garage_door_pin'])
    access_url = f"Advantech/{gd['controller_mac']}/ctl/do{pin}"
    rest_ep    = f"http://{gd['controller_ip']}/do_value/slot_0"
    return {
        'hardware_device_name':         gd['garage_door_name'],
        'input_type':                   pin,
        'device_access_url':            access_url,
        'rest_endpoint':                rest_ep,
        'rest_request_json_high_value': '{"DOVal": [{"Ch": 0,"Md": 0,"Val": 1}]}',
        'external_identifier':          str(gd['external_identifier']),
    }


def _build_gd_insert_sql(hw_id: int, gd: dict) -> str:
    """Generates a single INSERT INTO hardware_devices statement for a GARAGE_DOOR."""
    f   = _expected_gd_fields(gd)
    cid = gd['controller_id']
    return (
        f"INSERT INTO hardware_devices "
        f"(hardware_device_id, connection_type, type, port, url, input_type, response_type, "
        f"rest_endpoint, rest_request_json_high_value, device_access_url, max_connections_allowed, "
        f"hardware_device_name, hardware_username, hardware_password, ftp_username, ftp_password, "
        f"external_identifier, status, created_at, updated_at, controller_id, lane_id, is_stp) "
        f"VALUES ({hw_id}, 'MQTT_SERVER', 'GARAGE_DOOR', '1883', '0.0.0.0', '{f['input_type']}', 'JSON', "
        f"'{f['rest_endpoint']}', '{f['rest_request_json_high_value']}', '{f['device_access_url']}', "
        f"1, '{f['hardware_device_name']}', '', '', '', '', '{f['external_identifier']}', "
        f"'ACTIVE', now(), now(), {cid}, NULL, 1);"
    )


def _build_ctrl_insert_sql(cid: int, ip: str, mqtt_user: str, mqtt_pass: str,
                           device_name: str) -> str:
    """Generates an INSERT INTO enable_controllers for a GARAGE_DOOR-only controller."""
    hk = base64.b64encode(f"{mqtt_user}:{mqtt_pass}".encode()).decode()
    return (
        f"INSERT INTO enable_controllers "
        f"(controller_id, controller_name, header_key, ip, mqtt_port, MQTT_user, MQTT_password, "
        f"response_type, firmware_version, controller_device_name, controller_state, "
        f"timer_bw_pin_high_low, is_wise, created_at, updated_at, is_lane_independent_controller) "
        f"VALUES ({cid}, 'ADAM_HIGH', '{hk}', '{ip}', 1883, '{mqtt_user}', '{mqtt_pass}', "
        f"'JSON', 'A1.16 B02', '{device_name}', 'ACTIVE', 2, 1, now(), now(), 0);"
    )


def _build_hw_insert_sql(hw_id: int, hw_type: str, row: dict, lane_id: int) -> str:
    """Generates a single INSERT INTO hardware_devices statement."""
    f = _expected_hw_fields(row, hw_type)
    conn_type = "'FTP'" if hw_type in ['CERRADA_LPR', 'POS'] else "'MQTT_SERVER'"
    return (
        f"INSERT INTO hardware_devices "
        f"(hardware_device_id, connection_type, type, port, url, input_type, response_type, "
        f"rest_endpoint, rest_request_json_high_value, device_access_url, max_connections_allowed, "
        f"hardware_device_name, hardware_username, hardware_password, ftp_username, ftp_password, "
        f"external_identifier, status, created_at, updated_at, controller_id, lane_id, is_stp) "
        f"VALUES ({hw_id}, {conn_type}, '{hw_type}', '1883', '0.0.0.0', '{f['input_type']}', 'JSON', "
        f"'{f['rest_endpoint']}', '{f['rest_request_json_high_value']}', '{f['device_access_url']}', "
        f"1, '{f['hardware_device_name']}', '{f['hardware_username']}', '{f['hardware_password']}', "
        f"'', '', '', 'ACTIVE', now(), now(), {row['controller_id']}, {lane_id}, 1);"
    )
def _normalize_db_val(val):
    """
    Normalizes a DB value for safe comparison with JSON values.
    Handles: timedelta → int (total_seconds), Decimal → int/float, None → ''.
    """
    import datetime
    import decimal
    if val is None:
        return ''
    if isinstance(val, datetime.timedelta):
        return int(val.total_seconds())
    if isinstance(val, decimal.Decimal):
        return int(val) if val == int(val) else float(val)
    if isinstance(val, float):
        return int(val) if val == int(val) else val
    return val
# -------------------------------------------------------------------
# DB Update Handler
# -------------------------------------------------------------------
def handle_db_update(client, response_topic, payload):
    """
    Diffs the incoming JSON against current DB state and applies only
    the SQL needed for what actually changed. Handles:
      1.1  enable_config field changes
      2.1  Controller field changes (ip, credentials, name)
      2.2  New controller added
      2.3  Controller removed
      2.4  Lane field changes (loop_logic, intervals, lane_type, etc.)
      2.5  New lane added
      2.6  Lane removed (deletes hw_devices first)
      2.7  Hardware device field changes (pins, camera_ip, pos_ip, mac)
      2.8  New hardware type added to existing lane
      2.9  Hardware type removed from existing lane
      2.10 controller_device_name change → hw_device_name update
      2.11 Lane reassigned to different controller
      2.12 Same controller across multiple lanes → update once
      2.13 controller_mac change → device_access_url in hw rows
      2.14 boom_barrier_pin change → input_type + device_access_url
      2.15 camera_ip change → CERRADA_LPR device_access_url
      2.16 pos_ip change → POS/KIOSK_APP device_access_url
      2.17 hardware_type string completely replaced
      3.1  analytics_config field changes
      3.2  kiosk_config field changes
      4.1  No changes detected → respond with no_change
      4.2  Empty lane_controller_hardware → delete all lanes
      4.3  Missing section in JSON → skip that section
      4.6  lane_type change → prefix recalculated
      5.1  Controller delete order respects FK constraints
    """
    logger.info("Executing: db_update")
    db_json = payload.get("data")
    if not db_json:
        publish_response(client, response_topic, "db_update", "failed", {},
                         "No 'data' key found in payload")
        return
    try:
        conn = get_db_connection()
        sql_statements = []
        stats = {"inserted": 0, "updated": 0, "deleted": 0}
        with conn.cursor() as cursor:
            # === VALIDATION: Single-row table guard ===
            for table in ['enable_config', 'online_config', 'analytics_config', 'kiosk_config']:
                cursor.execute(f"SELECT COUNT(*) as cnt FROM `{table}`")
                count = cursor.fetchone()['cnt']
                if count > 1:
                    publish_response(client, response_topic, "db_update", "failed", {},
                                     f"Table '{table}' has {count} rows — expected exactly 1. Aborting.")
                    conn.close()
                    return
                if count == 0:
                    logger.warning(f"Table '{table}' is empty — skipping update for it.")
            # === FETCH CURRENT DB STATE ===
            cursor.execute("SELECT * FROM enable_config LIMIT 1")
            cur_enable = cursor.fetchone()
            cursor.execute("SELECT * FROM enable_controllers")
            cur_controllers = {row['controller_id']: row for row in cursor.fetchall()}
            # Lane keyed by cloud_lane_id for diffing, also keep id (= controller_id used as lane pk)
            cursor.execute("SELECT * FROM lanes")
            cur_lanes = {row['cloud_lane_id']: row for row in cursor.fetchall()}
            # Hardware keyed by lane_id → type (lane-linked hw only)
            cursor.execute(
                "SELECT * FROM hardware_devices WHERE lane_id IS NOT NULL ORDER BY hardware_device_id"
            )
            cur_hw_by_lane = {}
            for hw in cursor.fetchall():
                cur_hw_by_lane.setdefault(hw['lane_id'], {})[hw['type']] = hw
            # GARAGE_DOOR hardware keyed by external_identifier (lane_id IS NULL)
            cursor.execute(
                "SELECT * FROM hardware_devices WHERE type = 'GARAGE_DOOR' ORDER BY hardware_device_id"
            )
            cur_gd = {str(hw['external_identifier']): hw for hw in cursor.fetchall()}
            cursor.execute("SELECT * FROM analytics_config LIMIT 1")
            cur_analytics = cursor.fetchone()
            cursor.execute("SELECT * FROM kiosk_config LIMIT 1")
            cur_kiosk = cursor.fetchone()
            cursor.execute("SELECT COALESCE(MAX(hardware_device_id), 0) as max_id FROM hardware_devices")
            hw_id_counter = cursor.fetchone()['max_id'] + 1
            # Lane id counter — prevents PK conflict when lanes are reassigned
            # across controllers and new lanes are added to the same controller.
            # db_setup uses controller_id as lane id, so max(id) is always safe.
            cursor.execute("SELECT COALESCE(MAX(id), 0) as max_id FROM lanes")
            lane_id_counter = cursor.fetchone()['max_id'] + 1
        conn.close()
        # ================================================================
        # 1. enable_online_config → enable_config (single row)
        # ================================================================
        if 'enable_online_config' in db_json and cur_enable:
            new_ec = db_json['enable_online_config']
            field_map = {                  # json_key         : db_col
                'cloud_parking_id':                 'cloud_parking_id',
                'cloud_company_id':                 'cloud_company_id',
                'cloud_location_id':                'cloud_location_id',
                'company_name':                     'company_name',
                'tenant_name':                      'tenant_name',
                'parking_name':                     'parking_name',
                'nuc_device_name':                  'nuc_device_name',
                'timezone':                         'timezone',
                'geohash':                          'geo_hash',
                'country_code':                     'country_code',
                'parking_system_name':              'parking_system_name',
                'attach_not_found_lpr':             'attach_not_found_lpr',
                'open_barrier_on_sl_untrigger':     'open_barrier_on_sl_untrigger',
                'boom_barrier_async_trigger':       'boom_barrier_async_trigger',
                'analytics_enabled':                'analytics_enabled',
                'levenshtein_for_duplicate_check':  'levenshtein_for_duplicate_check',
                'place_of_issue_source':            'place_of_issue_source',
                'duplicate_request_expiry_time_sec':'duplicate_request_expiry_time_sec',
                'lane_logic_version':               'lane_logic_version',
                'tag_lp_to_image_enabled':          'tag_lp_to_image_enabled',
            }
            set_clauses = []
            for json_key, db_col in field_map.items():
                db_val = _normalize_db_val(cur_enable.get(db_col, ''))
                if json_key in new_ec and str(new_ec[json_key]) != str(db_val):
                    val = new_ec[json_key]
                    set_clauses.append(f"{db_col} = '{val}'" if isinstance(val, str) else f"{db_col} = {val}")
            if set_clauses:
                set_clauses.append("updated_at = now()")
                sql_statements.append(
                    f"UPDATE enable_config SET {', '.join(set_clauses)} "
                    f"WHERE cloud_parking_id = {cur_enable['cloud_parking_id']};"
                )
                stats['updated'] += 1
                logger.info(f"[db_update] enable_config: {len(set_clauses)-1} field(s) changed")
        # ================================================================
        # 2. lane_controller_hardware → enable_controllers + lanes + hw
        # ================================================================
        if 'lane_controller_hardware' in db_json:
            new_rows = db_json['lane_controller_hardware']
            # Build new-state indexes (case 2.12: controller seen once only)
            new_controllers = {}        # controller_id → first row that defines it
            new_lanes = {}              # cloud_lane_id → row
            for row in new_rows:
                if row['controller_id'] not in new_controllers:
                    new_controllers[row['controller_id']] = row
                new_lanes[row['cloud_lane_id']] = row
            cur_ctrl_ids  = set(cur_controllers.keys())
            new_ctrl_ids  = set(new_controllers.keys())
            cur_lane_ids  = set(cur_lanes.keys())
            new_lane_ids  = set(new_lanes.keys())
            # ---- INSERT new controllers first (lanes depend on them) ----
            for cid in new_ctrl_ids - cur_ctrl_ids:
                row = new_controllers[cid]
                hk  = base64.b64encode(f"{row['MQTT_user']}:{row['MQTT_password']}".encode()).decode()
                sql_statements.append(
                    f"INSERT INTO enable_controllers "
                    f"(controller_id, controller_name, header_key, ip, mqtt_port, MQTT_user, MQTT_password, "
                    f"response_type, firmware_version, controller_device_name, controller_state, "
                    f"timer_bw_pin_high_low, is_wise, created_at, updated_at, is_lane_independent_controller) "
                    f"VALUES ({cid}, '{row['controller_name']}', '{hk}', '{row['controller_ip']}', "
                    f"1883, '{row['MQTT_user']}', '{row['MQTT_password']}', 'JSON', 'A1.16 B02', "
                    f"'{row['controller_device_name']}', 'ACTIVE', 2, 1, now(), now(), 0);"
                )
                stats['inserted'] += 1
                logger.info(f"[db_update] INSERT controller_id={cid}")
            # ---- UPDATE existing controllers if fields changed ----
            for cid in cur_ctrl_ids & new_ctrl_ids:
                new_row = new_controllers[cid]
                cur_row = cur_controllers[cid]
                set_clauses = []
                if new_row['controller_name'] != str(cur_row.get('controller_name', '')):
                    set_clauses.append(f"controller_name = '{new_row['controller_name']}'")
                if new_row['controller_ip'] != str(cur_row.get('ip', '')):
                    set_clauses.append(f"ip = '{new_row['controller_ip']}'")
                if new_row['controller_device_name'] != str(cur_row.get('controller_device_name', '')):
                    set_clauses.append(f"controller_device_name = '{new_row['controller_device_name']}'")
                # Credentials changed → also regenerate header_key
                if (new_row['MQTT_user'] != str(cur_row.get('MQTT_user', '')) or
                        new_row['MQTT_password'] != str(cur_row.get('MQTT_password', ''))):
                    hk = base64.b64encode(f"{new_row['MQTT_user']}:{new_row['MQTT_password']}".encode()).decode()
                    set_clauses += [
                        f"MQTT_user = '{new_row['MQTT_user']}'",
                        f"MQTT_password = '{new_row['MQTT_password']}'",
                        f"header_key = '{hk}'",
                    ]
                if set_clauses:
                    set_clauses.append("updated_at = now()")
                    sql_statements.append(
                        f"UPDATE enable_controllers SET {', '.join(set_clauses)} WHERE controller_id = {cid};"
                    )
                    stats['updated'] += 1
                    logger.info(f"[db_update] UPDATE controller_id={cid}: {set_clauses}")
            # ---- INSERT new lanes ----
            for clid in new_lane_ids - cur_lane_ids:
                row    = new_lanes[clid]
                prefix = f"{row['lane_type']}_{clid}"
                # Use lane_id_counter instead of controller_id to avoid PK conflicts
                # when a lane is reassigned (old lane keeps the old id) and a new
                # lane is inserted on the same controller.
                lane_id = lane_id_counter
                lane_id_counter += 1
                sql_statements.append(
                    f"INSERT INTO lanes "
                    f"(id, cloud_lane_id, vehicle_type, prefix, loop_logic, loop1_interval, loop2_interval, "
                    f"boom_barrier_interval, is_boombarrier_active, is_ticket_dispenser_active, "
                    f"ticket_dispenser_suppressed_value_true, lane_type, timer_bw_loops, online_media_type, "
                    f"created_at, updated_at, lane_state, lane_name, max_time_between_loops, "
                    f"is_deep_health_check, controller_id, pos_reset_trigger, pos_trigger_enabled, "
                    f"is_lane_disabled, lane_pair_id, bb_sensor_wait_timeout, capture_lp_seconds) "
                    f"VALUES ({lane_id}, {clid}, 'CAR', '{prefix}', '{row['loop_logic']}', "
                    f"{row['loop1_interval']}, {row['loop2_interval']}, {row['boom_barrier_interval']}, "
                    f"{row['is_boombarrier_active']}, 0, 1, '{row['lane_type']}', 0, 'LPR', now(), now(), "
                    f"'ACTIVE', '{row['controller_device_name']}', 0, 0, {row['controller_id']}, "
                    f"{row.get('pos_reset_trigger', 0)}, {row.get('pos_trigger_enabled', 0)}, "
                    f"0, NULL, 30, 5);"
                )
                stats['inserted'] += 1
                logger.info(f"[db_update] INSERT lane cloud_lane_id={clid} id={lane_id}")
                for hw_type in [h.strip() for h in row['hardware_type'].split(',')]:
                    sql_statements.append(_build_hw_insert_sql(hw_id_counter, hw_type, row, lane_id))
                    hw_id_counter += 1
                    stats['inserted'] += 1
            # ---- UPDATE existing lanes (and their hardware) ----
            for clid in cur_lane_ids & new_lane_ids:
                new_row = new_lanes[clid]
                cur_row = cur_lanes[clid]
                lane_id = cur_row['id']     # PK of lanes (= controller_id at insert time)
                set_clauses = []
                # Case 2.4: basic lane fields
                lane_field_map = [
                    ('loop_logic',          'loop_logic',          str),
                    ('loop1_interval',      'loop1_interval',      int),
                    ('loop2_interval',      'loop2_interval',      int),
                    ('boom_barrier_interval','boom_barrier_interval',int),
                    ('is_boombarrier_active','is_boombarrier_active',int),
                    ('pos_reset_trigger',   'pos_reset_trigger',   int),
                    ('pos_trigger_enabled', 'pos_trigger_enabled', int),
                ]
                for json_key, db_col, t in lane_field_map:
                    nv = new_row.get(json_key)
                    db_val = _normalize_db_val(cur_row.get(db_col, ''))
                    if nv is not None and str(nv) != str(db_val):
                        set_clauses.append(f"{db_col} = '{nv}'" if t == str else f"{db_col} = {nv}")
                # Case 4.6: lane_type changed → recalculate prefix
                if new_row['lane_type'] != str(cur_row.get('lane_type', '')):
                    new_prefix = f"{new_row['lane_type']}_{clid}"
                    set_clauses += [f"lane_type = '{new_row['lane_type']}'", f"prefix = '{new_prefix}'"]
                # Case 2.10: controller_device_name → also updates lane_name
                if new_row['controller_device_name'] != str(cur_row.get('lane_name', '')):
                    set_clauses.append(f"lane_name = '{new_row['controller_device_name']}'")
                # Case 2.11: lane reassigned to a different controller
                if new_row['controller_id'] != cur_row.get('controller_id'):
                    set_clauses.append(f"controller_id = {new_row['controller_id']}")
                    sql_statements.append(
                        f"UPDATE hardware_devices SET controller_id = {new_row['controller_id']} "
                        f"WHERE lane_id = {lane_id};"
                    )
                    stats['updated'] += 1
                if set_clauses:
                    set_clauses.append("updated_at = now()")
                    sql_statements.append(
                        f"UPDATE lanes SET {', '.join(set_clauses)} WHERE cloud_lane_id = {clid};"
                    )
                    stats['updated'] += 1
                    logger.info(f"[db_update] UPDATE lane cloud_lane_id={clid}: {set_clauses}")
                # ---- Hardware diff for this lane ----
                cur_hw  = cur_hw_by_lane.get(lane_id, {})
                new_hw_types = set(h.strip() for h in new_row['hardware_type'].split(','))
                cur_hw_types = set(cur_hw.keys())
                # Case 2.9: hardware type removed
                for hw_type in cur_hw_types - new_hw_types:
                    sql_statements.append(
                        f"DELETE FROM hardware_devices WHERE lane_id = {lane_id} AND type = '{hw_type}';"
                    )
                    stats['deleted'] += 1
                    logger.info(f"[db_update] DELETE hw type={hw_type} lane_id={lane_id}")
                # Case 2.8: hardware type added
                for hw_type in new_hw_types - cur_hw_types:
                    sql_statements.append(_build_hw_insert_sql(hw_id_counter, hw_type, new_row, lane_id))
                    hw_id_counter += 1
                    stats['inserted'] += 1
                    logger.info(f"[db_update] INSERT hw type={hw_type} lane_id={lane_id}")
                # Case 2.7/2.10/2.13/2.14/2.15/2.16: existing hw field changes
                for hw_type in new_hw_types & cur_hw_types:
                    expected = _expected_hw_fields(new_row, hw_type)
                    cur_hw_row = cur_hw[hw_type]
                    hw_set = []
                    for db_col, new_val in expected.items():
                        db_val = _normalize_db_val(cur_hw_row.get(db_col, ''))
                        if str(new_val) != str(db_val):
                            hw_set.append(f"{db_col} = '{new_val}'")
                    if hw_set:
                        hw_set.append("updated_at = now()")
                        sql_statements.append(
                            f"UPDATE hardware_devices SET {', '.join(hw_set)} "
                            f"WHERE lane_id = {lane_id} AND type = '{hw_type}';"
                        )
                        stats['updated'] += 1
                        logger.info(f"[db_update] UPDATE hw type={hw_type} lane_id={lane_id}: {hw_set}")
            # ---- DELETE removed lanes (hw first, then lane) ----
            for clid in cur_lane_ids - new_lane_ids:
                lane_id = cur_lanes[clid]['id']
                sql_statements.append(f"DELETE FROM hardware_devices WHERE lane_id = {lane_id};")
                sql_statements.append(f"DELETE FROM lanes WHERE cloud_lane_id = {clid};")
                stats['deleted'] += 2
                logger.info(f"[db_update] DELETE lane cloud_lane_id={clid} (and its hw)")
            # ---- DELETE removed controllers (after their lanes are gone) ----
            # A controller is only deleted if it's absent from BOTH lane_controller_hardware
            # AND garage_door[] — handled after section 5 so new_gd_ctrl_ids is available
            _pending_ctrl_deletes = cur_ctrl_ids - new_ctrl_ids
        # ================================================================
        # 3. analytics_config — single row UPDATE
        # ================================================================
        if 'analytics_config' in db_json and cur_analytics:
            new_a = db_json['analytics_config']
            set_clauses = []
            for field in ['mqtt_broker_address', 'mqtt_port', 'mqtt_user', 'mqtt_password']:
                db_val = _normalize_db_val(cur_analytics.get(field, ''))
                if str(new_a.get(field, '')) != str(db_val):
                    val = new_a[field]
                    set_clauses.append(f"{field} = '{val}'" if isinstance(val, str) else f"{field} = {val}")
            if set_clauses:
                sql_statements.append(f"UPDATE analytics_config SET {', '.join(set_clauses)};")
                stats['updated'] += 1
                logger.info(f"[db_update] analytics_config: {len(set_clauses)} field(s) changed")
        # ================================================================
        # 4. kiosk_config — single row UPDATE
        # ================================================================
        if 'kiosk_config' in db_json and cur_kiosk:
            new_k = db_json['kiosk_config']
            set_clauses = []
            for field in ['mqtt_broker_address', 'mqtt_port', 'mqtt_user', 'mqtt_password']:
                db_val = _normalize_db_val(cur_kiosk.get(field, ''))
                if str(new_k.get(field, '')) != str(db_val):
                    val = new_k[field]
                    set_clauses.append(f"{field} = '{val}'" if isinstance(val, str) else f"{field} = {val}")
            if set_clauses:
                sql_statements.append(f"UPDATE kiosk_config SET {', '.join(set_clauses)};")
                stats['updated'] += 1
                logger.info(f"[db_update] kiosk_config: {len(set_clauses)} field(s) changed")
        # ================================================================
        # 5. garage_door — diff by external_identifier, lane_id = NULL
        # ================================================================
        new_gd_ctrl_ids = set()   # controller_ids referenced by incoming garage_door[]
        if 'garage_door' in db_json:
            new_gd_rows = db_json['garage_door']
            new_gd = {str(gd['external_identifier']): gd for gd in new_gd_rows}
            cur_gd_ids = set(cur_gd.keys())
            new_gd_ids = set(new_gd.keys())
            new_gd_ctrl_ids = {gd['controller_id'] for gd in new_gd_rows}

            # ---- INSERT new GARAGE_DOORs ----
            for ext_id in new_gd_ids - cur_gd_ids:
                gd = new_gd[ext_id]
                cid = gd['controller_id']
                # Insert controller if it doesn't exist (not from lane section either)
                if cid not in cur_controllers and cid not in {r['controller_id'] for r in
                        db_json.get('lane_controller_hardware', [])
                        if r['controller_id'] == cid}:
                    # Only insert if truly new
                    if cid not in cur_controllers:
                        sql_statements.append(_build_ctrl_insert_sql(
                            cid, gd['controller_ip'],
                            gd['MQTT_user'], gd['MQTT_password'],
                            gd['garage_door_name']
                        ))
                        stats['inserted'] += 1
                        logger.info(f"[db_update] INSERT GD controller_id={cid}")
                sql_statements.append(_build_gd_insert_sql(hw_id_counter, gd))
                hw_id_counter += 1
                stats['inserted'] += 1
                logger.info(f"[db_update] INSERT GARAGE_DOOR ext_id={ext_id}")

            # ---- UPDATE existing GARAGE_DOORs ----
            for ext_id in cur_gd_ids & new_gd_ids:
                gd       = new_gd[ext_id]
                cur_row  = cur_gd[ext_id]
                expected = _expected_gd_fields(gd)
                hw_set   = []
                for db_col, new_val in expected.items():
                    db_val = _normalize_db_val(cur_row.get(db_col, ''))
                    if str(new_val) != str(db_val):
                        hw_set.append(f"{db_col} = '{new_val}'")
                # Also check controller_id change
                if gd['controller_id'] != cur_row.get('controller_id'):
                    hw_set.append(f"controller_id = {gd['controller_id']}")
                if hw_set:
                    hw_set.append("updated_at = now()")
                    sql_statements.append(
                        f"UPDATE hardware_devices SET {', '.join(hw_set)} "
                        f"WHERE type = 'GARAGE_DOOR' AND external_identifier = '{ext_id}';"
                    )
                    stats['updated'] += 1
                    logger.info(f"[db_update] UPDATE GARAGE_DOOR ext_id={ext_id}: {hw_set}")

            # ---- DELETE removed GARAGE_DOORs ----
            for ext_id in cur_gd_ids - new_gd_ids:
                sql_statements.append(
                    f"DELETE FROM hardware_devices WHERE type = 'GARAGE_DOOR' "
                    f"AND external_identifier = '{ext_id}';"
                )
                stats['deleted'] += 1
                logger.info(f"[db_update] DELETE GARAGE_DOOR ext_id={ext_id}")

        # ================================================================
        # Controller deletion — only if absent from both lane AND garage_door
        # ================================================================
        if 'lane_controller_hardware' in db_json:
            for cid in _pending_ctrl_deletes:
                if cid not in new_gd_ctrl_ids:
                    # Also delete any GD hw on this controller before removing controller
                    sql_statements.append(
                        f"DELETE FROM hardware_devices "
                        f"WHERE controller_id = {cid} AND type = 'GARAGE_DOOR';"
                    )
                    sql_statements.append(
                        f"DELETE FROM enable_controllers WHERE controller_id = {cid};"
                    )
                    stats['deleted'] += 1
                    logger.info(f"[db_update] DELETE controller_id={cid}")

        # ================================================================
        # Case 4.1: No changes detected
        # ================================================================
        if not sql_statements:
            logger.info("[db_update] No changes detected.")
            publish_response(client, response_topic, "db_update", "success",
                             {"message": "No changes detected.", "stats": stats})
            return
        # Log all statements for audit trail
        for i, sql in enumerate(sql_statements, 1):
            logger.info(f"[db_update] Statement {i}:\n{sql}")
        # Execute everything in one transaction
        success, error = execute_sql_transaction(sql_statements)
        if success:
            publish_response(client, response_topic, "db_update", "success", {
                "message": "DB updated successfully.",
                "stats": stats,
                "statements_executed": len(sql_statements)
            })
            _restart_docker_if_enabled()
        else:
            publish_response(client, response_topic, "db_update", "failed", {}, error)
    except Exception as e:
        logger.error(f"db_update failed: {e}")
        publish_response(client, response_topic, "db_update", "failed", {}, str(e))
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
            # 4. hardware_devices — grouped by lane_id (lane-linked only)
            cursor.execute(
                "SELECT lane_id, type, input_type, device_access_url "
                "FROM hardware_devices WHERE lane_id IS NOT NULL ORDER BY hardware_device_id"
            )
            hw_by_lane = {}
            for hw in cursor.fetchall():
                lid = hw['lane_id']
                hw_by_lane.setdefault(lid, []).append(hw)
            # 5. garage_door hardware (lane_id IS NULL)
            cursor.execute(
                "SELECT controller_id, input_type, device_access_url, hardware_device_name, "
                "external_identifier, rest_endpoint "
                "FROM hardware_devices WHERE type = 'GARAGE_DOOR' ORDER BY hardware_device_id"
            )
            gd_rows_db = cursor.fetchall()
            # 6. analytics_config
            cursor.execute(
                "SELECT mqtt_broker_address, mqtt_port, mqtt_user, mqtt_password "
                "FROM analytics_config LIMIT 1"
            )
            analytics = cursor.fetchone()
            # 7. kiosk_config
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
        # --- Reconstruct garage_door[] ---
        garage_door = []
        for gd in gd_rows_db:
            ctrl = controllers.get(gd['controller_id'], {})
            # Recover controller_mac from device_access_url: "Advantech/{mac}/ctl/do{pin}"
            try:
                ctrl_mac = gd['device_access_url'].split('/')[1]
            except (IndexError, AttributeError):
                ctrl_mac = ''
            garage_door.append({
                "controller_id":       gd['controller_id'],
                "controller_ip":       ctrl.get('ip', ''),
                "controller_mac":      ctrl_mac,
                "MQTT_user":           ctrl.get('MQTT_user', ''),
                "MQTT_password":       ctrl.get('MQTT_password', ''),
                "garage_door_name":    gd['hardware_device_name'],
                "garage_door_pin":     gd['input_type'],
                "external_identifier": gd['external_identifier'],
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
                "geohash":                        ec['geo_hash'],
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
            "garage_door":             garage_door,
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
# Scheduled DB Update — Firebase Polling
# -------------------------------------------------------------------

def _restart_docker_if_enabled():
    """Restarts Docker containers listed in DOCKER_CONTAINERS if flag is enabled."""
    if not RESTART_DOCKER_ON_UPDATE:
        return
    for container in DOCKER_CONTAINERS:
        try:
            logger.info(f"[scheduler] Restarting Docker container: {container}")
            subprocess.run(["docker", "restart", container], check=True, timeout=30)
            logger.info(f"[scheduler] Container '{container}' restarted successfully.")
        except subprocess.CalledProcessError as e:
            logger.error(f"[scheduler] Failed to restart container '{container}': {e}")
        except subprocess.TimeoutExpired:
            logger.error(f"[scheduler] Timeout restarting container '{container}'")


def _get_db_schedule_params():
    """
    Reads tenant_name and cloud_parking_id from enable_config.
    Returns (tenant_name, cloud_parking_id, mac_with_colons) or None if DB not ready.
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            cursor.execute("SELECT tenant_name, cloud_parking_id FROM enable_config LIMIT 1")
            row = cursor.fetchone()
        conn.close()
        if not row:
            logger.warning("[scheduler] enable_config is empty — skipping schedule check.")
            return None
        mac = get_mac_address(strip_colons=False)   # e.g. 1c:69:7a:a6:9f:15
        return row['tenant_name'], row['cloud_parking_id'], mac
    except Exception as e:
        logger.error(f"[scheduler] Failed to read DB schedule params: {e}")
        return None


def _fetch_firebase_schedule_json(tenant, parking_id, mac):
    """
    Fetches the scheduled db.json from Firebase Storage using the Admin SDK.
    Returns parsed dict or None on any error.
    """
    try:
        import firebase_admin
        from firebase_admin import credentials, storage as fb_storage

        # Initialise app only once
        if not firebase_admin._apps:
            cred = credentials.Certificate(FIREBASE_SA_KEY_PATH)
            firebase_admin.initialize_app(cred, {'storageBucket': FIREBASE_BUCKET})

        bucket  = fb_storage.bucket()
        path    = f"nucConfigs/{tenant}/{parking_id}/{mac}/db.json"
        blob    = bucket.blob(path)

        if not blob.exists():
            logger.info(f"[scheduler] No schedule file found at '{path}'.")
            return None

        content = blob.download_as_text()
        return json.loads(content)
    except FileNotFoundError:
        logger.error(f"[scheduler] Service account key not found: {FIREBASE_SA_KEY_PATH}")
        return None
    except Exception as e:
        logger.error(f"[scheduler] Firebase fetch error: {e}")
        return None


def _read_last_schedule_state():
    """Returns the last executed scheduled_at string, or None if never executed."""
    try:
        with open(LAST_SCHEDULE_STATE_FILE, 'r') as f:
            return json.load(f).get('last_executed_scheduled_at')
    except (FileNotFoundError, json.JSONDecodeError):
        return None


def _write_last_schedule_state(scheduled_at_str):
    """Persists the last executed scheduled_at to disk."""
    try:
        os.makedirs(os.path.dirname(LAST_SCHEDULE_STATE_FILE), exist_ok=True)
        with open(LAST_SCHEDULE_STATE_FILE, 'w') as f:
            json.dump({'last_executed_scheduled_at': scheduled_at_str}, f)
    except Exception as e:
        logger.error(f"[scheduler] Failed to write schedule state: {e}")


def _should_execute_schedule(scheduled_at_str, last_executed_str):
    """
    Returns (should_execute: bool, reason: str).
    Decision table:
      - scheduled_at == last_executed  → skip (already ran)
      - scheduled_at > now (UTC)       → skip (future)
      - now - scheduled_at > MAX_LATE  → skip (too old)
      - otherwise                      → execute
    """
    if scheduled_at_str == last_executed_str:
        return False, "already executed"
    try:
        scheduled_at = datetime.datetime.fromisoformat(
            scheduled_at_str.replace('Z', '+00:00')
        )
        now_utc = datetime.datetime.now(datetime.timezone.utc)
        if scheduled_at > now_utc:
            return False, f"scheduled for {scheduled_at_str} (future)"
        age_hours = (now_utc - scheduled_at).total_seconds() / 3600
        if age_hours > MAX_LATE_EXECUTION_HOURS:
            return False, f"too old ({age_hours:.1f}h > {MAX_LATE_EXECUTION_HOURS}h limit)"
        return True, f"ready (scheduled={scheduled_at_str}, age={age_hours:.1f}h)"
    except (ValueError, TypeError) as e:
        return False, f"invalid scheduled_at format: {e}"


def _run_scheduled_db_update(schedule_json, scheduled_at_str):
    """
    Executes a db_update directly from the scheduler (no MQTT client/topic needed).
    Uses a no-op client stub so handle_db_update can publish responses without error.
    """
    class _NullClient:
        """Stub MQTT client — discards all publish calls from handle_db_update."""
        def publish(self, *args, **kwargs): pass

    logger.info(f"[scheduler] Executing scheduled db_update for scheduled_at={scheduled_at_str}")
    with _db_update_lock:
        handle_db_update(_NullClient(), "__scheduler__", schedule_json)
    logger.info(f"[scheduler] Scheduled db_update completed for scheduled_at={scheduled_at_str}")
    _write_last_schedule_state(scheduled_at_str)
    _restart_docker_if_enabled()


def check_and_apply_scheduled_update():
    """Main scheduler logic — called every SCHEDULE_POLL_INTERVAL seconds."""
    logger.debug("[scheduler] Checking Firebase for scheduled update...")

    params = _get_db_schedule_params()
    if not params:
        return
    tenant, parking_id, mac = params

    schedule_json = _fetch_firebase_schedule_json(tenant, parking_id, mac)
    if not schedule_json:
        return

    # Validate payload structure
    if schedule_json.get('action') != 'db_update':
        logger.warning("[scheduler] Firebase JSON action is not 'db_update' — skipping.")
        return
    if 'data' not in schedule_json:
        logger.warning("[scheduler] Firebase JSON missing 'data' key — skipping.")
        return
    scheduled_at_str = schedule_json.get('scheduled_at')
    if not scheduled_at_str:
        logger.warning("[scheduler] Firebase JSON missing 'scheduled_at' — skipping.")
        return

    last_executed = _read_last_schedule_state()
    should_run, reason = _should_execute_schedule(scheduled_at_str, last_executed)
    logger.info(f"[scheduler] scheduled_at={scheduled_at_str} | {('EXECUTE' if should_run else 'SKIP')}: {reason}")

    if should_run:
        _run_scheduled_db_update(schedule_json, scheduled_at_str)


def scheduled_update_loop():
    """Background daemon thread — polls Firebase every SCHEDULE_POLL_INTERVAL seconds."""
    logger.info(f"[scheduler] Started. Poll interval: {SCHEDULE_POLL_INTERVAL}s "
                f"| Max late execution: {MAX_LATE_EXECUTION_HOURS}h")
    while True:
        try:
            check_and_apply_scheduled_update()
        except Exception as e:
            logger.error(f"[scheduler] Unexpected error in scheduler loop: {e}")
        time.sleep(SCHEDULE_POLL_INTERVAL)


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
    "db_get":       handle_db_get,
    "db_update":    handle_db_update,      # <-- NEW
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
    RC_CODES = {0: "Success", 1: "Bad protocol", 2: "Client ID rejected",
                3: "Server unavailable", 4: "Bad credentials", 5: "Not authorized"}
    if rc == 0:
        logger.info(f"Connected to MQTT Broker. Subscribing to: {userdata['command_topic']}")
        client.subscribe(userdata['command_topic'])
    else:
        logger.error(f"MQTT connection refused: rc={rc} ({RC_CODES.get(rc, 'Unknown')})")

def on_disconnect(client, userdata, rc):
    if rc == 0:
        logger.info("Disconnected cleanly from MQTT Broker.")
    else:
        logger.warning(f"Unexpected MQTT disconnect (rc={rc}). Will auto-reconnect...")

if __name__ == "__main__":
    logger.info("Starting GMP Agent Service...")
    signal.signal(signal.SIGINT,  lambda s, f: sys.exit(0))
    signal.signal(signal.SIGTERM, lambda s, f: sys.exit(0))

    # Kill any other running agent instances to prevent client_id conflicts
    my_pid = os.getpid()
    try:
        import subprocess as _sp
        result = _sp.run(
            ["pgrep", "-f", "mqtt_v[0-9]+\\.py|gmp_agent\\.py"],
            capture_output=True, text=True
        )
        for pid_str in result.stdout.strip().split('\n'):
            if pid_str and int(pid_str) != my_pid:
                logger.warning(f"Killing stale agent process PID={pid_str}")
                os.kill(int(pid_str), signal.SIGTERM)
                time.sleep(1)
    except Exception:
        pass

    DEVICE_MAC_ID  = get_mac_address(strip_colons=True)
    COMMAND_TOPIC  = f"gmp/device/{DEVICE_MAC_ID}/command"
    RESPONSE_TOPIC = f"gmp/device/{DEVICE_MAC_ID}/response"
    logger.info(f"Agent ID: {DEVICE_MAC_ID} | PID: {my_pid}")

    client = mqtt.Client(
        client_id=f"gmp_agent_{DEVICE_MAC_ID}",
        clean_session=True,
        userdata={
            "command_topic":  COMMAND_TOPIC,
            "response_topic": RESPONSE_TOPIC
        }
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.tls_set()
    client.on_connect    = on_connect
    client.on_disconnect = on_disconnect
    client.on_message    = on_message

    # Prevent rapid reconnection loops (min 5s, max 30s between retries)
    client.reconnect_delay_set(min_delay=5, max_delay=30)

    try:
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

        # Start Firebase scheduled update background thread
        sched_thread = threading.Thread(target=scheduled_update_loop, daemon=True,
                                        name="scheduler")
        sched_thread.start()
        logger.info("[scheduler] Background thread started.")

        client.loop_forever()
    except Exception as e:
        logger.critical(f"Fatal error: {e}")
        sys.exit(1)
