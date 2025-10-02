import paho.mqtt.client as mqttClient
import time
import ssl
import json

Connected = False  # global variable for the state of the connection
mqtt_client = None

broker_address = "0.0.0.0"
port = 1883
user = ""
password = ""
parking = 1

tenant = "tenant_name"
company_id = "company_id"
parking_id = "parking_id"

t_prefix = tenant + "/" + company_id + "/"  + parking_id

def on_connect(client, userdata, flags, rc):
    print("on connect")
    if rc == 0:
        print("Connected to broker")
        global Connected  # Use global variable
        Connected = True  # Signal connection
        global mqtt_client
        global t_prefix
        try:
            topics = []
            topics.append(t_prefix + "/enable_update/pull_image")
            topics.append(t_prefix + "/enable_update/update_docker_compose")
            topics.append(t_prefix + "/enable_update/restart_docker")
            for topic in topics:
                mqtt_client.subscribe(topic, 0)
        except KeyboardInterrupt:
            mqtt_client.disconnect()
            mqtt_client.loop_stop()
    else:
        print("Connection failed")


def on_publish(client, user_data, result):
    print("message published")


# The callback for when a PUBLISH message is received from the server.
def on_message(client, userdata, msg):
    print("message received")
    req = json.loads(msg.payload.decode())
    topic = str(msg.topic)
    from _thread import start_new_thread
    start_new_thread(process_message, (req, topic,))

def process_message(req, topic):
    global t_prefix
    import docker
    client = docker.from_env()
    container_list = client.containers.list()
    enable_image = None
    running_docker_id = None
    docker_image_name = req['dockerImageToBeUpdate']
    print(docker_image_name)
    for container in container_list:
        running_image_name = container.attrs['Config']['Image']
        if "enable" in running_image_name:
            running_docker_id = container.short_id
            enable_image = running_image_name
            break
  
    print("123")
    if topic == t_prefix + "/enable_update/pull_image":
        print("456")
        t = topic + "/response"
        if enable_image == docker_image_name:
            print("same image")
            mqtt_client = get_mqtt_client()
            mqtt_client.publish(t, "success")
        else:
            print("image is different")
            try:
                import time
                downloaded_image = client.images.pull(docker_image_name)
                print("Pulling of the new image completed - " + downloaded_image.tags[0])
                print(t)
                mqtt_client = get_mqtt_client()
                mqtt_client.publish(t, "success")
            except Exception as e:
                print("error pulling new image" + str(e))
                mqtt_client = get_mqtt_client()
                mqtt_client.publish(t, "Fail - " + str(e))
            
    if topic == t_prefix + "/enable_update/update_docker_compose":
        t = topic + "/response"
        from subprocess import PIPE, run
        operation = "docker_compose_update"
        # command = ['sh', 'dockercompose.sh', '-d', '$running_docker_id', '-r', '$enable_image', '-n', '$docker_image_name']
        command = ['sh', 'dockercompose.sh', '-d', running_docker_id, '-r', enable_image, '-n', docker_image_name, '-p', operation]
        print(str(command))
        result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
        print(result.returncode, result.stdout, result.stderr)
        mqtt_client = get_mqtt_client()
        mqtt_client.publish(t, "success")
        
    if topic == t_prefix + "/enable_update/restart_docker":
        t = topic + "/response"
        from subprocess import PIPE, run
        operation = "docker_container_restart"
        # command = ['sh', 'dockercompose.sh', '-d', '$running_docker_id', '-r', '$enable_image', '-n', '$docker_image_name']
        command = ['sh', 'dockercompose.sh', '-d', running_docker_id, '-r', enable_image, '-n', docker_image_name, '-p', operation]
        print(str(command))
        result = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True)
        print(result.returncode, result.stdout, result.stderr)
        mqtt_client = get_mqtt_client()
        mqtt_client.publish(t, "success")

def get_mqtt_client():
    global mqtt_client
    return mqtt_client

def self_check():
    print("self check")
    global mqtt_client
    count = 0
    log_counter = 0
    while True:
        count += 1
        log_counter += 1
        if count % 10 == 0:
            count = 0
        time.sleep(1)
        if mqtt_client.is_connected():
            if log_counter % 6 == 0:
                log_counter = 0
                print("MQTT CLIENT SELF CHECK PASSED")
        else:
            print("MQTT CLIENT SELF CHECK FAILED - RECONNECTING")
            try:
                mqtt_client.reconnect()
            except Exception as err:
                print("Reconnect failed with - " + str(err))


if __name__ == "__main__":
    mqtt_client = mqttClient.Client("Enable - Backend")

    # ssl_ctx = ssl.create_default_context()
    # ssl_ctx.check_hostname = False  # Without this line, I does not work
    # mqtt_client.tls_set_context(ssl_ctx)
    # mqtt_client.tls_insecure_set(True)
    # mqtt_client.username_pw_set(user, password=password)    #set username and password

    mqtt_client.on_connect = on_connect  # attach function to callback 
    mqtt_client.on_publish = on_publish
    mqtt_client.on_message = on_message
    mqtt_client.set_clean_session = True
    try:
        mqtt_client.connect_async(host=broker_address, port=port, keepalive=60, bind_address='')
    except Exception as err:
        print(str(err))
        time.sleep(5)
    mqtt_client.reconnect_delay_set(1, 5)
    from _thread import start_new_thread
    start_new_thread(self_check, ())    
    mqtt_client.loop_forever(retry_first_connection=True)

