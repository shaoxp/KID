from pickle import FALSE
import pprint
import threading
from kafka import KafkaProducer
import json
import datetime
import _thread

class MessageProducer:
    broker = ""
    topic = ""
    producer = None

    def __init__(self, broker, topic):
        self.broker = broker
        self.topic = topic
        self.producer = KafkaProducer(bootstrap_servers=self.broker,
        value_serializer=lambda v: json.dumps(v).encode('utf-8'),
        key_serializer=str.encode,
        acks='all',
        retries = 3)


    def send_msg(self, msg):
        # print("sending message...")
        future = self.producer.send(self.topic,msg,msg["tag"]+"-"+ str(msg.get("device_name")))
        self.producer.flush()
        future.get(timeout=60)
        # print("message sent successfully...")
        return {'status_code':200, 'error':None}


class StatsReporter(threading.Thread):
    def __init__(self, interval, producer, event=None, raw_metrics=False):
        super(StatsReporter, self).__init__()
        self.interval = interval
        self.producer = producer
        self.event = event
        self.raw_metrics = raw_metrics

    def print_stats(self):
        metrics = self.producer.metrics()
        if self.raw_metrics:
            pprint.pprint(metrics)
        else:
            print('{record-send-rate} records/sec ({byte-rate} B/sec),'
                  ' {request-latency-avg} latency,'
                  ' {record-size-avg} record size,'
                  ' {batch-size-avg} batch size,'
                  ' {records-per-request-avg} records/req'
                  .format(**metrics['producer-metrics']))

    def print_final(self):
        self.print_stats()

    def run(self):
        while self.event and not self.event.wait(self.interval):
            self.print_stats()
        else:
            self.print_final()

def send_test_message( threadName, message_producer):
  sample_messages =[
    json.loads('''{"tag":"MTConnectDataItems","tenant":"atl","plant":"atl","agent":"atl-connectdata-bridge","schema_version":"3","agent_version":"2.1.0-rc.171","instance_id":"2.1.0-rc.171","creation_time":"2023-01-02T09:35:41.638Z","received_at":"2023-01-02T09:35:41.638Z","data_item_id":"analytics_welding_processparametersstep_a/robot_program_welding_time","timestamp":"2022-12-01T13:24:47.387000Z","data_item_name":"a/robot_program_welding_time","unavailable":false,"category":"SAMPLE","device_name":"analytics_welding_processparametersstep","device_uuid":"analytics_welding_processparametersstep","data_item_path":"analytics_welding_processparametersstep<Device>.a/robot_program_welding_time","data_item_type":"ACCUMULATED_TIME","collector_version":"2.1.0-rc.171","value_sample":1,"meta":[]}''')
    ,json.loads('''{"tag":"MTConnectDataItems","tenant":"atl","plant":"atl","agent":"atl.6004","schema_version":"3","agent_version":"1.3.0.18","instance_id":"1671118159","creation_time":"2022-12-16T07:41:44.000Z","received_at":"2022-12-16T07:41:44.828Z","data_item_id":"Automatic_SLA_350_1_laserPWM","timestamp":"2022-12-16T07:41:44.275Z","data_item_name":"LaserPulseWidthModulation","sequence":1022138,"unavailable":false,"category":"SAMPLE","value_sample":44,"device_name":"Automatic_SLA_350_1","device_uuid":"Automatic_SLA_350_1","data_item_type":"CONCENTRATION","data_item_path":"Automatic_SLA_350_1<Device>.Laser<sensor>","collector_version":"1.2.0-rc.4"}''')
    ,json.loads('''{"tag":"MTConnectDataItems","tenant":"atl","plant":"atl","agent":"atl.6003","schema_version":"3","agent_version":"1.3.0.18","instance_id":"1671117580","creation_time":"2022-12-16T07:41:45.000Z","received_at":"2022-12-16T07:41:45.701Z","data_item_id":"Demo_CNC_01__d3280f_Xact","timestamp":"2022-12-16T07:41:44.835Z","data_item_name":"Xact","sequence":467210,"sub_type":"ACTUAL","unavailable":false,"category":"SAMPLE","value_sample":-430.7043,"device_name":"Demo_CNC_01","device_uuid":"Demo_CNC_01","native_units":"MILLIMETER","data_item_type":"POSITION","units":"MILLIMETER","data_item_path":"Demo_CNC_01<Device>.base<axes>.X<linear>","collector_version":"1.2.0-rc.4"}''')
    ,json.loads('''{"tag":"MTConnectDataItems","tenant":"atl","plant":"atl","agent":"atl-connectdata-bridge","schema_version":"3","agent_version":"2.1.0-rc.171","instance_id":"2.1.0-rc.171","creation_time":"2023-01-02T09:35:41.638Z","received_at":"2023-01-02T09:35:41.638Z","data_item_id":"analytics_welding_processparametersstep_a/robot_program_welding_time","timestamp":"2022-12-01T13:24:47.387000Z","data_item_name":"a/robot_program_welding_time","unavailable":false,"category":"SAMPLE","device_name":"analytics_welding_processparametersstep","device_uuid":"analytics_welding_processparametersstep","data_item_path":"analytics_welding_processparametersstep<Device>.a/robot_program_welding_time","data_item_type":"ACCUMULATED_TIME","collector_version":"2.1.0-rc.171","value_sample":1,"meta":[]}''')
    ,json.loads('''{"tag":"MTConnectDataItems","tenant":"atl","plant":"atl","agent":"atl.6004","schema_version":"3","agent_version":"1.3.0.18","instance_id":"1671449224","creation_time":"2023-01-06T02:57:06.000Z","received_at":"2023-01-06T02:57:06.048Z","data_item_id":"Smart_SLA_1_resinLevelCurrent","timestamp":"2023-01-06T02:57:05.340Z","data_item_name":"ResinCurrentLevel","sequence":26690455,"unavailable":false,"category":"SAMPLE","value_sample":-0.00497841091326,"device_name":"Smart_SLA_1","device_uuid":"Smart_SLA_1","data_item_type":"POSITION","data_item_path":"Smart_SLA_1<Device>.Systems<systems>.Atmosphere<pneumatic>","collector_version":"1.2.0-rc.4"}''')
    ,json.loads('''{"tag":"MTConnectDataItems","tenant":"atl","plant":"atl","agent":"atl.6003","schema_version":"3","agent_version":"1.3.0.18","instance_id":"1672226137","creation_time":"2023-01-06T02:57:05.000Z","received_at":"2023-01-06T02:57:05.945Z","data_item_id":"heartbeat","timestamp":"2023-01-06T02:57:05.000Z","data_item_name":"heartbeat","data_item_type":"HEARTBEAT","unavailable":true,"collector_version":"1.2.0-rc.4"}''')
    ,json.loads('''{"tag":"MTConnectDataItems","tenant":"atl","plant":"atl","agent":"atl.6003","schema_version":"3","agent_version":"1.3.0.18","instance_id":"1672226137","creation_time":"2023-01-06T02:57:04.000Z","received_at":"2023-01-06T02:57:04.948Z","data_item_id":"Stratasys_F900_ovenCom","timestamp":"2023-01-06T02:57:04.468Z","data_item_name":"OvenCommandedTemperature","sequence":5724318,"sub_type":"COMMANDED","unavailable":false,"category":"SAMPLE","value_sample":90,"device_name":"Stratasys_F900","device_uuid":"Stratasys_F900","native_units":"CELSIUS","data_item_type":"TEMPERATURE","units":"CELSIUS","data_item_path":"Stratasys_F900<Device>.undefined<systems>.Oven<enclosure>","collector_version":"1.2.0-rc.4"}''')
    ,json.loads('''{"tag":"MTConnectDataItems","device_uuid":"bigrep24","data_item_name":"heartbeat-bigrep24","data_item_id":"heartbeat-bigrep24","timestamp":"2023-01-06T02:56:59.227Z","creation_time":"2023-01-06T02:57:04.917Z","received_at":"2023-01-06T02:57:04.917Z","tenant":"atl","plant":"atl","agent":"atl-connectdata-bridge","schema_version":"3","category":"heartbeat","data_item_type":"DEVICE_HEARTBEAT","unavailable":false,"collector_version":"2.1.0-rc.171"}''')
    ,json.loads('''{"tag":"MTConnectDataItems","tenant":"atl","plant":"atl","agent":"atl-connectdata-bridge","schema_version":"3","agent_version":"2.1.0-rc.171","instance_id":"2.1.0-rc.171","creation_time":"2022-12-30T15:28:48.624Z","received_at":"2022-12-30T15:28:48.624Z","data_item_id":"analytics_welding_lincolnelectric_a/part_model_last_updated","timestamp":"2023-01-01T00:00:00.000000Z","data_item_name":"a/part_model_last_updated","unavailable":false,"category":"SAMPLE","device_name":"analytics_welding_lincolnelectric","device_uuid":"analytics_welding_lincolnelectric","data_item_path":"analytics_welding_lincolnelectric<Device>.a/part_model_last_updated","data_item_type":"x:COUNT","collector_version":"2.1.0-rc.171","value_sample":0,"meta":[]}''')
    ,json.loads('''{"tag":"MTConnectDataItems","tenant":"atl","plant":"atl","agent":"atl-connectdata-bridge","schema_version":"3","agent_version":"2.1.0-rc.171","instance_id":"2.1.0-rc.171","creation_time":"2023-01-06T02:56:11.739Z","received_at":"2023-01-06T02:56:11.739Z","data_item_id":"f370D11111_Materials-Bay-4-Dimension","timestamp":"2023-01-06T02:54:38.745Z","data_item_name":"Materials-Bay-4-Dimension","unavailable":false,"category":"EVENT","device_name":"f370D11111","device_uuid":"f370D11111","data_item_path":"f370D11111<Device>.Materials-Bay-4-Dimension","data_item_type":"MESSAGE","collector_version":"2.1.0-rc.171","value_event":"Volume"}''')

  ]
  for x in range(1000):
    for y in range(1000):
        data = sample_messages[y%10]
        data['timestamp']=str(datetime.datetime.now())
        resp = message_producer.send_msg(data)
    # print(threadName,x,datetime.datetime.now())

broker = 'localhost:9092'
topic = 'test-topic-10'
message_producer = MessageProducer(broker,topic)

timer_stop = threading.Event()
timer = StatsReporter(10, message_producer.producer, event=timer_stop)
timer.start()

try:
   _thread.start_new_thread( send_test_message, ("Thread-1", message_producer, ) )
   _thread.start_new_thread( send_test_message, ("Thread-2", message_producer, ) )
#   _thread.start_new_thread( send_test_message, ("Thread-3", message_producer, ) )
#  _thread.start_new_thread( send_test_message, ("Thread-4", message_producer, ) )
except:
   print ("Error: unable to start thread")

