## Spine Adapter

Spine Adapter provides a reusable component that publishes the configured events on doctypes onto corresponding kafka topic. The component provides a configuration doctype (single) for the producer.

The adapter also provides a consumer for events published. The consumer allows any application/bench instance to listen for events published on kafka and provides configurable plugin points for taking action on such events.

### Installation - Producer Side
Before installing the application in any site, configure following properties with appropriate values in site_config.json.

<pre>
"kafka": {
    "bootstrap.servers": "localhost:9092",
    "client.id": "withrun.local",
    "default.topic.config": {"acks": "all"},
    "consumer.min.commit.count": 1,
    "group.id": "spine-client-withrun",
    "debug": "topic",
    "auto.offset.reset": "smallest",
    "request.required.acks": "1",
    "fetch.message.max.bytes": "81920"
 }
</pre>

For details of different properties that can be included in the above configuration, please refer to kafka client API documentation available  [here](https://docs.confluent.io/current/clients/confluent-kafka-python/index.html#configuration). Above configuration is passed as-is to the kafka client APIs.

### Installation - Consumer Side
For any application to consume the events published on Kafka, Configurations similar to above need to be included in <code>site_config.json</code> 
In addition to above, 
1. Developer Mode - Starting Frappe using bench, add the following to Procfile for the Consumer site -
<pre>
eventdispatcher: bench eventdispatcher --queue kafka_events --numworkers 2
jsonworker: bench jsonworker --queue kafka_events --numworkers 2
</pre>

2. Production Mode - Starting Frappe using Supervisor, add the following to supervisor.conf
<pre>
[program:<frappe>-bench-eventdispatcher]
command=<path_to_bench>/bench eventdispatcher --queue kafka_events
priority=4
autostart=true
autorestart=true
stdout_logfile=/home/frappe/<frappe>-bench/logs/eventdispatcher.log
stderr_logfile=/home/frappe/<frappe>-bench/logs/eventdispatcher.error.log
user=frappe
directory=/home/frappe/<frappe>-bench
numprocs=1
process_name=%(program_name)s-%(process_num)d
stopasgroup=true
stopsignal=QUIT


[program:<frappe>-bench-jsonworker]
command=<path_to_bench>/bench jsonworker --queue kafka_events
priority=4
autostart=true
autorestart=true
stdout_logfile=/home/frappe/<frappe>-bench/logs/jsonworker.log
stderr_logfile=/home/frappe/<frappe>-bench/logs/jsonworker.error.log
user=frappe
directory=/home/frappe/<frappe>-bench
numprocs=1
process_name=%(program_name)s-%(process_num)d
</pre>
Add, the above programs to the group of bench and reload supervisor.q

The value after <code>--numworkers</code> is the number of processes to start for the event dispatcher and json worker respectively.

Event dispatcher is responsible for reading the events published on Kafka and submit them on Redis Queue named <code>kafka_events</code> (as mentioned in above commands in Procfile) in the form of handlers configured for the received doctype.

JSON worker is responsible for execution of the handlers submitted by the event dispatcher. The queue name mentioned after <code>--queue</code> in both commands should be exactly same to ensure that the two processes can communicate with each other. 

### Spine Producer/Consumer Config

Once configured as above, standard bench get-app/install-app command will install the application. On installation, the application will start listening to all documents' "on_update" events and publish the ones that are configured under <code>Spine Producer Configuration</code>.

To update producer or consumer configuration, access Desk as Administrator and update values in doctypes <code>Spine Producer Config</code> and <code>Spine Consumer Config</code>. These are single datatypes and are cached at runtime by Frappe. Any updates to these doctypes deletes such cached entries forcing the new values to take effect.

The configurations have pairs of the doctypes and corresponding event handlers. 

For <code>Spine Producer Config</code> the handlers are used to filter the updates that get published onto Kafka. The producing application can control the events published using these handlers. The handler function is provided with the document and the event. The function is expected to return a document - either updated, or without any updates. Returning <code>None</code> from the handler will ignore the event (will not be published). The document updated by the handler is used as payload. If only the doctype is configured for producer, without any handler, the document will be used as-is for publishing, without any filtering. If the handler generates any exception, the error will be ignored (and logged), and the original document that was updated will be used as-is for publishing.

For <code>Spine Consumer Config</code>, the handlers can be a comma separated list of fully qualified names of the handler functions that accept the published event in the form of a JSON. This event handler is invoked in the context of a separate <code>bench</code> process and should ensure that the required context is made available inside the function (e.g. logging infrastructure, etc.).

### Consumer Side Processing
Spine adapter implements an internal queue backed by database using doctype named <code>Message Log</code>. The consumer by default saves all the received messages using <code>Message Log</code> doctype. A Scheduler is executed once every minute to process all pending messages. Scheduler updates the doctype entry with either <code>Error</code> or <code>Processed</code> status based on result of the processing.

#### License

MIT
