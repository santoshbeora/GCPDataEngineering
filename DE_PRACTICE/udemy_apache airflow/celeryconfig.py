from kombu import Exchange, Queue

task_queues = (
    Queue('default', Exchange('default'), routing_key='default'),
    Queue('special_queue', Exchange('special'), routing_key='special'),
)

task_routes = {
    'parallel_dag.transform': {'queue': 'special_queue'},
}
