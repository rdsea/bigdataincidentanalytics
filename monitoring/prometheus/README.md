## Rules

It's important to keep in mind the distinction between scraping/reporting directly from the target AND using an exporter. For example the below simple alert fires, if the _mqtt_ job is dead over a minute:

```
groups:
- name: MQTT_BROKER
  rules:
  - alert: MqttInstanceDown
    expr: up{job="mqtt"} == 0
    for: 1m
    labels:
      severity: critical
      pipeline_component: MQTT_BROKER
    annotations:
      summary: "Instance {{ $labels.instance }} down"
      description: "{{ $labels.instance }} of job {{ $labels.job }} has been down for more than 1 minute."
```
In our particular case, this alert is actually not suitable. Why? Because we use a sidecar exporter component (_mqtt-exporter_) which is responsible for reporting on the actual Mqtt component. As long as the exporter itself is reachable (= UP), Prometheus will infer that the _mqtt_ job is up, no matter whether the actual broker is running or not. **In such cases, the solution is to rely on metrics reported by the exporter that clearly indicate the actual component's _up_ or _down_ status. If you have access to the application layer, a better alternative would be to expose these signals directly.** (However, in this example Mosquitto cannot be instrumented, hence the exporter)

## Tips

To dynamically update configuration files, simply trigger a reload via the API:

```
PUT  /-/reload
POST /-/reload
```

For this to work, Prometheus has to be started with the `--web.enable-lifecycle` flag.

