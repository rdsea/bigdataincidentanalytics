import {PrometheusAlert} from "./prometheus-alert";
// This schema is specified by Prometheus Webhook config.
// See https://prometheus.io/docs/alerting/configuration/#webhook_config
export class PrometheusAlertGroup {
    readonly version: string;
    readonly groupKey: string;
    readonly status: string;
    readonly receiver: string;
    readonly groupLabels: object;
    readonly commonLabels: object;
    readonly commonAnnotations: object;
    readonly externalURL: string;
    readonly alerts: [PrometheusAlert]
}
