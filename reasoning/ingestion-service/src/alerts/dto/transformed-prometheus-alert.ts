import {PrometheusAlert} from "./prometheus-alert";

export class TransformedPrometheusAlert {
    readonly signal_type: string;
    readonly status: string;
    readonly labels: object;
    readonly annotations: object;
    readonly startsAt: string;
    readonly endsAt: string;
    readonly generatorURL: string;

    constructor(originalPromAlert: PrometheusAlert, signalType: string) {
        // eslint-disable-next-line @typescript-eslint/camelcase
        this.signal_type = signalType;
        this.status = originalPromAlert.status;
        this.labels = originalPromAlert.labels;
        this.annotations = originalPromAlert.annotations;
        this.startsAt = originalPromAlert.startsAt;
        this.endsAt = originalPromAlert.endsAt;
        this.generatorURL = originalPromAlert.generatorURL;
    }
}
