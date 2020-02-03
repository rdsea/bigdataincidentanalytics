import { Injectable } from '@nestjs/common';
import {TransformedPrometheusAlert} from "../dto/transformed-prometheus-alert";
import {PrometheusAlert} from "../dto/prometheus-alert";

@Injectable()
export class AlertTransformerService {
    private signalType = "PROMETHEUS_ALERT";

    async transform(original: PrometheusAlert): Promise<TransformedPrometheusAlert> {
        return new Promise(resolve => resolve(new TransformedPrometheusAlert(original, this.signalType)));
    }
}
