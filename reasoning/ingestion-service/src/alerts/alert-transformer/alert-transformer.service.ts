import { Injectable } from '@nestjs/common';
import {PrometheusAlertDto} from "../../../dist/alerts/dto/prometheus-alert-dto";
import {TransformedPrometheusAlert} from "../dto/transformed-prometheus-alert";

@Injectable()
export class AlertTransformerService {
    private signalType = "PROMETHEUS_ALERT";

    async transform(original: PrometheusAlertDto): Promise<TransformedPrometheusAlert> {
        return new Promise(resolve => resolve(new TransformedPrometheusAlert(original, this.signalType)));
    }
}
