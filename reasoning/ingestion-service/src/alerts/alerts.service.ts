import {Inject, Injectable} from '@nestjs/common';
import {PrometheusAlertGroup} from "./dto/prometheus-alert-group";
import {ClientKafka} from "@nestjs/microservices";
import {KAFKA_TOPIC} from "../global/globals";
import {AlertTransformerService} from "./alert-transformer/alert-transformer.service";

@Injectable()
export class AlertsService {
    constructor(
        @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
        private readonly alertTransformer: AlertTransformerService
    ) {
    }

    async create(alertGroup: PrometheusAlertGroup): Promise<boolean> {
        return Promise.all(
            alertGroup.alerts.map(
                (originalAlert) => {
                    this.alertTransformer.transform(originalAlert).then((transformedAlert) => {
                        this.kafkaClient.emit(KAFKA_TOPIC, JSON.stringify(transformedAlert)).toPromise()
                    })
                })
        )
            .then(() => true)
            .catch(error => {
                console.error(error);
                return false;
            });
    }
}
