import {Inject, Injectable} from '@nestjs/common';
import {PrometheusAlertGroupDto} from "./dto/prometheus-alert-group-dto";
import {ClientKafka} from "@nestjs/microservices";
import {KAFKA_TOPIC} from "../global/globals";

@Injectable()
export class AlertsService {
    constructor(
        @Inject('KAFKA_CLIENT') private readonly kafkaClient: ClientKafka,
    ) {
    }

    async create(promGroupAlert: PrometheusAlertGroupDto): Promise<boolean> {
        console.log(promGroupAlert);
        this.kafkaClient.emit(KAFKA_TOPIC, promGroupAlert);
        return Promise.resolve(true);
    }
}
