import {Body, Controller, Post} from '@nestjs/common';
import {AlertsService} from "./alerts.service";
import {PrometheusAlertGroupDto} from "./dto/prometheus-alert-group-dto";

@Controller('alerts')
export class AlertsController {
    constructor(private readonly alertsService: AlertsService) {
    }

    @Post()
    async create(@Body() promGroupAlerts: PrometheusAlertGroupDto) {
        await this.alertsService.create(promGroupAlerts)
    }
}
