import {Body, Controller, Post} from '@nestjs/common';
import {AlertsService} from "./alerts.service";
import {PrometheusAlertGroup} from "./dto/prometheus-alert-group";

@Controller('alerts')
export class AlertsController {
    constructor(private readonly alertsService: AlertsService) {
    }

    @Post()
    async create(@Body() promGroupAlerts: PrometheusAlertGroup): Promise<void> {
        await this.alertsService.create(promGroupAlerts)
    }
}
