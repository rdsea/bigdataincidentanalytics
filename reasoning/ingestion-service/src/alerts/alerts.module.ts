import {Module} from '@nestjs/common';
import {AlertsController} from './alerts.controller';
import {AlertsService} from './alerts.service';
import {ClientsModule, Transport} from "@nestjs/microservices";
import {KAFKA_BROKERS} from "../global/globals";
import { AlertTransformerService } from './alert-transformer/alert-transformer.service';

@Module({
  imports: [
    ClientsModule.register([
      {
        name: 'KAFKA_CLIENT',
        transport: Transport.KAFKA,
        options: {
          client: {
            clientId: "nest-js",
            brokers: KAFKA_BROKERS,
            connectionTimeout: 3000,
          }
        }
      }
    ])
  ],
  controllers: [AlertsController],
  providers: [AlertsService, AlertTransformerService]
})
export class AlertsModule {
}
