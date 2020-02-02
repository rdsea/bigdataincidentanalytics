import {Module} from '@nestjs/common';
import {AlertsController} from './alerts.controller';
import {AlertsService} from './alerts.service';
import {ClientsModule, Transport} from "@nestjs/microservices";
import {KAFKA_BROKERS} from "../global/globals";

@Module({
  controllers: [AlertsController],
  providers: [AlertsService],
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
  ]
})
export class AlertsModule {
}
