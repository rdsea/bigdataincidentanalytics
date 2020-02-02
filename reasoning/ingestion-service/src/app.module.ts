import { Module } from '@nestjs/common';
import { AppController } from './app.controller';
import {AppService} from './app.service';
import {AlertsModule} from './alerts/alerts.module';

@Module({
  imports: [AlertsModule],
  controllers: [AppController],
  providers: [AppService],
})
export class AppModule {}
