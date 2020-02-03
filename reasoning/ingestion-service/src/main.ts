import {NestFactory} from '@nestjs/core';
import {AppModule} from './app.module';
import {FastifyAdapter, NestFastifyApplication} from "@nestjs/platform-fastify";
import {Transport} from "@nestjs/microservices";
import {KAFKA_BROKERS} from "./global/globals";

async function bootstrap(): Promise<void> {
    const app = await NestFactory.create<NestFastifyApplication>(
        AppModule,
        new FastifyAdapter({logger: true})
    );
    app.connectMicroservice({
        transport: Transport.KAFKA,
        options: {
            client: {
                clientId: "nest-js",
                brokers: KAFKA_BROKERS,
                connectionTimeout: 3000,
            }
        }
    });
    await app.startAllMicroservicesAsync();
    await app.listen(3000, '0.0.0.0');
}

bootstrap().then();
