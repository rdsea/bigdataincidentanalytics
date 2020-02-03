export class PrometheusAlert {
    readonly status: string;
    readonly labels: object;
    readonly annotations: object;
    readonly startsAt: string;
    readonly endsAt: string;
    readonly generatorURL: string;
}
