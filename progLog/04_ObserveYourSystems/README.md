# 04_ObserveYourSystems

Questo capitolo aggiunge osservabilita al servizio gRPC in una cartella separata.
Il codice di `03_SecureYourServices` non viene modificato.

## Cosa significa osservabilita

L'osservabilita permette di capire lo stato interno di un sistema osservando i
suoi output. Usiamo tre forme di telemetria:

- **metriche**: numeri aggregabili nel tempo;
- **log strutturati**: eventi con campi chiave-valore;
- **tracce**: il percorso e la durata di una richiesta.

Le quattro golden signals sono:

- latency;
- traffic;
- errors;
- saturation.

## Struttura

```text
04_ObserveYourSystems/
|-- Makefile
|-- README.md
|-- client/main.go
|-- api/v1/
|   |-- log.proto
|   |-- log.pb.go
|   |-- log_grpc.pb.go
|   `-- error.go
`-- internal/server/
    `-- server.go
```

## Implementazione

Il server in `internal/server/server.go` configura tre livelli di osservabilita
come interceptor e stat handler gRPC:

1. `grpc_zap`: emette log strutturati JSON per ogni RPC;
2. `grpc_ctxtags`: aggiunge metadati della chiamata al contesto;
3. `ocgrpc.ServerHandler`: registra metriche e tracce OpenCensus.

Il server continua a usare le astrazioni dei capitoli precedenti:

```go
type CommitLog interface {
    Append(*api.Record) (uint64, error)
    Read(uint64) (*api.Record, error)
}

type Authorizer interface {
    Authorize(subject, object, action string) error
}
```

In questo modo osservabilita, autorizzazione e persistenza restano separabili.

## Log strutturati con Zap

Il logger viene nominato `server` e registra la durata della RPC nel campo
`grpc.time_ns`.

Un evento contiene normalmente informazioni come:

```json
{
  "level": "info",
  "msg": "finished call",
  "grpc.service": "log.v1.Log",
  "grpc.method": "Produce",
  "grpc.code": "OK",
  "grpc.time_ns": 1200000
}
```

In produzione il logger puo essere configurato per scrivere su stdout o su un
collector centralizzato. Nei test e utile usare `zap.NewDevelopment()` per
leggere rapidamente gli eventi.

## Metriche OpenCensus

`view.Register(ocgrpc.DefaultServerViews...)` abilita le view standard del
server gRPC, tra cui:

- byte ricevuti per RPC;
- byte inviati per RPC;
- latenza;
- RPC completate e relativo status.

`ocgrpc.ServerHandler` collega queste view alle richieste gestite dal server.

## Tracing OpenCensus

Durante lo sviluppo viene usato:

```go
trace.ApplyConfig(trace.Config{
    DefaultSampler: trace.AlwaysSample(),
})
```

In questo modo ogni richiesta viene tracciata. In produzione e preferibile un
sampler probabilistico o una policy che campioni sempre le richieste critiche,
limitando costo e volume dei dati.

## Dipendenze

Il capitolo usa:

- `go.uber.org/zap` per i log;
- `github.com/grpc-ecosystem/go-grpc-middleware` per gli interceptor;
- `go.opencensus.io` per metriche e tracing;
- `google.golang.org/grpc` per server e protocollo.

## Comandi

Il client CLI consente di generare traffico osservabile:

```bash
go run ./progLog/04_ObserveYourSystems/client -op produce -value "hello"
go run ./progLog/04_ObserveYourSystems/client -op consume -offset 0
```

Per un server configurato con TLS si possono passare `-tls`, `-ca`, `-cert`,
`-key` e `-server-name`.

Dalla radice del repository:

```bash
go test -race ./progLog/04_ObserveYourSystems/...
```

Per rigenerare il codice protobuf:

```bash
make -C progLog/04_ObserveYourSystems compile
```

Per visualizzare i log, le metriche e le tracce durante i test del server:

```bash
go test -v ./progLog/04_ObserveYourSystems/internal/server -args -debug=true
```

Il flag `-debug` configura Zap e crea file temporanei per l'exporter OpenCensus;
i percorsi dei file vengono stampati nell'output del test.

## Separazione dai capitoli precedenti

- `02_ServeRequestsWithgRPC`: server gRPC base;
- `03_SecureYourServices`: TLS, mTLS e ACL;
- `04_ObserveYourSystems`: log, metriche e tracing.

Ogni cartella rappresenta un capitolo indipendente e non altera il codice dei
capitoli precedenti.
