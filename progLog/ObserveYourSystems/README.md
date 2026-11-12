# ObserveYourSystems

Questo capitolo aggiunge osservabilita al servizio gRPC in una cartella separata.
Il codice di `SecureYourServices` non viene modificato.

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
ObserveYourSystems/
|-- Makefile
|-- README.md
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

Dalla radice del repository:

```bash
go test -race ./progLog/ObserveYourSystems/...
```

Per rigenerare il codice protobuf:

```bash
make -C progLog/ObserveYourSystems compile
```

Per visualizzare i log durante i test del server si puo inizializzare Zap con
`zap.NewDevelopment()` e sostituire il logger globale con `zap.ReplaceGlobals`.
Per esportare metriche e tracce su file, si puo aggiungere un exporter
OpenCensus nel setup dei test.

## Separazione dai capitoli precedenti

- `ServeRequestsWithgRPC`: server gRPC base;
- `SecureYourServices`: TLS, mTLS e ACL;
- `ObserveYourSystems`: log, metriche e tracing.

Ogni cartella rappresenta un capitolo indipendente e non altera il codice dei
capitoli precedenti.
