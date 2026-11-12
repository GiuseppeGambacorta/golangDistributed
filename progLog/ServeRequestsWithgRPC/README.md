# ServeRequestsWithgRPC

Questa cartella contiene il capitolo dedicato a gRPC. Il codice e separato dal
capitolo `StructureDataWithProtobuf`, che contiene soltanto la definizione base
del record.

## Struttura

```text
ServeRequestsWithgRPC/
|-- Makefile
|-- api/v1/
|   |-- log.proto
|   |-- log.pb.go
|   |-- log_grpc.pb.go
|   `-- error.go
`-- internal/
    |-- log/
    |   |-- config.go
    |   |-- store.go
    |   |-- index.go
    |   |-- segment.go
    |   `-- log.go
    `-- server/
        |-- server.go
        `-- server_test.go
```

## Protocollo protobuf

Il file `api/v1/log.proto` definisce il messaggio `Record` e il servizio `Log`.
Il servizio espone quattro RPC:

- `Produce`: riceve un record e restituisce il suo offset.
- `Consume`: riceve un offset e restituisce il record.
- `ProduceStream`: stream bidirezionale per inviare piu record e ricevere un
  offset per ciascuno.
- `ConsumeStream`: stream server-side che invia i record a partire da un
  offset.

I file `log.pb.go` e `log_grpc.pb.go` sono generati automaticamente e non
vanno modificati manualmente.

## Flusso di una richiesta

```text
Client gRPC
    |
    v
LogServer
    |
    v
CommitLog interface
    |
    v
Log persistente
    |
    +--> Segment
          +--> Store: record serializzati con proto.Marshal
          `--> Index: offset -> posizione nel file store
```

Il server gRPC non dipende da una implementazione concreta del log. Usa
l'interfaccia `CommitLog`:

```go
type CommitLog interface {
    Append(*api.Record) (uint64, error)
    Read(uint64) (*api.Record, error)
}
```

Questo permette di usare il log persistente in produzione e, nei test, una
implementazione in memoria piu semplice e veloce.

## Serializzazione

Quando viene aggiunto un record, `segment.go` esegue:

```go
data, err := proto.Marshal(record)
```

I bytes protobuf vengono salvati nel file `.store`. Durante la lettura,
`proto.Unmarshal` ricostruisce `api.Record`.

Il protocollo di rete usa invece gRPC, che trasporta automaticamente i
messaggi protobuf definiti nel file `.proto`.

## Gestione degli errori

Quando l'offset richiesto non esiste, il log restituisce
`api.ErrOffsetOutOfRange`.

L'errore:

- usa il codice gRPC `codes.OutOfRange`;
- contiene un messaggio leggibile;
- aggiunge un `LocalizedMessage` come dettaglio protobuf;
- permette a `ConsumeStream` di aspettare nuovi record invece di chiudere lo
  stream.

## Generazione del codice

Dalla cartella `ServeRequestsWithgRPC`:

```bash
make compile
```

Il comando usa `protoc` con i plugin Go e gRPC e genera i file nella cartella
`api/v1`.

## Test

Dalla cartella principale del repository:

```bash
go test -race ./progLog/ServeRequestsWithgRPC/...
```

I test del server avviano un listener TCP locale, registrano il servizio gRPC,
creano un client e verificano:

- Produce e Consume unary;
- ProduceStream e ConsumeStream;
- errore quando si legge oltre l'ultimo offset.
