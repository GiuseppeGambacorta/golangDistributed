# 05 — Service discovery server-to-server

Questo capitolo rende autonomo il servizio di log e aggiunge il primo
collegamento tra più nodi. Ogni processo è un **Agent** composto da:

1. un log segmentato locale;
2. un server gRPC per `Produce`, `Consume` e gli stream;
3. un agente Hashicorp Serf per scoprire i membri;
4. un `Replicator` che esegue il pull dello stream gRPC dei peer.

Non viene modificato nessun capitolo precedente. I file protobuf e le
interfacce `CommitLog`/`Authorizer` sono copie adattate delle API già viste,
ma hanno un import path proprio.

## Struttura

```text
05_ServerToServerServiceDiscovery/
├── api/v1/                 # protobuf, codice generato ed errori gRPC
├── internal/log/           # log persistente e replicazione pull-based
├── internal/discovery/     # wrapper Serf e traduzione Join/Leave
├── internal/server/        # servizio gRPC del commit log
├── internal/auth/          # ACL Casbin opzionale
├── internal/agent/         # wiring e lifecycle dell'intero nodo
├── cmd/server/             # esempio di processo eseguibile
├── Makefile
└── README.md
```

## Discovery e replicazione

Serf ascolta su `BindAddr`. Il tag `rpc_addr` pubblica l'indirizzo gRPC,
separato dalla porta di gossip. Quando arriva un evento `MemberJoin`, il
membership chiama `Replicator.Join(name, rpc_addr)`; su `MemberLeave` chiama
`Leave`.

Il replicatore apre `ConsumeStream` sul peer e produce ogni record ricevuto
sul proprio server gRPC. `StartOffset` parte dal watermark locale e
`CurrentOffset` scarta record già presenti: questo evita il ciclo normale
quando entrambi i nodi osservano lo stesso membro. Le scritture concorrenti
su nodi diversi non sono però un protocollo di consenso: per conflitti,
riordinamenti o recupero da buchi serve una versione successiva con identità
globale dei record e coordinamento.

La versione didattica non persiste il cursore per peer e non ritenta
automaticamente una connessione fallita. Un nuovo evento di membership
riavvia il pull.

## Avvio locale senza certificati

Le opzioni TLS sono puntatori opzionali. Per una prova rapida:

```bash
go run ./progLog/05_ServerToServerServiceDiscovery/cmd/server \
  -name node-0 -bind 127.0.0.1:7946 -rpc 8400 -data ./data/node-0

go run ./progLog/05_ServerToServerServiceDiscovery/cmd/server \
  -name node-1 -bind 127.0.0.1:7947 -rpc 8401 -join 127.0.0.1:7946 \
  -data ./data/node-1
```

Le directory dati devono essere diverse. Il client può usare l'API gRPC
generata in `api/v1`; in un ambiente reale si abilita TLS passando le
configurazioni a `agent.Config` (e, se necessario, i file ACL Casbin).

## Comandi

Dalla radice del repository:

```bash
go test ./progLog/05_ServerToServerServiceDiscovery/...
go test -race ./progLog/05_ServerToServerServiceDiscovery/...
```

Per rigenerare il protobuf (servono `protoc`, `protoc-gen-go` e
`protoc-gen-go-grpc`):

```bash
make -C progLog/05_ServerToServerServiceDiscovery compile
```

## Relazione con i capitoli precedenti

- `02_ServeRequestsWithgRPC` fornisce il contratto RPC e il log;
- `03_SecureYourServices` mostra TLS/mTLS e ACL: qui sono mantenuti come
  opzioni, mentre i test locali usano `WithInsecure`;
- `04_ObserveYourSystems` introduce metriche, tracing e log strutturati. Questo
  capitolo concentra invece l'attenzione su membership e replica; gli
  interceptor osservabili possono essere aggiunti al `NewGRPCServer` senza
  cambiare discovery o log.

Serf è un protocollo di membership, non un sistema di consenso né un
coordinatore del log. La replica qui è volutamente **at-least-once** e
didattica: non offre snapshot, retention distribuita, autenticazione Serf o
garanzie di consistenza forte.
