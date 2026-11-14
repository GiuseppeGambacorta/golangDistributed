# 07 — Scoprire i server e bilanciare il carico

Questo capitolo è autonomo e non modifica il capitolo 06. Ogni nodo espone
gli RPC del log e `Discovery.GetServers`. La risposta contiene l'identità
Raft, l'indirizzo Raft, l'indirizzo gRPC, il ruolo (leader/follower) e
l'indicazione di voter, oltre al leader corrente.

## Client

`internal/client` registra uno schema gRPC `raft` e il balancer
`raft_routing`. `Dial` riceve l'indirizzo di **un** nodo noto:

```go
conn, err := client.Dial(ctx, "127.0.0.1:8400", grpc.WithInsecure())
if err != nil {
    log.Fatal(err)
}
defer conn.Close()
logClient := logv1.NewLogClient(conn)
```

Il resolver interroga periodicamente `GetServers` e aggiorna le SubConn.
Il picker esamina il nome del metodo:

* `Produce` e `ProduceStream` vanno sempre al leader;
* `Consume` e `ConsumeStream` usano round-robin tra i follower;
* in un cluster a un solo nodo le letture usano il leader.

L'API usa le interfacce resolver/balancer disponibili in
`google.golang.org/grpc v1.32.0`, quindi non richiede le API moderne di gRPC.

## Avvio

```bash
go run ./progLog/07_DiscoverServersAndLoadBalance/cmd/server \
  -node-id node-0 -bootstrap \
  -rpc-addr 127.0.0.1:8400 -raft-addr 127.0.0.1:7400 \
  -data ./data/ch07-node-0
```

Avviare il secondo nodo con porte e directory diverse, quindi registrarlo dal
leader. Perché i client possano raggiungerlo bisogna fornire anche il suo
indirizzo gRPC:

```go
leader.AddVoterWithRPC("node-1", "127.0.0.1:7401", "127.0.0.1:8401")
```

`AddVoter` resta disponibile per compatibilità ma, senza l'indirizzo gRPC,
la discovery può solo pubblicare l'indirizzo Raft come fallback.

## Test

```bash
go test ./progLog/07_DiscoverServersAndLoadBalance/...
go test -race ./progLog/07_DiscoverServersAndLoadBalance/...
```

## Discovery diretta o proxy

Il bilanciamento è **client-side**: ogni client mantiene le connessioni ai
nodi e decide localmente dove inviare ogni RPC. Non c'è un proxy centrale che
termina gRPC, quindi si riduce un hop e il proxy non è un collo di bottiglia.
In cambio ogni client deve poter raggiungere tutti gli indirizzi pubblicati,
pollare la discovery e gestire cambiamenti temporanei di leader. Un proxy può
nascondere la topologia ai client e applicare policy centralizzate, ma aggiunge
latenza, costo operativo e un ulteriore punto di guasto.

## Limitazioni e sicurezza

La discovery è una fotografia soggetta a race durante elezioni o
riconfigurazioni; il picker non sostituisce i controlli Raft e un errore
transitorio può richiedere un nuovo tentativo RPC. Gli indirizzi sono dati di
configurazione fidati: il capitolo usa `WithInsecure` solo per il laboratorio.
In produzione servono TLS/mTLS, autenticazione e autorizzazione della
discovery, validazione degli endpoint e transport Raft autenticato. Non sono
implementati ACL, retention, health check applicativi o riconfigurazione
automatica.
