# 06 — Coordinare con il consenso

Questo capitolo sostituisce la replica best-effort del capitolo 05 con
**Hashicorp Raft**. La directory è autonoma e non modifica i capitoli
precedenti.

## Architettura

* `internal/log/Log` è il log segmentato locale già usato nei capitoli
  precedenti.
* `DistributedLog.Append` serializza un `Record`, lo invia a Raft e ritorna
  solo dopo il commit su una maggioranza. I follower rifiutano le scritture
  con `ErrNotLeader`; le letture sono locali.
* `fsm` applica gli entry committed al log locale. Gli snapshot serializzano
  i record e `Restore` ricostruisce il log.
* Raft usa un transport TCP, `raft-boltdb` per log/stable store persistenti e
  `FileSnapshotStore` per gli snapshot.
* `Agent` avvia il log distribuito e il server gRPC (`Produce`, `Consume` e
  gli stream). `AddVoter` aggiunge un nodo al cluster.

Gli offset sono assegnati dal log locale nell'ordine degli entry Raft; non si
deve chiamare `Append` direttamente sui follower.

## Esecuzione

Dalla radice del repository:

```bash
go run ./progLog/06_CoordinateWithConsensus/cmd/server \
  -node-id node-0 -bootstrap \
  -rpc-addr 127.0.0.1:8400 -raft-addr 127.0.0.1:7400 \
  -data ./data/node-0
```

Un secondo processo usa directory, porte e ID diversi e parte senza
`-bootstrap`:

```bash
go run ./progLog/06_CoordinateWithConsensus/cmd/server \
  -node-id node-1 \
  -rpc-addr 127.0.0.1:8401 -raft-addr 127.0.0.1:7401 \
  -data ./data/node-1
```

In un programma Go, dopo l'avvio di entrambi i processi, il leader si
aggiunge con:

```go
if err := leader.AddVoter("node-1", "127.0.0.1:7401"); err != nil {
    log.Fatal(err)
}
```

Il client gRPC deve inviare `Produce` al leader (o ridirigere dopo
`codes.FailedPrecondition`) e può usare `Consume` su qualsiasi membro già
aggiornato.

## Test

```bash
go test ./progLog/06_CoordinateWithConsensus/...
go test -race ./progLog/06_CoordinateWithConsensus/...
```

I test verificano l'elezione di un nodo singolo e la lettura di un append
committed dal secondo nodo.

## Limitazioni didattiche

Il leader discovery e il redirect dei client non sono un protocollo RPC
completo: l'indirizzo del leader va gestito dall'applicazione. I dati dei
record sono interamente inclusi negli snapshot, quindi snapshot molto grandi
richiedono più spazio e tempo. Non sono inclusi TLS, ACL, retention o
reconfiguration automatica; in produzione bisogna autenticare il transport
Raft, proteggere il gRPC e gestire il ciclo di vita dei membri.
