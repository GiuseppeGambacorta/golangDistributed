# 08 — Deploy localmente con Docker e Kubernetes

Questo capitolo è autonomo: copia l'agente del capitolo 07 e aggiunge un
eseguibile operativo, un endpoint HTTP di health/join e gli artefatti per
Docker e Helm. Le directory dei capitoli precedenti non vengono modificate.

## CLI dell'agente

```bash
go run ./progLog/08_DeployLocally/cmd/agent \
  -node-id node-0 -data ./data/node-0 \
  -rpc-addr 127.0.0.1:8400 -rpc-advertise 127.0.0.1:8400 \
  -raft-addr 127.0.0.1:7400 -raft-advertise 127.0.0.1:7400 \
  -http-addr 127.0.0.1:8080 -bootstrap
```

`-config file.json` carica gli stessi campi della struct `agent.Config`; i
flag passati esplicitamente prevalgono sui valori del file. In Kubernetes è
importante valorizzare **sia** `-raft-addr` (bind, spesso `0.0.0.0`) sia
`-raft-advertise` (DNS stabile del pod), oltre a `-rpc-advertise`. Pubblicare
`localhost` o l'indirizzo effimero del listener impedisce ai peer di
raggiungere il nodo.

Il primo nodo usa `-bootstrap`. Gli altri usano `-join-id`,
`-join-raft-addr`, `-join-rpc-addr` e `-join-address` (endpoint HTTP `/join`
del leader); il comando ritenta finché il leader è disponibile. `GET
/healthz` è la liveness probe, `GET /readyz` richiede un leader noto.

Per vedere la membership:

```bash
go run ./progLog/08_DeployLocally/cmd/getservers -addr 127.0.0.1:8400
```

## CLI del cluster

`proglog` interagisce con un nodo già avviato. I comandi gRPC (`produce`,
`consume`, `servers`) accettano l'indirizzo di un qualsiasi membro con `-addr`;
il client usa la discovery per instradare le richieste. I comandi HTTP
(`health`, `ready`, `raft status`) usano invece l'indirizzo dell'agent HTTP.

Senza argomenti, oppure con il comando `tui`, si apre una dashboard interattiva
nel terminale:

```bash
go run ./progLog/08_DeployLocally/cmd/proglog tui \
  -addr 127.0.0.1:8400 -http-addr 127.0.0.1:8080
```

La TUI aggiorna automaticamente health, readiness, leader Raft e membership.
I tasti sono `p` per produrre un record, `c` per consumare un offset, `r` per
aggiornare subito e `q` per uscire. Durante l'inserimento, `INVIO` conferma ed
`ESC` annulla. I sottocomandi testuali restano disponibili per script e CI.

```bash
go run ./progLog/08_DeployLocally/cmd/proglog produce \
  -addr 127.0.0.1:8400 -value "hello cluster"
go run ./progLog/08_DeployLocally/cmd/proglog consume \
  -addr 127.0.0.1:8400 -offset 0
go run ./progLog/08_DeployLocally/cmd/proglog servers -addr 127.0.0.1:8400
go run ./progLog/08_DeployLocally/cmd/proglog health -addr 127.0.0.1:8080
go run ./progLog/08_DeployLocally/cmd/proglog ready -addr 127.0.0.1:8080
go run ./progLog/08_DeployLocally/cmd/proglog raft status -addr 127.0.0.1:8080
```

Le risposte sono JSON; `produce` stampa l'offset assegnato e `consume` il
record (il campo `value` è codificato in base64 dal formato JSON). Per valori
binari usare `-value-base64`. Il comando `raft status` interroga l'endpoint
HTTP di sola lettura `/raftz`.

## Docker

Il Dockerfile è multi-stage e va costruito dalla root del repository:

```bash
docker build -f progLog/08_DeployLocally/Dockerfile -t chapter08-log:latest .
```

Per kind, caricare l'immagine dopo la build:

```bash
kind load docker-image chapter08-log:latest
```

L'immagine contiene `agent`, `getservers` e `proglog`, espone gRPC `8400`, Raft
`7400` e HTTP `8080`, e usa `/var/lib/log` come volume persistente. Non è incluso
un cluster Docker Compose: StatefulSet e PVC sono la configurazione
riproducibile di riferimento.

## Kubernetes e Helm

Il chart usa un **StatefulSet**, non un Deployment: ogni replica mantiene un
nome ordinale e un PVC distinto. Il `fsGroup` del pod assegna al processo
non-root l'accesso al volume senza usare init container privilegiati. Il
headless Service fornisce DNS come
`pod-0.<release>-log-cluster-headless.<namespace>.svc.cluster.local`; questi
nomi sono gli indirizzi Raft pubblicizzati. Un init container prepara i
permessi del volume e una ConfigMap contiene lo script che bootstrap-izza
solo `-0` e fa join degli altri pod.

Rendering senza creare risorse:

```bash
helm template log ./progLog/08_DeployLocally/helm/log-cluster
```

Esecuzione locale con kind:

```bash
helm upgrade --install log ./progLog/08_DeployLocally/helm/log-cluster \
  --set image.repository=chapter08-log --set image.tag=latest
kubectl -n default get pods,pvc,svc
kubectl -n default port-forward pod/log-log-cluster-0 8400:8400
go run ./progLog/08_DeployLocally/cmd/getservers -addr 127.0.0.1:8400
```

Il chart non crea uno StorageClass: il cluster deve averne una di default,
oppure va impostato `--set storage.className=...`. Il bootstrap è pensato per
un cluster nuovo a tre nodi; modificare `replicaCount` dopo il bootstrap non
riconfigura automaticamente Raft. Non applicare il chart a un cluster
condiviso senza prima verificare storage, rete e permessi.

## Test e limitazioni

```bash
go test ./progLog/08_DeployLocally/...
go test -race ./progLog/08_DeployLocally/...
```

Il join HTTP è un endpoint di laboratorio senza autenticazione; il traffico
gRPC/Raft è in chiaro e non sono implementati TLS, ACL, cifratura dei PVC,
backup o riconfigurazione automatica. `getservers` mostra una fotografia
eventualmente transitoria durante elezioni.
