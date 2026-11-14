# Avviare il cluster Proglog in locale

Questa guida spiega come avviare il servizio in un cluster Kubernetes locale
usando Docker, Kind, kubectl e Helm.

## Strumenti utilizzati

- **Docker**: costruisce ed esegue container.
- **Kind**: crea un cluster Kubernetes locale usando container Docker come nodi.
- **kubectl**: permette di interrogare e gestire il cluster Kubernetes.
- **Helm**: installa il chart Kubernetes del progetto e gestisce il deployment.

Verifica che gli strumenti siano installati:

```bash
docker version
kind version
kubectl version --client
helm version
```

## 1. Creare il cluster Kind

```bash
kind create cluster --name proglog
kubectl config use-context kind-proglog
kubectl cluster-info
```

Kind crea un cluster Kubernetes locale e configura automaticamente il contesto
usato da `kubectl`.

## 2. Costruire l'immagine Docker

Dalla radice del repository:

```bash
docker build \
  -f progLog/08_DeployLocally/Dockerfile \
  -t chapter08-log:latest \
  .
```

Il Dockerfile usa una build multi-stage: compila i binari Go in un'immagine di
build e poi crea un'immagine runtime più piccola.

## 3. Caricare l'immagine in Kind

```bash
kind load docker-image chapter08-log:latest --name proglog
```

Kind usa nodi Docker separati. Per questo l'immagine locale deve essere
caricata esplicitamente dentro il nodo del cluster.

## 4. Installare il chart Helm

```bash
helm upgrade --install log \
  ./progLog/08_DeployLocally/helm/log-cluster \
  --set image.repository=chapter08-log \
  --set image.tag=latest
```

Helm legge il chart e crea le risorse Kubernetes:

- `StatefulSet` per i nodi del servizio;
- `Service` headless per il DNS stabile dei pod;
- `PersistentVolumeClaim` per i dati del log;
- `ConfigMap` per lo script di avvio e bootstrap;
- probe HTTP di liveness e readiness.

Il pod usa `fsGroup` per rendere scrivibile il volume dal processo non-root;
non sono necessari init container privilegiati.

## 5. Controllare il deployment

```bash
kubectl get pods,pvc,svc
kubectl get pods -w
```

Attendi che i pod siano `Running` e `READY`, ad esempio:

```text
NAME           READY   STATUS    RESTARTS   AGE
log-cluster-0  1/1     Running   0          1m
log-cluster-1  1/1     Running   0          1m
log-cluster-2  1/1     Running   0          1m
```

In caso di problemi:

```bash
kubectl describe pod log-cluster-0
kubectl logs log-cluster-0
kubectl logs log-cluster-1
```

## 6. Accedere al primo nodo

In un terminale lascia attivo il port-forward:

```bash
kubectl port-forward pod/log-cluster-0 8080:8080 8400:8400
```

Il port-forward collega le porte locali alle porte del pod:

- `8080`: endpoint HTTP di health;
- `8400`: server gRPC.

In un altro terminale verifica lo stato:

```bash
curl http://127.0.0.1:8080/healthz
curl http://127.0.0.1:8080/readyz
```

Le risposte attese sono:

```text
ok
ready
```

## 7. Leggere la membership Raft

```bash
go run ./progLog/08_DeployLocally/cmd/getservers \
  -addr 127.0.0.1:8400
```

Il comando interroga l'endpoint gRPC di discovery e mostra i nodi conosciuti,
il leader e gli indirizzi pubblicizzati.

## 8. Renderizzare il chart senza installarlo

Per controllare i manifest generati da Helm senza modificare il cluster:

```bash
helm template log \
  ./progLog/08_DeployLocally/helm/log-cluster \
  --set image.repository=chapter08-log \
  --set image.tag=latest
```

## 9. Aggiornare il deployment

Dopo aver ricostruito una nuova immagine:

```bash
docker build \
  -f progLog/08_DeployLocally/Dockerfile \
  -t chapter08-log:latest \
  .
kind load docker-image chapter08-log:latest --name proglog
helm upgrade log \
  ./progLog/08_DeployLocally/helm/log-cluster \
  --set image.repository=chapter08-log \
  --set image.tag=latest
```

Controlla il rollout:

```bash
kubectl rollout status statefulset/log-cluster
```

## 10. Rimuovere il deployment

Per rimuovere le risorse Helm:

```bash
helm uninstall log
```

Per rimuovere anche i volumi persistenti:

```bash
kubectl delete pvc -l app.kubernetes.io/instance=log
```

Per eliminare completamente il cluster Kind:

```bash
kind delete cluster --name proglog
```

La cancellazione del cluster elimina anche i dati locali contenuti nei nodi
Kind. In un ambiente reale è necessario configurare backup e storage
persistente appropriato.
