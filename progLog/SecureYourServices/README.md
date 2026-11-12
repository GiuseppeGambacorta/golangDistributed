# SecureYourServices

Questo capitolo raccoglie la sicurezza del servizio gRPC in una cartella
separata. Non modifica il codice di `ServeRequestsWithgRPC`: il capitolo
precedente resta il riferimento per il server gRPC base, mentre qui aggiungiamo
la sicurezza per gradi.

## Obiettivi

Il servizio viene protetto in tre passaggi:

1. **TLS**: cifra i dati durante il trasporto e autentica il server.
2. **Mutual TLS (mTLS)**: autentica sia il server sia il client.
3. **ACL**: autorizza le operazioni in base all'identita del client.

La distinzione e importante:

- TLS risponde a: "La connessione e cifrata?"
- Autenticazione risponde a: "Chi sta usando il servizio?"
- Autorizzazione risponde a: "Cosa puo fare questo client?"

## Struttura implementata

```text
SecureYourServices/
|-- README.md
|-- Makefile
|-- api/v1/
|   |-- log.proto
|   |-- log.pb.go
|   |-- log_grpc.pb.go
|   `-- error.go
|-- internal/
|   |-- auth/
|   |   `-- authorizer.go
|   |-- config/
|   |   |-- files.go
|   |   `-- tls.go
|   `-- server/
|       `-- server.go
`-- test/
    |-- ca-csr.json
    |-- ca-config.json
    |-- server-csr.json
    |-- client-csr.json
    |-- model.conf
    `-- policy.csv
```

I certificati generati non devono essere salvati nel repository. Il Makefile li
sposta nella directory:

```text
$HOME/.proglog/
```

La directory puo essere sostituita nei test impostando `CONFIG_DIR`.

## 1. Certificate Authority

Una Certificate Authority (CA) e l'autorita che firma i certificati. In un
ambiente interno possiamo gestire una CA privata invece di usare una CA
pubblica.

CFSSL fornisce due comandi utili:

- `cfssl`: crea e firma certificati;
- `cfssljson`: salva il risultato in file `.pem`, `.csr` e `.key`.

Installazione dei tool:

```bash
go install github.com/cloudflare/cfssl/cmd/cfssl@v1.4.1
go install github.com/cloudflare/cfssl/cmd/cfssljson@v1.4.1
```

I file JSON descrivono i certificati:

- `ca-csr.json`: configura la CA;
- `ca-config.json`: definisce le policy di firma;
- `server-csr.json`: certificato del server;
- `client-csr.json`: certificato del client.

Il campo `hosts` del certificato server deve contenere i nomi con cui il client
si colleghera, ad esempio `localhost` e `127.0.0.1`.

## 2. TLS del server

Il server presenta:

- `server.pem`: certificato pubblico;
- `server-key.pem`: chiave privata;
- `ca.pem`: certificato della CA che il client deve considerare attendibile.

Il client costruisce un `tls.Config` con `RootCAs`. Questo significa:

```text
client -> verifica che il certificato del server sia firmato dalla CA
server -> accetta la connessione cifrata
```

Nel codice gRPC le credenziali vengono installate con:

```go
credentials.NewTLS(tlsConfig)
grpc.NewServer(grpc.Creds(serverCreds))
```

Il client usa invece:

```go
grpc.Dial(address, grpc.WithTransportCredentials(clientCreds))
```

A differenza di `grpc.WithInsecure()`, questa configurazione verifica il
certificato e cifra il traffico.

## 3. Mutual TLS

Con TLS semplice il client verifica il server. Con mTLS anche il server verifica
il client.

Il client presenta:

- `client.pem` o un certificato specifico per l'identita;
- la relativa chiave privata;
- la CA con cui verificare il server.

Il server configura:

```go
tlsConfig.ClientCAs = ca
tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
```

Il flusso diventa:

```text
client verifica server
server verifica client
entrambi comunicano su un canale cifrato
```

Il Common Name (CN) del certificato identifica il client. In questo capitolo
useremo, ad esempio, `root` e `nobody`.

## 4. Configurazione TLS condivisa

`internal/config/files.go` centralizza i percorsi dei certificati. In questo
modo i test e il server non costruiscono manualmente i path in piu punti.

`internal/config/tls.go` espone una funzione comune:

```go
func SetupTLSConfig(cfg TLSConfig) (*tls.Config, error)
```

La funzione puo creare tre configurazioni:

- client che verifica il server;
- client mTLS che presenta anche il proprio certificato;
- server mTLS che richiede e verifica il certificato del client.

Il `ServerAddress` deve corrispondere a uno degli host presenti nel certificato
server. In caso contrario la verifica TLS fallisce anche se la CA e corretta.

## 5. Autorizzazione con ACL

L'autenticazione dice chi e il client. L'autorizzazione decide cosa puo fare.

Useremo Casbin con una policy ACL. Ogni regola ha questa forma:

```text
subject, object, action
```

Esempio:

```text
p, root, *, produce
p, root, *, consume
```

Queste regole permettono al client `root` di produrre e consumare. Un client
come `nobody`, non presente nella policy, riceve:

```text
codes.PermissionDenied
```

Il package `internal/auth` nasconde Casbin dietro una piccola astrazione:

```go
type Authorizer interface {
    Authorize(subject, object, action string) error
}
```

Questo mantiene il server indipendente dall'implementazione concreta del
sistema ACL.

Il `CommitLog` viene passato nella `Config` del server. In questo modo il
capitolo di sicurezza non copia ne modifica il log persistente dei capitoli
precedenti: puo usare un log su disco in produzione oppure uno fake in memoria
nei test.

## Interceptor gRPC

Per autorizzare ogni richiesta, un interceptor legge l'identita dal certificato
TLS del client:

```text
RPC context
    |
    v
peer.AuthInfo
    |
    v
certificato TLS
    |
    v
Subject / Common Name
    |
    v
Authorizer
```

Il subject viene inserito nel `context.Context`. Le RPC `Produce` e `Consume`
chiamano poi l'authorizer con:

```text
object: *
action: produce oppure consume
```

Gli interceptor unary e streaming vengono registrati con
`grpc.UnaryInterceptor` e `grpc.StreamInterceptor`.

## Makefile

I target principali saranno:

```bash
make init     # crea la directory dei certificati
make gencert  # genera CA, certificato server e certificati client
make compile  # genera il codice protobuf e gRPC
make test     # esegue i test con race detector
```

I certificati sono artefatti locali e non devono essere committati. E bene
aggiungere i file `.pem`, `.csr` e le chiavi private al `.gitignore`.

## Codice implementato

Il server in `internal/server/server.go` include:

- `NewGRPCServer`, con opzioni gRPC e credenziali TLS;
- interceptor unary e streaming per estrarre il Common Name dal certificato;
- autorizzazione di `Produce` e `Consume` tramite `Authorizer`;
- gestione degli stream e degli errori `OutOfRange`.

`internal/config/tls.go` costruisce configurazioni TLS per client, server e
mTLS. `internal/auth/authorizer.go` incapsula Casbin e restituisce
`codes.PermissionDenied` quando una regola ACL nega l'operazione.

## Test di sicurezza

I test del capitolo dovranno verificare almeno:

- un client autorizzato puo eseguire `Produce`;
- un client autorizzato puo eseguire `Consume`;
- il server rifiuta un certificato firmato da una CA diversa;
- il server rifiuta un client senza certificato quando e attivo mTLS;
- il client rifiuta un server con certificato non attendibile;
- il client `nobody` riceve `codes.PermissionDenied`;
- il client `root` supera le regole ACL;
- gli stream rispettano autenticazione e autorizzazione.

## Comandi di verifica

Dalla radice del repository:

```bash
go test -race ./progLog/SecureYourServices/...
```

Durante lo sviluppo dei certificati, verificare anche che i file esistano nella
directory configurata:

```bash
ls "$HOME/.proglog"
```

La compilazione del capitolo e verificabile anche senza certificati:

```bash
go test -race ./progLog/SecureYourServices/...
```

## Nota sul capitolo precedente

`ServeRequestsWithgRPC` contiene il server gRPC non autenticato e i suoi test.
`SecureYourServices` e invece il capitolo successivo: qui si aggiungono TLS,
mTLS e ACL senza mescolare i file dei due capitoli.
