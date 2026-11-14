Cos'è un log: È una sequenza di record di tipo append-only (si scrive solo in coda). I dati vengono letti dall'inizio alla fine (dal più vecchio al più recente), come con un tail -f. Non riguarda solo testo formattato per umani, ma qualsiasi tipo di dato, compresi payload binari per la comunicazione tra macchine.

Ordinamento e offset: Ogni volta che si appende un record, gli viene assegnato un offset progressivo e univoco (che funge da ID). Il log è ordinato temporalmente e indicizzato per offset e timestamp.

Segmentazione (gestione dello spazio su disco): Poiché lo spazio su disco è limitato, il log viene diviso in una lista di segmenti.

    I vecchi segmenti già elaborati o archiviati vengono cancellati in background senza bloccare il resto del sistema.

    C'è un solo active segment (segmento attivo), ovvero l'unico in cui avvengono le scritture correnti. Raggiunto il limite di dimensione, se ne crea uno nuovo che diventa il nuovo segmento attivo.

Anatomia di un segmento: Ogni segmento è composto da due file distinti:

    Store file: Contiene i dati effettivi dei record, accodati sequenzialmente.

    Index file: Mappa l'offset del record con la sua posizione fisica (byte offset) all'interno dello store file.

Lettura veloce tramite indice: Per leggere un record a partire dal suo offset si fa una lookup in due passaggi (prima la posizione nell'indice, poi il dato nello store). Essendo l'indice molto leggero (coppie offset/posizione), può essere mappato in memoria tramite mmap (memory-map), garantendo tempi di accesso paragonabili a operazioni in-memory.

---

• Record—the data stored in our log.
• Store—the file we store records in.
• Index—the file we store index entries in.
• Segment—the abstraction that ties a store and an index together.
• Log—the abstraction that ties all the segments together.