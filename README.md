# sd-storage – Sistem Distribuit de Stocare a Fișierelor

Proiect pentru disciplina **Sisteme Distribuite** – sistem de stocare a fișierelor pe mai multe noduri, cu replicare în 2 copii distincte, descoperire automată prin UDP multicast, heartbeat și detectare defecte, distribuire a încărcării, alegere a coordonatorului prin algoritmul Bully, control prin Java RMI și recuperare automată a metadatelor.

## Cerințe de mediu
- Java Development Kit (JDK) 17+
- Apache Maven 3.8+ (sau Maven integrat în IntelliJ IDEA)
- Git

## Compilare
```bash
mvn -q package
```
*(În IntelliJ IDEA: panoul **Maven** din dreapta → `sd-storage` → `Lifecycle` → dublu-click pe `package`.)*

## Pornirea proceselor

### 1. Nodurile de stocare (3 terminale separate)
```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --coordinator
java -cp target/classes ro.ubm.sd.node.StorageNode --id n2 --port 7002 --dir data/n2
java -cp target/classes ro.ubm.sd.node.StorageNode --id n3 --port 7003 --dir data/n3
```

Alternativ (Linux / Git Bash):
```bash
chmod +x scripts/start-local.sh
./scripts/start-local.sh
```

### 2. Clientul (al patrulea terminal)
```bash
java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001
```

### 3. Adăugarea unui nod nou la rulare (opțional, al cincilea terminal)
```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n4 --port 7004 --dir data/n4
```

## Comenzi în client
- `put <cale_fisier>` – încarcă fișierul în sistem (2 copii distincte + SHA-256)
- `get <nume_sau_id> [director_destinatie]` – descarcă fișierul și verifică SHA-256 (implicit în `./out`)
- `ls` – listează fișierele, dimensiunea și nodurile care dețin copiile
- `rm <nume_sau_id>` – șterge fișierul din sistem (cu *tombstones* dacă un nod este căzut)
- `nodes` – afișează nodurile cunoscute, starea lor (`ACTIV`, `SUSPECT`, `CAZUT`) și coordonatorul activ
- `exit` – închide clientul

## Documentație
Specificația completă (cerințe funcționale, model de date, obiective măsurabile, scenariul demonstrației și schița protocolului) se află în [`docs/cerinte.md`](docs/cerinte.md).
