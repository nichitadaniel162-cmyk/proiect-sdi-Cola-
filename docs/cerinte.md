# Proiect `sd-storage` – Cerințe, Model de Date, Scenariu și Protocol

## 1. Descriere generală și arhitectură
Sistem distribuit care stochează fișiere pe mai multe noduri, păstrând exact **2 copii pe noduri distincte**, astfel încât fișierele să rămână accesibile la căderea unui nod.

- **Client:** interfață în terminal (`put`, `get`, `ls`, `rm`, `nodes`, `exit`).
- **Nod de stocare:** păstrează fișierele într-un director local (ex: `data/n2/`). Toate nodurile rulează același cod.
- **Coordonator:** unul dintre noduri (stabilit temporar prin `--coordinator`, ales prin Bully în Etapa 5); gestionează metadatele și lista nodurilor.
- **Separarea planului de control de cel de date:** metadatele și operațiile de control trec prin coordonator (Java RMI), iar conținutul fișierelor circulă direct între client și nodurile de stocare prin TCP.

---

## 2. Cerințe funcționale (Operațiile sistemului)

| Operație | Comanda | Pașii prin sistem | Rezultat | Erori tratate |
| :--- | :--- | :--- | :--- | :--- |
| **Încărcare** | `put raport.pdf` | 1. Clientul cere coordonatorului două noduri active, distincte; 2. Trimite fișierul primului nod; 3. Primul nod îl copiază pe al doilea; 4. După ambele confirmări, coordonatorul înregistrează metadatele. | ID-ul fișierului și cele două noduri care dețin copiile. | Mai puțin de 2 noduri active (refuz, nimic stocat); fișier local inexistent; depășire limită dimensiune; o copie eșuată (se curăță și se raportează eroare). |
| **Descărcare** | `get raport.pdf ./out/` | 1. Clientul cere coordonatorului locațiile copiilor; 2. Descarcă de la primul nod disponibil; 3. Verifică checksum-ul SHA-256. | Fișierul salvat în `./out/` cu checksum verificat. | Fișier necunoscut (`NOT_FOUND`); director local invalid; primul nod nu răspunde sau checksum greșit (încearcă a doua copie); nicio copie accesibilă (eroare explicită). |
| **Listare fișiere** | `ls` | 1. Clientul apelează listarea pe coordonator; 2. Coordonatorul citește tabela de metadate; 3. Returnează ID-ul, numele, dimensiunea și nodurile fiecărui fișier. | Tabel afișat în terminal cu toate fișierele stocate și locațiile lor. | Coordonator inaccesibil (timeout / eroare conexiune); sistem gol (mesaj explicit). |
| **Ștergere** | `rm raport.pdf` | 1. Clientul cere ștergerea pe coordonator; 2. Coordonatorul trimite cereri de ștergere nodurilor active care dețin copii; 3. Elimină intrarea din metadate; 4. Dacă un nod cu o copie este `CAZUT`, ID-ul se pune într-o listă de ștergeri amânate (*tombstone*) pentru curățare automată la revenirea nodului. | Confirmarea ștergerii și avertisment dacă o replică este programată pentru curățare ulterioară. | Fișier inexistent (`NOT_FOUND`); nod căzut la momentul ștergerii (fișierul dispare din sistem, copia reziduală se curăță la reconectare); coordonator indisponibil. |
| **Starea sistemului** | `nodes` | 1. Clientul interoghează coordonatorul; 2. Coordonatorul returnează toate nodurile cunoscute, adresa/portul, starea (`ACTIV`, `SUSPECT`, `CAZUT`) și cine este coordonatorul activ. | Tabel cu nodurile din cluster, starea fiecăruia și marcajul coordonatorului. | Coordonator indisponibil (eroare de conexiune / timeout). |
| **Adăugare nod nou la rulare** *(perspectiva sistemului)* | Pornirea unui nou proces `StorageNode` | 1. Nodul nou își creează directorul local și emite mesaje de descoperire/heartbeat prin UDP multicast; 2. Celelalte noduri și coordonatorul îi recepționează ID-ul, adresa și portul; 3. Coordonatorul îl adaugă în tabela nodurilor cu starea `ACTIV`; 4. Nodul intră imediat în pool-ul de alocare fără oprirea serviciului. | Nodul apare în `nodes` și poate primi replici noi. | Port deja ocupat (procesul se oprește cu eroare explicită); lipsă permisiuni pe directorul `--dir`. |

### Decizii de design
- **Identificare fișier:** Utilizatorul folosește **numele fișierului** în comenzile CLI (`get`, `rm`), dar intern sistemul asociază un **UUID** unic fiecărui fișier (folosit ca nume pe disc `<uuid>.bin` pentru a evita coliziuni). Comenzile acceptă și UUID-ul direct.
- **Încărcare nume deja existent:** Suprascrierea implicită este **respinsă** cu eroarea `ALREADY_EXISTS`. Utilizatorul trebuie să șteargă mai întâi fișierul vechi (`rm`) sau să îl încarce sub alt nume.
- **Dimensiune maximă:** Limită de **512 MB** per fișier, verificată la alocare.
- **Regula de alegere a nodurilor (Etapa 4):** Coordonatorul selectează **2 noduri distincte cu starea `ACTIV`** care au cel mai mic număr de replici stocate (*least-loaded*), departajând egalitatea după ID-ul nodului.

---

## 3. Modelul de date

Coordonatorul menține în memorie două tabele: metadatele fișierelor (`Map<UUID, FileMetadata>`) și tabela nodurilor (`Map<String, NodeInfo>`).

### Metadatele unui fișier (`FileMetadata`)
| Câmp | Tip Java | Exemplu | Stabilit de |
| :--- | :--- | :--- | :--- |
| `id` | `UUID` | `3f2a9c1e-5b7d-4e8a-9c21-7d4e5f6a8b90` | Coordonator, la încărcare |
| `name` | `String` | `raport.pdf` | Utilizator |
| `size` | `long` (octeți) | `482133` | Client |
| `checksum` | `String` (SHA-256, hex) | `9f86d081884c7d65...` | Client, verificat de fiecare nod |
| `replicas` | `List<String>` | `["n2", "n3"]` | Coordonator, după confirmări |

### Informațiile unui nod (`NodeInfo`)
| Câmp | Tip Java | Exemplu |
| :--- | :--- | :--- |
| `nodeId` | `String` | `n2` |
| `host` | `String` | `127.0.0.1` |
| `port` | `int` | `7002` |
| `rmiPort` | `int` | `8002` |
| `status` | `NodeStatus` (`enum`) | `ACTIV`, `SUSPECT`, `CAZUT` |
| `lastHeartbeat` | `long` (ms) | `System.currentTimeMillis()` |

### Integritate și recuperarea metadatelor
- **Checksum:** Calculat cu `MessageDigest.getInstance("SHA-256")` în format hexazecimal; verificat la stocare pe noduri și la descărcare de către client.
- **Recuperare la căderea coordonatorului (Etapa 6):** Fiecare nod păstrează local, lângă fișierul binar (`<uuid>.bin`), un fișier de metadate locale (`<uuid>.meta` cu `id`, `name`, `size`, `checksum`). După alegerea unui nou coordonator prin Bully, acesta interoghează toate nodurile active prin RMI (`getLocalFiles()`) și reconstruiește integral tabela de metadate.

---

## 4. Cerințe nefuncționale, obiective măsurabile și ipoteze

### Ipoteze de lucru
- Sistemul rulează într-o **rețea locală (LAN / localhost)** fără partiționări persistente.
- Defectările sunt de tip **crash-stop** (proces oprit brusc, ex. `kill -9`), nu defecte bizantine.
- Cade **cel mult un nod la un moment dat**.

### Obiective măsurabile
| Cerință | Obiectiv măsurabil | Cum se măsoară | Etapa |
| :--- | :--- | :--- | :---: |
| **Integritate** | `0` fișiere cu checksum diferit din `100` descărcări. | Script care încarcă și descarcă 100 de fișiere și compară SHA-256. | 2 |
| **Descoperire** | Un nod nou apare în lista celorlalte în cel mult `3 s`. | Diferența dintre timestamp-ul de pornire și cel din logul coordonatorului. | 2 |
| **Detectarea defectelor** | Un nod oprit cu `kill -9` este marcat `CAZUT` în cel mult `5 s`. | Loguri cu timestamp la oprirea nodului și la schimbarea stării în `CAZUT`. | 3 |
| **Scalabilitate** | Timpul total de încărcare concurentă pe 3 noduri scade față de 1 nod. | Cronometrarea încărcării paralele pe 1 nod vs. 3 noduri. | 4 |
| **Coordonare** | Noul coordonator este ales în cel mult `10 s` după căderea celui vechi. | Logurile cu timestamp ale algoritmului Bully. | 5 |
| **Replicare** | `100%` dintre fișiere au exact `2` copii pe noduri distincte. | Comanda `ls` și verificarea directoarelor `data/n*/`. | 6 |
| **Disponibilitate** | După oprirea cu `kill -9` a oricărui nod, `100%` din fișiere rămân descărcabile. | Oprirea unui nod urmată de `get` pentru toate fișierele existente. | 7 |

---

## 5. Scenariul demonstrației

| Pas | Acțiunea operatorului | Comanda exactă în terminal | Ce trebuie să se vadă | Funcționează din etapa |
| :---: | :--- | :--- | :--- | :---: |
| 1 | Pornește `n1`, `n2`, `n3` în terminale separate | `java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --coordinator` (plus `n2`, `n3`) | Fiecare nod afișează configurația și îi descoperă pe ceilalți; `n1` este coordonator. | 2 |
| 2 | Pornește clientul și încarcă `test.txt` | `java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001` apoi `put test.txt` | UUID-ul fișierului și cele 2 noduri distincte care îl dețin. | 2 |
| 3 | Descarcă `test.txt` | `get test.txt ./out/` | Fișierul salvat în `./out/test.txt` cu checksum SHA-256 identic. | 2 |
| 4 | Pornește nodul `n4` fără a opri serviciul | `java -cp target/classes ro.ubm.sd.node.StorageNode --id n4 --port 7004 --dir data/n4` apoi `nodes` | `n4` apare `ACTIV` în `nodes`; încărcările noi îl pot folosi. | 4 |
| 5 | Oprește un nod care deține `test.txt` și descarcă fișierul | `kill -9 <PID_nod>` (sau `Ctrl+C`) apoi `nodes` și `get test.txt ./out/` | Nodul este marcat `CAZUT`; descărcarea reușește din a doua copie. | 3 |
| 6 | Oprește coordonatorul (`n1`) | `kill -9 <PID_n1>` (sau `Ctrl+C`) | Nodurile rămase rulează Bully și aleg un nou coordonator. | 5 |
| 7 | Listează și descarcă `test.txt` după realegere | `ls` apoi `get test.txt ./out/` | Metadatele sunt reconstruite; fișierul apare în `ls` și se descarcă corect. | 6 |

---

## 6. Schița protocolului

Pe TCP, fiecare mesaj începe cu un **antet text pe o singură linie** terminat cu `\n`. Pentru mesajele care transportă conținut binar (`PUT`, `REPLICATE`, răspunsul `GET`), imediat după `\n` urmează **exact `dimensiune` octeți** ai fișierului. Operațiile de control (metadate, listare, ștergere, stare cluster, Bully, recuperare metadate) sunt expuse prin **Java RMI** (`NodeRemoteService`).

| Mesaj / Operație | Direcție | Canal | Conținut | Răspuns |
| :--- | :--- | :---: | :--- | :--- |
| `ALLOCATE` (`allocateUpload`) | client → coordonator | RMI | `nume`, `dimensiune` | `AllocationPlan` (UUID + 2 noduri) sau eroare |
| `PUT` | client → nod primar | TCP | `uuid`, `nume`, `dimensiune`, `checksum`, `nod_secundar` + octeții | `STORED` sau `ERROR` |
| `REPLICATE` | nod primar → nod secundar | TCP | `uuid`, `nume`, `dimensiune`, `checksum` + octeții | `STORED` sau `ERROR` |
| `COMMIT` (`commitUpload`) | client → coordonator | RMI | `FileMetadata` (UUID, nume, dimensiune, checksum, noduri) | confirmare |
| `LOCATE` (`locateFile`) | client → coordonator | RMI | `nume` sau `uuid` | `LocateResult` (metadate + noduri active) sau `null` |
| `GET` | client → nod | TCP | `uuid` | `OK <dimensiune> <checksum>\n` + octeții, sau `NOT_FOUND` |
| `LIST` (`listFiles`) | client → coordonator | RMI | *(fără parametri)* | `List<FileMetadata>` |
| `DELETE` (`deleteFile`) | client → coordonator | RMI | `nume` sau `uuid` | mesaj confirmare / `NOT_FOUND` |
| `DELETE_REPLICA` (`deleteLocalReplica`) | coordonator → nod | RMI | `uuid` | `boolean` |
| `NODES` (`getClusterNodes`) | client → coordonator | RMI | *(fără parametri)* | `List<NodeInfo>` |
