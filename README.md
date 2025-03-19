# DB Obfuscator

Uno strumento Python per creare una versione offuscata di un database MySQL, utile per ambienti di test, audit e per garantire la privacy dei dati.

## Panoramica

DB Obfuscator permette di creare una copia esatta di un database MySQL sostituendo i dati sensibili con versioni offuscate ma sintatticamente coerenti con i dati originali. Questo è particolarmente utile quando si ha bisogno di condividere dati per debugging, test o audit, mantenendo la riservatezza delle informazioni sensibili.

Lo script mantiene inalterata la struttura del database, inclusi gli schemi delle tabelle, le chiavi primarie e i vincoli, modificando soltanto i contenuti delle colonne specificate.

## Funzionalità principali

- Offuscamento selettivo delle tabelle e dei campi specificati
- Preservazione della struttura completa del database
- Tre metodologie di offuscamento specifiche per tipo di dato:
  - **Testo**: Sostituzione di caratteri alfanumerici mantenendo maiuscole/minuscole e punteggiatura
  - **Date**: Shift temporale casuale ma deterministico
  - **Numeri**: Modifica preservando formato e numero di cifre
- Rilevamento automatico del tipo di dato delle colonne
- Consistenza nell'offuscamento (lo stesso valore produrrà sempre lo stesso risultato offuscato)
- Report dettagliato delle operazioni e diagnostica degli errori
- Gestione avanzata degli errori (tabelle o campi inesistenti)
- Elaborazione batch efficiente per migliori performance

## Requisiti

- Python 3.6+
- MySQL/MariaDB
- Librerie Python:
  - `mysql-connector-python`
  - `pyyaml`

## Installazione

1. Clona il repository o scarica i file dello script
2. Installa le dipendenze:

```bash
pip install mysql-connector-python pyyaml
```

## Configurazione

Lo script richiede due file di configurazione:

### 1. File di configurazione database (`config.yaml`)

Questo file YAML contiene le credenziali e i dettagli di connessione dei database sorgente e destinazione:

```yaml
source:
  host: localhost
  user: root
  password: S0urc3P@ss
  database: production_db
destination:
  host: localhost
  user: auditor
  password: Aud1tP@ss
  database: obfuscated_db
```

### 2. File definizione campi (`obfuscate_fields.txt`)

Questo file di testo specifica quali tabelle e campi devono essere offuscati, utilizzando il formato `TABELLA - CAMPO` (uno per riga):

```
users - first_name
users - last_name
users - email
customers - birth_date
orders - total_amount
payments - card_number
```

## Utilizzo

Eseguire lo script Python con:

```bash
python db_obfuscator.py
```

Lo script:
1. Legge le configurazioni
2. Si connette ai database sorgente e destinazione
3. Per ogni tabella specificata:
   - Ricrea la tabella nel database destinazione
   - Identifica i campi da offuscare e il loro tipo
   - Copia i dati, offuscando i campi specificati
4. Genera un report dettagliato dell'operazione

## Funzioni di offuscamento

### Offuscamento testo

La funzione di offuscamento testo:
- Preserva il formato originale (maiuscole/minuscole, punteggiatura)
- Sostituisce lettere minuscole con altre lettere minuscole
- Sostituisce lettere maiuscole con altre lettere maiuscole
- Sostituisce cifre con altre cifre
- Mantiene intatti i caratteri non alfanumerici
- È deterministica (stesso input → stesso output)

### Offuscamento date

L'offuscamento delle date:
- Applica uno shift temporale casuale ma deterministico (±180 giorni)
- Mantiene la validità della data
- Preserva il formato originale

### Offuscamento numeri

L'offuscamento numerico:
- Mantiene lo stesso numero di cifre intere e decimali
- Preserva i formati speciali (es. numeri con zeri iniziali)
- Mantiene un ordine di grandezza simile al valore originale

## Log e report

Lo script genera:
- Log dettagliati su console e file (`obfuscation.log`)
- Statistiche sull'elaborazione di tabelle e record
- Report riassuntivo al termine dell'esecuzione
- Avvisi e errori chiaramente evidenziati

## Miglioramenti futuri

Possibili evoluzioni per versioni future:

1. **Modalità di anteprima**: Mostrare esempi di dati offuscati senza eseguire la copia completa
2. **Supporto per altri database**: Estendere il supporto a PostgreSQL, SQLite, ecc.
3. **Funzioni di offuscamento personalizzate**: Permettere all'utente di definire funzioni di offuscamento specifiche per determinate colonne
4. **Pseudonimizzazione intelligente**: Utilizzare dizionari/liste realistiche per sostituire nomi, indirizzi, ecc.
5. **Mascheramento parziale**: Offuscare solo parti dei valori (es. solo le ultime 4 cifre di una carta di credito)
6. **Supporto per relazioni**: Mantenere le relazioni tra tabelle durante l'offuscamento (foreign keys)
7. **Interfaccia grafica**: Sviluppare una GUI per la configurazione e il monitoraggio
8. **Supporto per dati binari**: Gestire campi BLOB e altri tipi di dati binari
9. **Modalità incrementale**: Aggiornare solo i dati modificati dopo l'ultima esecuzione
10. **Supporto per cluster DB**: Gestire database distribuiti o in cluster

## Licenza

MIT License

Copyright (c) 2025

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.