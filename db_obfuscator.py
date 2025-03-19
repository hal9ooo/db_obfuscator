#!/usr/bin/env python
import yaml
import hashlib
import random
import re
from datetime import datetime, timedelta
import mysql.connector
from mysql.connector import Error
import logging
from typing import Dict, List, Any, Tuple, Optional, Union

# Configurazione logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler('obfuscation.log'), logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

class DbObfuscator:
    def __init__(self, config_file: str, fields_file: str):
        """
        Inizializza l'oggetto obfuscator
        
        Args:
            config_file: Path del file di configurazione YAML
            fields_file: Path del file con i campi da offuscare
        """
        self.config = self._load_config(config_file)
        self.fields_to_obfuscate = self._load_fields(fields_file)
        self.source_conn = None
        self.dest_conn = None
        
        # Dizionario per assicurare consistenza nell'offuscamento
        self.text_cache = {}
        self.date_shift_cache = {}
        self.number_factor_cache = {}
        
        # Mappatura tipi MySQL → categoria offuscamento
        self.type_mapping = {
            'varchar': 'text', 'char': 'text', 'text': 'text', 'tinytext': 'text', 
            'mediumtext': 'text', 'longtext': 'text', 'enum': 'text',
            'date': 'date', 'datetime': 'date', 'timestamp': 'date', 
            'time': 'date', 'year': 'date',
            'tinyint': 'number', 'smallint': 'number', 'mediumint': 'number', 
            'int': 'number', 'bigint': 'number', 'float': 'number', 
            'double': 'number', 'decimal': 'number'
        }
    
    def _load_config(self, config_file: str) -> Dict:
        """Carica la configurazione dal file YAML"""
        try:
            with open(config_file, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Errore nel caricamento del file di configurazione: {e}")
            raise
    
    def _load_fields(self, fields_file: str) -> Dict[str, List[str]]:
        """
        Carica i campi da offuscare dal file di testo
        Format: TABELLA - NOMECAMPO (una per riga)
        
        Returns:
            Dict[str, List[str]]: Dizionario con chiave=tabella, valore=lista di campi
        """
        fields_dict = {}
        try:
            with open(fields_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if not line or ' - ' not in line:
                        continue
                    
                    parts = line.split(' - ', 1)
                    if len(parts) != 2:
                        logger.warning(f"Linea ignorata, formato non valido: {line}")
                        continue
                    
                    table, field = parts
                    table = table.strip()
                    field = field.strip()
                    
                    if table not in fields_dict:
                        fields_dict[table] = []
                    
                    fields_dict[table].append(field)
            
            return fields_dict
        except Exception as e:
            logger.error(f"Errore nel caricamento del file dei campi: {e}")
            raise
    
    def connect(self):
        """Stabilisce connessione ai database source e destination"""
        try:
            # Connessione al database sorgente
            self.source_conn = mysql.connector.connect(
                host=self.config['source']['host'],
                user=self.config['source']['user'],
                password=self.config['source']['password'],
                database=self.config['source']['database']
            )
            logger.info(f"Connesso al DB sorgente: {self.config['source']['database']}")
            
            # Connessione al database destinazione
            self.dest_conn = mysql.connector.connect(
                host=self.config['destination']['host'],
                user=self.config['destination']['user'],
                password=self.config['destination']['password'],
                database=self.config['destination']['database']
            )
            logger.info(f"Connesso al DB destinazione: {self.config['destination']['database']}")
            
        except Error as e:
            logger.error(f"Errore nella connessione al database: {e}")
            raise
    
    def close(self):
        """Chiude le connessioni ai database"""
        if self.source_conn and self.source_conn.is_connected():
            self.source_conn.close()
            logger.info("Connessione al DB sorgente chiusa")
        
        if self.dest_conn and self.dest_conn.is_connected():
            self.dest_conn.close()
            logger.info("Connessione al DB destinazione chiusa")
    
    def get_table_structure(self, table_name: str) -> Tuple[List[Dict], str]:
        """
        Ottiene la struttura della tabella dal database sorgente
        
        Returns:
            Tuple[List[Dict], str]: Lista di colonne e query per creare la tabella
        """
        cursor = self.source_conn.cursor(dictionary=True)
        
        # Ottiene informazioni sulle colonne
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        
        # Ottiene la query CREATE TABLE
        cursor.execute(f"SHOW CREATE TABLE {table_name}")
        create_table = cursor.fetchone()['Create Table']
        
        cursor.close()
        return columns, create_table
    
    def get_column_type(self, table_name: str, column_name: str) -> Optional[str]:
        """
        Determina il tipo di una colonna
        
        Returns:
            Il tipo della colonna oppure None se la colonna non esiste
        """
        cursor = self.source_conn.cursor(dictionary=True)
        cursor.execute(f"DESCRIBE {table_name}")
        columns = cursor.fetchall()
        cursor.close()
        
        for col in columns:
            if col['Field'] == column_name:
                # Estrae il tipo base (es. varchar(255) -> varchar)
                data_type = col['Type']
                return re.split(r'\(|\s', data_type)[0].lower()
        
        return None  # La colonna non esiste nella tabella
    
    def obfuscate_text(self, value: str) -> str:
        """
        Offusca valori testuali preservando la punteggiatura ma modificando tutti
        i caratteri alfanumerici. Genera lo stesso output per lo stesso input.
        
        Args:
            value: Testo da offuscare
            
        Returns:
            str: Testo offuscato
        """
        if value is None or value == '':
            return value
            
        # Converti a stringa se non lo è già
        value = str(value)
        
        # Cache per consistenza
        if value in self.text_cache:
            return self.text_cache[value]
        
        # Genera un seed unico per questo valore usando un hash
        hash_obj = hashlib.md5(str(value).encode())
        seed = int(hash_obj.hexdigest(), 16)
        
        # Inizializza il generatore di numeri casuali con il seed
        random.seed(seed)
        
        # Set di caratteri possibili per la sostituzione
        lowercase = list('abcdefghijklmnopqrstuvwxyz')
        uppercase = list('ABCDEFGHIJKLMNOPQRSTUVWXYZ')
        digits = list('0123456789')
        all_chars = lowercase + uppercase + digits
        
        result = ''
        for char in value:
            if char.isalnum():  # Alfanumerico (lettere o cifre)
                if char.islower():
                    # Sostituisci con una lettera minuscola casuale
                    result += random.choice(lowercase)
                elif char.isupper():
                    # Sostituisci con una lettera maiuscola casuale
                    result += random.choice(uppercase)
                elif char.isdigit():
                    # Sostituisci con una cifra casuale
                    result += random.choice(digits)
                else:
                    # Per sicurezza, sostituisci con un carattere casuale
                    result += random.choice(all_chars)
            else:
                # Preserva punteggiatura e altri caratteri non alfanumerici
                result += char
        
        # Memorizza per uso futuro
        self.text_cache[value] = result
        return result
    
    def obfuscate_date(self, value: Any) -> Any:
        """
        Offusca date applicando uno shift temporale
        
        Args:
            value: Data originale
            
        Returns:
            Data offuscata
        """
        if value is None:
            return None
        
        # Genera uno shift consistente basato sulla data originale
        str_value = str(value)
        if str_value in self.date_shift_cache:
            days_shift = self.date_shift_cache[str_value]
        else:
            # Genera shift unico per questa data (±180 giorni)
            hash_val = int(hashlib.md5(str_value.encode()).hexdigest(), 16)
            days_shift = (hash_val % 360) - 180
            self.date_shift_cache[str_value] = days_shift
        
        # Applica lo shift alla data originale
        if isinstance(value, datetime):
            return value + timedelta(days=days_shift)
        else:
            # Converti a datetime, applica shift, riconverti al tipo originale
            try:
                dt = datetime.fromisoformat(str_value)
                shifted = dt + timedelta(days=days_shift)
                return shifted
            except:
                # Fallback se la conversione fallisce
                logger.warning(f"Impossibile offuscare il valore data: {value}")
                return value
    
    def obfuscate_number(self, value: Any) -> Any:
        """
        Offusca numeri mantenendo lo stesso formato
        
        Args:
            value: Valore numerico
            
        Returns:
            Valore numerico offuscato
        """
        if value is None:
            return None
        
        str_value = str(value)
        
        # Usa fattore di trasformazione dalla cache per consistenza
        if str_value in self.number_factor_cache:
            factor, offset = self.number_factor_cache[str_value]
        else:
            # Crea fattori unici per questo valore
            hash_val = int(hashlib.md5(str_value.encode()).hexdigest(), 16)
            factor = 0.5 + (hash_val % 1000) / 1000  # Fattore tra 0.5-1.5
            offset = (hash_val % 100) - 50  # Offset tra -50 e 49
            self.number_factor_cache[str_value] = (factor, offset)
        
        # Gestisce numeri interi vs decimali
        if isinstance(value, int) or '.' not in str_value:
            # Mantiene lo stesso numero di cifre
            original_len = len(str_value)
            new_val = int(abs(int(value) * factor + offset))
            
            # Assicura stesso numero di cifre
            new_val_str = str(new_val)
            if len(new_val_str) > original_len:
                new_val = int(new_val_str[:original_len])
            elif len(new_val_str) < original_len:
                new_val = int(new_val_str.zfill(original_len))
                
            return new_val
        else:
            # Gestisce numeri decimali
            parts = str_value.split('.')
            int_part = parts[0]
            decimal_part = parts[1] if len(parts) > 1 else ''
            
            # Calcola nuova parte intera
            new_int = int(abs(int(int_part or '0') * factor + offset))
            
            # Mantiene lo stesso numero di cifre
            if len(str(new_int)) > len(int_part):
                new_int = int(str(new_int)[:len(int_part)])
            elif len(str(new_int)) < len(int_part):
                new_int = int(str(new_int).zfill(len(int_part)))
            
            # Offusca parte decimale se presente
            if decimal_part:
                decimal_hash = int(hashlib.md5((decimal_part).encode()).hexdigest(), 16)
                new_decimal = str(decimal_hash)[:len(decimal_part)]
            else:
                new_decimal = ''
            
            # Ricostruisce il numero
            if new_decimal:
                result = float(f"{new_int}.{new_decimal}")
            else:
                result = float(new_int)
                
            return result
    
    def table_exists(self, table_name: str) -> bool:
        """
        Verifica se una tabella esiste nel database di origine
        
        Args:
            table_name: Nome della tabella da verificare
            
        Returns:
            bool: True se la tabella esiste, False altrimenti
        """
        try:
            cursor = self.source_conn.cursor()
            cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
            result = cursor.fetchone()
            cursor.close()
            return result is not None
        except Error as e:
            logger.error(f"Errore durante la verifica dell'esistenza della tabella {table_name}: {e}")
            return False
            
    def process_table(self, table_name: str) -> None:
        """
        Elabora una singola tabella, offuscando i campi specificati
        
        Args:
            table_name: Nome della tabella da elaborare
        """
        logger.info(f"Elaborazione tabella: {table_name}")
        
        # Verifica se la tabella esiste
        if not self.table_exists(table_name):
            logger.error(f"ERRORE: La tabella '{table_name}' non esiste nel database di origine. Questa tabella verrà ignorata.")
            return
        
        source_cursor = self.source_conn.cursor(dictionary=True)
        dest_cursor = self.dest_conn.cursor()
        
        try:
            # Ottiene struttura tabella
            columns, create_table_sql = self.get_table_structure(table_name)
            
            # Elimina tabella destinazione se esiste
            dest_cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            dest_cursor.execute(create_table_sql)
            self.dest_conn.commit()
            logger.info(f"Tabella {table_name} ricreata nel database destinazione")
            
            # Ottiene tutti i record
            source_cursor.execute(f"SELECT * FROM {table_name}")
            records = source_cursor.fetchall()
            logger.info(f"Trovati {len(records)} record nella tabella {table_name}")
            
            # Ottiene lista campi da offuscare per questa tabella
            fields_to_obfuscate = self.fields_to_obfuscate.get(table_name, [])
            
            if not fields_to_obfuscate:
                # Se nessun campo deve essere offuscato, copia direttamente
                logger.info(f"Nessun campo da offuscare nella tabella {table_name}, copia diretta")
                # Costruisci istruzione INSERT con tutti i campi
                if records:
                    fields = list(records[0].keys())
                    placeholders = ', '.join(['%s'] * len(fields))
                    insert_query = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})"
                    
                    # Prepara i valori (in batch per efficienza)
                    batch_size = 1000
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i+batch_size]
                        values = [[record[field] for field in fields] for record in batch]
                        dest_cursor.executemany(insert_query, values)
                        self.dest_conn.commit()
                        logger.info(f"Copiati {min(i+batch_size, len(records))}/{len(records)} record nella tabella {table_name}")
            else:
                # Determina tipo di dato per ciascun campo da offuscare
                field_types = {}
                valid_fields = []
                
                for field in fields_to_obfuscate:
                    data_type = self.get_column_type(table_name, field)
                    
                    if data_type is None:
                        logger.warning(f"ATTENZIONE: Campo '{field}' non trovato nella tabella '{table_name}'. Questo campo verrà ignorato.")
                        continue
                    
                    valid_fields.append(field)
                    obfuscation_type = self.type_mapping.get(data_type, 'text')
                    field_types[field] = obfuscation_type
                    logger.info(f"Campo {field}: tipo {data_type} -> offuscamento {obfuscation_type}")
                
                # Aggiorna la lista dei campi da offuscare con solo quelli validi
                fields_to_obfuscate = valid_fields
                
                # Elabora record per record
                if records:
                    fields = list(records[0].keys())
                    placeholders = ', '.join(['%s'] * len(fields))
                    insert_query = f"INSERT INTO {table_name} ({', '.join(fields)}) VALUES ({placeholders})"
                    
                    # Batch processing
                    batch_size = 1000
                    total_processed = 0
                    
                    for i in range(0, len(records), batch_size):
                        batch = records[i:i+batch_size]
                        values_batch = []
                        
                        for record in batch:
                            # Crea copia del record
                            new_record = list(record.values())
                            
                            # Offusca i campi necessari
                            for idx, field in enumerate(fields):
                                if field in fields_to_obfuscate:
                                    obf_type = field_types[field]
                                    value = new_record[idx]
                                    
                                    if obf_type == 'text':
                                        new_record[idx] = self.obfuscate_text(value)
                                    elif obf_type == 'date':
                                        new_record[idx] = self.obfuscate_date(value)
                                    elif obf_type == 'number':
                                        new_record[idx] = self.obfuscate_number(value)
                            
                            values_batch.append(new_record)
                        
                        # Esegue inserimento batch
                        dest_cursor.executemany(insert_query, values_batch)
                        self.dest_conn.commit()
                        
                        total_processed += len(batch)
                        logger.info(f"Elaborati {total_processed}/{len(records)} record nella tabella {table_name}")
            
            logger.info(f"Tabella {table_name} elaborata con successo")
        
        except Error as e:
            self.dest_conn.rollback()
            logger.error(f"Errore nell'elaborazione della tabella {table_name}: {e}")
            raise
        finally:
            source_cursor.close()
            dest_cursor.close()
    
    def run(self):
        """Esegue il processo di offuscamento"""
        logger.info("Avvio processo di offuscamento del database")
        
        # Tracciamento risultati
        tables_processed = []
        tables_skipped = []
        fields_skipped = {}
        
        try:
            self.connect()
            
            # Processa ogni tabella specificata nel file di configurazione
            for table in self.fields_to_obfuscate.keys():
                if table not in tables_processed and table not in tables_skipped:
                    # Verifica se la tabella esiste
                    if not self.table_exists(table):
                        logger.warning(f"Tabella '{table}' non trovata nel database di origine. Verrà ignorata.")
                        tables_skipped.append(table)
                        continue
                    
                    # Verifica quali campi esistono nella tabella
                    fields = self.fields_to_obfuscate.get(table, [])
                    invalid_fields = []
                    
                    for field in fields:
                        if self.get_column_type(table, field) is None:
                            invalid_fields.append(field)
                    
                    if invalid_fields:
                        fields_skipped[table] = invalid_fields
                    
                    # Processa la tabella
                    self.process_table(table)
                    tables_processed.append(table)
            
            # Riepilogo finale
            logger.info("\n\n=== RIEPILOGO ESECUZIONE ===")
            
            # Tabelle elaborate con successo
            if tables_processed:
                logger.info(f"Tabelle elaborate con successo: {len(tables_processed)}")
                for table in tables_processed:
                    logger.info(f" - {table}")
            else:
                logger.warning("Nessuna tabella è stata elaborata!")
            
            # Tabelle ignorate
            if tables_skipped:
                logger.warning(f"Tabelle ignorate (non esistenti nel DB sorgente): {len(tables_skipped)}")
                for table in tables_skipped:
                    logger.warning(f" - {table}")
            
            # Campi ignorati
            if fields_skipped:
                logger.warning("Campi ignorati nelle tabelle elaborate (non esistenti):")
                for table, fields in fields_skipped.items():
                    if table in tables_processed:
                        logger.warning(f" - Tabella '{table}': {', '.join(fields)}")
            
            logger.info("===========================")
            logger.info("Processo di offuscamento completato.")
        
        except Exception as e:
            logger.error(f"Errore durante il processo di offuscamento: {e}")
            raise
        finally:
            self.close()


def main():
    """Funzione principale"""
    try:
        logger.info("Avvio script di offuscamento del database")
        obfuscator = DbObfuscator('config.yaml', 'obfuscate_fields.txt')
        obfuscator.run()
        logger.info("Script completato con successo")
        return 0
    except Exception as e:
        logger.error(f"Errore critico: {e}", exc_info=True)
        return 1


if __name__ == "__main__":
    main()