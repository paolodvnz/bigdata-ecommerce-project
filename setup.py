#!/usr/bin/env python3
"""
BigData E-commerce Project - Setup Automatico

Questo script automatizza:
1. Avvio container Docker (MinIO + MLflow)
2. Configurazione servizi MinIO e MLflow
3. Generazione dataset (SAMPLE o FULL a scelta utente)
4. Upload dataset su MinIO

Author: Paolo
Date: 2026-01-20
"""

import subprocess
import sys
import time
from pathlib import Path


class Colors:
    """Colori ANSI per output terminal"""
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'


def print_header(message):
    """Stampa intestazione colorata"""
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'='*70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{message:^70}{Colors.ENDC}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'='*70}{Colors.ENDC}\n")


def print_step(step_num, total_steps, message):
    """Stampa step corrente"""
    print(f"{Colors.OKCYAN}[{step_num}/{total_steps}]{Colors.ENDC} {Colors.BOLD}{message}{Colors.ENDC}")


def print_success(message):
    """Stampa messaggio di successo"""
    print(f"{Colors.OKGREEN}✓ {message}{Colors.ENDC}")


def print_error(message):
    """Stampa messaggio di errore"""
    print(f"{Colors.FAIL}✗ {message}{Colors.ENDC}")


def print_warning(message):
    """Stampa messaggio di warning"""
    print(f"{Colors.WARNING}⚠ {message}{Colors.ENDC}")


def check_docker():
    """
    Verifica che Docker sia installato e in esecuzione
    
    Returns:
        bool: True se Docker è disponibile, False altrimenti
    """
    print_step(1, 4, "Verifica Docker")
    
    # Check comando docker
    try:
        result = subprocess.run(
            ['docker', '--version'],
            capture_output=True,
            text=True,
            check=True
        )
        print_success(f"Docker installato: {result.stdout.strip()}")
    except (subprocess.CalledProcessError, FileNotFoundError):
        print_error("Docker non trovato. Installa Docker Desktop e riprova.")
        return False
    
    # Check Docker daemon
    try:
        result = subprocess.run(
            ['docker', 'ps'],
            capture_output=True,
            text=True,
            check=True
        )
        print_success("Docker daemon in esecuzione")
        return True
    except subprocess.CalledProcessError:
        print_error("Docker daemon non in esecuzione. Avvia Docker Desktop e riprova.")
        return False


def start_docker_compose():
    """
    Avvia i container Docker usando docker-compose.yml
    
    Returns:
        bool: True se avvio riuscito, False altrimenti
    """
    print_step(2, 4, "Avvio container Docker (MinIO + MLflow)")
    
    # Verifica che docker-compose.yml esista
    compose_file = Path('docker-compose.yml')
    if not compose_file.exists():
        print_error("File docker-compose.yml non trovato nella directory corrente")
        print_warning("Assicurati di eseguire lo script dalla root del progetto")
        return False
    
    # Avvia container
    try:
        print("   Avvio container in modalità detached...")
        result = subprocess.run(
            ['docker', 'compose', 'up', '-d'],
            capture_output=True,
            text=True,
            check=True
        )
        
        # Attesa avvio servizi
        print("   Attesa avvio servizi (15 secondi)...")
        for i in range(15, 0, -1):
            print(f"   {i}...", end='\r')
            time.sleep(1)
        print("   " + " "*20)  # Clear countdown
        
        # Verifica container running
        result = subprocess.run(
            ['docker', 'compose', 'ps'],
            capture_output=True,
            text=True,
            check=True
        )
        
        print_success("Container avviati con successo")
        print(f"\n{result.stdout}")
        return True
        
    except subprocess.CalledProcessError as e:
        print_error(f"Errore durante l'avvio dei container: {e}")
        print_warning(f"Output: {e.stderr}")
        return False


def configure_services():
    """
    Configura MinIO e MLflow eseguendo gli script di configurazione
    
    Returns:
        bool: True se configurazione riuscita, False altrimenti
    """
    print_step(3, 4, "Configurazione servizi (MinIO + MLflow)")
    
    # Configura MinIO
    print("   Configurazione MinIO...")
    minio_config = Path('config/minio_config.py')
    if not minio_config.exists():
        print_error("File config/minio_config.py non trovato")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, str(minio_config)],
            capture_output=True,
            text=True,
            check=True
        )
        print_success("MinIO configurato")
        
    except subprocess.CalledProcessError as e:
        print_error(f"Errore configurazione MinIO: {e}")
        print_warning(f"Output: {e.stderr}")
        return False
    
    # Configura MLflow
    print("   Configurazione MLflow...")
    mlflow_config = Path('config/mlflow_config.py')
    if not mlflow_config.exists():
        print_error("File config/mlflow_config.py non trovato")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, str(mlflow_config)],
            capture_output=True,
            text=True,
            check=True
        )
        print_success("MLflow configurato")
        return True
        
    except subprocess.CalledProcessError as e:
        print_error(f"Errore configurazione MLflow: {e}")
        print_warning(f"Output: {e.stderr}")
        return False


def generate_and_upload_dataset():
    """
    Genera dataset e upload su MinIO con scelta interattiva
    
    Returns:
        bool: True se operazione riuscita, False altrimenti
    """
    print_step(4, 4, "Generazione e Upload Dataset")
    
    # Scelta dataset
    print("\nScegli quale dataset generare:\n")
    print("  1. SAMPLE  - 1K customers, 100 products, 100K transactions (~1 minuto)")
    print("  2. FULL    - 1M customers, 50K products, 100M transactions (~15-20 minuti)")
    print("  3. ENTRAMBI - Prima SAMPLE poi FULL\n")
    
    while True:
        choice = input("Scelta (1/2/3): ").strip()
        if choice in ['1', '2', '3']:
            break
        print_warning("Scelta non valida. Inserisci 1, 2 o 3.")
    
    mode_map = {'1': 'sample', '2': 'full', '3': 'both'}
    mode = mode_map[choice]
    
    print(f"\n   Modalità selezionata: {mode.upper()}\n")
    
    # Genera dataset
    print("   Generazione dataset in corso...")
    dataset_script = Path('scripts/generate_dataset.py')
    if not dataset_script.exists():
        print_error("File scripts/generate_dataset.py non trovato")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, str(dataset_script), '--mode', mode],
            capture_output=False,  # Mostra output in real-time
            text=True,
            check=True
        )
        print_success("Dataset generati")
        
    except subprocess.CalledProcessError as e:
        print_error(f"Errore generazione dataset: {e}")
        return False
    
    # Upload su MinIO
    print("\n   Upload dataset su MinIO...")
    upload_script = Path('scripts/upload_to_minio.py')
    if not upload_script.exists():
        print_error("File scripts/upload_to_minio.py non trovato")
        return False
    
    try:
        result = subprocess.run(
            [sys.executable, str(upload_script), '--mode', mode],
            capture_output=False,  # Mostra output in real-time
            text=True,
            check=True
        )
        print_success("Dataset caricati su MinIO")
        return True
        
    except subprocess.CalledProcessError as e:
        print_error(f"Errore upload dataset: {e}")
        return False


def main():
    """Funzione principale"""
    print_header("BigData E-commerce Project - Setup Automatico")
    
    # Step 1: Check Docker
    if not check_docker():
        print_error("\nSetup abortito: Docker non disponibile")
        sys.exit(1)
    
    # Step 2: Start Docker Compose
    if not start_docker_compose():
        print_error("\nSetup abortito: Errore avvio container")
        sys.exit(1)
    
    # Step 3: Configure services
    if not configure_services():
        print_error("\nSetup abortito: Errore configurazione servizi")
        sys.exit(1)
    
    # Step 4: Generate and upload dataset
    if not generate_and_upload_dataset():
        print_error("\nSetup abortito: Errore generazione/upload dataset")
        sys.exit(1)
    
    # Success
    print_header("Setup Completato con Successo!")
    
    print(f"{Colors.OKGREEN}{Colors.BOLD}Servizi disponibili:{Colors.ENDC}\n")
    print(f"  • MinIO Console:  {Colors.OKBLUE}http://localhost:9001{Colors.ENDC}")
    print(f"    Credenziali:    minioadmin / minioadmin")
    print(f"\n  • MLflow UI:      {Colors.OKBLUE}http://localhost:5000{Colors.ENDC}")
    print(f"\n  • Jupyter:        {Colors.OKBLUE}jupyter notebook{Colors.ENDC}")
    
if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(f"\n\n{Colors.WARNING}Setup interrotto dall'utente{Colors.ENDC}")
        sys.exit(1)
    except Exception as e:
        print_error(f"\nErrore inaspettato: {e}")
        sys.exit(1)
