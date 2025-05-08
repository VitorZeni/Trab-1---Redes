import socket
import os
import hashlib
import time
import math

# --- Configurações ---
ENCODING = 'raw-unicode-escape'
IP = socket.gethostbyname(socket.gethostname())
PORTA = 10000
BUFFER_SIZE = 2048 # Buffer maior para receber ACKs enquanto envia
SEGMENT_SIZE = 1024 # Tamanho dos dados do arquivo por segmento
WINDOW_SIZE = 2    # Tamanho da janela deslizante
ACK_TIMEOUT = 0.5   # Timeout para esperar por ACKs (segundos) - Curto
RETRANSMISSION_TIMEOUT = 1.5 # Timeout para reenviar segmento não confirmado (segundos) - Mais longo
MAX_RETRANSMISSIONS = 5 # Máximo de tentativas por segmento

def calculate_hash(data_bytes):
    """Calcula o hash MD5 dos bytes fornecidos."""
    return hashlib.md5(data_bytes).hexdigest().encode(ENCODING)

# 1) --- Criação do Socket --- 
servidor = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
try:
    servidor.bind((IP, PORTA))
    print(f"Servidor UDP escutando em {IP}:{PORTA}")
except socket.error as e:
    print(f"Erro no bind: {e}")
    exit()

# 3) --- Loop Principal Aguardando Clientes ---
while True:
    print("\nAguardando nova requisição GET...")
    client_address = None
    segmentos = []
    total_segmentos = 0

    while client_address is None: # Espera por um GET válido
        try:
            # Espera indefinidamente pelo primeiro GET
            servidor.settimeout(None)
            dados, temp_address = servidor.recvfrom(BUFFER_SIZE)
            mensagem_cliente = dados.decode(ENCODING)

            if mensagem_cliente.startswith("GET "):
                caminho_arquivo = mensagem_cliente[4:].strip().replace("/", "")
                print(f"Cliente {temp_address} solicitou: {caminho_arquivo}")

                # Verificar existência e segmentar o arquivo
                if not os.path.exists(caminho_arquivo):
                    print(f"Arquivo não encontrado: {caminho_arquivo}")
                    erro_msg = f"Erro|Arquivo '{caminho_arquivo}' não encontrado".encode(ENCODING)
                    servidor.sendto(erro_msg, temp_address)
                    continue # Volta a esperar por GET

                try:
                    with open(caminho_arquivo, 'rb') as f:
                        dados_arquivo = f.read()

                    segmentos = []
                    num_seq = 0
                    for i in range(0, len(dados_arquivo), SEGMENT_SIZE):
                        payload = dados_arquivo[i:i+SEGMENT_SIZE]
                        hash_seg = calculate_hash(payload)
                        # Formato: SEQ|HASH|PAYLOAD
                        segmento = f"{num_seq}|".encode(ENCODING) + hash_seg + b'|' + payload
                        segmentos.append(segmento)
                        num_seq += 1

                    total_segmentos = len(segmentos)
                    client_address = temp_address # Confirma o endereço do cliente
                    print(f"Arquivo '{caminho_arquivo}' segmentado em {total_segmentos} partes para {client_address}.")
                    
                except Exception as e:
                    print(f"Erro ao ler/segmentar arquivo {caminho_arquivo}: {e}")
                    erro_msg = "Erro|Falha ao processar arquivo no servidor".encode(ENCODING)
                    servidor.sendto(erro_msg, temp_address)
                    continue # Volta a esperar por GET
            else:
                 print(f"Recebido '{mensagem_cliente[:50]}' de {temp_address}. Esperando GET.")

        except (UnicodeDecodeError, ConnectionResetError) as e:
             print(f"Erro de decodificação ou conexão resetada: {e}. Aguardando novamente.")
             continue # Ignora pacotes malformados ou erros de conexão UDP
        except Exception as e:
             print(f"Erro inesperado ao aguardar GET: {e}")
             time.sleep(1) # Evita loop de erro muito rápido


    # --- Lógica da Janela Deslizante ---
    base = 0
    proximo_seq_num = 0
    acks_recebidos = set()
    timers_envio = {} # {seq_num: timestamp}
    contagem_tentativas = {} # {seq_num: count}
    transferencia_ativa = True

    while base < total_segmentos and transferencia_ativa:
        # 1. Enviar novos segmentos dentro da janela
        while proximo_seq_num < base + WINDOW_SIZE and proximo_seq_num < total_segmentos:
            if proximo_seq_num not in timers_envio: # Enviar apenas se não foi enviado ou timeout ocorreu
                try:
                    servidor.sendto(segmentos[proximo_seq_num], client_address)
                    timers_envio[proximo_seq_num] = time.time()
                    contagem_tentativas[proximo_seq_num] = 1
                    # print(f"[ENVIO] Segmento {proximo_seq_num} enviado (tentativa 1)")
                except socket.error as e:
                     print(f"Erro de socket ao enviar seg {proximo_seq_num}: {e}")
                     # Poderia abortar aqui ou tentar mais tarde
                     transferencia_ativa = False; break # Abortar transferência neste caso
                except Exception as e:
                     print(f"Erro inesperado ao enviar seg {proximo_seq_num}: {e}")
                     transferencia_ativa = False; break
            proximo_seq_num += 1
        if not transferencia_ativa: break

        # 2. Verificar Timeouts e Retransmitir
        agora = time.time()
        for seq_num in range(base, proximo_seq_num):
            if seq_num not in acks_recebidos:
                tempo_envio = timers_envio.get(seq_num)
                if tempo_envio is not None and agora - tempo_envio > RETRANSMISSION_TIMEOUT:
                    if contagem_tentativas.get(seq_num, 0) >= MAX_RETRANSMISSIONS:
                        print(f"[ERRO FATAL] Segmento {seq_num} excedeu {MAX_RETRANSMISSIONS} tentativas. Abortando envio para {client_address}.")
                        transferencia_ativa = False
                        break # Sai do loop de verificação de timeout

                    print(f"[TIMEOUT] Timeout para ACK do segmento {seq_num}. Reenviando...")
                    try:
                        servidor.sendto(segmentos[seq_num], client_address)
                        timers_envio[seq_num] = agora # Atualiza timer
                        contagem_tentativas[seq_num] = contagem_tentativas.get(seq_num, 0) + 1
                        print(f"[REENVIO] Segmento {seq_num} reenviado (tentativa {contagem_tentativas[seq_num]})")
                    except socket.error as e:
                        print(f"Erro de socket ao reenviar seg {seq_num}: {e}")
                        transferencia_ativa = False; break
                    except Exception as e:
                        print(f"Erro inesperado ao reenviar seg {seq_num}: {e}")
                        transferencia_ativa = False; break
        if not transferencia_ativa: break

        # 3. Tentar Receber ACKs (com timeout curto)
        try:
            servidor.settimeout(ACK_TIMEOUT) # Timeout curto para ACKs
            dados_ack, addr_ack = servidor.recvfrom(BUFFER_SIZE)

            if addr_ack == client_address: # Processar apenas ACKs do cliente atual
                ack_str = dados_ack.decode(ENCODING)
                if ack_str.startswith("ACK|"):
                    ack_seq = int(ack_str.split('|')[1])
                    # print(f"[ACK] Recebido ACK para {ack_seq}")
                    acks_recebidos.add(ack_seq)

                    # Avançar a base da janela
                    while base in acks_recebidos:
                        # Remover do gerenciamento de timer/tentativas para economizar memória
                        timers_envio.pop(base, None)
                        contagem_tentativas.pop(base, None)
                        base += 1
                    # print(f"Janela avançou para base {base}")
                else:
                    print(f"Recebido msg não ACK de {client_address}: {ack_str[:50]}")
            else:
                 print(f"Recebido pacote de endereço inesperado {addr_ack}. Ignorando.")

        except socket.timeout:
            # Timeout esperando por ACK é normal, apenas continua o loop
            pass
        except (UnicodeDecodeError, ValueError, IndexError) as e:
             print(f"Erro ao processar ACK recebido: {e}. Ignorando.")
        except ConnectionResetError:
             print(f"Erro: Conexão resetada pelo cliente {client_address}. Abortando transferência.")
             transferencia_ativa = False
        except Exception as e:
             print(f"Erro inesperado ao receber ACK: {e}")
             transferencia_ativa = False


    # --- Fim da Janela Deslizante ou Abort ---

    if not transferencia_ativa:
        print(f"Transferência para {client_address} foi abortada.")
        continue # Volta ao loop principal esperando por GET

    if base == total_segmentos:
        print(f"\nTodos os {total_segmentos} segmentos de dados confirmados (ACKs recebidos). Enviando EOF...")
        # --- Envio Confiável do EOF ---
        eof_confirmado = False
        tentativas_eof = 0
        # Formato: EOF|HASH_DO_PAYLOAD_EOF (payload é só b"EOF")
        eof_payload = b"EOF"
        eof_hash = calculate_hash(eof_payload)
        segmento_eof = b"EOF|" + eof_hash # Não precisa de numero de sequencia

        while not eof_confirmado and tentativas_eof < MAX_RETRANSMISSIONS:
            print(f"[ENVIO EOF] Enviando sinal de EOF (tentativa {tentativas_eof + 1})...")
            try:
                servidor.sendto(segmento_eof, client_address)
            except Exception as e:
                 print(f"Erro ao enviar EOF: {e}")
                 break # Aborta se não conseguir enviar EOF

            # Espera pelo ACK_EOF
            try:
                servidor.settimeout(RETRANSMISSION_TIMEOUT) # Usa timeout maior para ACK_EOF
                data_eof_ack, addr_eof_ack = servidor.recvfrom(BUFFER_SIZE)
                if addr_eof_ack == client_address and data_eof_ack == b"ACK_EOF":
                    eof_confirmado = True
                    print("[EOF CONFIRMADO] Cliente reconheceu EOF.")
                else:
                    print(f"Recebido msg inesperada ({data_eof_ack[:50]}) de {addr_eof_ack} esperando ACK_EOF.")

            except socket.timeout:
                print("[TIMEOUT EOF] Timeout esperando por ACK_EOF.")
                tentativas_eof += 1
            except Exception as e:
                 print(f"Erro recebendo ACK_EOF: {e}")
                 break # Sai do loop de EOF

        if not eof_confirmado:
            print(f"[FALHA EOF] Cliente {client_address} não confirmou recebimento de EOF após {tentativas_eof} tentativas.")

        print(f"[SUCESSO] Transferência para {client_address} finalizada.")

    else:
         print(f"Transferência para {client_address} finalizou inesperadamente (base={base}, total={total_segmentos}).")
