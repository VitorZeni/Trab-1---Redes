import socket
import hashlib
import time
import math

# --- Configurações ---
ENCODING = 'raw-unicode-escape'
BUFFER_SIZE = 2048        # Buffer de recepção (maior para caber segmento + cabeçalho)
RECEIVE_TIMEOUT = 5.0   # Timeout principal do cliente esperando pacotes (segundos)
MAX_TIMEOUTS_CONSECUTIVOS = 3 # Número de timeouts seguidos antes de desistir

def calculate_hash(data_bytes):
    """Calcula o hash MD5 dos bytes fornecidos."""
    return hashlib.md5(data_bytes).hexdigest().encode(ENCODING)

def send_ack(sock, address, seq_num, encoding):
    """Envia uma mensagem ACK para o número de sequência especificado."""
    try:
        ack_msg = f"ACK|{seq_num}".encode(encoding)
        sock.sendto(ack_msg, address)
        # print(f"ACK para {seq_num} enviado.") # Descomente para depuração detalhada de ACKs
    except socket.error as e:
        print(f"Erro de socket ao enviar ACK para {seq_num}: {e}")
    except Exception as e:
        print(f"Erro inesperado ao enviar ACK para {seq_num}: {e}")


# --- Obter informações do usuário ---
while True:
    try:
        ip_servidor = input("Digite o endereço IP do servidor: ")
        # Validação básica de IP (não muito rigorosa)
        socket.inet_aton(ip_servidor) # Gera erro se inválido
        break
    except socket.error:
        print("Endereço IP inválido. Tente novamente.")

while True:
    porta_servidor_str = input("Digite o número da porta do servidor (> 1024): ")
    try:
        porta_servidor = int(porta_servidor_str)
        if porta_servidor <= 1024 or porta_servidor > 65535 :
             print("Porta inválida. Use um valor entre 1025 e 65535.")
        else:
             break
    except ValueError:
        print("Porta inválida. Por favor, digite um número inteiro.")

while True:
    nome_arquivo = input("Digite o nome do arquivo que deseja solicitar (ex: meu_arquivo.txt): ")
    if nome_arquivo: # Verifica se não está vazio
         break
    else:
         print("Nome do arquivo não pode ser vazio.")


# Simulação da perda de pacotes
segmentos_ignorados_str = input("Digite os números dos segmentos a ignorar (separados por vírgula, ex: 1,5,10), ou deixe em branco: ")
segmentos_ignorados = set()
if segmentos_ignorados_str.strip():
    try:
        segmentos_ignorados = set(map(int, segmentos_ignorados_str.strip().split(",")))
        print(f"Configurado para ignorar (não enviar ACK para) os segmentos: {sorted(list(segmentos_ignorados))}")
    except ValueError:
        print("Entrada inválida para segmentos a ignorar. Nenhum segmento será ignorado.")

# --- Inicialização do Socket ---
try:
    cliente = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Tentar aumentar o buffer de recebimento (SO_RCVBUF) - Opcional, mas pode ajudar
    try:
        default_rcvbuf = cliente.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        print(f"Tamanho original do buffer de recebimento (SO_RCVBUF): {default_rcvbuf}")
        new_rcvbuf = 2 * 1024 * 1024 # Tentar 2MB
        cliente.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, new_rcvbuf)
        actual_rcvbuf = cliente.getsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF)
        print(f"Tentativa de definir SO_RCVBUF para {new_rcvbuf}, valor atual: {actual_rcvbuf}")
    except Exception as e:
        print(f"Aviso: Não foi possível alterar SO_RCVBUF: {e}")

    # Definir timeout principal para recebimento
    cliente.settimeout(RECEIVE_TIMEOUT)

except socket.error as e:
    print(f"Erro ao criar o socket: {e}")
    exit()
except Exception as e:
     print(f"Erro inesperado na inicialização: {e}")
     exit()

# 2) --- Envio da Requisição Inicial ---
mensagem_get = f"GET /{nome_arquivo}"
server_address = (ip_servidor, porta_servidor)
try:
    cliente.sendto(mensagem_get.encode(ENCODING), server_address)
    print(f"Solicitação enviada para {ip_servidor}:{porta_servidor}: '{nome_arquivo}'")
except socket.error as e:
    print(f"Erro de socket ao enviar requisição GET: {e}")
    cliente.close()
    exit()
except Exception as e:
     print(f"Erro inesperado ao enviar GET: {e}")
     cliente.close()
     exit()


# --- Recepção dos Segmentos e Envio de ACKs ---
segmentos_recebidos = {}
segmentos_corrompidos_log = set() # Apenas para log
eof_confirmado = False
timeouts_consecutivos = 0
ultimo_ack_enviado = -1 # Para rastrear ACKs enviados
proximo_segmento_esperado = 0
segmentos_fora_de_ordem = {} 

print("\nAguardando segmentos do arquivo...")

while not eof_confirmado and timeouts_consecutivos < MAX_TIMEOUTS_CONSECUTIVOS:
    try:
        dados, endereco_servidor = cliente.recvfrom(BUFFER_SIZE)

        # Reinicia contador de timeouts se receber algo
        timeouts_consecutivos = 0

        # Verificar se é do servidor esperado
        if endereco_servidor != server_address:
            print(f"Recebido pacote de endereço inesperado {endereco_servidor}. Ignorando.")
            continue

        # --- Verificar mensagem de ERRO do servidor ---
        # Supõe formato "Erro|Mensagem"
        is_error = False
        if len(dados) > 5 and dados[:5] == b"Erro|":
            try:
                mensagem_erro = dados.split(b"|", 1)[1].decode(ENCODING)
                print(f"\n[ERRO DO SERVIDOR] {mensagem_erro}")
            except Exception: # Erro no split ou decode
                 print(f"\n[ERRO DO SERVIDOR] Mensagem de erro malformada recebida: {dados[:50]}")
            eof_confirmado = True # Considera fim, mas com erro
            break # Sai do loop principal

        # --- Verificar mensagem de EOF do servidor ---
        # Supõe formato "EOF|HASH"
        is_eof = False
        if len(dados) > 4 and dados[:4] == b"EOF|":
             is_eof = True
             try:
                 hash_eof_recv = dados.split(b"|")[1]
                 if calculate_hash(b"EOF") == hash_eof_recv:
                      print("[EOF RECEBIDO] EOF recebido e validado.")
                      # Enviar ACK_EOF de forma confiável (tenta algumas vezes)
                      ack_eof_msg = b"ACK_EOF"
                      for i in range(3): # Tenta enviar ACK_EOF algumas vezes
                           try:
                               cliente.sendto(ack_eof_msg, server_address)
                               print(f"ACK_EOF enviado (tentativa {i+1}).")
                               eof_confirmado = True
                               break
                               # Consideramos confirmado após enviar algumas vezes
                               # O servidor também retransmite EOF, então deve funcionar
                           except Exception as e:
                                print(f"Erro ao enviar ACK_EOF (tentativa {i+1}): {e}")
                           time.sleep(0.1) # Pequena pausa entre envios de ACK_EOF
                     # eof_confirmado = True
                      #break # Sai do loop principal, EOF confirmado
                 else:
                      print("[CORRUPÇÃO EOF] Hash inválido no EOF recebido. Ignorado.")
                      # Não envia ACK_EOF, espera servidor reenviar EOF
             except Exception as e:
                  print(f"Erro ao processar EOF: {e}. Ignorado.")
             continue # Processou EOF (bem ou mal), espera próximo pacote


        # --- Processar Segmento de Dados ---
        if not is_error and not is_eof:
            try:
                # Formato: SEQ(4)|HASH(32)|PAYLOAD
                partes = dados.split(b"|", 2) # Separa SEQ, HASH, PAYLOAD
                
                if len(partes) < 3:
                     print(f"Segmento de dados malformado. Ignorado.")
                     continue
                
                seq_num_bytes = partes[0]
                hash_recebido = partes[1] # Hash já está em bytes codificados
                segmento_dados = partes[2]
                numero_sequencia = int(seq_num_bytes.decode(ENCODING))

                # >>> SIMULAÇÃO DE PERDA <<<
                if numero_sequencia in segmentos_ignorados:
                    print(f"[Simulação de perda] Segmento {numero_sequencia} ignorado. ACK NÃO enviado.")
                    # Opcional: Remover para permitir aceitação futura?
                    segmentos_ignorados.discard(numero_sequencia)
                    continue # Pula o resto, não envia ACK

                # 4) Validação de Hash
                hash_calculado = calculate_hash(segmento_dados)
                if hash_calculado != hash_recebido:
                    print(f"[CORRUPÇÃO] Hash inválido para segmento {numero_sequencia}. Ignorado. ACK NÃO enviado.")
                    segmentos_corrompidos_log.add(numero_sequencia)
                    continue # Não processa, não envia ACK

                # Segmento Válido - Armazenar e Enviar ACK
                ultimo_ack_enviado = max(ultimo_ack_enviado, numero_sequencia) # Atualiza log
            
                if numero_sequencia == proximo_segmento_esperado:
                    print(f"Segmento {numero_sequencia} recebido OK (em ordem).")
                    segmentos_recebidos[numero_sequencia] = segmento_dados
                    send_ack(cliente, server_address, numero_sequencia, ENCODING) # Envia ACK
                    proximo_segmento_esperado += 1

                    while proximo_segmento_esperado in segmentos_fora_de_ordem:
                        print(f"Processando segmento {proximo_segmento_esperado} do buffer.")
                        segmento_buffered = segmentos_fora_de_ordem.pop(proximo_segmento_esperado)
                        segmentos_recebidos[proximo_segmento_esperado] = segmento_buffered
                        # ACK já foi enviado quando ele entrou no buffer
                        proximo_segmento_esperado += 1

                elif numero_sequencia > proximo_segmento_esperado:
                    # Duplicado válido - Reenviar ACK
                    # print(f"Segmento {numero_sequencia} duplicado válido recebido.")
                    if numero_sequencia not in segmentos_fora_de_ordem and numero_sequencia not in segmentos_recebidos:
                        print(f"Segmento {numero_sequencia} recebido OK (fora de ordem). Armazenando no buffer.")
                        segmentos_fora_de_ordem[numero_sequencia] = segmento_dados
                        send_ack(cliente, server_address, numero_sequencia, ENCODING) # Envia ACK imediatamente
                    else:
                         # Duplicado de algo já no buffer ou já processado
                         # print(f"Segmento {numero_sequencia} duplicado (fora de ordem) recebido.")
                         send_ack(cliente, server_address, numero_sequencia, ENCODING) # Reenvia ACK


                else:
                    print(f"Segmento {numero_sequencia} duplicado (antigo) recebido.")
                    send_ack(cliente, server_address, numero_sequencia, ENCODING) # Reenvia ACK para garantir

            except (ValueError, IndexError, UnicodeDecodeError) as e:
                seq_bytes_repr = seq_num_bytes if 'seq_num_bytes' in locals() else b'N/A'
                print(f"Erro ao processar segmento de dados (SeqBytes: {seq_bytes_repr}): {e}. Ignorado. Dados: {dados[:60]}")
                continue
            except Exception as e:
                 print(f"Erro inesperado ao processar segmento: {e}")

    except socket.timeout:
        timeouts_consecutivos += 1
        print(f"Timeout esperando por dados... ({timeouts_consecutivos}/{MAX_TIMEOUTS_CONSECUTIVOS})")
        if not eof_confirmado and timeouts_consecutivos >= MAX_TIMEOUTS_CONSECUTIVOS:
             print("Número máximo de timeouts consecutivos atingido. Desistindo.")
             break # Sai do loop principal
        # Se não atingiu o max, continua esperando no loop

    except ConnectionResetError:
         print("Erro: Servidor parece ter fechado a conexão inesperadamente (ConnectionResetError).")
         eof_confirmado = False # Garante que não vai salvar
         break
    except Exception as e:
        print(f"Erro inesperado no loop de recepção: {e}")
        eof_confirmado = False # Garante que não vai salvar
        break # Sai em caso de erro grave


# --- Verificação Final e Montagem ---
if eof_confirmado and not any(isinstance(v, bytes) and v.startswith(b"Erro|") for v in segmentos_recebidos.values()): # Checa se EOF foi confirmado E não houve erro explícito do servidor
    print("\nTransferência concluída (EOF confirmado). Verificando integridade final...")
    # Idealmente, precisaríamos saber o número total de segmentos esperado do EOF
    # para garantir que não faltou nada, mas a lógica de ACKs no servidor
    # já garante isso se a janela deslizante chegou ao fim (base == total_segmentos).
    # Vamos apenas montar o que temos em ordem.

    if not segmentos_recebidos:
         print("Nenhum segmento de dados foi recebido.")
    elif len(segmentos_fora_de_ordem) > 0:
         print(f"[ERRO FINAL] Transferência concluída, mas restaram {len(segmentos_fora_de_ordem)} segmentos no buffer fora de ordem: {sorted(segmentos_fora_de_ordem.keys())}.")
         print("Isso indica que segmentos anteriores a estes nunca foram recebidos.")
         # Você pode optar por salvar o arquivo parcial ou não aqui.
    else:
        print("Montando o arquivo...")
        arquivo_completo = b""
        chaves_ordenadas = sorted(segmentos_recebidos.keys())
        expected_final_check = 0
        completo = True
        for chave in chaves_ordenadas:
            if chave == expected_final_check:
                arquivo_completo += segmentos_recebidos[chave]
                expected_final_check += 1
            else:
                print(f"[ERRO DE MONTAGEM FINAL] Falta o segmento {expected_final_check}. Sequência quebrada em {chave}.")
                completo = False
                break

        if completo:
             # Salvar o arquivo recebido localmente
             nome_arquivo_local = f"recebido_{nome_arquivo}"
             try:
                 with open(nome_arquivo_local, 'wb') as arquivo_destino:
                     arquivo_destino.write(arquivo_completo)
                 print(f"Arquivo '{nome_arquivo}' recebido ({len(arquivo_completo)} bytes) e salvo como '{nome_arquivo_local}'.")
                 print(f"Número total de segmentos de dados recebidos: {len(segmentos_recebidos)}")
                 if segmentos_corrompidos_log:
                      print(f"Segmentos detectados como corrompidos (e ignorados): {sorted(list(segmentos_corrompidos_log))}")

             except IOError as e:
                 print(f"Erro ao salvar o arquivo recebido: {e}")
        else:
             print("Arquivo não foi salvo devido a segmentos faltando na sequência final.")

elif timeouts_consecutivos >= MAX_TIMEOUTS_CONSECUTIVOS :
    print("\n[ERRO FINAL] Transferência falhou devido a timeouts excessivos esperando dados do servidor.")
    print(f"Próximo segmento que era esperado em ordem: {proximo_segmento_esperado}")
    print(f"Segmentos recebidos fora de ordem (no buffer): {sorted(segmentos_fora_de_ordem.keys())}")
    print(f"Total de segmentos recebidos e validados (em ordem + fora de ordem): {len(segmentos_recebidos) + len(segmentos_fora_de_ordem)}")
    print(f"Último ACK enviado foi para (aprox): {ultimo_ack_enviado}") # Nota: ACKs agora são mais frequentes
else: # Falha por outro motivo (Erro do servidor, EOF não confirmado, etc)
     print("\n[ERRO FINAL] Transferência não concluída com sucesso.")
     if not eof_confirmado and len(segmentos_recebidos) > 0:
          print("EOF não foi confirmado pelo servidor.")

# --- Finalização ---
print("Fechando o socket do cliente.")
cliente.close()