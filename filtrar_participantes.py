import pandas as pd 

# --- 1. CONFIGURAÇÃO ---

# Caminho do arquivo de entrada (o gigante)
# Verifique se o nome do arquivo é este mesmo
arquivo_completo = "DADOS/PARTICIPANTES_2024.csv" 

# Caminho do arquivo de saída (o seu filtro, bem menor)
arquivo_filtrado = "DADOS/FILTRO_PARTICIPANTES_VALE_SP.csv"

# Coluna que vamos usar para filtrar os municípios
# (Como não temos residência, usamos o local da PROVA)
coluna_filtro_cidade = "CO_MUNICIPIO_PROVA" 

# !! IMPORTANTE: Complete esta lista com TODOS os códigos IBGE
# das cidades do Vale do São Patrício que você quer analisar.
# Eu adicionei os principais como exemplo:
codigos_cidades_desejadas = [
    5208608, # Goianésia
    5211800, # Jaraguá
    5211206, # Itapuranga
    5210901, # Itapaci
    5205406, # Ceres
    5218904, # Rubiataba
    5221700, # Uruana
    5203203, # Barro Alto
    5218607, # Rialma
    5205000, # Carmo do Rio Verde
    5214861, # Nova Glória
    5220157, # São Luís do Norte
    5218706, # Rianápolis
    5219357, # Santa Isabel
    5209804, # Hidrolina
    5219456, # Santa Rita do Novo Destino
    5210158, # Ipiranga de Goiás
    5214705, # Nova América
    5213855, # Morro Agudo de Goiás
    5216908, # Pilar de Goiás
    5220280, # São Patrício
    5209291, # Guaraíta
    5209457 # Guarinos
    
] 

# Lista EXATA de colunas que você pediu para o RapidMiner
# (Baseado no Dicionário PARTICIPANTES_2024.csv)
colunas_desejadas = [
    "CO_MUNICIPIO_PROVA", # Chave do filtro
    "TP_FAIXA_ETARIA",
    "TP_SEXO",
    "TP_ANO_CONCLUIU",
    "NO_MUNICIPIO_PROVA",
    "SG_UF_PROVA",
    "Q007", # Renda da família
    "Q019", # Possui TV por assinatura?
    "Q021", # Possui computador/notebook?
    "Q023"  # Em que tipo de escola frequentou o Ensino Médio?
]

# --- 2. O PROCESSAMENTO ---

print(f"Iniciando a filtragem do arquivo: {arquivo_completo}")
print(f"Salvando resultados em: {arquivo_filtrado}")

# Tamanho de cada "pedaço" de leitura (100 mil linhas por vez)
tamanho_chunk = 100000 

# Para salvar o cabeçalho apenas no primeiro pedaço
primeiro_chunk = True 

try:
    # Criar um leitor de arquivo (iterador)
    leitor_csv = pd.read_csv(
        arquivo_completo, 
        sep=';',           
        encoding='latin1', # Encoding comum para dados do Inep
        usecols=colunas_desejadas, # Só carrega as colunas da lista
        chunksize=tamanho_chunk,   # Lê o arquivo em pedaços
        low_memory=False
    )
    
    # Loop por cada pedaço do arquivo
    for chunk in leitor_csv:
        print("Processando um chunk de 100.000 linhas...")
        
        # Filtra o pedaço mantendo apenas as linhas
        # cujos códigos de cidade estão na sua lista
        chunk_filtrado = chunk[chunk[coluna_filtro_cidade].isin(codigos_cidades_desejadas)]
        
        # Se encontramos dados, salvamos no novo arquivo
        if not chunk_filtrado.empty:
            print(f"-> Encontrados {len(chunk_filtrado)} participantes neste chunk. Salvando...")
            if primeiro_chunk:
                # Salva o primeiro chunk com cabeçalho
                chunk_filtrado.to_csv(arquivo_filtrado, mode='w', header=True, index=False, sep=';')
                primeiro_chunk = False
            else:
                # Anexa os próximos chunks sem o cabeçalho
                chunk_filtrado.to_csv(arquivo_filtrado, mode='a', header=False, index=False, sep=';')
        
    print(f"\nFiltragem concluída!")
    if primeiro_chunk:
        print("AVISO: Nenhum participante foi encontrado com os códigos de cidade fornecidos.")
        print("Verifique a lista 'codigos_cidades_desejadas' e a coluna 'coluna_filtro_cidade'.")
    else:
        print(f"Seu novo arquivo está pronto em: {arquivo_filtrado}")

except FileNotFoundError:
    print(f"ERRO: Arquivo não encontrado em: {arquivo_completo}")
    print("Verifique se o nome e o caminho do arquivo .csv estão corretos.")
except Exception as e:
    print(f"Ocorreu um erro: {e}")
    print("Dica: Verifique se os nomes das colunas em 'colunas_desejadas' estão 100% corretos")
    print("e se o 'sep' (separador) está certo (ex: ';' ou ',').")