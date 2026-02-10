import pandas as pd
import matplotlib.pyplot as plt
import clickhouse_connect

# 🔌 Conexão ClickHouse
client = clickhouse_connect.get_client(
    host='localhost',
    username='default',
    password='',
    database='default'
)

# 📅 Parâmetros
dia = '2025-09-01'
verbose = False  # Ativar para debug

# Consulta de todas as ofertas do dia
query = f"""
SELECT periodo, tipo_oferta, volume, preco
FROM ofertas
WHERE data = '{dia}' 
  AND status IN ('O')
"""

data = client.query(query).result_rows
df = pd.DataFrame(data, columns=['periodo', 'tipo_oferta', 'volume', 'preco'])

# Lista para armazenar clearing prices por período
clearing_list = []

# Iterar sobre cada período/hora
for periodo, group in df.groupby('periodo'):
    compras = group[group['tipo_oferta'] == 'C'].sort_values(by='preco', ascending=False).reset_index(drop=True)
    vendas  = group[group['tipo_oferta'] == 'V'].sort_values(by='preco', ascending=True).reset_index(drop=True)
    
    compras['vol_acum'] = compras['volume'].cumsum()
    vendas['vol_acum']  = vendas['volume'].cumsum()
    
    # Calcular clearing price com a nova lógica
    clearing_price = None
    clearing_volume = None
    i = 0
    j = 0
    iavancou = False
    
    while i < len(compras) and j < len(vendas):
        if verbose:
            print(f"Período {periodo} - C:{i}({compras.iloc[i]['preco']:.4f}) V:{j}({vendas.iloc[j]['preco']:.4f}) | VolC:{compras.iloc[i]['vol_acum']:.2f} VolV:{vendas.iloc[j]['vol_acum']:.2f}")
        
        # Condição de cruzamento de preços (A compra ficou barata demais ou a venda cara demais)
        if round(compras.iloc[i]["preco"], 2) < round(vendas.iloc[j]["preco"], 2):
            if iavancou:  # O último incremento foi na COMPRA (i aumentou)
                clearing_volume = compras.iloc[i]["vol_acum"]
                clearing_price = vendas.iloc[j]["preco"]
            else:  # O último incremento foi na VENDA (j aumentou)
                clearing_volume = vendas.iloc[j]["vol_acum"]
                clearing_price = compras.iloc[i]["preco"]
            break
        
        # Lógica de avanço normal enquanto há casamento de preço
        if round(compras.iloc[i]["vol_acum"], 2) < round(vendas.iloc[j]["vol_acum"], 2):
            i += 1
            iavancou = True
        else:
            j += 1
            iavancou = False
    
    # Caso não tenha encontrado cruzamento
    if clearing_price is None and (i >= len(compras) or j >= len(vendas)):
        if i >= len(compras):
            clearing_volume = compras.iloc[-1]["vol_acum"]
            clearing_price = vendas.iloc[j]["preco"]
        else:
            clearing_volume = vendas.iloc[-1]["vol_acum"]
            clearing_price = compras.iloc[i]["preco"]
    
    clearing_list.append({
        'periodo': periodo,
        'clearing_price': clearing_price,
        'volume_comercializado': clearing_volume
    })
    
    # Plot da curva por período
    plt.figure(figsize=(8, 5))
    plt.step(compras['vol_acum'], compras['preco'], where='post', color='blue', label='Demanda (Compras)')
    plt.step(vendas['vol_acum'], vendas['preco'], where='pre', color='red', label='Oferta (Vendas)')
    
    if clearing_price is not None:
        plt.plot(clearing_volume, clearing_price, 'go', markersize=10,
                 label=f'Clearing Price = {clearing_price:.2f} €\nVolume = {clearing_volume:.2f} MWh')
    
    plt.xlabel('Volume Acumulado (MWh)')
    plt.ylabel('Preço (€)')
    plt.title(f'Curva de Clearing - Dia {dia} Período {periodo}')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# Criar DataFrame resumo
df_clearing = pd.DataFrame(clearing_list)
print("📊 Resumo diário de Clearing Price:")
print(df_clearing.sort_values('periodo'))