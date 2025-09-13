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

# 📥 Parâmetros
dia = '2025-09-01'

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
    compras = group[group['tipo_oferta']=='C'].sort_values(by='preco', ascending=False)
    vendas  = group[group['tipo_oferta']=='V'].sort_values(by='preco', ascending=True)
    
    compras['vol_acum'] = compras['volume'].cumsum()
    vendas['vol_acum']  = vendas['volume'].cumsum()
    
    # Calcular clearing price
    clearing_price = None
    clearing_volume = None
    for i, row in vendas.iterrows():
        demanda_max = compras[compras['preco'] >= row['preco']]['volume'].sum()
        if row['vol_acum'] >= demanda_max:
            clearing_price = round(row['preco'], 2)
            clearing_volume = demanda_max
            break
    
    clearing_list.append({
        'periodo': periodo,
        'clearing_price': clearing_price,
        'volume_comercializado': clearing_volume
    })
    
    # Plot da curva por período
    plt.figure(figsize=(8,5))
    plt.step(compras['vol_acum'], compras['preco'], where='post', color='blue', label='Demanda (Compras)')
    plt.step(vendas['vol_acum'], vendas['preco'], where='post', color='red', label='Oferta (Vendas)')
    
    if clearing_price is not None:
        plt.plot(clearing_volume, clearing_price, 'go', markersize=10,
                 label=f'Clearing Price = {clearing_price} €\nVolume = {clearing_volume:.2f} MWh')
    
    plt.xlabel('Volume Acumulado (MWh)')
    plt.ylabel('Preço (€)')
    plt.title(f'Curva de Clearing - Dia {dia} Período {periodo}')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

# Criar DataFrame resumo
df_clearing = pd.DataFrame(clearing_list)
print("💡 Resumo diário de Clearing Price:")
print(df_clearing.sort_values('periodo'))
