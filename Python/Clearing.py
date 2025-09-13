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
periodo = 12  # hora do mercado (1 a 24)

# Consulta filtrando por dia e período
query = f"""
SELECT tipo_oferta, volume, preco
FROM ofertas
WHERE data = '{dia}' 
  AND periodo = {periodo}
  AND status IN ('C', 'O')
"""
data = client.query(query).result_rows
df = pd.DataFrame(data, columns=['tipo_oferta', 'volume', 'preco'])

# Separar compras e vendas
compras = df[df['tipo_oferta'] == 'C'].copy()
vendas  = df[df['tipo_oferta'] == 'V'].copy()

# Ordenar e acumular volumes
compras = compras.sort_values(by='preco', ascending=False)
vendas  = vendas.sort_values(by='preco', ascending=True)

compras['vol_acum'] = compras['volume'].cumsum()
vendas['vol_acum']  = vendas['volume'].cumsum()

# 🔎 Calcular clearing price e volume comercializado
clearing_price = None
clearing_volume = None

for i, row in vendas.iterrows():
    demanda_max = compras[compras['preco'] >= row['preco']]['volume'].sum()
    if row['vol_acum'] >= demanda_max:
        clearing_price = round(row['preco'], 2)  # 2 casas decimais
        clearing_volume = demanda_max
        break

print(f"💡 Clearing Price: {clearing_price} € , Volume Comercializado: {clearing_volume:.2f} MWh")

# 📊 Plotar curvas
plt.figure(figsize=(10,6))

# Curva de demanda
plt.step(compras['vol_acum'], compras['preco'], where='post', color='blue', label='Demanda (Compras)')

# Curva de oferta
plt.step(vendas['vol_acum'], vendas['preco'], where='post', color='red', label='Oferta (Vendas)')

# Ponto de clearing
if clearing_price is not None:
    plt.plot(clearing_volume, clearing_price, 'go', markersize=10,
             label=f'Clearing Price = {clearing_price} €\nVolume = {clearing_volume:.2f} Wh')

plt.xlabel('Volume Acumulado (Wh)')
plt.ylabel('Preço (€)')
plt.title(f'Curva de Clearing Price - Dia {dia} Período {periodo}')
plt.legend()
plt.grid(True)
plt.show()
