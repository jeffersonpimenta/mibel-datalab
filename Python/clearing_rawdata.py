import pandas as pd
import matplotlib.pyplot as plt
from io import StringIO

def parse_number(series):
    """
    Converte números no formato europeu:
    milhar = .
    decimal = ,
    """
    return (
        series
        .astype(str)
        .str.replace(".", "", regex=False)   # remove milhar
        .str.replace(",", ".", regex=False)  # troca decimal
        .astype(float)
    )



    
def clearing_price_with_plot(bids_text,pais="MI",periodo="H1Q1",verbose=True,plot=True):
    # Ler dados
    df = pd.read_csv(StringIO(bids_text), sep=";", dtype=str)
    df.columns = [c.strip() for c in df.columns]

    # Converter números usando sua função parse_number
    df["Potencia Compra/Venta"] = parse_number(df["Potencia Compra/Venta"])
    df["Precio Compra/Venta"]   = parse_number(df["Precio Compra/Venta"])

    # Separar compras e vendas
    compras = df[(df["Periodo"] == periodo) & (df["Tipo Oferta"] == "C") & (df["Pais"] == pais)].copy()
    vendas  = df[(df["Periodo"] == periodo) & (df["Tipo Oferta"] == "V") & (df["Pais"] == pais)].copy()

    # Ordenação de mercado
    compras = compras.sort_values("Precio Compra/Venta", ascending=False)
    vendas  = vendas.sort_values("Precio Compra/Venta", ascending=True)

    # Volumes acumulados
    compras["Volume_Acumulado"] = compras["Potencia Compra/Venta"].cumsum()
    vendas["Volume_Acumulado"]  = vendas["Potencia Compra/Venta"].cumsum()

    # Clearing
    i = j = 0
    clearing_price = None
    clearing_volume = 0.0
    iavancou = False # Inicialização necessária

    while i < len(compras) and j < len(vendas):
        #buy  = compras.iloc[i]
        #sell = vendas.iloc[j]
        
        if verbose:
            print(f"C:{i}({compras.iloc[i]['Precio Compra/Venta']:.4f}) V:{j}({vendas.iloc[j]['Precio Compra/Venta']:.4f}) | VolC:{compras.iloc[i]['Volume_Acumulado']:.2f} VolV:{vendas.iloc[j]['Volume_Acumulado']:.2f}")

        # Condição de cruzamento de preços (A compra ficou barata demais ou a venda cara demais)
        if round(compras.iloc[i]["Precio Compra/Venta"],2) < round(vendas.iloc[j]["Precio Compra/Venta"],2):
            
            if iavancou: # O último incremento foi na COMPRA (i aumentou)
                while ((vendas.iloc[j]["Precio Compra/Venta"]<=compras.iloc[i-1]["Precio Compra/Venta"])and(vendas.iloc[j]["Volume_Acumulado"]<=compras.iloc[i]["Volume_Acumulado"])):
                    j=j+1 # Avança as compras até que o volume esteja englobado na última bid
                    if verbose:
                        print(f"C:{i}({compras.iloc[i]['Precio Compra/Venta']:.4f}) V:{j}({vendas.iloc[j]['Precio Compra/Venta']:.4f}) | VolC:{compras.iloc[i]['Volume_Acumulado']:.2f} VolV:{vendas.iloc[j]['Volume_Acumulado']:.2f}")
                        
                while ((vendas.iloc[j-1]["Precio Compra/Venta"]<=compras.iloc[i]["Precio Compra/Venta"])and(vendas.iloc[j]["Volume_Acumulado"]>=compras.iloc[i]["Volume_Acumulado"])):
                    i=i+1 # Avança as compras até que o volume esteja englobado na última bid
                    if verbose:
                        print(f"C:{i}({compras.iloc[i]['Precio Compra/Venta']:.4f}) V:{j}({vendas.iloc[j]['Precio Compra/Venta']:.4f}) | VolC:{compras.iloc[i]['Volume_Acumulado']:.2f} VolV:{vendas.iloc[j]['Volume_Acumulado']:.2f}")                        
                if compras.iloc[i]["Volume_Acumulado"] >= vendas.iloc[j]["Volume_Acumulado"]:
                    clearing_volume = vendas.iloc[j]["Volume_Acumulado"]
                    clearing_price = compras.iloc[i-1]["Precio Compra/Venta"]
                else:
                    clearing_volume = compras.iloc[i]["Volume_Acumulado"]
                    clearing_price = vendas.iloc[j-1]["Precio Compra/Venta"] 
            
            else: # O último incremento foi na VENDA (j aumedntou)
                while ((vendas.iloc[j-1]["Precio Compra/Venta"]<=compras.iloc[i]["Precio Compra/Venta"])and(vendas.iloc[j]["Volume_Acumulado"]>=compras.iloc[i]["Volume_Acumulado"])):
                    i=i+1 # Avança as compras até que o volume esteja englobado na última bid 
                    if verbose:
                        print(f"C:{i}({compras.iloc[i]['Precio Compra/Venta']:.4f}) V:{j}({vendas.iloc[j]['Precio Compra/Venta']:.4f}) | VolC:{compras.iloc[i]['Volume_Acumulado']:.2f} VolV:{vendas.iloc[j]['Volume_Acumulado']:.2f}")
                        
                while ((vendas.iloc[j]["Precio Compra/Venta"]<=compras.iloc[i-1]["Precio Compra/Venta"])and(vendas.iloc[j]["Volume_Acumulado"]>=compras.iloc[i]["Volume_Acumulado"])):
                    i=i+1 # Avança as compras até que o volume esteja englobado na última bid
                    if verbose:
                        print(f"C:{i}({compras.iloc[i]['Precio Compra/Venta']:.4f}) V:{j}({vendas.iloc[j]['Precio Compra/Venta']:.4f}) | VolC:{compras.iloc[i]['Volume_Acumulado']:.2f} VolV:{vendas.iloc[j]['Volume_Acumulado']:.2f}")                        
                if compras.iloc[i]["Volume_Acumulado"] >= vendas.iloc[j]["Volume_Acumulado"]:
                    clearing_volume = vendas.iloc[j]["Volume_Acumulado"]
                    clearing_price = compras.iloc[i-1]["Precio Compra/Venta"]
                else:
                    clearing_volume = compras.iloc[i]["Volume_Acumulado"]
                    clearing_price = vendas.iloc[j-1]["Precio Compra/Venta"] 
            break
            
        # Lógica de avanço normal enquanto há casamento de preço
        if round(compras.iloc[i]["Volume_Acumulado"],2) < round(vendas.iloc[j]["Volume_Acumulado"],2):
            i += 1
            iavancou = True
        else:
            j += 1
            iavancou = False

    # -----------------------
    #  GRÁFICO
    # -----------------------
    
    if plot:
        plt.figure()

        # Curva de procura (compras)
        plt.step(
            compras["Volume_Acumulado"],
            compras["Precio Compra/Venta"],
            where="post",
            label="Procura (Compras)"
        )

        # Curva de oferta (vendas)
        plt.step(
            vendas["Volume_Acumulado"],
            vendas["Precio Compra/Venta"],
            where="post",
            label="Oferta (Vendas)"
        )


        # Marcadores individuais para cada bid de venda
        #plt.scatter(vendas["Volume_Acumulado"], vendas["Precio Compra/Venta"], 
        #            color='red', s=10, edgecolors='black', label="Final de Bid (V)", zorder=3)

        # Marcadores individuais para cada bid de venda
        #plt.scatter(compras["Volume_Acumulado"], compras["Precio Compra/Venta"], 
        #            color='red', s=10, edgecolors='black', label="Final de Bid (C)", zorder=3)


        # Marcar clearing
        if clearing_price is not None:
            plt.axhline(clearing_price, linestyle="--", label="Preço de clearing")
            plt.axvline(clearing_volume, linestyle="--", label="Volume casado")

        plt.xlabel("Potência acumulada [MW]")
        plt.ylabel("Preço [€/MWh]")
        plt.title("Market Clearing – Oferta x Procura")
        plt.legend()
        plt.grid(True)
        plt.show()

    return clearing_price, clearing_volume

bids="""

"""
def read_bids_file(path):
    with open(path, "r", encoding="latin-1") as f:
        return f.read()

price, volume = clearing_price_with_plot(read_bids_file("C:\\Users\\jmelo\\Documents\\Python\\curva_pbc_20260203.1"),"ES",plot=True,periodo="H12Q1")

print(f"Preço de clearing: {price:.4f} €/MWh")
print(f"Volume casado: {volume: .2f} MW")