import pandas as pd

# ─────────────────────────────────────────────────────────────────────────────
#  CLEARING ALGORITHM
# ─────────────────────────────────────────────────────────────────────────────

def clearing(
    compras_df: pd.DataFrame,
    vendas_df:  pd.DataFrame,
    col_preco:  str  = "Precio",
    col_vol:    str  = "Volume_Acumulado",
    verbose:    bool = False
):
    """
    Algoritmo de market clearing iterativo.

    Percorre as curvas de compra (DESC) e venda (ASC) com dois ponteiros i, j.
    Guarda o último par (last_i, last_j) com preço de compra >= preço de venda
    e aplica as regras de decisão de preço após o cruzamento.

    Caso especial — degrau de venda
    --------------------------------
    Quando a curva de venda contém um degrau vertical grande (um único bid com
    volume muito superior ao gap de demanda), o ponteiro j pode "saltar" esse
    degrau numa iteração anterior. Em iterações subsequentes, i avança enquanto
    pc >= pv_degrau, e o break ocorre quando pc < pv_degrau.

    Detectado por: last_j == j no momento do break (j não avançou na última
    iteração — foi i quem avançou dentro do degrau).

    Nesse caso:
      - Preço  = pv[j-1]  (preço do pé do degrau, última venda antes do salto)
      - Volume = vc do primeiro bid de compra com pc < pv[j-1]
                 (demanda acumulada que "entrou" no degrau, incluindo o bid
                  de ruptura como marcador da fronteira)

    Caso especial — degrau de compra
    ----------------------------------
    O caso simétrico (bid de compra grande que "salta" sobre bids de venda
    intermediários) é tratado naturalmente pelas regras existentes: quando
    i avança para o degrau e pc < pv, last=(i_teto, j_atual) com vc < vv,
    e a regra `last_j > 0 → return pv_last, vc_last` retorna o preço correto.
    """
    c = compras_df.reset_index(drop=True)
    v = vendas_df.reset_index(drop=True)

    i = j = 0
    last_i = last_j = None

    while i < len(c) and j < len(v):
        pc = round(c.iloc[i][col_preco], 2)
        pv = round(v.iloc[j][col_preco],  2)
        vc = c.iloc[i][col_vol]
        vv = v.iloc[j][col_vol]

        if verbose:
            print(f"  C:{i}(P={pc:.4f}, V={vc:.2f})  V:{j}(P={pv:.4f}, V={vv:.2f})")

        if pc < pv:
            if verbose:
                print(f"  → Cruzamento de preços. last=C:{last_i} V:{last_j}")

            # ── Degrau de venda ───────────────────────────────────────────
            # Se j não avançou na última iteração (last_j == j), o break foi
            # causado por i ter avançado para dentro de um degrau de preço de
            # venda. Nesse caso o preço de casamento é o pé do degrau (pv[j-1])
            # e o volume é o vc do bid de compra que cruzou esse piso.
            if last_j is not None and last_j == j and j > 0:
                pv_prev = round(v.iloc[j - 1][col_preco], 2)
                if pc >= pv_prev:
                    # Avança i até o primeiro bid com pc < pv_prev
                    while i < len(c) and round(c.iloc[i][col_preco], 2) >= pv_prev:
                        i += 1
                    # Volume = vc do bid que cruzou o piso (primeiro rejeitado)
                    vc_final = (c.iloc[i][col_vol] if i < len(c)
                                else c.iloc[i - 1][col_vol])
                    if verbose:
                        print(f"  → Degrau de venda detectado: "
                              f"pv_prev={pv_prev:.4f}, vc_final={vc_final:.2f}")
                    return pv_prev, vc_final

            break

        last_i, last_j = i, j

        if   round(vc, 2) < round(vv, 2): i += 1
        elif round(vc, 2) > round(vv, 2): j += 1
        else:                              i += 1; j += 1

    if last_i is None or last_j is None:
        return None, None

    pc_last = c.iloc[last_i][col_preco]
    pv_last = v.iloc[last_j][col_preco]
    vc_last = c.iloc[last_i][col_vol]
    vv_last = v.iloc[last_j][col_vol]

    if verbose:
        print(f"  last: C:{last_i}(P={pc_last:.4f}, V={vc_last:.2f})  "
              f"V:{last_j}(P={pv_last:.4f}, V={vv_last:.2f})")

    if round(vc_last, 2) == round(vv_last, 2):
        return (pc_last + pv_last) / 2.0, vc_last

    if round(vc_last, 2) > round(vv_last, 2):
        return pc_last, vv_last

    if last_j > 0:
        return pv_last, vc_last
    else:
        i_next = last_i + 1
        if (i_next < len(c)
                and round(c.iloc[i_next][col_preco], 2) < round(pv_last, 2)):
            return pv_last, vc_last
        else:
            return pc_last, vc_last