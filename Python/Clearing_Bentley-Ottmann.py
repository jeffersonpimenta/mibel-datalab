"""
Algoritmo de Bentley-Ottmann para detecção de interseções entre curvas.

Dado dois vetores com pontos (xn, yn) de duas curvas, retorna todas as
coordenadas (x, y) de interseção entre os segmentos das curvas.
"""

import heapq
from typing import Optional
from dataclasses import dataclass, field


# ─────────────────────────────────────────
# Estruturas de dados
# ─────────────────────────────────────────

@dataclass
class Point:
    x: float
    y: float

    def __lt__(self, other: "Point") -> bool:
        return (self.x, self.y) < (other.x, other.y)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, Point):
            return False
        return abs(self.x - other.x) < 1e-9 and abs(self.y - other.y) < 1e-9

    def __hash__(self):
        return hash((round(self.x, 9), round(self.y, 9)))

    def __repr__(self):
        return f"({self.x:.6g}, {self.y:.6g})"


@dataclass(order=False)
class Segment:
    """Segmento de linha com identificador de qual curva pertence."""
    p: Point      # extremidade esquerda (menor x)
    q: Point      # extremidade direita  (maior x)
    curve_id: int # 0 = curva A, 1 = curva B
    seg_id: int   # índice do segmento dentro da curva

    def __post_init__(self):
        # Garante que p.x <= q.x
        if self.p.x > self.q.x or (self.p.x == self.q.x and self.p.y > self.q.y):
            self.p, self.q = self.q, self.p

    def y_at(self, x: float) -> float:
        """Calcula a coordenada y do segmento em uma dada posição x."""
        if abs(self.q.x - self.p.x) < 1e-12:
            return (self.p.y + self.q.y) / 2.0
        t = (x - self.p.x) / (self.q.x - self.p.x)
        return self.p.y + t * (self.q.y - self.p.y)

    def __repr__(self):
        return f"Seg(curve={self.curve_id}, id={self.seg_id}, {self.p}→{self.q})"


# ─────────────────────────────────────────
# Cálculo de interseção entre dois segmentos
# ─────────────────────────────────────────

def _cross2d(o: Point, a: Point, b: Point) -> float:
    return (a.x - o.x) * (b.y - o.y) - (a.y - o.y) * (b.x - o.x)


def segment_intersection(s1: Segment, s2: Segment) -> Optional[Point]:
    """
    Retorna o ponto de interseção entre dois segmentos, ou None se não há interseção.
    Usa o método de Cramer (solução do sistema linear paramétrico).
    """
    p, r = s1.p, Point(s1.q.x - s1.p.x, s1.q.y - s1.p.y)
    q, s = s2.p, Point(s2.q.x - s2.p.x, s2.q.y - s2.p.y)

    denom = r.x * s.y - r.y * s.x

    if abs(denom) < 1e-12:
        return None  # Paralelos ou colineares

    t = ((q.x - p.x) * s.y - (q.y - p.y) * s.x) / denom
    u = ((q.x - p.x) * r.y - (q.y - p.y) * r.x) / denom

    eps = 1e-9
    if -eps <= t <= 1 + eps and -eps <= u <= 1 + eps:
        ix = p.x + t * r.x
        iy = p.y + t * r.y
        return Point(ix, iy)

    return None


# ─────────────────────────────────────────
# Fila de eventos (Event Queue)
# ─────────────────────────────────────────

LEFT_ENDPOINT  = 0
RIGHT_ENDPOINT = 1
INTERSECTION   = 2

@dataclass(order=True)
class Event:
    x: float
    y: float
    kind: int                                      # LEFT, RIGHT ou INTERSECTION
    seg: Segment = field(compare=False)
    seg2: Optional[Segment] = field(default=None, compare=False)  # para interseção


# ─────────────────────────────────────────
# Status (linha de varredura) com lista ordenada simples
# ─────────────────────────────────────────

class SweepStatus:
    """
    Mantém os segmentos ativos ordenados por y na linha de varredura atual.
    Usa lista simples (eficiente para entradas de tamanho moderado).
    """

    def __init__(self):
        self._segments: list[Segment] = []
        self._sweep_x: float = 0.0

    def set_sweep_x(self, x: float):
        self._sweep_x = x

    def _key(self, seg: Segment) -> float:
        return seg.y_at(self._sweep_x)

    def insert(self, seg: Segment):
        y = self._key(seg)
        idx = 0
        while idx < len(self._segments) and self._key(self._segments[idx]) < y:
            idx += 1
        self._segments.insert(idx, seg)

    def remove(self, seg: Segment):
        try:
            self._segments.remove(seg)
        except ValueError:
            pass  # Já removido (evento duplicado)

    def predecessor(self, seg: Segment) -> Optional[Segment]:
        try:
            idx = self._segments.index(seg)
            return self._segments[idx - 1] if idx > 0 else None
        except ValueError:
            return None

    def successor(self, seg: Segment) -> Optional[Segment]:
        try:
            idx = self._segments.index(seg)
            return self._segments[idx + 1] if idx + 1 < len(self._segments) else None
        except ValueError:
            return None

    def swap(self, seg1: Segment, seg2: Segment):
        try:
            i = self._segments.index(seg1)
            j = self._segments.index(seg2)
            self._segments[i], self._segments[j] = self._segments[j], self._segments[i]
        except ValueError:
            pass


# ─────────────────────────────────────────
# Algoritmo principal de Bentley-Ottmann
# ─────────────────────────────────────────

def bentley_ottmann(
    x_a: list[float], y_a: list[float],
    x_b: list[float], y_b: list[float]
) -> list[tuple[float, float]]:
    """
    Encontra todas as interseções entre os segmentos da curva A e da curva B
    usando o algoritmo de Bentley-Ottmann.

    Parâmetros
    ----------
    x_a, y_a : coordenadas dos pontos da curva A (conectados em sequência)
    x_b, y_b : coordenadas dos pontos da curva B (conectados em sequência)

    Retorna
    -------
    Lista de tuplas (x, y) com as coordenadas de cada interseção encontrada.
    As coordenadas são ordenadas pelo eixo x.
    """
    if len(x_a) != len(y_a) or len(x_a) < 2:
        raise ValueError("Curva A precisa de ao menos 2 pontos com x e y de mesmo tamanho.")
    if len(x_b) != len(y_b) or len(x_b) < 2:
        raise ValueError("Curva B precisa de ao menos 2 pontos com x e y de mesmo tamanho.")

    # ── 1. Constrói os segmentos ──────────────────────────────────────────────
    segments: list[Segment] = []

    for i in range(len(x_a) - 1):
        p = Point(x_a[i], y_a[i])
        q = Point(x_a[i + 1], y_a[i + 1])
        if p != q:
            segments.append(Segment(p, q, curve_id=0, seg_id=i))

    for i in range(len(x_b) - 1):
        p = Point(x_b[i], y_b[i])
        q = Point(x_b[i + 1], y_b[i + 1])
        if p != q:
            segments.append(Segment(p, q, curve_id=1, seg_id=i))

    # ── 2. Popula a fila de eventos com os extremos ───────────────────────────
    event_heap: list[tuple] = []
    seen_intersections: set[frozenset] = set()

    def push_event(e: Event):
        heapq.heappush(event_heap, (e.x, e.y, e.kind, id(e.seg), e))

    for seg in segments:
        push_event(Event(seg.p.x, seg.p.y, LEFT_ENDPOINT,  seg))
        push_event(Event(seg.q.x, seg.q.y, RIGHT_ENDPOINT, seg))

    def maybe_add_intersection(s1: Segment, s2: Segment, sweep_x: float):
        """Adiciona evento de interseção se ainda não foi registrado."""
        key = frozenset([id(s1), id(s2)])
        if key in seen_intersections:
            return
        pt = segment_intersection(s1, s2)
        if pt is not None and pt.x >= sweep_x - 1e-9:
            seen_intersections.add(key)
            push_event(Event(pt.x, pt.y, INTERSECTION, s1, s2))

    # ── 3. Loop principal ─────────────────────────────────────────────────────
    status  = SweepStatus()
    results: list[Point] = []

    while event_heap:
        _, _, _, _, event = heapq.heappop(event_heap)

        sweep_x = event.x
        status.set_sweep_x(sweep_x)

        if event.kind == LEFT_ENDPOINT:
            seg = event.seg
            status.insert(seg)

            pred = status.predecessor(seg)
            succ = status.successor(seg)

            if pred is not None:
                maybe_add_intersection(pred, seg, sweep_x)
            if succ is not None:
                maybe_add_intersection(seg, succ, sweep_x)

        elif event.kind == RIGHT_ENDPOINT:
            seg = event.seg
            pred = status.predecessor(seg)
            succ = status.successor(seg)

            if pred is not None and succ is not None:
                maybe_add_intersection(pred, succ, sweep_x)

            status.remove(seg)

        else:  # INTERSECTION
            s1, s2 = event.seg, event.seg2

            # Registra apenas interseções entre curvas diferentes
            if s1.curve_id != s2.curve_id:
                pt = Point(event.x, event.y)
                if not any(pt == r for r in results):
                    results.append(pt)

            # Troca a ordem no status e verifica novos vizinhos
            status.set_sweep_x(sweep_x + 1e-9)
            status.swap(s1, s2)
            status.set_sweep_x(sweep_x)

            upper = status.predecessor(s2) if status.predecessor(s2) is not s1 else status.predecessor(s1)
            lower = status.successor(s1)   if status.successor(s1)   is not s2 else status.successor(s2)

            if upper is not None:
                maybe_add_intersection(upper, s2, sweep_x)
            if lower is not None:
                maybe_add_intersection(s1, lower, sweep_x)

    # ── 4. Ordenar e retornar ─────────────────────────────────────────────────
    results.sort(key=lambda p: (p.x, p.y))
    return [(p.x, p.y) for p in results]


# ─────────────────────────────────────────
# Exemplos de uso
# ─────────────────────────────────────────

def segments_to_xy(segments: list[dict]) -> tuple[list[float], list[float]]:
    """
    Converte a lista de segmentos (dicionários com x1, y1, x2, y2) em dois
    vetores de coordenadas x e y encadeados, prontos para bentley_ottmann.

    Cada segmento { x1, y1, x2, y2 } vira dois pontos consecutivos na lista.
    Segmentos adjacentes compartilham o ponto de conexão, então são deduplicados.
    """
    if not segments:
        return [], []

    xs: list[float] = [segments[0]["x1"], segments[0]["x2"]]
    ys: list[float] = [segments[0]["y1"], segments[0]["y2"]]

    for seg in segments[1:]:
        # O ponto inicial de cada segmento é o final do anterior — não duplicar
        xs.append(seg["x2"])
        ys.append(seg["y2"])

    return xs, ys


if __name__ == "__main__":
    import pandas as pd
    import matplotlib.pyplot as plt
    from io import StringIO

    def parse_number(series):
        return (
            series
            .astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False)
            .astype(float)
        )

    def read_bids_file(path):
        with open(path, "r", encoding="latin-1") as f:
            return f.read()

    def get_segments(df):
        """
        Transforma os pontos acumulados em segmentos horizontais e verticais.
        Simula o comportamento 'where=post'.
        """
        segments = []
        curr_vol = 0.0

        for i in range(len(df)):
            next_vol = df.iloc[i]["Volume_Acumulado"]
            price    = df.iloc[i]["Precio Compra/Venta"]

            # Segmento horizontal (patamar de preço)
            segments.append({"x1": curr_vol, "x2": next_vol, "y1": price, "y2": price, "type": "H"})

            # Segmento vertical (degrau para o próximo preço)
            if i < len(df) - 1:
                next_price = df.iloc[i + 1]["Precio Compra/Venta"]
                segments.append({"x1": next_vol, "x2": next_vol, "y1": price, "y2": next_price, "type": "V"})

            curr_vol = next_vol

        return segments

    # ── Leitura e preparação dos dados ────────────────────────────────────────
    bids_text = read_bids_file("C:\\Users\\jmelo\\Documents\\Python\\curva_pbc_20260203.1")

    df = pd.read_csv(StringIO(bids_text), sep=";", dtype=str)
    df.columns = [c.strip() for c in df.columns]

    df["Potencia Compra/Venta"] = parse_number(df["Potencia Compra/Venta"])
    df["Precio Compra/Venta"]   = parse_number(df["Precio Compra/Venta"])

    compras = df[(df["Periodo"] == "H12Q1") & (df["Tipo Oferta"] == "C") & (df["Pais"] == "ES")].copy()
    vendas  = df[(df["Periodo"] == "H12Q1") & (df["Tipo Oferta"] == "V") & (df["Pais"] == "ES")].copy()

    compras = compras.sort_values("Precio Compra/Venta", ascending=False)
    vendas  = vendas.sort_values("Precio Compra/Venta", ascending=True)

    compras["Volume_Acumulado"] = compras["Potencia Compra/Venta"].cumsum()
    vendas["Volume_Acumulado"]  = vendas["Potencia Compra/Venta"].cumsum()

    # ── Constrói segmentos e converte para vetores x/y ────────────────────────
    seg_compras = get_segments(compras)
    seg_vendas  = get_segments(vendas)

    x_compras, y_compras = segments_to_xy(seg_compras)
    x_vendas,  y_vendas  = segments_to_xy(seg_vendas)

    # ── Chama Bentley-Ottmann ─────────────────────────────────────────────────
    pts = bentley_ottmann(x_compras, y_compras, x_vendas, y_vendas)

    print(f"Interseções encontradas: {len(pts)}")
    for p in pts:
        print(f"  Volume = {p[0]:.2f} MW   Preço = {p[1]:.2f} €/MWh")

    # ── Plot das curvas e do ponto de equilíbrio ──────────────────────────────
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.step(x_compras, y_compras, where="post", color="blue",  label="Compras (Demanda)")
    ax.step(x_vendas,  y_vendas,  where="post", color="red",   label="Vendas (Oferta)")

    for px, py in pts:
        ax.scatter(px, py, color="green", zorder=5, s=80)
        ax.annotate(
            f"  ({px:.1f} MW, {py:.1f} €)",
            xy=(px, py), fontsize=9, color="green"
        )

    ax.set_xlabel("Volume Acumulado (MW)")
    ax.set_ylabel("Preço (€/MWh)")
    ax.set_title("Curvas de Oferta e Demanda — Ponto de Equilíbrio")
    ax.legend()
    ax.grid(True, linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.show()