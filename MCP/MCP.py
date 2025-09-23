import os
import subprocess
import requests
import shutil
from fastmcp import FastMCP
import logging

# Configuração global do log
logging.basicConfig(
    level=logging.INFO,  # Pode trocar para DEBUG se quiser mais detalhe
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",  # Formato do timestamp
)

logger = logging.getLogger(__name__)

# Nome da subpasta onde todos os arquivos serão criados
SUBPASTA = "C:\\Users\\jmelo\\Documents\\GitHub\\mibel-datalab"

# Cria a subpasta se não existir
os.makedirs(SUBPASTA, exist_ok=True)

# Inicializa o servidor MCP
mcp = FastMCP("Filesystem MCP Server")


def caminho_final(filename: str) -> str:
    """Garante que o arquivo será criado dentro da subpasta base, respeitando subdiretórios"""
    # Remove possíveis caminhos absolutos para evitar sair da subpasta
    safe_path = os.path.normpath(filename).lstrip(os.sep)

    # Combina com a subpasta base
    final_path = os.path.join(SUBPASTA, safe_path)

    # Cria subpastas se não existirem
    os.makedirs(os.path.dirname(final_path), exist_ok=True)

    return final_path


@mcp.tool
def create_file(path, content) -> str:
    path = str(path)
    content = str(content)
    final_path = caminho_final(path)
    with open(final_path, "w", encoding="utf-8") as f:
        f.write(content)
    return f"Arquivo criado em {final_path}"


@mcp.tool
def replace_in_file(path: str, old: str, new: str) -> str:
    """
    Substitui todas as ocorrências de um texto em um ficheiro.

    Args:
        path: caminho relativo ao projeto
        old: texto a substituir
        new: novo texto que vai entrar no lugar

    Returns:
        Mensagem indicando sucesso
    """
    final_path = caminho_final(path)

    if not os.path.exists(final_path):
        return f"Arquivo {final_path} não encontrado."

    with open(final_path, "r", encoding="utf-8") as f:
        content = f.read()

    if old not in content:
        return f"O texto '{old}' não foi encontrado em {final_path}."

    updated = content.replace(old, new)

    with open(final_path, "w", encoding="utf-8") as f:
        f.write(updated)

    return f"Texto '{old}' substituído por '{new}' em {final_path}"


@mcp.tool
def read_file(path) -> str:
    path = str(path)
    final_path = caminho_final(path)
    if not os.path.exists(final_path):
        return "Arquivo não encontrado"
    with open(final_path, "r", encoding="utf-8") as f:
        return f.read()


@mcp.tool
def read_file_chunks(path, chunk_size: int = 1048576) -> str:
    """
    Lê um arquivo em blocos de tamanho `chunk_size` (bytes).  
    Útil para arquivos muito grandes que não cabem na memória.

    Args:
        path: caminho relativo ao projeto.
        chunk_size: tamanho do bloco a ser lido. O valor padrão é 1 MiB.

    Returns:
        Uma string contendo todos os blocos concatenados, separados por
        um delimitador visível para facilitar a visualização.
    """
    final_path = caminho_final(str(path))
    if not os.path.exists(final_path):
        return "Arquivo não encontrado"

    chunks = []
    try:
        with open(final_path, "r", encoding="utf-8", errors="ignore") as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                chunks.append(data)
    except Exception as e:
        return f"Erro ao ler o arquivo em blocos: {e}"

    # O separador ajuda a distinguir onde termina um bloco e começa outro.
    separator = "\n--- CHUNK SEPARATOR ({} bytes) ---\n".format(chunk_size)
    return separator.join(chunks)


@mcp.tool
def read_file_lines_range(path, start_line: int = 1, end_line: int | None = None) -> str:
    """
    Lê apenas um intervalo de linhas de um arquivo.

    Args:
        path: caminho relativo ao projeto.
        start_line: primeira linha a ser lida (inclusiva). Por padrão é 1.
        end_line: última linha a ser lida (inclusiva). Se None, lê até o fim do arquivo.

    Returns:
        As linhas solicitadas concatenadas em uma única string.  
        Se o intervalo for inválido ou inexistente, retorna mensagem de erro.
    """
    final_path = caminho_final(str(path))
    if not os.path.exists(final_path):
        return "Arquivo não encontrado"

    lines = []
    try:
        with open(final_path, "r", encoding="utf-8", errors="ignore") as f:
            for idx, line in enumerate(f, start=1):
                if idx < start_line:
                    continue
                if end_line is not None and idx > end_line:
                    break
                lines.append(line.rstrip("\n"))
    except Exception as e:
        return f"Erro ao ler linhas: {e}"

    if not lines:
        return f"Nenhuma linha encontrada no intervalo [{start_line}:{end_line}]"

    return "\n".join(lines)


@mcp.tool
def list_files(path: str = "") -> list:
    """
    Lista arquivos e pastas dentro de SUBPASTA/path
    """
    safe_path = os.path.normpath(os.path.join(SUBPASTA, path))

    if not os.path.exists(safe_path):
        return [f"Pasta {safe_path} não encontrada"]

    return os.listdir(safe_path)


@mcp.tool
def delete_file(path: str) -> str:
    """
    Apaga um arquivo ou pasta dentro da subpasta.
    """
    final_path = caminho_final(path)

    if not os.path.exists(final_path):
        return "Arquivo ou pasta não encontrado."

    try:
        if os.path.isfile(final_path):
            os.remove(final_path)
            return f"Arquivo {final_path} removido."
        elif os.path.isdir(final_path):
            shutil.rmtree(final_path)
            return f"Pasta {final_path} removida."
    except Exception as e:
        return f"Erro ao remover {final_path}: {e}"


@mcp.tool
def create_folder(folder_name: str) -> str:
    """
    Cria uma nova pasta dentro de SUBPASTA
    """
    folder_path = os.path.join(SUBPASTA, folder_name)
    os.makedirs(folder_path, exist_ok=True)
    return f"Pasta criada em {folder_path}"


@mcp.tool
def list_structure(path: str = "") -> dict:
    """
    Retorna a estrutura completa de arquivos e pastas a partir de SUBPASTA/path
    """
    # Normaliza o caminho (aceita "/" ou "\")
    safe_path = os.path.normpath(os.path.join(SUBPASTA, path))

    if not os.path.exists(safe_path):
        return {"error": f"Pasta {safe_path} não encontrada."}

    structure = {}
    for root, dirs, files in os.walk(safe_path):
        rel_root = os.path.relpath(root, SUBPASTA)
        structure[rel_root] = {
            "folders": dirs,
            "files": files
        }
    return structure


@mcp.tool
def search_files(query: str, search_content: bool = False) -> list:
    """
    Pesquisa por arquivos no projeto.

    Args:
        query: termo a procurar (nome do ficheiro ou conteúdo)
        search_content: se True, também pesquisa dentro do conteúdo dos arquivos

    Returns:
        Lista de caminhos encontrados
    """
    resultados = []

    for root, _, files in os.walk(SUBPASTA):
        for f in files:
            file_path = os.path.join(root, f)
            rel_path = os.path.relpath(file_path, SUBPASTA)

            # Pesquisa no nome do arquivo
            if query.lower() in f.lower():
                resultados.append(rel_path)
                continue

            # Pesquisa no conteúdo (se ativado)
            if search_content:
                try:
                    with open(file_path, "r", encoding="utf-8") as arq:
                        if query.lower() in arq.read().lower():
                            resultados.append(rel_path)
                except Exception:
                    # Ignora arquivos binários ou ilegíveis
                    pass

    return resultados if resultados else [f"Nada encontrado para '{query}'"]


@mcp.tool
def move_file(src: str, dest: str) -> str:
    """
    Move ou renomeia um arquivo/pasta dentro do projeto.

    Args:
        src: caminho relativo do ficheiro/pasta de origem
        dest: caminho relativo do destino

    Returns:
        Mensagem de sucesso ou erro
    """
    final_src = caminho_final(src)
    final_dest = caminho_final(dest)

    if not os.path.exists(final_src):
        return f"Origem {final_src} não encontrada."

    os.makedirs(os.path.dirname(final_dest), exist_ok=True)

    try:
        shutil.move(final_src, final_dest)
        return f"Movido de {final_src} para {final_dest}"
    except Exception as e:
        return f"Erro ao mover: {e}"


@mcp.tool
def copy_file(src: str, dest: str) -> str:
    """
    Copia um arquivo ou pasta dentro do projeto.

    Args:
        src: caminho relativo do arquivo/pasta de origem
        dest: caminho relativo do destino

    Returns:
        Mensagem de sucesso ou erro
    """
    final_src = caminho_final(src)
    final_dest = caminho_final(dest)

    if not os.path.exists(final_src):
        return f"Origem {final_src} não encontrada."

    try:
        os.makedirs(os.path.dirname(final_dest), exist_ok=True)

        if os.path.isdir(final_src):
            # Copia diretório recursivamente
            if os.path.exists(final_dest):
                return f"Destino {final_dest} já existe (pasta)."
            shutil.copytree(final_src, final_dest)
            return f"Pasta copiada de {final_src} para {final_dest}"
        else:
            # Copia ficheiro
            shutil.copy2(final_src, final_dest)
            return f"Arquivo copiado de {final_src} para {final_dest}"
    except Exception as e:
        return f"Erro ao copiar: {e}"


@mcp.tool
def search_in_file_or_dir(path: str, query: str) -> str:
    """
    Procura por uma string dentro de um ficheiro ou em todos os ficheiros de uma pasta na SUBPASTA.

    Args:
        path: caminho relativo ao ficheiro ou pasta dentro de SUBPASTA.
        query: texto a procurar.

    Returns:
        Lista das ocorrências encontradas ou mensagem se não for encontrada.
    """
    base_path = os.path.normpath(os.path.join(SUBPASTA, path))

    if not os.path.exists(base_path):
        return f"Caminho não encontrado: {base_path}"

    results = []

    def search_in_file(file_path: str):
        local_results = []
        try:
            with open(file_path, "r", encoding="utf-8", errors="ignore") as f:
                for i, line in enumerate(f, start=1):
                    if query in line:
                        local_results.append(f"{file_path} (linha {i}): {line.strip()}")
        except Exception as e:
            local_results.append(f"Erro ao ler {file_path}: {e}")
        return local_results

    if os.path.isfile(base_path):
        results.extend(search_in_file(base_path))
    else:
        for root, _, files in os.walk(base_path):
            for file in files:
                file_path = os.path.join(root, file)
                results.extend(search_in_file(file_path))

    if not results:
        return f"Nenhuma ocorrência de '{query}' encontrada em {path}."

    return "\n".join(results)


if __name__ == "__main__":
    mcp.run(transport="streamable-http", host="127.0.0.1", port=8052)
