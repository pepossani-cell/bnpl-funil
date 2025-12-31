"""
Runner simples para executar arquivos .sql no Snowflake usando o connector do projeto.

Motivação:
  - Evitar problemas de quoting no PowerShell com `python -c` e SQL longo.
  - Manter execução "Snowflake-first" (Worksheet-like), mas via script.

Uso:
  python src/run_sql_file.py --file queries/audit/audit_pre_analyses.sql --max-statements 8
  python src/run_sql_file.py --file queries/audit/audit_pre_analyses.sql --set months_back=12

Observações:
  - Suporta múltiplos statements separados por ';' (com parser simples que respeita aspas).
  - Para statements sem result set (SET/DDL/DML), imprime apenas "OK".
"""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
from snowflake.connector.errors import NotSupportedError
import sys

from src.utils.snowflake_connection import get_snowflake_connection


@dataclass(frozen=True)
class Statement:
    idx: int
    sql: str


def _split_sql_statements(sql_text: str) -> List[str]:
    """
    Split por ';' no nível top (não dentro de strings).
    Parser simples, suficiente para scripts do repositório.
    """
    out: List[str] = []
    buf: List[str] = []

    in_single = False
    in_double = False
    in_line_comment = False
    in_block_comment = False
    i = 0
    while i < len(sql_text):
        ch = sql_text[i]
        nxt = sql_text[i + 1] if i + 1 < len(sql_text) else ""

        # Comentário de linha: -- até \n
        if in_line_comment:
            buf.append(ch)
            if ch == "\n":
                in_line_comment = False
            i += 1
            continue

        # Comentário de bloco: /* ... */
        if in_block_comment:
            buf.append(ch)
            if ch == "*" and nxt == "/":
                buf.append(nxt)
                in_block_comment = False
                i += 2
                continue
            i += 1
            continue

        # Início de comentários (apenas fora de strings)
        if not in_single and not in_double:
            if ch == "-" and nxt == "-":
                buf.append(ch)
                buf.append(nxt)
                in_line_comment = True
                i += 2
                continue
            if ch == "/" and nxt == "*":
                buf.append(ch)
                buf.append(nxt)
                in_block_comment = True
                i += 2
                continue

        # Toggle aspas simples (ignora '' como escape)
        if ch == "'" and not in_double:
            if in_single and nxt == "'":
                buf.append(ch)
                buf.append(nxt)
                i += 2
                continue
            in_single = not in_single
            buf.append(ch)
            i += 1
            continue

        # Toggle aspas duplas (ignora "" como escape)
        if ch == '"' and not in_single:
            if in_double and nxt == '"':
                buf.append(ch)
                buf.append(nxt)
                i += 2
                continue
            in_double = not in_double
            buf.append(ch)
            i += 1
            continue

        # Separador de statement
        if ch == ";" and not in_single and not in_double:
            stmt = "".join(buf).strip()
            if stmt:
                out.append(stmt)
            buf = []
            i += 1
            continue

        buf.append(ch)
        i += 1

    last = "".join(buf).strip()
    if last:
        out.append(last)
    return out


def _apply_sets(sets: Iterable[str]) -> List[str]:
    stmts: List[str] = []
    for item in sets:
        if "=" not in item:
            raise ValueError(f"--set inválido: {item!r} (use NAME=VALUE)")
        name, value = item.split("=", 1)
        name = name.strip()
        value = value.strip()
        # Sempre tratar VALUE como string (o usuário pode passar número sem aspas; Snowflake aceita string e faz cast em TO_NUMBER)
        stmts.append(f"SET {name} = '{value}';")
    return stmts


def _print_df(df: pd.DataFrame, max_rows: int) -> None:
    if df.empty:
        print("(result set vazio)")
        return
    if len(df) <= max_rows:
        print(df.to_string(index=False))
        return
    print(df.head(max_rows).to_string(index=False))
    print(f"... ({len(df)} linhas no total; mostrando {max_rows})")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--file", required=True, help="Caminho para o arquivo .sql")
    parser.add_argument("--max-statements", type=int, default=None, help="Executa no máximo N statements (ordem do arquivo)")
    parser.add_argument("--start-at", type=int, default=1, help="Executa a partir do statement N (1-based)")
    parser.add_argument("--set", action="append", default=[], help="Override de variáveis de sessão: NAME=VALUE (pode repetir)")
    parser.add_argument("--print-sql", action="store_true", help="Imprime o SQL de cada statement antes de executar")
    parser.add_argument("--max-rows", type=int, default=40, help="Máximo de linhas para imprimir por result set")
    args = parser.parse_args()

    # Windows/PowerShell às vezes usa cp1252 e quebra com Unicode.
    # Deixamos a saída resiliente para não interromper execuções longas.
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass

    sql_path = Path(args.file)
    sql_text = sql_path.read_text(encoding="utf-8")
    statements = _split_sql_statements(sql_text)

    start_at = max(1, int(args.start_at))
    end_at_exclusive: Optional[int] = None
    if args.max_statements is not None:
        end_at_exclusive = start_at - 1 + int(args.max_statements)

    # Overrides via SET antes de tudo
    override_stmts = _apply_sets(args.set)

    conn = get_snowflake_connection()
    if conn is None:
        return 2

    try:
        cur = conn.cursor()

        # Aplica overrides
        for s in override_stmts:
            cur.execute(s)

        for idx0, stmt in enumerate(statements, start=1):
            if idx0 < start_at:
                continue
            if end_at_exclusive is not None and idx0 > end_at_exclusive:
                break

            sql_stmt = stmt.strip()
            if not sql_stmt:
                continue

            print("\n" + "=" * 90)
            print(f"[statement {idx0}/{len(statements)}] {sql_path}")
            if args.print_sql:
                print(sql_stmt)

            cur.execute(sql_stmt)
            if cur.description is None:
                print("OK (sem result set)")
                continue

            try:
                df = cur.fetch_pandas_all()
            except NotSupportedError:
                rows = cur.fetchall()
                cols = [d[0] for d in (cur.description or [])]
                df = pd.DataFrame(rows, columns=cols)
            _print_df(df, max_rows=args.max_rows)

        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())


