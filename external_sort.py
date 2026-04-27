import os
import heapq
import time
import math
import struct

from heap_file import (
    RECORD_FORMAT,
    HEADER_FORMAT,
    convertir_registro_employee_a_binario,
    read_page,
    write_page,
    count_pages
)


def generate_runs(heap_path: str, page_size: int, buffer_size: int, sort_key: str) -> list[str]:
    if buffer_size < page_size:
        raise ValueError("buffer_size debe ser al menos igual a page_size.")

    B = buffer_size // page_size
    if B <= 0:
        raise ValueError("No cabe ni una página en RAM.")

    if sort_key != "hire_date":
        raise ValueError("Esta implementación solo soporta sort_key = 'hire_date'.")

    total_pages = count_pages(heap_path, page_size)
    if total_pages == 0:
        return []

    total_runs = math.ceil(total_pages / B)
    run_paths = []

    record_struct = struct.Struct(RECORD_FORMAT)
    header_struct = struct.Struct(HEADER_FORMAT)
    records_per_page = (page_size - header_struct.size) // record_struct.size

    base_name, _ = os.path.splitext(heap_path)

    for run_id in range(total_runs):
        start_page = run_id * B
        end_page = min(start_page + B, total_pages)

        registros_en_memoria = []
        for page_id in range(start_page, end_page):
            registros_pagina = read_page(heap_path, page_id, page_size)
            registros_en_memoria.extend(registros_pagina)

        registros_en_memoria.sort(key=lambda r: r[3])

        run_path = f"{base_name}_run_{run_id}.heap"
        open(run_path, "wb").close()
        run_paths.append(run_path)

        pagina_actual = []
        run_page_id = 0

        for record in registros_en_memoria:
            record_binario = convertir_registro_employee_a_binario(record)
            pagina_actual.append(record_binario)

            if len(pagina_actual) == records_per_page:
                write_page(run_path, run_page_id, pagina_actual, page_size)
                run_page_id += 1
                pagina_actual = []

        if pagina_actual:
            write_page(run_path, run_page_id, pagina_actual, page_size)

    return run_paths


def obtener_clave_sort(record: tuple, sort_key: str):
    if sort_key == "hire_date":
        return record[3]
    raise ValueError("Esta implementación solo soporta sort_key = 'hire_date'.")


def multiway_merge(run_paths: list[str], output_path: str, page_size: int, buffer_size: int, sort_key: str):
    if buffer_size < 2 * page_size:
        raise ValueError("buffer_size debe permitir al menos 1 buffer de entrada y 1 de salida.")

    B = buffer_size // page_size
    k = B - 1

    if k <= 0:
        raise ValueError("No hay suficientes buffers de entrada.")

    if len(run_paths) == 0:
        open(output_path, "wb").close()
        return {
            "pages_read": 0,
            "pages_written": 0
        }

    if len(run_paths) > k:
        raise ValueError(
            f"Esta llamada solo puede mezclar hasta {k} runs a la vez, pero recibió {len(run_paths)}."
        )

    record_struct = struct.Struct(RECORD_FORMAT)
    header_struct = struct.Struct(HEADER_FORMAT)
    records_per_page = (page_size - header_struct.size) // record_struct.size

    open(output_path, "wb").close()

    pages_read = 0
    pages_written = 0
    run_states = []

    for run_path in run_paths:
        total_pages = count_pages(run_path, page_size)
        estado = {
            "path": run_path,
            "current_page_id": 0,
            "total_pages": total_pages,
            "buffer_records": [],
            "buffer_pos": 0
        }

        if total_pages > 0:
            estado["buffer_records"] = read_page(run_path, 0, page_size)
            estado["buffer_pos"] = 0
            estado["current_page_id"] = 1
            pages_read += 1

        run_states.append(estado)

    min_heap = []

    for run_idx, estado in enumerate(run_states):
        if estado["buffer_pos"] < len(estado["buffer_records"]):
            record = estado["buffer_records"][estado["buffer_pos"]]
            estado["buffer_pos"] += 1
            heapq.heappush(min_heap, (obtener_clave_sort(record, sort_key), run_idx, record))

    output_buffer = []
    output_page_id = 0

    while min_heap:
        _, run_idx, record = heapq.heappop(min_heap)
        output_buffer.append(convertir_registro_employee_a_binario(record))

        if len(output_buffer) == records_per_page:
            write_page(output_path, output_page_id, output_buffer, page_size)
            output_page_id += 1
            pages_written += 1
            output_buffer = []

        estado = run_states[run_idx]

        if estado["buffer_pos"] >= len(estado["buffer_records"]):
            if estado["current_page_id"] < estado["total_pages"]:
                estado["buffer_records"] = read_page(estado["path"], estado["current_page_id"], page_size)
                estado["buffer_pos"] = 0
                estado["current_page_id"] += 1
                pages_read += 1
            else:
                estado["buffer_records"] = []
                estado["buffer_pos"] = 0

        if estado["buffer_pos"] < len(estado["buffer_records"]):
            next_record = estado["buffer_records"][estado["buffer_pos"]]
            estado["buffer_pos"] += 1
            heapq.heappush(min_heap, (obtener_clave_sort(next_record, sort_key), run_idx, next_record))

    if output_buffer:
        write_page(output_path, output_page_id, output_buffer, page_size)
        pages_written += 1

    return {
        "pages_read": pages_read,
        "pages_written": pages_written
    }


def external_sort(heap_path: str, output_path: str, page_size: int, buffer_size: int, sort_key: str) -> dict:
    if sort_key != "hire_date":
        raise ValueError("Esta implementación solo soporta sort_key = 'hire_date'.")

    inicio_total = time.time()

    inicio_phase1 = time.time()
    run_paths = generate_runs(heap_path, page_size, buffer_size, sort_key)
    fin_phase1 = time.time()

    runs_generated = len(run_paths)

    total_pages_heap = count_pages(heap_path, page_size) if os.path.exists(heap_path) and os.path.getsize(heap_path) > 0 else 0
    pages_read_phase1 = total_pages_heap

    pages_written_phase1 = 0
    for run_path in run_paths:
        pages_written_phase1 += count_pages(run_path, page_size)

    inicio_phase2 = time.time()

    B = buffer_size // page_size
    k = B - 1
    if k <= 0:
        raise ValueError("No hay suficientes buffers para la Fase 2.")

    current_runs = run_paths[:]
    merge_round = 0
    pages_read_phase2 = 0
    pages_written_phase2 = 0

    if len(current_runs) == 0:
        open(output_path, "wb").close()

    elif len(current_runs) == 1:
        with open(current_runs[0], "rb") as src, open(output_path, "wb") as dst:
            while True:
                bloque = src.read(page_size)
                if not bloque:
                    break
                dst.write(bloque)

        pages_read_phase2 += count_pages(current_runs[0], page_size)
        pages_written_phase2 += count_pages(output_path, page_size)

    else:
        while len(current_runs) > 1:
            nuevos_runs = []

            for i in range(0, len(current_runs), k):
                grupo = current_runs[i:i + k]

                if len(current_runs) <= k and i == 0 and len(grupo) == len(current_runs):
                    merged_output = output_path
                else:
                    merged_output = f"merge_round_{merge_round}_group_{i // k}.heap"

                metricas_merge = multiway_merge(grupo, merged_output, page_size, buffer_size, sort_key)
                pages_read_phase2 += metricas_merge["pages_read"]
                pages_written_phase2 += metricas_merge["pages_written"]
                nuevos_runs.append(merged_output)

            current_runs = nuevos_runs
            merge_round += 1

    fin_phase2 = time.time()
    fin_total = time.time()

    return {
        "runs_generated": runs_generated,
        "pages_read": pages_read_phase1 + pages_read_phase2,
        "pages_written": pages_written_phase1 + pages_written_phase2,
        "time_phase1_sec": fin_phase1 - inicio_phase1,
        "time_phase2_sec": fin_phase2 - inicio_phase2,
        "time_total_sec": fin_total - inicio_total
    }