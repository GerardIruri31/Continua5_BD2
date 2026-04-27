
import struct
import os
import csv


def export_department_employee_to_heap(csv_path: str, heap_path: str, page_size: int):
    record_struct = struct.Struct("<q4s10s10s")
    header_struct = struct.Struct("<I")

    record_size = record_struct.size
    records_per_page = (page_size - header_struct.size) // record_size

    if records_per_page <= 0:
        raise ValueError("El tamaño de página es muy pequeño para department_employee.")

    pagina_registros = []

    def escribir_pagina(archivo_heap, registros):
        contenido = bytearray()
        contenido.extend(header_struct.pack(len(registros)))
        for reg in registros:
            contenido.extend(reg)
        contenido.extend(b"\x00" * (page_size - len(contenido)))
        archivo_heap.write(contenido)

    with open(csv_path, "r", encoding="utf-8", newline="") as archivo_csv, \
         open(heap_path, "wb") as archivo_heap:

        lector = csv.reader(archivo_csv)
        next(lector, None)

        for fila in lector:
            if not fila:
                continue
            fila = [campo.strip() for campo in fila]
            if fila[0] == r"\.":
                continue
            if len(fila) < 4:
                continue

            employee_id = int(fila[0])
            department_id = fila[1].encode("utf-8")[:4].ljust(4, b"\x00")
            from_date = fila[2].encode("utf-8")[:10].ljust(10, b"\x00")
            to_date = fila[3].encode("utf-8")[:10].ljust(10, b"\x00")

            registro_binario = record_struct.pack(
                employee_id,
                department_id,
                from_date,
                to_date
            )
            pagina_registros.append(registro_binario)

            if len(pagina_registros) == records_per_page:
                escribir_pagina(archivo_heap, pagina_registros)
                pagina_registros = []

        if pagina_registros:
            escribir_pagina(archivo_heap, pagina_registros)



def count_pages(heap_path: str, page_size: int) -> int:
    if page_size <= 0:
        raise ValueError("page_size debe ser mayor que 0.")

    file_size = os.path.getsize(heap_path)

    if file_size % page_size != 0:
        raise ValueError("El heap file está corrupto o page_size no coincide con su estructura.")

    return file_size // page_size


def read_page_department_employee(heap_path: str, page_id: int, page_size: int) -> list[tuple]:
    record_struct = struct.Struct("<q4s10s10s")
    header_struct = struct.Struct("<I")
    registros = []

    with open(heap_path, "rb") as archivo_heap:
        offset = page_id * page_size
        archivo_heap.seek(offset)
        pagina = archivo_heap.read(page_size)

        if len(pagina) < page_size:
            return []

        cantidad_registros = header_struct.unpack(pagina[:4])[0]
        inicio_datos = header_struct.size
        tam_registro = record_struct.size

        for i in range(cantidad_registros):
            inicio = inicio_datos + i * tam_registro
            fin = inicio + tam_registro
            bloque = pagina[inicio:fin]

            emp_no, dept_no, from_date, to_date = record_struct.unpack(bloque)

            dept_no = dept_no.rstrip(b"\x00").decode("utf-8")
            from_date = from_date.rstrip(b"\x00").decode("utf-8")
            to_date = to_date.rstrip(b"\x00").decode("utf-8")

            registros.append((emp_no, dept_no, from_date, to_date))

    return registros


def convertir_registro_department_employee_a_binario(record: tuple) -> tuple:
    emp_no, dept_no, from_date, to_date = record

    return (
        emp_no,
        dept_no.encode("utf-8")[:4].ljust(4, b"\x00"),
        from_date.encode("utf-8")[:10].ljust(10, b"\x00"),
        to_date.encode("utf-8")[:10].ljust(10, b"\x00")
    )


def write_page_department_employee(heap_path: str, page_id: int, records: list[tuple], page_size: int):
    record_struct = struct.Struct("<q4s10s10s")
    header_struct = struct.Struct("<I")

    record_size = record_struct.size
    max_records_per_page = (page_size - header_struct.size) // record_size

    if max_records_per_page <= 0:
        raise ValueError("El tamaño de página es muy pequeño para este tipo de registro.")

    if len(records) > max_records_per_page:
        raise ValueError(
            f"La página solo soporta {max_records_per_page} registros, pero se recibieron {len(records)}."
        )

    page_data = bytearray()
    page_data.extend(header_struct.pack(len(records)))

    for record in records:
        packed_record = record_struct.pack(*record)
        page_data.extend(packed_record)

    faltante = page_size - len(page_data)
    if faltante < 0:
        raise ValueError("La página construida excede el tamaño page_size.")

    page_data.extend(b"\x00" * faltante)

    offset = page_id * page_size

    with open(heap_path, "r+b") as heap_file:
        heap_file.seek(offset)
        heap_file.write(page_data)


def hash_particion(valor: str) -> int:
    h = 0
    for c in valor:
        h = (h * 31 + ord(c)) & 0xFFFFFFFF
    return h


def partition_data(heap_path: str, page_size: int, buffer_size: int, group_key: str) -> list[str]:
    if group_key != "from_date":
        raise ValueError("Esta implementación solo soporta group_key = 'from_date'.")
    if buffer_size < 2 * page_size:
        raise ValueError("buffer_size debe permitir al menos 1 buffer de entrada y 1 de salida.")
    B = buffer_size // page_size
    k = B - 1
    if k <= 0:
        raise ValueError("No hay suficientes buffers para particionar.")
    total_pages = count_pages(heap_path, page_size)
    if total_pages == 0:
        return []
    record_struct = struct.Struct("<q4s10s10s")
    header_struct = struct.Struct("<I")
    records_per_page = (page_size - header_struct.size) // record_struct.size

    base_name, _ = os.path.splitext(heap_path)

    partition_paths = []
    for i in range(k):
        partition_path = f"{base_name}_part_{i}.heap"
        open(partition_path, "wb").close()
        partition_paths.append(partition_path)

    partition_buffers = [[] for _ in range(k)]
    partition_page_ids = [0 for _ in range(k)]

    for page_id in range(total_pages):
        registros = read_page_department_employee(heap_path, page_id, page_size)

        for record in registros:
            emp_no, dept_no, from_date, to_date = record

            partition_id = hash_particion(from_date) % k

            record_binario = convertir_registro_department_employee_a_binario(record)
            partition_buffers[partition_id].append(record_binario)

            if len(partition_buffers[partition_id]) == records_per_page:
                write_page_department_employee(
                    partition_paths[partition_id],
                    partition_page_ids[partition_id],
                    partition_buffers[partition_id],
                    page_size
                )
                partition_page_ids[partition_id] += 1
                partition_buffers[partition_id] = []

    for partition_id in range(k):
        if partition_buffers[partition_id]:
            write_page_department_employee(
                partition_paths[partition_id],
                partition_page_ids[partition_id],
                partition_buffers[partition_id],
                page_size
            )
    return partition_paths


def aggregate_partitions(partition_paths: list[str], page_size: int, buffer_size: int, group_key: str) -> dict:
    if group_key != "from_date":
        raise ValueError("Esta implementación solo soporta group_key = 'from_date'.")

    resultado_global = {}

    for partition_path in partition_paths:
        total_pages = count_pages(partition_path, page_size)

        # Verificar que la partición quepa en RAM
        tam_particion_bytes = total_pages * page_size
        if tam_particion_bytes > buffer_size:
            raise ValueError(
                f"La partición {partition_path} no cabe en RAM durante la Fase 2."
            )

        tabla_hash = {}

        for page_id in range(total_pages):
            registros = read_page_department_employee(partition_path, page_id, page_size)

            for record in registros:
                emp_no, dept_no, from_date, to_date = record

                if from_date not in tabla_hash:
                    tabla_hash[from_date] = 0

                tabla_hash[from_date] += 1

        # Combinar resultados de esta partición en el resultado global
        for clave_grupo, count in tabla_hash.items():
            if clave_grupo not in resultado_global:
                resultado_global[clave_grupo] = 0

            resultado_global[clave_grupo] += count

    return resultado_global


import time
import os


def external_hash_group_by(heap_path: str, page_size: int, buffer_size: int, group_key: str) -> dict:
    if group_key != "from_date":
        raise ValueError("Esta implementación solo soporta group_key = 'from_date'.")

    inicio_total = time.time()

    # -------------------------
    # FASE 1: PARTICIONAMIENTO
    # -------------------------
    inicio_phase1 = time.time()
    partition_paths = partition_data(heap_path, page_size, buffer_size, group_key)
    fin_phase1 = time.time()

    partitions_created = len(partition_paths)

    # Páginas leídas en Fase 1:
    # se lee una vez todo el heap original
    pages_read_phase1 = 0
    if os.path.exists(heap_path) and os.path.getsize(heap_path) > 0:
        pages_read_phase1 = count_pages(heap_path, page_size)

    # Páginas escritas en Fase 1:
    # suma de páginas de todas las particiones creadas
    pages_written_phase1 = 0
    for partition_path in partition_paths:
        if os.path.exists(partition_path) and os.path.getsize(partition_path) > 0:
            pages_written_phase1 += count_pages(partition_path, page_size)

    # -------------------------
    # FASE 2: AGREGACIÓN
    # -------------------------
    inicio_phase2 = time.time()
    result = aggregate_partitions(partition_paths, page_size, buffer_size, group_key)
    fin_phase2 = time.time()

    # Páginas leídas en Fase 2:
    # se recorre una vez cada partición
    pages_read_phase2 = 0
    for partition_path in partition_paths:
        if os.path.exists(partition_path) and os.path.getsize(partition_path) > 0:
            pages_read_phase2 += count_pages(partition_path, page_size)

    # Páginas escritas en Fase 2:
    # en esta implementación no se escribe archivo de salida intermedio/final,
    # solo se retorna el diccionario en memoria
    pages_written_phase2 = 0

    fin_total = time.time()

    return {
        "result": result,
        "partitions_created": partitions_created,
        "pages_read": pages_read_phase1 + pages_read_phase2,
        "pages_written": pages_written_phase1 + pages_written_phase2,
        "time_phase1_sec": fin_phase1 - inicio_phase1,
        "time_phase2_sec": fin_phase2 - inicio_phase2,
        "time_total_sec": fin_total - inicio_total
    }