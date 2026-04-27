import csv
import struct
import os

RECORD_FORMAT = "<q20s20s10s"
HEADER_FORMAT = "<I"


def convertir_registro_employee_a_binario(record: tuple) -> tuple:
    employee_id, first_name, last_name, hire_date = record

    return (
        employee_id,
        first_name.encode("utf-8")[:20].ljust(20, b"\x00"),
        last_name.encode("utf-8")[:20].ljust(20, b"\x00"),
        hire_date.encode("utf-8")[:10].ljust(10, b"\x00")
    )


def export_to_heap(csv_path: str, heap_path: str, page_size: int):
    record_struct = struct.Struct(RECORD_FORMAT)
    header_struct = struct.Struct(HEADER_FORMAT)

    record_size = record_struct.size
    records_per_page = (page_size - header_struct.size) // record_size
    if records_per_page <= 0:
        raise ValueError("El tamaño de página es muy pequeño para este tipo de registro.")

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

            # Saltar marcador de fin tipo PostgreSQL: \.
            if fila[0].strip() == r"\.":
                continue

            employee_id = int(fila[0].strip())
            first_name = fila[2].strip().encode("utf-8")[:20].ljust(20, b"\x00")
            last_name = fila[3].strip().encode("utf-8")[:20].ljust(20, b"\x00")
            hire_date = fila[5].strip().encode("utf-8")[:10].ljust(10, b"\x00")

            registro_binario = record_struct.pack(employee_id, first_name, last_name, hire_date)
            pagina_registros.append(registro_binario)

            if len(pagina_registros) == records_per_page:
                escribir_pagina(archivo_heap, pagina_registros)
                pagina_registros = []

        if pagina_registros:
            escribir_pagina(archivo_heap, pagina_registros)


def read_page(heap_path: str, page_id: int, page_size: int) -> list[tuple]:
    record_struct = struct.Struct(RECORD_FORMAT)
    header_struct = struct.Struct(HEADER_FORMAT)
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

            employee_id, first_name, last_name, hire_date = record_struct.unpack(bloque)

            first_name = first_name.rstrip(b"\x00").decode("utf-8")
            last_name = last_name.rstrip(b"\x00").decode("utf-8")
            hire_date = hire_date.rstrip(b"\x00").decode("utf-8")

            registros.append((employee_id, first_name, last_name, hire_date))

    return registros


def write_page(heap_path: str, page_id: int, records: list[tuple], page_size: int):
    record_struct = struct.Struct(RECORD_FORMAT)
    header_struct = struct.Struct(HEADER_FORMAT)

    record_size = record_struct.size
    max_records_per_page = (page_size - header_struct.size) // record_size

    if max_records_per_page <= 0:
        raise ValueError("El tamaño de página es muy pequeño para el tamaño del registro.")

    if len(records) > max_records_per_page:
        raise ValueError(
            f"La página solo soporta {max_records_per_page} registros, pero se recibieron {len(records)}."
        )

    page_data = bytearray()
    page_data.extend(header_struct.pack(len(records)))

    for record in records:
        packed_record = record_struct.pack(*record)
        page_data.extend(packed_record)

    remaining_bytes = page_size - len(page_data)
    if remaining_bytes < 0:
        raise ValueError("La página construida excede el tamaño page_size.")

    page_data.extend(b"\x00" * remaining_bytes)
    offset = page_id * page_size

    with open(heap_path, "r+b") as heap_file:
        heap_file.seek(offset)
        heap_file.write(page_data)


def count_pages(heap_path: str, page_size: int) -> int:
    if page_size <= 0:
        raise ValueError("page_size debe ser mayor que 0.")

    file_size = os.path.getsize(heap_path)

    if file_size % page_size != 0:
        raise ValueError("El heap file está corrupto o page_size no coincide con su estructura.")
    return file_size // page_size