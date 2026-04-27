
import os

from heap_file import export_to_heap, count_pages, read_page
from external_sort import external_sort
from external_hashing import (
    export_department_employee_to_heap,
    external_hash_group_by,
    read_page_department_employee
)

PAGE_SIZE = 4096
BUFFER_SIZE = 262144

EMPLOYEE_CSV = "employee.csv"
EMPLOYEE_BIN = "employee.bin"
EMPLOYEE_SORTED_BIN = "employee_sorted.bin"

DEPT_EMPLOYEE_CSV = "department_employee.csv"
DEPT_EMPLOYEE_BIN = "department_employee.bin"


def limpiar_archivos_generados():
    archivos_fijos = [
        EMPLOYEE_BIN,
        EMPLOYEE_SORTED_BIN,
        DEPT_EMPLOYEE_BIN,
    ]

    for archivo in archivos_fijos:
        if os.path.exists(archivo):
            os.remove(archivo)

    for nombre in os.listdir("."):
        if nombre.startswith("employee_run_") and nombre.endswith(".heap"):
            os.remove(nombre)
        elif nombre.startswith("employee_run_") and nombre.endswith(".bin"):
            os.remove(nombre)
        elif nombre.startswith("merge_round_") and nombre.endswith(".heap"):
            os.remove(nombre)
        elif nombre.startswith("merge_round_") and nombre.endswith(".bin"):
            os.remove(nombre)
        elif nombre.startswith("department_employee_part_") and nombre.endswith(".heap"):
            os.remove(nombre)
        elif nombre.startswith("department_employee_part_") and nombre.endswith(".bin"):
            os.remove(nombre)


def probar_external_sort():
    print("\n==============================")
    print("PRUEBA: EXTERNAL SORT (TPMMS)")
    print("==============================")

    if not os.path.exists(EMPLOYEE_CSV):
        raise FileNotFoundError(f"No existe el archivo {EMPLOYEE_CSV}")

    print("1. Exportando employee.csv a bin...")
    export_to_heap(EMPLOYEE_CSV, EMPLOYEE_BIN, PAGE_SIZE)

    total_pages_heap = count_pages(EMPLOYEE_BIN, PAGE_SIZE)
    print(f"Archivo creado: {EMPLOYEE_BIN}")
    print(f"Total páginas employee.bin: {total_pages_heap}")

    print("2. Ejecutando external_sort por hire_date...")
    metricas = external_sort(
        heap_path=EMPLOYEE_BIN,
        output_path=EMPLOYEE_SORTED_BIN,
        page_size=PAGE_SIZE,
        buffer_size=BUFFER_SIZE,
        sort_key="hire_date"
    )

    print("Métricas external_sort:")
    print(metricas)

    total_pages_sorted = count_pages(EMPLOYEE_SORTED_BIN, PAGE_SIZE)
    print(f"Total páginas employee_sorted.bin: {total_pages_sorted}")

    print("3. Mostrando primeros registros del archivo ordenado...")
    registros_muestra = []
    paginas_a_leer = min(2, total_pages_sorted)

    for page_id in range(paginas_a_leer):
        registros_muestra.extend(read_page(EMPLOYEE_SORTED_BIN, page_id, PAGE_SIZE))

    for reg in registros_muestra[:10]:
        print(reg)

    print("4. Verificando que las hire_date estén en orden no decreciente...")
    fechas = [r[3] for r in registros_muestra]
    esta_ordenado = all(fechas[i] <= fechas[i + 1] for i in range(len(fechas) - 1))

    print("¿Orden correcto en la muestra?:", esta_ordenado)

    if not esta_ordenado:
        raise AssertionError("employee_sorted.bin no está correctamente ordenado por hire_date en la muestra.")


def probar_external_hashing():
    print("\n========================================")
    print("PRUEBA: EXTERNAL HASHING (GROUP BY)")
    print("========================================")

    if not os.path.exists(DEPT_EMPLOYEE_CSV):
        raise FileNotFoundError(f"No existe el archivo {DEPT_EMPLOYEE_CSV}")

    print("1. Exportando department_employee.csv a bin...")
    export_department_employee_to_heap(DEPT_EMPLOYEE_CSV, DEPT_EMPLOYEE_BIN, PAGE_SIZE)

    total_pages_heap = count_pages(DEPT_EMPLOYEE_BIN, PAGE_SIZE)
    print(f"Archivo creado: {DEPT_EMPLOYEE_BIN}")
    print(f"Total páginas department_employee.bin: {total_pages_heap}")

    print("2. Ejecutando external_hash_group_by por from_date...")
    resultado = external_hash_group_by(
        heap_path=DEPT_EMPLOYEE_BIN,
        page_size=PAGE_SIZE,
        buffer_size=BUFFER_SIZE,
        group_key="from_date"
    )

    print("Métricas external_hash_group_by:")
    print({
        "partitions_created": resultado["partitions_created"],
        "pages_read": resultado["pages_read"],
        "pages_written": resultado["pages_written"],
        "time_phase1_sec": resultado["time_phase1_sec"],
        "time_phase2_sec": resultado["time_phase2_sec"],
        "time_total_sec": resultado["time_total_sec"],
    })

    print("3. Mostrando resultado GROUP BY from_date...")
    result_dict = resultado["result"]

    fechas_ordenadas = sorted(result_dict.keys())
    for fecha in fechas_ordenadas[:20]:
        print(fecha, "->", result_dict[fecha])

    print("4. Mostrando primeros registros de department_employee.bin...")
    registros_muestra = []
    paginas_a_leer = min(2, total_pages_heap)

    for page_id in range(paginas_a_leer):
        registros_muestra.extend(read_page_department_employee(DEPT_EMPLOYEE_BIN, page_id, PAGE_SIZE))

    for reg in registros_muestra[:10]:
        print(reg)

    if len(result_dict) == 0:
        raise AssertionError("El resultado del GROUP BY está vacío.")


def main():
    try:
        limpiar_archivos_generados()
        probar_external_sort()
        probar_external_hashing()

        print("\n===================================")
        print("TODAS LAS PRUEBAS TERMINARON BIEN")
        print("===================================")

    except Exception as e:
        print("\n====================")
        print("ERROR EN LAS PRUEBAS")
        print("====================")
        print(type(e).__name__, "-", e)
        raise


if __name__ == "__main__":
    main()