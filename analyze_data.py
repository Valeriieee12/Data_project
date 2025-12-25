#!/usr/bin/env python3
import pandas as pd
import pyarrow.parquet as pq
from pathlib import Path
import sys

def main():
    print("=" * 70)
    print("АНАЛИЗ ДАННЫХ ДОСТАВОК")
    print("=" * 70)
    
    file_path = "airflow/data/deliveries.parquet"
    
    if not Path(file_path).exists():
        print(f"❌ Файл не найден: {file_path}")
        sys.exit(1)
    
    print(f"✓ Файл найден: {file_path}")
    file_size = Path(file_path).stat().st_size / (1024*1024)
    print(f"✓ Размер файла: {file_size:.2f} MB")
    
    try:
        # Читаем метаданные
        parquet_file = pq.ParquetFile(file_path)
        print(f"\n✓ Количество строк: {parquet_file.metadata.num_rows:,}")
        print(f"✓ Количество колонок: {parquet_file.metadata.num_columns}")
        
        # Список колонок
        print(f"\nСПИСОК ВСЕХ КОЛОНОК:")
        schema = parquet_file.schema
        for i in range(schema.num_fields):
            field = schema.field(i)
            print(f"{i+1:3d}. {field.name}")
        
        # Загружаем первые 3 строки
        print(f"\nПЕРВЫЕ 3 СТРОКИ ДАННЫХ:")
        df = pd.read_parquet(file_path, nrows=3)
        print(df)
        
    except Exception as e:
        print(f"\n❌ Ошибка: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
