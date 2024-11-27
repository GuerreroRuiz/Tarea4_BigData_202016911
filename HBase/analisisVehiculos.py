import happybase
import pandas as pd
import re
from datetime import datetime

# Bloque principal de ejecución
try:
    # 1. Establecer conexión con HBase
    connection = happybase.Connection('localhost')
    print("Conexión establecida con HBase")

    # 2. Crear la tabla con las familias de columnas
    table_name = 'used_cars'
    families = {
        'basic': dict(),  # Información básica del coche
        'specs': dict(),  # Especificaciones técnicas
        'sales': dict(),  # Información de venta
        'condition': dict()  # Estado del vehículo
    }

    # Eliminar la tabla si ya existe
    if table_name.encode() in connection.tables():
        print(f"Eliminando tabla existente - {table_name}")
        connection.delete_table(table_name, disable=True)

    # Crear nueva tabla
    connection.create_table(table_name, families)
    table = connection.table(table_name)
    print("Tabla 'used_cars' creada exitosamente")

    # 3. Cargar datos del CSV
    car_data = pd.read_csv('Car_details_v3.csv')

    # Iterar sobre el DataFrame usando el índice
    for index, row in car_data.iterrows():
        # Generar row key basado en el índice
        row_key = f'car_{index}'.encode()
        # Organizar los datos en familias de columnas
        data = {
            b'basic:name': str(row['name']).encode(),
            b'basic:year': str(row['year']).encode(),
            b'basic:transmission': str(row['transmission']).encode(),
            b'basic:fuel': str(row['fuel']).encode(),
            
            b'specs:engine': str(row['engine']).encode(),
            b'specs:max_power': str(row['max_power']).encode(),
            b'specs:torque': str(row['torque']).encode(),
            b'specs:seats': str(row['seats']).encode(),
            b'specs:mileage': str(row['mileage']).encode(),
            
            b'sales:selling_price': str(row['selling_price']).encode(),
            b'sales:seller_type': str(row['seller_type']).encode(),
            
            b'condition:km_driven': str(row['km_driven']).encode(),
            b'condition:owner': str(row['owner']).encode()
        }
        table.put(row_key, data)
    print("Datos cargados exitosamente")

    # 4. Consultas y análisis de datos
    print("\n=== Todos los coches en la base de datos (primeros 5) ===")
    count = 0
    for key, data in table.scan():
        if count < 5:  # Limitamos a 5 para el ejemplo
            print(f"\nCoche ID: {key.decode()}")
            print(f"Nombre: {data[b'basic:name'].decode()}")
            print(f"Año: {data[b'basic:year'].decode()}")
            print(f"Precio: {data[b'sales:selling_price'].decode()}")
            count += 1

    # 5. Encontrar coches por rango de precio
    print("\n=== Coches con precio mayor a 6000000 ===")
    for key, data in table.scan():
        if int(data[b'sales:selling_price'].decode()) > 6000000:
            print(f"\nCoche ID: {key.decode()}")
            print(f"Nombre: {data[b'basic:name'].decode()}")
            print(f"Precio: {data[b'sales:selling_price'].decode()}")

    # 6. Análisis de propietarios
    print("\n=== Coches por tipo de propietario ===")
    owner_stats = {}
    for key, data in table.scan():
        owner = data[b'condition:owner'].decode()
        owner_stats[owner] = owner_stats.get(owner, 0) + 1
    for owner, count in owner_stats.items():
        print(f"{owner}: {count} coches")

    # 7. Análisis de precios por tipo de combustible
    print("\n=== Precio promedio por tipo de combustible ===")
    fuel_prices = {}
    fuel_counts = {}
    for key, data in table.scan():
        fuel = data[b'basic:fuel'].decode()
        price = int(data[b'sales:selling_price'].decode())
        fuel_prices[fuel] = fuel_prices.get(fuel, 0) + price
        fuel_counts[fuel] = fuel_counts.get(fuel, 0) + 1
    for fuel in fuel_prices:
        avg_price = fuel_prices[fuel] / fuel_counts[fuel]
        print(f"{fuel}: {avg_price:.2f}")

    # 8. Top 5 coches con mayor kilometraje
    print("\n=== Top 5 coches con mayor kilometraje ===")
    cars_by_km = []
    for key, data in table.scan():
        cars_by_km.append({
            'id': key.decode(),
            'name': data[b'basic:name'].decode(),
            'km': int(data[b'condition:km_driven'].decode()),
            'price': int(data[b'sales:selling_price'].decode())
        })
    for car in sorted(cars_by_km, key=lambda x: x['km'], reverse=True)[:5]:
        print(f"ID: {car['id']}")
        print(f"Nombre: {car['name']}")
        print(f"Kilometraje: {car['km']}")
        print(f"Precio: {car['price']}\n")

    # 9. Análisis de precios por tipo de transmisión
    print("\n=== Precio promedio por tipo de transmisión ===")
    transmission_prices = {}
    transmission_counts = {}
    for key, data in table.scan():
        trans = data[b'basic:transmission'].decode()
        price = int(data[b'sales:selling_price'].decode())
        transmission_prices[trans] = transmission_prices.get(trans, 0) + price
        transmission_counts[trans] = transmission_counts.get(trans, 0) + 1
    for trans in transmission_prices:
        avg_price = transmission_prices[trans] / transmission_counts[trans]
        print(f"{trans}: {avg_price:.2f}")

    # 10. Ejemplo de actualización de precio
    car_to_update = 'car_0'
    new_price = 460000
    table.put(car_to_update.encode(), {b'sales:selling_price': str(new_price).encode()})
    print(f"\nPrecio actualizado para el coche ID: {car_to_update}")       

    # 11. Vehículos más recientes por tipo de combustible
    print("\n=== Vehículos más recientes por tipo de combustible ===")
    recent_cars = {}
    for key, data in table.scan():
        fuel = data[b'basic:fuel'].decode()
        year = int(data[b'basic:year'].decode())
        if fuel not in recent_cars or recent_cars[fuel]['year'] < year:
            recent_cars[fuel] = {'id': key.decode(), 'name': data[b'basic:name'].decode(), 'year': year}
    for fuel, car in recent_cars.items():
        print(f"{fuel}: {car['name']} ({car['year']}) - ID: {car['id']}")
        
        
    # 12. Precio promedio por año de fabricación
    print("\n=== Precio promedio por año de fabricación ===")
    year_prices = {}
    year_counts = {}
    for key, data in table.scan():
        year = int(data[b'basic:year'].decode())
        price = int(data[b'sales:selling_price'].decode())
        year_prices[year] = year_prices.get(year, 0) + price
        year_counts[year] = year_counts.get(year, 0) + 1
    for year in sorted(year_prices.keys()):
        avg_price = year_prices[year] / year_counts[year]
        print(f"Año {year}: Precio promedio {avg_price:.2f}")
        
   
    # 13. Vehículos con mayor antiguos
    print("\n=== Vehículos más antiguos ===")
    oldest_cars = []
    for key, data in table.scan():
        year = int(data[b'basic:year'].decode())
        oldest_cars.append({'id': key.decode(), 'name': data[b'basic:name'].decode(), 'year': year})
    for car in sorted(oldest_cars, key=lambda x: x['year'])[:5]:
        print(f"ID: {car['id']}, Nombre: {car['name']}, Año: {car['year']}")
        

    # 14. Promedio de rendimiento por transmisión y combustible
    print("\n=== Promedio de rendimiento (mileage) por tipo de transmisión y combustible ===")

    # Diccionarios para acumular los valores de rendimiento y contar ocurrencias por categoría
    mileage_data = {}

    for key, data in table.scan():
        try:
            fuel = data[b'basic:fuel'].decode()  # Tipo de combustible
            transmission = data[b'basic:transmission'].decode()  # Tipo de transmisión
            mileage_raw = data[b'specs:mileage'].decode()  # Valor de rendimiento

            # Extraer el valor numérico y la unidad de medida
            match = re.match(r"(\d+(\.\d+)?)(\s*\w+)", mileage_raw)  # Ejemplo: "22.5 kmpl"
            if match:
                mileage_value = float(match.group(1))  # El número (22.5)
                mileage_unit = match.group(3).strip()  # La unidad (kmpl, km/kg)

                # Usar la combinación de transmisión y combustible como clave
                key = (transmission, fuel, mileage_unit)
                if key not in mileage_data:
                    mileage_data[key] = {"total_mileage": 0, "count": 0}
                
                # Acumular valores
                mileage_data[key]["total_mileage"] += mileage_value
                mileage_data[key]["count"] += 1
        except Exception as e:
            print(f"Error al procesar coche ID {key.decode()}: {e}")

    # Calcular y mostrar promedios
    for (transmission, fuel, unit), stats in mileage_data.items():
        avg_mileage = stats["total_mileage"] / stats["count"]
        print(f"Transmisión: {transmission}, Combustible: {fuel}, Unidad: {unit} -> Promedio: {avg_mileage:.2f} {unit}")


except Exception as e:
    print(f"Error: {str(e)}")

finally:
    # Cerrar la conexión
    connection.close()
