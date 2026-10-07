import yaml
import json
import requests
import asyncio
import uuid
from concurrent.futures import ThreadPoolExecutor

# Configuración global del controlador
CONTROLLER_IP = '10.20.12.111' # IP del controlador Floodlight en tu topología

class Student:
    def __init__(self, name, mac):
        self.name = name
        self.mac = mac

class Service:
    def __init__(self, name, protocol, port):
        self.name = name
        self.protocol = protocol
        self.port = port

class Server:
    def __init__(self, name, ip, services):
        self.name = name
        self.ip = ip
        self.services = [Service(s['name'], s['protocol'], s['port']) for s in services]

class Course:
    def __init__(self, name, state, students, servers, code=None):
        self.name = name
        self.state = state
        self.students = students if students else []
        self.servers = servers if servers else []
        self.code = code

def load_data(filename):
    with open(filename, "r") as file:
        data = yaml.safe_load(file)
    students = [Student(x["name"], x["mac"]) for x in data["students"]]
    servers = [Server(y["name"], y["ip"], y["services"]) for y in data["servers"]]
    
    courses = []
    for z in data["courses"]:
        course = Course(z["name"], z["state"], z.get("students", []), z.get("servers", []), z.get("code"))
        courses.append(course)
        
    return students, servers, courses

def get_attachment_point(controller_ip, mac):
    endpoint = '/wm/device/'
    url = 'http://' + controller_ip + ':8080' + endpoint
    response = requests.get(url, params={'mac': mac})
    
    if response.status_code != 200:
        return None, None
        
    result = response.json()
    if not result:
        return None, None
        
    attachment_points = result[0].get("attachmentPoint", [])
    if not attachment_points:
        return None, None

    attachment_point = attachment_points[0]
    return attachment_point["switchDPID"], attachment_point["port"]

def get_route(controller_ip, src_dpid, src_port, dst_dpid, dst_port):
    endpoint = f'/wm/topology/route/{src_dpid}/{src_port}/{dst_dpid}/{dst_port}/json'
    url = 'http://' + controller_ip + ':8080' + endpoint
    response = requests.get(url)
    return response.json()

# Variable global para almacenar las conexiones (rutas creadas manualmente)
connections = {}

def push_flow(flow_dict):
    """Inserta una regla de flujo en el controlador"""
    url = f'http://{CONTROLLER_IP}:8080/wm/staticflowpusher/json'
    headers = {'Content-type': 'application/json'}
    response = requests.post(url, data=json.dumps(flow_dict), headers=headers)
    return response.status_code

def delete_flow(flow_name):
    """Elimina una regla de flujo por su nombre"""
    url = f'http://{CONTROLLER_IP}:8080/wm/staticflowpusher/json'
    data = json.dumps({"name": flow_name})
    headers = {'Content-type': 'application/json'}
    requests.delete(url, data=data, headers=headers)

async def build_route_async(route_info, student_mac, server_ip, server_mac, service, handler_id):
    """Método asíncrono que inserta los flujos en los switches"""
    loop = asyncio.get_running_loop()
    with ThreadPoolExecutor() as pool:
        tasks = []
        flow_names_created = []
        
        # Procesar ruta extraída del JSON de Floodlight
        for i in range(0, len(route_info), 2):
            if i+1 >= len(route_info): 
                break
            
            node1 = route_info[i]
            node2 = route_info[i+1]
            switch_dpid = node1['switch']
            in_port = node1['port']
            out_port = node2['port']
            
            # Formateo del protocolo
            ip_proto = "0x06" if service.protocol.upper() == "TCP" else "0x11"
            tp_dst_key = "tcp_dst" if service.protocol.upper() == "TCP" else "udp_dst"
            tp_src_key = "tcp_src" if service.protocol.upper() == "TCP" else "udp_src"

            # Flujo 1: Estudiante -> Servidor (L2, L3, L4)
            flow_name_1 = f"{handler_id}_fwd_{switch_dpid}"
            flow_1 = {
                "switch": switch_dpid,
                "name": flow_name_1,
                "cookie": "0",
                "priority": "32768",
                "in_port": str(in_port),
                "eth_type": "0x0800",
                "ipv4_dst": server_ip,
                "ip_proto": ip_proto,
                tp_dst_key: str(service.port),
                "active": "true",
                "actions": f"output={out_port}"
            }
            
            # Flujo 2: Servidor -> Estudiante (L2, L3, L4)
            flow_name_2 = f"{handler_id}_rev_{switch_dpid}"
            flow_2 = {
                "switch": switch_dpid,
                "name": flow_name_2,
                "cookie": "0",
                "priority": "32768",
                "in_port": str(out_port),
                "eth_type": "0x0800",
                "ipv4_src": server_ip,
                "ip_proto": ip_proto,
                tp_src_key: str(service.port),
                "active": "true",
                "actions": f"output={in_port}"
            }
            
            # Flujo 3 y 4: Tráfico ARP (Bidireccional)
            arp_fwd = f"{handler_id}_arp_fwd_{switch_dpid}"
            flow_arp_1 = {
                "switch": switch_dpid, "name": arp_fwd,
                "cookie": "0", "priority": "32768",
                "in_port": str(in_port), "eth_type": "0x0806",
                "active": "true", "actions": f"output={out_port}"
            }
            arp_rev = f"{handler_id}_arp_rev_{switch_dpid}"
            flow_arp_2 = {
                "switch": switch_dpid, "name": arp_rev,
                "cookie": "0", "priority": "32768",
                "in_port": str(out_port), "eth_type": "0x0806",
                "active": "true", "actions": f"output={in_port}"
            }

            # Enviar requests de forma asíncrona
            for f in [flow_1, flow_2, flow_arp_1, flow_arp_2]:
                tasks.append(loop.run_in_executor(pool, push_flow, f))
                flow_names_created.append(f["name"])
        
        await asyncio.gather(*tasks)
        return flow_names_created

def build_route(student, server, service, dst_mac):
    handler_id = str(uuid.uuid4())[:8]
    print(f"Estableciendo conexión... Handler asignado: {handler_id}")
    
    src_dpid, src_port = get_attachment_point(CONTROLLER_IP, student.mac)
    dst_dpid, dst_port = get_attachment_point(CONTROLLER_IP, dst_mac)
    
    if not src_dpid or not dst_dpid:
        print("Error: No se pudo localizar los puntos de anclaje (attachment points). Verifica que los hosts han enviado tráfico (ej. ping).")
        return None
        
    route = get_route(CONTROLLER_IP, src_dpid, src_port, dst_dpid, dst_port)
    
    # Ejecutar asíncronamente
    flow_names = asyncio.run(build_route_async(route, student.mac, server.ip, dst_mac, service, handler_id))
    
    connections[handler_id] = {
        "student": student.name,
        "server": server.name,
        "service": service.name,
        "flows": flow_names
    }
    print(f"Ruta instalada correctamente bajo el handler: {handler_id}")

def is_authorized(student, server_name, service_name, courses):
    """Valida si el alumno está matriculado y tiene acceso al servicio."""
    for c in courses:
        if c.state == "ACTIVE" and student.name in c.students:
            for s in c.servers:
                if s["name"] == server_name and service_name in s.get("allowed_services", []):
                    return True
    return False

def menu(students, servers, courses):
    while True:
        print("\n" + "#" * 60)
        print("Network Policy Manager de la UPSM")
        print("#" * 60)
        print("\nSeleccione una opción:")
        print("1) Importar")
        print("2) Exportar")
        print("3) Cursos")
        print("4) Alumnos")
        print("5) Servidores")
        print("6) Políticas")
        print("7) Conexiones")
        print("8) Salir")
        
        op = input(">>> ")
        
        if op == "3":
            print("\n--- Menú Cursos ---")
            print("1. Listar")
            print("2. Mostrar detalles")
            print("3. Gestionar alumnos")
            cop = input(">>> ")
            if cop == "1":
                for c in courses: 
                    print(f"- [{c.code}] {c.name} ({c.state})")
            elif cop == "2":
                c_code = input("Código del curso (ej. HGW101): ")
                c = next((x for x in courses if x.code == c_code), None)
                if c:
                    print(f"\nCurso: {c.name} ({c.code}) | Estado: {c.state}")
                    print(f"Alumnos matriculados: {', '.join(c.students)}")
                    print("Servidores y servicios permitidos:")
                    for srv in c.servers:
                        print(f"  * {srv['name']}: {', '.join(srv.get('allowed_services', []))}")
                else: 
                    print("Curso no encontrado.")
            elif cop == "3":
                c_code = input("Código del curso: ")
                c = next((x for x in courses if x.code == c_code), None)
                if c:
                    accion = input("1. Agregar alumno | 2. Eliminar alumno: ")
                    s_name = input("Nombre del alumno: ")
                    if accion == "1":
                        if s_name not in c.students: 
                            c.students.append(s_name)
                            print(f"Alumno '{s_name}' agregado al curso {c.code}.")
                        else:
                            print("El alumno ya estaba matriculado.")
                    elif accion == "2":
                        if s_name in c.students: 
                            c.students.remove(s_name)
                            print(f"Alumno '{s_name}' eliminado del curso {c.code}.")
                        else:
                            print("El alumno no está en la lista.")
                else: 
                    print("Curso no encontrado.")
                
        elif op == "4":
            print("\n--- Menú Alumnos ---")
            print("1. Crear")
            print("2. Listar")
            print("3. Mostrar detalles")
            aop = input(">>> ")
            if aop == "1":
                nombre = input("Nombre del alumno: ")
                mac = input("Dirección MAC: ")
                students.append(Student(nombre, mac))
                print(f"Alumno '{nombre}' creado exitosamente.")
            elif aop == "2":
                curso_filtro = input("Filtrar por código de curso (presiona Enter para listar todos): ").strip()
                if curso_filtro:
                    c = next((x for x in courses if x.code == curso_filtro), None)
                    if c:
                        print(f"\nAlumnos en {c.code}:")
                        for s_name in c.students:
                            st = next((x for x in students if x.name == s_name), None)
                            mac_str = st.mac if st else "MAC desconocida"
                            print(f"- {s_name} ({mac_str})")
                    else:
                        print("Curso no encontrado.")
                else:
                    print("\nTodos los alumnos:")
                    for s in students: 
                        print(f"- {s.name} ({s.mac})")
            elif aop == "3":
                nombre = input("Nombre del alumno: ")
                s = next((x for x in students if x.name == nombre), None)
                if s: 
                    print(f"\nNombre: {s.name} | MAC: {s.mac}")
                else: 
                    print("Alumno no encontrado.")

        elif op == "5":
            print("\n--- Menú Servidores ---")
            print("1. Listar")
            print("2. Mostrar servicios")
            sop = input(">>> ")
            if sop == "1":
                for s in servers: 
                    print(f"- {s.name} (IP: {s.ip})")
            elif sop == "2":
                nombre = input("Nombre del servidor (ej. Server 3): ")
                s = next((x for x in servers if x.name == nombre), None)
                if s:
                    print(f"\nServicios de {s.name}:")
                    for srv in s.services: 
                        print(f"- {srv.name} (Protocolo: {srv.protocol}, Puerto: {srv.port})")
                else: 
                    print("Servidor no encontrado.")

        elif op == "7":
            print("\n--- Menú Conexiones ---")
            print("1. Crear")
            print("2. Listar")
            print("3. Borrar")
            conop = input(">>> ")
            if conop == "1":
                student_name = input("Nombre del alumno: ")
                server_name = input("Nombre del servidor (ej. Server 3): ")
                service_name = input("Nombre del servicio (ej. ssh): ")
                server_mac = input("MAC del servidor: ")
                
                stu = next((x for x in students if x.name == student_name), None)
                srv = next((x for x in servers if x.name == server_name), None)
                
                if stu and srv:
                    svc = next((x for x in srv.services if x.name == service_name), None)
                    if not svc:
                        print("El servicio no existe en este servidor.")
                        continue
                        
                    # Validación de Autorización
                    if is_authorized(stu, server_name, service_name, courses):
                        print("Usuario autorizado. Procediendo a calcular e instalar la ruta...")
                        build_route(stu, srv, svc, server_mac)
                    else:
                        print("Error: El alumno NO está autorizado (no pertenece a un curso activo con acceso a este servicio).")
                else:
                    print("Alumno o servidor no encontrados.")
                    
            elif conop == "2":
                if not connections:
                    print("No hay conexiones manuales activas.")
                else:
                    for handler, data in connections.items():
                        print(f"[{handler}] Alumno: {data['student']} <-> Servidor: {data['server']} | Servicio: {data['service']}")
                    
            elif conop == "3":
                handler = input("Ingrese el handler de la conexión a borrar: ")
                if handler in connections:
                    print("Eliminando flujos en los switches a través del controlador...")
                    for flow_name in connections[handler]["flows"]:
                        delete_flow(flow_name)
                    del connections[handler]
                    print("Conexión borrada exitosamente.")
                else:
                    print("Handler no encontrado.")

        elif op == "8":
            print("Saliendo del programa...")
            break
        else:
            print("Opción no disponible o no requerida para este laboratorio.")

def main():
    # Cargar datos desde database.yaml
    students, servers, courses = load_data("database.yaml")
    
    # Iniciar la interfaz interactiva (CLI)
    menu(students, servers, courses)

if __name__ == "__main__":
    main()