import datetime
import struct
import io
import asyncio
import uuid
import argparse
from enum import Enum
from pprint import pp


class MessageException(BaseException):
    def __init__(self, service_id, message):
        self.service_id = service_id
        self.message = message
        super().__init__(self.service_id + ": " + self.message)


class PacketType(Enum):
    NEGOTIATE = b'\x00'
    CONNECT = b'\x01'
    CONNECT_ACK = b'\x02'
    DISCONNECT = b'\x04'
    ENDPOINT_OPEN = b'\x0b'
    ENDPOINT_OPEN_ACK = b'\x0c'
    ENDPOINT_CLOSE = b'\x0d'
    ENDPOINT_MESSAGE = b'\x0e'
    ENDPOINT_FAILURE = b'\x0f'
    KEEP_ALIVE = b'\x10'


class ParamType(Enum):
    UNKNOWN_TYPE = b'\x00'
    BOOLEAN = b'\x01'
    BYTE = b'\x02'
    SHORT = b'\x03'
    INT = b'\x04'
    LONG = b'\x05'
    FLOAT = b'\x06'
    DOUBLE = b'\x07'
    SIZE = b'\x08'
    NULLABLE_SIZE = b'\x09'
    STRING = b'\x0a'
    UUID = b'\x0b'
    TYPE = b'\x0c'
    ENDPOINT_ID = b'\x0d'


class EndpointDataType(Enum):
    VOID_MESSAGE = b'\x00'
    MESSAGE = b'\x01'
    EXCEPTION = b'\xff'


class MessageType(Enum):
    GET_AGENT_ADMINS_REQUEST = chr(0).encode()
    GET_AGENT_ADMINS_RESPONSE = chr(1).encode()
    GET_CLUSTER_ADMINS_REQUEST = chr(2).encode()
    GET_CLUSTER_ADMINS_RESPONSE = chr(3).encode()
    REG_AGENT_ADMIN_REQUEST = chr(4).encode()
    REG_CLUSTER_ADMIN_REQUEST = chr(5).encode()
    UNREG_AGENT_ADMIN_REQUEST = chr(6).encode()
    UNREG_CLUSTER_ADMIN_REQUEST = chr(7).encode()
    AUTHENTICATE_AGENT_REQUEST = chr(8).encode()  # Auth on agent
    AUTHENTICATE_REQUEST = chr(9).encode()  # Auth on cluster
    ADD_AUTHENTICATION_REQUEST = chr(10).encode()  # Auth on infobase
    GET_CLUSTERS_REQUEST = chr(11).encode()
    GET_CLUSTERS_RESPONSE = chr(12).encode()
    GET_CLUSTER_INFO_REQUEST = chr(13).encode()
    GET_CLUSTER_INFO_RESPONSE = chr(14).encode()
    REG_CLUSTER_REQUEST = chr(15).encode()
    REG_CLUSTER_RESPONSE = chr(16).encode()
    UNREG_CLUSTER_REQUEST = chr(17).encode()
    GET_CLUSTER_MANAGERS_REQUEST = chr(18).encode()
    GET_CLUSTER_MANAGERS_RESPONSE = chr(19).encode()
    GET_CLUSTER_MANAGER_INFO_REQUEST = chr(20).encode()
    GET_CLUSTER_MANAGER_INFO_RESPONSE = chr(21).encode()
    GET_WORKING_SERVERS_REQUEST = chr(22).encode()
    GET_WORKING_SERVERS_RESPONSE = chr(23).encode()
    GET_WORKING_SERVER_INFO_REQUEST = chr(24).encode()
    GET_WORKING_SERVER_INFO_RESPONSE = chr(25).encode()
    REG_WORKING_SERVER_REQUEST = chr(26).encode()
    REG_WORKING_SERVER_RESPONSE = chr(27).encode()
    UNREG_WORKING_SERVER_REQUEST = chr(28).encode()
    GET_WORKING_PROCESSES_REQUEST = chr(29).encode()
    GET_WORKING_PROCESSES_RESPONSE = chr(30).encode()
    GET_WORKING_PROCESS_INFO_REQUEST = chr(31).encode()
    GET_WORKING_PROCESS_INFO_RESPONSE = chr(32).encode()
    GET_SERVER_WORKING_PROCESSES_REQUEST = chr(33).encode()
    GET_SERVER_WORKING_PROCESSES_RESPONSE = chr(34).encode()
    GET_CLUSTER_SERVICES_REQUEST = chr(35).encode()
    GET_CLUSTER_SERVICES_RESPONSE = chr(36).encode()
    CREATE_INFOBASE_REQUEST = chr(37).encode()
    CREATE_INFOBASE_RESPONSE = chr(38).encode()
    UPDATE_INFOBASE_SHORT_REQUEST = chr(39).encode()
    UPDATE_INFOBASE_REQUEST = chr(40).encode()
    DROP_INFOBASE_REQUEST = chr(41).encode()
    GET_INFOBASES_SHORT_REQUEST = chr(42).encode()
    GET_INFOBASES_SHORT_RESPONSE = chr(43).encode()
    GET_INFOBASES_REQUEST = chr(44).encode()
    GET_INFOBASES_RESPONSE = chr(45).encode()
    GET_INFOBASE_SHORT_INFO_REQUEST = chr(46).encode()
    GET_INFOBASE_SHORT_INFO_RESPONSE = chr(47).encode()
    GET_INFOBASE_INFO_REQUEST = chr(48).encode()
    GET_INFOBASE_INFO_RESPONSE = chr(49).encode()
    GET_CONNECTIONS_SHORT_REQUEST = chr(50).encode()
    GET_CONNECTIONS_SHORT_RESPONSE = chr(51).encode()
    GET_INFOBASE_CONNECTIONS_SHORT_REQUEST = chr(52).encode()
    GET_INFOBASE_CONNECTIONS_SHORT_RESPONSE = chr(53).encode()
    GET_CONNECTION_INFO_SHORT_REQUEST = chr(54).encode()
    GET_CONNECTION_INFO_SHORT_RESPONSE = chr(55).encode()
    GET_INFOBASE_CONNECTIONS_REQUEST = chr(56).encode()
    GET_INFOBASE_CONNECTIONS_RESPONSE = chr(57).encode()
    # reserved 58 to 63  # Unknown messages types
    DISCONNECT_REQUEST = chr(64).encode()
    GET_SESSIONS_REQUEST = chr(65).encode()
    GET_SESSIONS_RESPONSE = chr(66).encode()
    GET_INFOBASE_SESSIONS_REQUEST = chr(67).encode()
    GET_INFOBASE_SESSIONS_RESPONSE = chr(68).encode()
    GET_SESSION_INFO_REQUEST = chr(69).encode()
    GET_SESSION_INFO_RESPONSE = chr(70).encode()
    TERMINATE_SESSION_REQUEST = chr(71).encode()
    GET_LOCKS_REQUEST = chr(72).encode()
    GET_LOCKS_RESPONSE = chr(73).encode()
    GET_INFOBASE_LOCKS_REQUEST = chr(74).encode()
    GET_INFOBASE_LOCKS_RESPONSE = chr(75).encode()
    GET_CONNECTION_LOCKS_REQUEST = chr(76).encode()
    GET_CONNECTION_LOCKS_RESPONSE = chr(77).encode()
    GET_SESSION_LOCKS_REQUEST = chr(78).encode()
    GET_SESSION_LOCKS_RESPONSE = chr(79).encode()
    # reserved 80  # Unknown messages type
    APPLY_ASSIGNMENT_RULES_REQUEST = chr(81).encode()
    REG_ASSIGNMENT_RULE_REQUEST = chr(82).encode()
    REG_ASSIGNMENT_RULE_RESPONSE = chr(83).encode()
    UNREG_ASSIGNMENT_RULE_REQUEST = chr(84).encode()
    GET_ASSIGNMENT_RULES_REQUEST = chr(85).encode()
    GET_ASSIGNMENT_RULES_RESPONSE = chr(86).encode()
    GET_ASSIGNMENT_RULE_INFO_REQUEST = chr(87).encode()
    GET_ASSIGNMENT_RULE_INFO_RESPONSE = chr(88).encode()
    GET_SECURITY_PROFILES_REQUEST = chr(89).encode()
    GET_SECURITY_PROFILES_RESPONSE = chr(90).encode()
    CREATE_SECURITY_PROFILE_REQUEST = chr(91).encode()
    DROP_SECURITY_PROFILE_REQUEST = chr(92).encode()
    GET_VIRTUAL_DIRECTORIES_REQUEST = chr(93).encode()
    GET_VIRTUAL_DIRECTORIES_RESPONSE = chr(94).encode()
    CREATE_VIRTUAL_DIRECTORY_REQUEST = chr(95).encode()
    DROP_VIRTUAL_DIRECTORY_REQUEST = chr(96).encode()
    GET_COM_CLASSES_REQUEST = chr(97).encode()
    GET_COM_CLASSES_RESPONSE = chr(98).encode()
    CREATE_COM_CLASS_REQUEST = chr(99).encode()
    DROP_COM_CLASS_REQUEST = chr(100).encode()
    GET_ALLOWED_ADDINS_REQUEST = chr(101).encode()
    GET_ALLOWED_ADDINS_RESPONSE = chr(102).encode()
    CREATE_ALLOWED_ADDIN_REQUEST = chr(103).encode()
    DROP_ALLOWED_ADDIN_REQUEST = chr(104).encode()
    GET_EXTERNAL_MODULES_REQUEST = chr(105).encode()
    GET_EXTERNAL_MODULES_RESPONSE = chr(106).encode()
    CREATE_EXTERNAL_MODULE_REQUEST = chr(107).encode()
    DROP_EXTERNAL_MODULE_REQUEST = chr(108).encode()
    GET_ALLOWED_APPLICATIONS_REQUEST = chr(109).encode()
    GET_ALLOWED_APPLICATIONS_RESPONSE = chr(110).encode()
    CREATE_ALLOWED_APPLICATION_REQUEST = chr(111).encode()
    DROP_ALLOWED_APPLICATION_REQUEST = chr(112).encode()
    GET_INTERNET_RESOURCES_REQUEST = chr(113).encode()
    GET_INTERNET_RESOURCES_RESPONSE = chr(114).encode()
    CREATE_INTERNET_RESOURCE_REQUEST = chr(115).encode()
    DROP_INTERNET_RESOURCE_REQUEST = chr(116).encode()
    INTERRUPT_SESSION_CURRENT_SERVER_CALL_REQUEST = chr(117).encode()
    GET_RESOURCE_COUNTERS_REQUEST = chr(118).encode()
    GET_RESOURCE_COUNTERS_RESPONSE = chr(119).encode()
    GET_RESOURCE_COUNTER_INFO_REQUEST = chr(120).encode()
    GET_RESOURCE_COUNTER_INFO_RESPONSE = chr(121).encode()
    REG_RESOURCE_COUNTER_REQUEST = chr(122).encode()
    UNREG_RESOURCE_COUNTER_REQUEST = chr(123).encode()
    GET_RESOURCE_LIMITS_REQUEST = chr(124).encode()
    GET_RESOURCE_LIMITS_RESPONSE = chr(125).encode()
    GET_RESOURCE_LIMIT_INFO_REQUEST = chr(126).encode()
    GET_RESOURCE_LIMIT_INFO_RESPONSE = chr(127).encode()
    REG_RESOURCE_LIMIT_REQUEST = chr(128).encode()
    UNREG_RESOURCE_LIMIT_REQUEST = chr(129).encode()
    GET_COUNTER_VALUES_REQUEST = chr(130).encode()
    GET_COUNTER_VALUES_RESPONSE = chr(131).encode()
    CLEAR_COUNTER_VALUE_REQUEST = chr(132).encode()
    GET_COUNTER_ACCUMULATED_VALUES_REQUEST = chr(133).encode()
    GET_COUNTER_ACCUMULATED_VALUES_RESPONSE = chr(134).encode()
    GET_AGENT_VERSION_REQUEST = chr(135).encode()
    GET_AGENT_VERSION_RESPONSE = chr(136).encode()


def pack_varint_base64(val):
    total = b''
    while val >= 0x40:
        bits = val & 0x3F
        val >>= 6
        total += struct.pack('B', (0x40 | bits))
    bits = val & 0x3F
    total += struct.pack('B', bits)
    return total


def pack_varint_base128(val):
    total = b''
    while val >= 0x80:
        bits = val & 0x7F
        val >>= 7
        total += struct.pack('B', (0x80 | bits))
    bits = val & 0x7F
    total += struct.pack('B', bits)
    return total


async def unpack_varint_base64(stream):
    total = 0
    shift = 0
    val = 0x40
    while val & 0x40:
        if type(stream) is io.BytesIO:
            data = stream.read(1)
        else:
            data = await stream.read(1)
        if data == b'':
            return None
        unpacked_data = struct.unpack('B', data)
        val = unpacked_data[0]
        total |= ((val & 0x3F) << shift)
        shift += 6
    return total


async def unpack_varint_base128(stream):
    total = 0
    shift = 0
    val = 0x80
    while val & 0x80:
        if type(stream) is io.BytesIO:
            data = stream.read(1)
        else:
            data = await stream.read(1)
        if data == b'':
            return None
        unpacked_data = struct.unpack('B', data)
        val = unpacked_data[0]
        total |= ((val & 0x7F) << shift)
        shift += 7
    return total


def varint_prefixed_data_base64(data):
    return pack_varint_base64(len(data)) + data


def read_uuid(reader):
    return uuid.UUID(bytes=reader.read(16))


def read_uint16(reader):
    return struct.unpack('>H', reader.read(2))[0]


def write_uint16(value):
    return struct.pack('>H', value)


def read_int32(reader):
    return struct.unpack('>i', reader.read(4))[0]


def write_int32(value):
    return struct.pack('>i', value)


def read_int64(reader):
    return struct.unpack('>q', reader.read(8))[0]


def write_int64(value):
    return struct.pack('>q', value)


async def read_bytes(reader):
    return reader.read(await unpack_varint_base64(reader))


async def read_string(reader):
    return (await read_bytes(reader)).decode()


def read_bool(reader):
    return reader.read(1) == b'\x01'


def write_bool(value):
    return b'\x01' if value == 1 or value is True else b'\x00'


def read_double(reader):
    return struct.unpack('>d', reader.read(8))[0]


def write_double(value):
    return struct.pack('>d', value)


def age_delta():
    return 621355968000000


def date_from_int64(value):
    if not value:
        return None
    return datetime.datetime.fromtimestamp((value - age_delta())/10000, datetime.timezone.utc)


def date_to_int64(value):
    if not value:
        return 0
    return int(value.timestamp() * 10000 + age_delta())


class Packet:
    def __init__(self, packet_type, message_type=None):
        self.data = []
        self.type = packet_type
        if packet_type == PacketType.ENDPOINT_MESSAGE:
            self.append_header()
            self.append_raw(message_type.value)

    def append(self, element):
        self.data.append(varint_prefixed_data_base64(element))

    def append_raw(self, element):
        self.data.append(element)

    def append_header(self):
        self.append_raw(b'\x01\x00\x00\x01')

    def get_parts(self):
        body = b''.join(self.data)
        if self.type != PacketType.ENDPOINT_MESSAGE:
            body += b'\x80'
        packet_length = pack_varint_base128(len(body))
        header = self.type.value + packet_length
        return header, body


async def read_cluster(packet):
    cluster = {}
    cluster['cluster'] = read_uuid(packet)
    cluster['expiration-timeout'] = read_int32(packet)
    cluster['host'] = await read_string(packet)
    cluster['lifetime-limit'] = read_int32(packet)
    cluster['port'] = read_uint16(packet)
    cluster['max-memory-size'] = read_int32(packet)  # deprecated
    cluster['max-memory-time-limit'] = read_int32(packet)  # deprecated
    cluster['name'] = await read_string(packet)
    cluster['security-level'] = read_int32(packet)
    cluster['session-fault-tolerance-level'] = read_int32(packet)
    cluster['load-balancing-mode'] = 'performance' if read_int32(packet) == 0 else 'memory'
    cluster['errors-count-threshold'] = read_int32(packet)  # deprecated
    cluster['kill-problem-processes'] = int(read_bool(packet))
    cluster['kill-by-memory-with-dump'] = int(read_bool(packet))
    return cluster


def write_cluster(cluster, packet):
    packet.append_raw(cluster['cluster'].bytes)
    packet.append_raw(write_int32(cluster['expiration-timeout']))
    packet.append(cluster['host'].encode())
    packet.append_raw(write_int32(cluster['lifetime-limit']))
    packet.append_raw(write_uint16(cluster['port']))
    packet.append_raw(write_int32(cluster['max-memory-size']))
    packet.append_raw(write_int32(cluster['max-memory-time-limit']))
    packet.append(cluster['name'].encode())
    packet.append_raw(write_int32(cluster['security-level']))
    packet.append_raw(write_int32(cluster['session-fault-tolerance-level']))
    packet.append_raw(write_int32(0 if cluster['load-balancing-mode'] == 'performance' else 1))
    packet.append_raw(write_int32(cluster['errors-count-threshold']))
    packet.append_raw(write_bool(cluster['kill-problem-processes']))
    packet.append_raw(write_bool(cluster['kill-by-memory-with-dump']))


async def read_infobase(packet):
    infobase = {}
    infobase['infobase'] = read_uuid(packet)
    infobase['date_offset'] = read_int32(packet)
    infobase['dbms'] = await read_string(packet)
    infobase['db_name'] = await read_string(packet)
    infobase['db_password'] = await read_bytes(packet)
    infobase['db_server_name'] = await read_string(packet)
    infobase['db_user'] = await read_string(packet)
    infobase['denied_from'] = date_from_int64(read_int64(packet))
    infobase['denied_message'] = await read_string(packet)
    infobase['denied_parameter'] = await read_string(packet)
    infobase['denied_to'] = date_from_int64(read_int64(packet))
    infobase['descr'] = await read_string(packet)
    infobase['locale'] = await read_string(packet)
    infobase['name'] = await read_string(packet)
    infobase['permission_code'] = await read_string(packet)
    infobase['scheduled_jobs_denied'] = read_bool(packet)
    infobase['security_level'] = read_int32(packet)
    infobase['sessions_denied'] = read_bool(packet)
    infobase['license_distribution'] = read_int32(packet)
    infobase['external_connection_string'] = await read_string(packet)
    infobase['external_session_manager_required'] = read_bool(packet)
    infobase['securirty_profile'] = await read_string(packet)
    infobase['safe_mode_securirty_profile'] = await read_string(packet)
    infobase['reserve_working_processes'] = read_bool(packet)
    return infobase


def write_infobase(infobase, packet):
    packet.append_raw(infobase['infobase'].bytes)
    packet.append_raw(write_int32(infobase['date_offset']))
    packet.append(infobase['dbms'].encode())
    packet.append(infobase['db_name'].encode())
    packet.append(infobase['db_password'])
    packet.append(infobase['db_server_name'].encode())
    packet.append(infobase['db_user'].encode())
    packet.append_raw(write_int64(date_to_int64(infobase['denied_from'])))
    packet.append(infobase['denied_message'].encode())
    packet.append(infobase['denied_parameter'].encode())
    packet.append_raw(write_int64(date_to_int64(infobase['denied_to'])))
    packet.append(infobase['descr'].encode())
    packet.append(infobase['locale'].encode())
    packet.append(infobase['name'].encode())
    packet.append(infobase['permission_code'].encode())
    packet.append_raw(write_bool(infobase['scheduled_jobs_denied']))
    packet.append_raw(write_int32(infobase['security_level']))
    packet.append_raw(write_bool(infobase['sessions_denied']))
    packet.append_raw(write_int32(infobase['license_distribution']))
    packet.append(infobase['external_connection_string'].encode())
    packet.append_raw(write_bool(infobase['external_session_manager_required']))
    packet.append(infobase['securirty_profile'].encode())
    packet.append(infobase['safe_mode_securirty_profile'].encode())
    packet.append_raw(write_bool(infobase['reserve_working_processes']))


async def read_infobase_short(packet):
    infobase = {}
    infobase['infobase'] = read_uuid(packet)
    infobase['descr'] = await read_string(packet)
    infobase['name'] = await read_string(packet)
    return infobase


async def read_session(packet):
    session = {}
    session['session_id'] = read_uuid(packet)
    session['app_id'] = await read_string(packet)
    session['blocked_by_dbms'] = read_int32(packet)
    session['blocked_by_ls'] = read_int32(packet)
    session['bytes_all'] = read_int64(packet)
    session['bytes_last5min'] = read_int64(packet)
    session['calls_all'] = read_int32(packet)
    session['calls_last5min'] = read_int64(packet)
    session['connection_id'] = read_uuid(packet)
    session['dbms_bytes_all'] = read_int64(packet)
    session['dbms_bytes_last5min'] = read_int64(packet)
    session['db_proc_info'] = await read_string(packet)
    session['db_proc_took'] = read_int32(packet)
    session['db_proc_took_at'] = read_int64(packet)
    session['duration_all'] = read_int32(packet)
    session['duration_all_dbms'] = read_int32(packet)
    session['duration_current'] = read_int32(packet)
    session['duration_current_dbms'] = read_int32(packet)
    session['duration_last_5_min'] = read_int64(packet)
    session['duration_last_5_min_dbms'] = read_int64(packet)
    session['host'] = await read_string(packet)
    session['infobase_id'] = read_uuid(packet)
    session['last_active_at'] = read_int64(packet)
    session['hibernate'] = read_bool(packet)
    session['passive_session_hibernate_time'] = read_int32(packet)
    session['hibernate_session_terminate_time'] = read_int32(packet)
    session['licenses'] = []
    lic_count = await unpack_varint_base64(packet)
    for lic_number in range(lic_count):
        lic = {}
        lic['full_name'] = await read_string(packet)
        lic['full_presentation'] = await read_string(packet)
        lic['issued_by_server'] = read_bool(packet)
        lic['license_type'] = read_int32(packet)
        lic['max_users_all'] = read_int32(packet)
        lic['max_users_cur'] = read_int32(packet)
        lic['net'] = read_bool(packet)
        lic['rmngr_address'] = await read_string(packet)
        lic['rmngr_pid'] = await read_string(packet)
        lic['rmngr_port'] = read_int32(packet)
        lic['series'] = await read_string(packet)
        lic['short_presentation'] = await read_string(packet)
        session['licenses'].append(lic)
    session['locale'] = await read_string(packet)
    session['process_id'] = read_uuid(packet)
    session['id'] = read_int32(packet)
    session['started_at'] = read_int64(packet)
    session['user_name'] = await read_string(packet)
    # version >= 4
    session['memory_current'] = read_int64(packet)
    session['memory_last5min'] = read_int64(packet)
    session['memory_total'] = read_int64(packet)
    session['read_current'] = read_int64(packet)
    session['read_last5min'] = read_int64(packet)
    session['read_total'] = read_int64(packet)
    session['write_current'] = read_int64(packet)
    session['write_last5min'] = read_int64(packet)
    session['write_total'] = read_int64(packet)
    # version >= 5
    session['duration_current_service'] = read_int32(packet)
    session['duration_last5min_service'] = read_int64(packet)
    session['duration_all_service'] = read_int32(packet)
    session['current_service_name'] = await read_string(packet)
    # version >= 6
    session['cpu_time_current'] = read_int64(packet)
    session['cpu_time_last5min'] = read_int64(packet)
    session['cpu_time_total'] = read_int64(packet)
    # version >= 7
    session['data_separation'] = await read_string(packet)
    # version >= 10
    session['client_ip_address'] = await read_string(packet)
    return session


async def read_packet(reader):
    packet_type = PacketType(await reader.read(1))
    packet_size = await unpack_varint_base128(reader)
    packet_data = (await reader.readexactly(packet_size))
    packet = io.BytesIO(packet_data)
    packet_array = []
    if packet_type == PacketType.ENDPOINT_OPEN_ACK:
        packet_array.append(await read_bytes(packet))
        packet_array.append(await read_bytes(packet))
        packet_array.append(await unpack_varint_base64(packet))
    elif packet_type == PacketType.ENDPOINT_FAILURE:
        service_id = await read_string(packet)
        version = await read_string(packet)
        endpoint_id = await unpack_varint_base128(packet)
        class_cause = await read_bytes(packet)
        message = await read_string(packet)
        raise MessageException(service_id, message)
    elif packet_type == PacketType.ENDPOINT_MESSAGE:
        endpoint_id = await unpack_varint_base128(packet)
        endpoint_format = packet.read(2)
        endpoint_data_type = EndpointDataType(packet.read(1))
        if endpoint_data_type == EndpointDataType.EXCEPTION:
            service_id = await read_string(packet)
            message = await read_string(packet)
            raise MessageException(service_id, message)
        if endpoint_data_type == EndpointDataType.MESSAGE:
            raw_ras_data_type = packet.read(1)
            ras_data_type = MessageType(raw_ras_data_type)
            if ras_data_type == MessageType.GET_CLUSTER_INFO_RESPONSE:
                cluster = await read_cluster(packet)
                packet_array.append(cluster)
            elif ras_data_type == MessageType.GET_INFOBASE_INFO_RESPONSE:
                infobase = await read_infobase(packet)
                packet_array.append(infobase)
            else:
                ras_data_count = await unpack_varint_base128(packet)
                for ras_data_number in range(ras_data_count):
                    if ras_data_type == MessageType.GET_CLUSTERS_RESPONSE:
                        cluster = await read_cluster(packet)
                        packet_array.append(cluster)
                    elif ras_data_type == MessageType.GET_INFOBASES_SHORT_RESPONSE:
                        infobase = read_infobase_short(packet)
                        packet_array.append(infobase)
                    elif ras_data_type in [MessageType.GET_INFOBASE_SESSIONS_RESPONSE,
                                           MessageType.GET_SESSIONS_RESPONSE]:
                        session = await read_session(packet)
                        packet_array.append(session)

    return packet_type, packet_array


def send_packet(writer, packet):
    header, body = packet.get_parts()
    writer.write(header)
    writer.write(body)


async def ras_command(ras_args):
    reader, writer = await asyncio.open_connection(
        ras_args.ras_host, ras_args.ras_port)
    data = struct.pack(">ihh", 475223888, 256, 256)  # magic numbers voodoo numbers
    writer.write(data)
    packet = Packet(PacketType.CONNECT)
    packet.append_raw(b'\x01')
    packet.append(b'connect.timeout')
    packet.append_raw(ParamType.INT.value + struct.pack(">i", 2000))
    send_packet(writer, packet)
    packet_type, packet_array = await read_packet(reader)
    assert packet_type == PacketType.CONNECT_ACK
    assert len(packet_array) == 0

    packet = Packet(PacketType.ENDPOINT_OPEN)
    packet.append(b'v8.service.Admin.Cluster')
    packet.append(b'10.0')
    send_packet(writer, packet)
    packet_type, packet_array = await read_packet(reader)
    assert packet_type == PacketType.ENDPOINT_OPEN_ACK
    assert packet_array[0] == b'v8.service.Admin.Cluster'
    assert packet_array[1] == b'10.0'
    endpoint_id = packet_array[2]

    if ras_args.command == 'cluster':
        if ras_args.subcommand1 == 'list':
            packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.GET_CLUSTERS_REQUEST)
            send_packet(writer, packet)
            packet_type, packet_array = await read_packet(reader)
            assert packet_type == PacketType.ENDPOINT_MESSAGE
            pp(packet_array)
        elif ras_args.subcommand1 == 'info' or ras_args.subcommand1 == 'update':
            cluster_id = uuid.UUID(ras_args.cluster).bytes
            packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.GET_CLUSTER_INFO_REQUEST)
            packet.append_raw(cluster_id)
            send_packet(writer, packet)
            packet_type, packet_array = await read_packet(reader)
            assert packet_type == PacketType.ENDPOINT_MESSAGE
            if ras_args.subcommand1 == 'info':
                pp(packet_array)
            else:
                cluster = packet_array[0]
                if ras_args.lifetime_limit is not None:
                    cluster['lifetime-limit'] = ras_args.lifetime_limit
                if ras_args.expiration_timeout is not None:
                    cluster['expiration-timeout'] = ras_args.expiration_timeout
                if ras_args.name is not None:
                    cluster['name'] = ras_args.name
                if ras_args.agent_user is not None and ras_args.agent_pwd is not None:
                    packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.AUTHENTICATE_AGENT_REQUEST)
                    packet.append(ras_args.agent_user.encode())
                    packet.append(ras_args.agent_pwd.encode())
                    send_packet(writer, packet)
                    packet_type, packet_array = await read_packet(reader)
                    assert packet_type == PacketType.ENDPOINT_MESSAGE
                packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.REG_CLUSTER_REQUEST)
                write_cluster(cluster, packet)
                send_packet(writer, packet)
                packet_type, packet_array = await read_packet(reader)
                assert packet_type == PacketType.ENDPOINT_MESSAGE


    if hasattr(ras_args, 'cluster_user') and hasattr(ras_args,
                                                     'cluster_pwd') and ras_args.cluster_user is not None and ras_args.cluster_pwd is not None:
        packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.AUTHENTICATE_REQUEST)
        packet.append_raw(uuid.UUID(ras_args.cluster).bytes)
        packet.append(ras_args.cluster_user.encode())
        packet.append(ras_args.cluster_pwd.encode())
        send_packet(writer, packet)
        packet_type, packet_array = await read_packet(reader)
        assert packet_type == PacketType.ENDPOINT_MESSAGE

    if ras_args.command == 'infobase':
        cluster_id = uuid.UUID(ras_args.cluster).bytes
        if ras_args.subcommand1 == 'summary' and ras_args.subcommand2 == 'list':
            packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.GET_INFOBASES_SHORT_REQUEST)
            packet.append_raw(cluster_id)
            send_packet(writer, packet)
            packet_type, packet_array = await read_packet(reader)
            assert packet_type == PacketType.ENDPOINT_MESSAGE
            pp(packet_array)

        if hasattr(ras_args, 'infobase_user') and hasattr(ras_args, 'infobase_pwd') and ras_args.infobase_user is not None and ras_args.infobase_pwd is not None:
            packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.ADD_AUTHENTICATION_REQUEST)
            packet.append_raw(cluster_id)
            packet.append(ras_args.infobase_user.encode())
            packet.append(ras_args.infobase_pwd.encode())
            send_packet(writer, packet)
            packet_type, packet_array = await read_packet(reader)
            assert packet_type == PacketType.ENDPOINT_MESSAGE

        if ras_args.subcommand1 == 'info' or ras_args.subcommand1 == 'update':
            packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.GET_INFOBASE_INFO_REQUEST)
            packet.append_raw(cluster_id)
            packet.append_raw(uuid.UUID(ras_args.infobase).bytes)
            send_packet(writer, packet)
            packet_type, packet_array = await read_packet(reader)
            assert packet_type == PacketType.ENDPOINT_MESSAGE
            if ras_args.subcommand1 == 'update':
                packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.UPDATE_INFOBASE_REQUEST)
                packet.append_raw(cluster_id)
                infobase = packet_array[0]
                if ras_args.descr is not None:
                    infobase['descr'] = ras_args.descr
                if ras_args.denied_message is not None:
                    infobase['denied_message'] = ras_args.denied_message
                if ras_args.permission_code is not None:
                    infobase['permission_code'] = ras_args.permission_code
                if ras_args.sessions_deny in ['on', 'off']:
                    infobase['sessions_denied'] = ras_args.sessions_deny == 'on'
                if ras_args.scheduled_jobs_deny in ['on', 'off']:
                    infobase['scheduled_jobs_denied'] = ras_args.scheduled_jobs_deny == 'on'
                if ras_args.denied_from is not None:
                    infobase['denied_from'] = ras_args.denied_from
                if ras_args.denied_to is not None:
                    infobase['denied_to'] = ras_args.denied_to
                write_infobase(infobase, packet)
                send_packet(writer, packet)
                packet_type, packet_array = await read_packet(reader)
                assert packet_type == PacketType.ENDPOINT_MESSAGE
            else:
                pp(packet_array)

    if ras_args.command == 'session':
        cluster_id = uuid.UUID(ras_args.cluster).bytes
        session_ids = []
        session_info = {}
        if ras_args.subcommand1 == 'list' or (ras_args.subcommand1 == 'terminate' and not ras_args.session):
            if hasattr(ras_args, 'infobase') and ras_args.infobase:
                packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.GET_INFOBASE_SESSIONS_REQUEST)
                packet.append_raw(cluster_id)
                packet.append_raw(uuid.UUID(ras_args.infobase).bytes)
                send_packet(writer, packet)
                packet_type, packet_array = await read_packet(reader)
                assert packet_type == PacketType.ENDPOINT_MESSAGE
                if ras_args.subcommand1 == 'list':
                    pp(packet_array)
                else:
                    session_ids.extend(
                        [session['session_id'].bytes for session in packet_array if session['app_id'] != 'RAS'])
                    session_info.update({session['session_id'].bytes: session for session in packet_array})
            else:
                packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.GET_SESSIONS_REQUEST)
                packet.append_raw(cluster_id)
                send_packet(writer, packet)
                packet_type, packet_array = await read_packet(reader)
                assert packet_type == PacketType.ENDPOINT_MESSAGE
                if ras_args.subcommand1 == 'list':
                    pp(packet_array)
                else:
                    session_ids.extend(
                        [session['session_id'].bytes for session in packet_array if session['app_id'] != 'RAS'])
                    session_info.update({session['session_id'].bytes: session for session in packet_array})
        if ras_args.subcommand1 == 'terminate':
            if ras_args.session:
                session_id = uuid.UUID(ras_args.session).bytes
                session_ids.append(session_id)
            for session_id in session_ids:
                packet = Packet(PacketType.ENDPOINT_MESSAGE, MessageType.TERMINATE_SESSION_REQUEST)
                packet.append_raw(cluster_id)
                packet.append_raw(session_id)
                if hasattr(ras_args, 'error_message') and ras_args.error_message:
                    packet.append(ras_args.error_message.encode())
                else:
                    packet.append(b'Session terminated by admin')
                send_packet(writer, packet)
                try:
                    packet_type, packet_array = await read_packet(reader)
                    assert packet_type == PacketType.ENDPOINT_MESSAGE
                    if session_id in session_info.keys():
                        print("Terminated session", uuid.UUID(bytes=session_id), session_info[session_id]['app_id'])
                    else:
                        print("Terminated session", uuid.UUID(bytes=session_id))
                except MessageException as e:
                    print("Can't terminate session", uuid.UUID(bytes=session_id), "-", e)

    packet = Packet(PacketType.ENDPOINT_CLOSE)
    packet.append(pack_varint_base64(endpoint_id))
    send_packet(writer, packet)
    packet = Packet(PacketType.DISCONNECT)
    send_packet(writer, packet)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--ras-host', default='localhost',
                        help='адрес сервера администрирования (по-умолчанию: localhost)')
    parser.add_argument('--ras-port', default=1545, type=int,
                        help='порт сервера администрирования (по-умолчанию: 1545)')

    sub_parsers = parser.add_subparsers(help='Группы команд', dest='command', required=True)

    parser_cluster = sub_parsers.add_parser('cluster', help='Режим администрирования кластера серверов')
    cluster_sub_parsers = parser_cluster.add_subparsers(help='Команды администрирования кластера серверов',
                                                        required=True, dest='subcommand1')
    parser_cluster_list = cluster_sub_parsers.add_parser('list', help='получение списка информации о кластерах')
    parser_cluster_info = cluster_sub_parsers.add_parser('info', help='получение информации о кластере')
    parser_cluster_info.add_argument('--cluster',
                                     help='идентификатор кластера серверов', required=True)
    parser_cluster_update = cluster_sub_parsers.add_parser('update', help='обновление параметров кластера')
    parser_cluster_update.add_argument('--cluster',
                                     help='идентификатор кластера серверов', required=True)
    parser_cluster_update.add_argument('--agent-user',
                                 help='имя администратора агента кластера', required=False)
    parser_cluster_update.add_argument('--agent-pwd',
                                 help='пароль администратора агента кластера', required=False)
    parser_cluster_update.add_argument('--lifetime-limit', type=int,
                                       help='период перезапуска рабочих процессов кластера (в секундах)', required=False)
    parser_cluster_update.add_argument('--expiration-timeout', type=int,
                                       help='период принудительного завершения (в секундах)', required=False)
    parser_cluster_update.add_argument('--name',
                                       help='имя (представление) кластера',
                                       required=False)

    parser_infobase = sub_parsers.add_parser('infobase', help='Режим администрирования информационных баз')
    parser_infobase.add_argument('--cluster',
                                 help='идентификатор кластера серверов', required=True)
    parser_infobase.add_argument('--cluster-user',
                                 help='имя администратора кластера', required=False)
    parser_infobase.add_argument('--cluster-pwd',
                                 help='пароль администратора кластера', required=False)
    infobase_sub_parsers = parser_infobase.add_subparsers(help='Команды администрирования информационных баз',
                                                          required=True, dest='subcommand1')
    parser_infobase_summary = infobase_sub_parsers.add_parser('summary',
                                                              help='управление краткой информацией об информационных базах')
    infobase_summary_sub_parsers = parser_infobase_summary.add_subparsers(help='Дополнительные команды', required=True,
                                                                          dest='subcommand2')
    parser_infobase_summary_list = infobase_summary_sub_parsers.add_parser('list',
                                                                           help='получение списка краткой информации об информационных базах')
    parser_infobase_info = infobase_sub_parsers.add_parser('info', help='получение информации об информационной базе')
    parser_infobase_info.add_argument('--infobase',
                                      help='идентификатор информационной базы', required=True)
    parser_infobase_info.add_argument('--infobase-user',
                                      help='имя администратора информационной базы', required=False)
    parser_infobase_info.add_argument('--infobase-pwd',
                                      help='пароль администратора информационной базы', required=False)
    parser_infobase_update = infobase_sub_parsers.add_parser('update',
                                                             help='обновление информации об информационной базе')
    parser_infobase_update.add_argument('--infobase',
                                        help='идентификатор информационной базы', required=True)
    parser_infobase_update.add_argument('--infobase-user',
                                        help='имя администратора информационной базы', required=False)
    parser_infobase_update.add_argument('--infobase-pwd',
                                        help='пароль администратора информационной базы', required=False)
    parser_infobase_update.add_argument('--descr',
                                        help='описание информационной базы', required=False)
    parser_infobase_update.add_argument('--denied-from',
                                        help='начало интервала времени, в течение которого действует режим блокировки сеансов, YYYY-MM-DD HH:mm:ss',
                                        type=lambda s: datetime.datetime.fromisoformat(s).replace(tzinfo=datetime.timezone.utc) if len(s) > 0 else 0,
                                        required=False)
    parser_infobase_update.add_argument('--denied-to',
                                        help='конец интервала времени, в течение которого действует режим блокировки сеансов, YYYY-MM-DD HH:mm:ss',
                                        type=lambda s: datetime.datetime.fromisoformat(s).replace(tzinfo=datetime.timezone.utc) if len(s) > 0 else 0,
                                        required=False)
    parser_infobase_update.add_argument('--denied-message',
                                        help='сообщение, выдаваемое при попытке нарушения блокировки сеансов',
                                        required=False)
    parser_infobase_update.add_argument('--denied-parameter',
                                        help='параметр блокировки сеансов',
                                        required=False)
    parser_infobase_update.add_argument('--permission-code',
                                        help='код разрешения, разрешающий начало сеанса вопреки действующей блокировке сеансов',
                                        required=False)
    parser_infobase_update.add_argument('--sessions-deny', choices=['on', 'off'],
                                        help="""управление режимом блокировки сеансов:
                on - режим блокировки начала сеансов включен
                off - режим блокировки начала сеансов выключен""",
                                        required=False)
    parser_infobase_update.add_argument('--scheduled-jobs-deny', choices=['on', 'off'],
                                        help="""управление блокировкой выполнения регламентных заданий:
                on - выполнение регламентных заданий запрещено
                off - выполнение регламентных заданий разрешено""",
                                        required=False)

    parser_session = sub_parsers.add_parser('session', help='Режим администрирования сеансов информационных баз')
    parser_session.add_argument('--cluster',
                                help='идентификатор кластера серверов', required=True)
    parser_session.add_argument('--cluster-user',
                                help='имя администратора кластера', required=False)
    parser_session.add_argument('--cluster-pwd',
                                help='пароль администратора кластера', required=False)
    session_sub_parsers = parser_session.add_subparsers(help='Команды администрирования сеансов информационных баз',
                                                        required=True, dest='subcommand1')
    parser_session_list = session_sub_parsers.add_parser('list',
                                                         help='получение списка информации о сеансах')
    parser_session_list.add_argument('--infobase',
                                     help='идентификатор информационной базы')
    parser_session_list.add_argument('--licenses', action='store_true',
                                     help='вывод информации о лицензиях, полученных сеансом')
    parser_session_terminate = session_sub_parsers.add_parser('terminate',
                                                              help='принудительное завершение сеанса')
    parser_session_terminate.add_argument('--session',
                                          help='идентификатор сеанса информационной базы')
    parser_session_terminate.add_argument('--infobase',
                                          help='идентификатор информационной базы')
    parser_session_terminate.add_argument('--error-message',
                                          help='сообщение о причине завершения сеанса')

    args = parser.parse_args()
    asyncio.run(ras_command(args))
