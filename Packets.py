PROTOCOL_NAME: str = 'MQTT'
PROTOCOL_VERSION: int = 5


class Type:
    CONNECT = 1
    CONNACK = 2
    PUBLISH = 3
    PUBACK = 4
    PUBREC = 5
    PUBREL = 6
    PUBCOMP = 7
    SUBSCRIBE = 8
    SUBACK = 9
    UNSUBSCRIBE = 10
    UNSUBACK = 11
    PINGREQ = 12
    PINGRESP = 13
    DISCONNECT = 14
    AUTH = 15


class ReasonCode:
    SUCCESS = 0
    NORMAL_DISCONNECTION = 0
    GRANTED_QOS_0 = 0
    GRANTED_QOS_1 = 1
    GRANTED_QOS_2 = 2
    DISCONNECT_WITH_WILL_MESSAGE = 4
    NO_MATCHING_SUBSCRIBERS = 16
    NO_SUBSCRIPTION_EXISTED = 17
    CONTINUE_AUTHENTICATION = 24
    RE_AUTHENTICATE = 25
    UNSPECIFIED_ERROR = 128
    MALFORMED_PACKET = 129
    PROTOCOL_ERROR = 130
    IMPLEMENTATION_SPECIFIC_ERROR = 131
    UNSUPORTED_PROTOCOL_VERSION = 132
    CLIENT_IDENTIFIER_NOT_VALID = 133
    BAD_USER_NAME_OR_PASSWORD = 134
    NOT_AUTHORIZED = 135
    SERVER_UNAVAILABLE = 136
    SERVER_BUSY = 137
    BANNED = 138
    SERVER_SHUTTING_DOWN = 139
    BAD_AUTHENTICATION_METHOD = 140
    KEEP_ALIVE_TIMEOUT = 141
    SESSION_TAKEN_OVER = 142
    TOPIC_FILTER_INVALID = 143
    TOPIC_NAME_INVALID = 144
    PACKET_IDENTIFIER_IN_USE = 145
    PACKET_IDENTIFIER_NOT_FOUND = 146
    TOPIC_ALIAS_INVALID = 148
    PACKET_TOO_LARGE = 149
    MESSAGE_RATE_TOO_HIGH = 150
    QUOTA_EXCEEDED = 151
    ADMINISTRATIVE_ACTION = 152
    PAYLOAD_FORMAT_INVALID = 153
    RETAIN_NOT_SUPPORTED = 154
    QOS_NOT_SUPPORTED = 155
    USE_ANOTHER_SERVER = 156
    SERVER_MOVED = 157
    SHARED_SUBSCRIPTIONS_NOT_SUPPORTED = 158
    CONNECTION_RATE_EXCEEDED = 159
    MAXIMUM_CONNECT_TIME = 160
    SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED = 161
    WILDCARD_SUBSCRIPTIONS_NOT_SUPPORTED = 162


class Packet:
    def __init__(self, packet_type: int):
        # Fixed Header
        self.type = packet_type
        self.flag = _FLAG[self.type]


class UserProperties(tuple):
    def __new__(cls, *user_property, **kw_user_property):
        property_tuple = ()
        for value in user_property:
            property_tuple += ((str(value[0]), str(value[1])),)
        for key, value in kw_user_property.items():
            property_tuple += ((key, str(value)),)
        return super(UserProperties, cls).__new__(cls, property_tuple)


class QoSPacket(Packet):
    def __init__(
            self, packet_type, packet_identifier: int = 0, reason_code: int = None, reason_string: str = None,
            user_properties: UserProperties = None
    ):
        super().__init__(packet_type=packet_type)

        # Variable Header
        self.packetIdentifier = packet_identifier
        self.reasonCode = reason_code

        # Properties
        self.reasonString = reason_string
        self.userProperties = user_properties


class Subscription:
    def __init__(self, topic: str, qos: int = 0, nl: bool = False, rap: bool = False, retain_handling: int = 0):
        self.topic = topic
        self.QoS = qos
        self.NL = nl
        self.RAP = rap
        self.retainHandling = retain_handling


class Will:
    def __init__(
            self, topic: str = '', payload: bytes = b'', qos: int = 0, retain: bool = False,
            will_delay_interval: int = None, payload_format_indicator: bool = None, message_expiry_interval: int = None,
            content_type: str = None, response_topic: str = None, correlation_data: bytes = None,
            user_properties: UserProperties = None
    ):
        # Payload
        self.topic = topic
        self.payload = payload

        # Flags
        self.QoS = qos
        self.retain = retain

        # Properties
        self.willDelayInterval = will_delay_interval
        self.payloadFormatIndicator = payload_format_indicator
        self.messageExpiryInterval = message_expiry_interval
        self.contentType = content_type
        self.responseTopic = response_topic
        self.correlationData = correlation_data
        self.userProperties = user_properties


class CONNECT(Packet):
    def __init__(
            self, client_id: str = '', username: str = None, password: bytes = None, will: Will = None,
            clean_start: bool = False, keep_alive: int = 0, session_expiry_interval: int = None,
            receive_maximum: int = None, maximum_packet_size: int = None, topic_alias_maximum: int = None,
            request_response_information: bool = None, request_problem_information: bool = None,
            user_properties: UserProperties = None, authentication_method: str = None, authentication_data: bytes = None
    ):
        super().__init__(packet_type=Type.CONNECT)

        # Payload
        self.clientID = client_id
        self.username = username
        self.password = password

        # Flags
        self.will = will
        self.cleanStart = clean_start

        # Variable Header
        self.keepAlive = keep_alive

        # Properties
        self.sessionExpiryInterval = session_expiry_interval
        self.receiveMaximum = receive_maximum
        self.maximumPacketSize = maximum_packet_size
        self.topicAliasMaximum = topic_alias_maximum
        self.requestResponseInformation = request_response_information
        self.requestProblemInformation = request_problem_information
        self.userProperties = user_properties
        self.authenticationMethod = authentication_method
        self.authenticationData = authentication_data


class CONNACK(Packet):
    def __init__(
            self, reason_code: int = ReasonCode.SUCCESS, session_present: bool = False,
            session_expiry_interval: int = None, receive_maximum: int = None, maximum_qos: int = None,
            retain_available: bool = None, maximum_packet_size: int = None, assigned_client_identifier: str = None,
            topic_alias_maximum: int = None, reason_string: str = None, user_properties: UserProperties = None,
            wildcard_subscription_available: bool = None,
            subscription_identifiers_available: bool = None, shared_subscription_available: bool = None,
            server_keep_alive: int = None, response_information: str = None, server_reference: str = None,
            authentication_method: str = None, authentication_data: bytes = None
    ):
        super().__init__(packet_type=Type.CONNACK)

        # Flags
        self.sessionPresent = session_present

        # Variable Header
        self.reasonCode = reason_code

        # Properties
        self.sessionExpiryInterval = session_expiry_interval
        self.receiveMaximum = receive_maximum
        self.maximumQoS = maximum_qos
        self.retainAvailable = retain_available
        self.maximumPacketSize = maximum_packet_size
        self.assignedClientIdentifier = assigned_client_identifier
        self.topicAliasMaximum = topic_alias_maximum
        self.reasonString = reason_string
        self.userProperties = user_properties
        self.wildcardSubscriptionAvailable = wildcard_subscription_available
        self.subscriptionIdentifiersAvailable = subscription_identifiers_available
        self.sharedSubscriptionAvailable = shared_subscription_available
        self.serverKeepAlive = server_keep_alive
        self.responseInformation = response_information
        self.serverReference = server_reference
        self.authenticationMethod = authentication_method
        self.authenticationData = authentication_data


class PUBLISH(Packet):
    def __init__(
            self, topic: str = '', payload: bytes = None, packet_identifier: int = 0, dup: bool = False, qos: int = 0,
            retain: bool = False, payload_format_indicator: bool = None, message_expiry_interval: int = None,
            topic_alias: int = None, response_topic: str = None, correlation_data: bytes = None,
            user_properties: UserProperties = None, subscription_identifier: int = None, content_type: str = None
    ):
        super().__init__(packet_type=Type.PUBLISH)

        # Fixed Header
        self.DUP = dup
        self.QoS = qos
        self.RETAIN = retain

        # Payload
        self.payload = payload

        # Variable Header
        self.topic = topic
        self.packetIdentifier = packet_identifier

        # Properties
        self.payloadFormatIndicator = payload_format_indicator
        self.messageExpiryInterval = message_expiry_interval
        self.topicAlias = topic_alias
        self.responseTopic = response_topic
        self.correlationData = correlation_data
        self.userProperties = user_properties
        self.subscriptionIdentifier = subscription_identifier
        self.contentType = content_type


class PUBACK(QoSPacket):
    def __init__(
            self, packet_identifier: int = 0, reason_code: int = None, reason_string: str = None,
            user_properties: UserProperties = None
    ):
        super().__init__(
            packet_type=Type.PUBACK, packet_identifier=packet_identifier, reason_code=reason_code,
            reason_string=reason_string, user_properties=user_properties
        )


class PUBREC(QoSPacket):
    def __init__(
            self, packet_identifier: int = 0, reason_code: int = None, reason_string: str = None,
            user_properties: UserProperties = None
    ):
        super().__init__(
            packet_type=Type.PUBREC, packet_identifier=packet_identifier, reason_code=reason_code,
            reason_string=reason_string, user_properties=user_properties
        )


class PUBREL(QoSPacket):
    def __init__(
            self, packet_identifier: int = 0, reason_code: int = None, reason_string: str = None,
            user_properties: UserProperties = None
    ):
        super().__init__(
            packet_type=Type.PUBREL, packet_identifier=packet_identifier, reason_code=reason_code,
            reason_string=reason_string, user_properties=user_properties
        )


class PUBCOMP(QoSPacket):
    def __init__(
            self, packet_identifier: int = 0, reason_code: int = None, reason_string: str = None,
            user_properties: UserProperties = None
    ):
        super().__init__(
            packet_type=Type.PUBCOMP, packet_identifier=packet_identifier, reason_code=reason_code,
            reason_string=reason_string, user_properties=user_properties
        )


class SUBSCRIBE(Packet):
    def __init__(
            self, *subscriptions, packet_identifier: int = 0, subscription_identifier: int = None,
            user_properties: UserProperties = None
    ):
        super().__init__(packet_type=Type.SUBSCRIBE)

        # Payload
        self.subscriptions = subscriptions

        # Variable Header
        self.packetIdentifier = packet_identifier

        # Properties
        self.subscriptionIdentifier = subscription_identifier
        self.userProperties = user_properties


class SUBACK(Packet):
    def __init__(
            self, *reason_codes, packet_identifier: int = 0, reason_string: str = None,
            user_properties: UserProperties = None
    ):
        super().__init__(packet_type=Type.SUBACK)

        # Payload
        self.reasonCodes = reason_codes

        # Variable Header
        self.packetIdentifier = packet_identifier

        # Properties
        self.reasonString = reason_string
        self.userProperties = user_properties


class UNSUBSCRIBE(Packet):
    def __init__(
            self, *topics, packet_identifier: int = 0, user_properties: UserProperties = None
    ):
        super().__init__(packet_type=Type.UNSUBSCRIBE)

        # Payload
        self.topics = topics

        # Variable Header
        self.packetIdentifier = packet_identifier

        # Properties
        self.userProperties = user_properties


class UNSUBACK(Packet):
    def __init__(
            self, *reason_codes, packet_identifier: int = 0, reason_string: str = None,
            user_properties: UserProperties = None
    ):
        super().__init__(packet_type=Type.UNSUBACK)

        # Payload
        self.reasonCodes = reason_codes

        # Variable Header
        self.packetIdentifier = packet_identifier

        # Properties
        self.reasonString = reason_string
        self.userProperties = user_properties


class PINGREQ(Packet):
    def __init__(self):
        super().__init__(packet_type=Type.PINGREQ)


class PINGRESP(Packet):
    def __init__(self):
        super().__init__(packet_type=Type.PINGRESP)


class DISCONNECT(Packet):
    def __init__(
            self, reason_code: int = None, session_expiry_interval: int = None, reason_string: str = None,
            user_properties: UserProperties = None, server_reference: str = None
    ):
        super().__init__(packet_type=Type.DISCONNECT)

        # Variable Header
        self.reasonCode = reason_code

        # Properties
        self.sessionExpiryInterval = session_expiry_interval
        self.reasonString = reason_string
        self.userProperties = user_properties
        self.serverReference = server_reference


class AUTH(Packet):
    def __init__(
            self, reason_code: int = None, authentication_method: str = None, authentication_data: bytes = None,
            reason_string: str = None, user_properties: UserProperties = None
    ):
        super().__init__(packet_type=Type.AUTH)

        # Variable Header
        self.reasonCode = reason_code

        # Properties
        self.authenticationMethod = authentication_method
        self.authenticationData = authentication_data
        self.reasonString = reason_string
        self.userProperties = user_properties


class MalformedPacket(Exception):
    pass


_FLAG = {
    Type.CONNECT: 0b0000,
    Type.CONNACK: 0b0000,
    Type.PUBLISH: lambda dup, qos, retain: (dup << 3) + (qos << 1) + retain,
    Type.PUBACK: 0b0000,
    Type.PUBREC: 0b0000,
    Type.PUBREL: 0b0010,
    Type.PUBCOMP: 0b0000,
    Type.SUBSCRIBE: 0b0010,
    Type.SUBACK: 0b0000,
    Type.UNSUBSCRIBE: 0b0010,
    Type.UNSUBACK: 0b0000,
    Type.PINGREQ: 0b0000,
    Type.PINGRESP: 0b0000,
    Type.DISCONNECT: 0b0000,
    Type.AUTH: 0b0000
}


_PACKET_CLASS = {
    Type.CONNECT: CONNECT,
    Type.CONNACK: CONNACK,
    Type.PUBLISH: PUBLISH,
    Type.PUBACK: PUBACK,
    Type.PUBREC: PUBREC,
    Type.PUBREL: PUBREL,
    Type.PUBCOMP: PUBCOMP,
    Type.SUBSCRIBE: SUBSCRIBE,
    Type.SUBACK: SUBACK,
    Type.UNSUBSCRIBE: UNSUBSCRIBE,
    Type.UNSUBACK: UNSUBACK,
    Type.PINGREQ: PINGREQ,
    Type.PINGRESP: PINGRESP,
    Type.DISCONNECT: DISCONNECT,
    Type.AUTH: AUTH
}


class _PropertyIdentifier:
    PAYLOAD_FORMAT_INDICATOR = 1
    MESSAGE_EXPIRY_INTERVAL = 2
    CONTENT_TYPE = 3
    RESPONSE_TOPIC = 8
    CORRELATION_DATA = 9
    SUBSCRIPTION_IDENTIFIER = 11
    SESSION_EXPIRY_INTERVAL = 17
    ASSIGNED_CLIENT_IDENTIFIER = 18
    SERVER_KEEP_ALIVE = 19
    AUTHENTICATION_METHOD = 21
    AUTHENTICATION_DATA = 22
    REQUEST_PROBLEM_INFORMATION = 23
    WILL_DELAY_INTERVAL = 24
    REQUEST_RESPONSE_INFORMATION = 25
    RESPONSE_INFORMATION = 26
    SERVER_REFERENCE = 28
    REASON_STRING = 31
    RECEIVE_MAXIMUM = 33
    TOPIC_ALIAS_MAXIMUM = 34
    TOPIC_ALIAS = 35
    MAXIMUM_QOS = 36
    RETAIN_AVAILABLE = 37
    USER_PROPERTY = 38
    MAXIMUM_PACKET_SIZE = 39
    WILDCARD_SUBSCRIPTION_AVAILABLE = 40
    SUBSCRIPTION_IDENTIFIER_AVAILABLE = 41
    SHARED_SUBSCRIPTION_AVAILABLE = 42


_PROPERTY_NAME = {
    _PropertyIdentifier.PAYLOAD_FORMAT_INDICATOR: 'payloadFormatIndicator',
    _PropertyIdentifier.MESSAGE_EXPIRY_INTERVAL: 'messageExpiryInterval',
    _PropertyIdentifier.CONTENT_TYPE: 'contentType',
    _PropertyIdentifier.RESPONSE_TOPIC: 'responseTopic',
    _PropertyIdentifier.CORRELATION_DATA: 'correlationData',
    _PropertyIdentifier.SUBSCRIPTION_IDENTIFIER: 'subscriptionIdentifier',
    _PropertyIdentifier.SESSION_EXPIRY_INTERVAL: 'sessionExpiryInterval',
    _PropertyIdentifier.ASSIGNED_CLIENT_IDENTIFIER: 'assignedClientIdentifier',
    _PropertyIdentifier.SERVER_KEEP_ALIVE: 'serverKeepAlive',
    _PropertyIdentifier.AUTHENTICATION_METHOD: 'authenticationMethod',
    _PropertyIdentifier.AUTHENTICATION_DATA: 'authenticationData',
    _PropertyIdentifier.REQUEST_PROBLEM_INFORMATION: 'requestProblemInformation',
    _PropertyIdentifier.WILL_DELAY_INTERVAL: 'willDelayInterval',
    _PropertyIdentifier.REQUEST_RESPONSE_INFORMATION: 'requestResponseInformation',
    _PropertyIdentifier.RESPONSE_INFORMATION: 'responseInformation',
    _PropertyIdentifier.SERVER_REFERENCE: 'serverReference',
    _PropertyIdentifier.REASON_STRING: 'reasonString',
    _PropertyIdentifier.RECEIVE_MAXIMUM: 'receiveMaximum',
    _PropertyIdentifier.TOPIC_ALIAS_MAXIMUM: 'topicAliasMaximum',
    _PropertyIdentifier.TOPIC_ALIAS: 'topicAlias',
    _PropertyIdentifier.MAXIMUM_QOS: 'maximumQoS',
    _PropertyIdentifier.RETAIN_AVAILABLE: 'retainAvailable',
    _PropertyIdentifier.MAXIMUM_PACKET_SIZE: 'maximumPacketSize',
    _PropertyIdentifier.WILDCARD_SUBSCRIPTION_AVAILABLE: 'wildcardSubscriptionAvailable',
    _PropertyIdentifier.SUBSCRIPTION_IDENTIFIER_AVAILABLE: 'subscriptionIdentifiersAvailable',
    _PropertyIdentifier.SHARED_SUBSCRIPTION_AVAILABLE: 'sharedSubscriptionAvailable',
}

_PROPERTY_ENCODING = {
    _PropertyIdentifier.PAYLOAD_FORMAT_INDICATOR: lambda value: _encode_b(value),
    _PropertyIdentifier.MESSAGE_EXPIRY_INTERVAL: lambda value: _encode_b(value, 4),
    _PropertyIdentifier.CONTENT_TYPE: lambda value: _encode_utf8(value),
    _PropertyIdentifier.RESPONSE_TOPIC: lambda value: _encode_utf8(value),
    _PropertyIdentifier.CORRELATION_DATA: lambda value: _encode_bd(value),
    _PropertyIdentifier.SUBSCRIPTION_IDENTIFIER: lambda value: _encode_vbi(value),
    _PropertyIdentifier.SESSION_EXPIRY_INTERVAL: lambda value: _encode_b(value, 4),
    _PropertyIdentifier.ASSIGNED_CLIENT_IDENTIFIER: lambda value: _encode_utf8(value),
    _PropertyIdentifier.SERVER_KEEP_ALIVE: lambda value: _encode_b(value, 2),
    _PropertyIdentifier.AUTHENTICATION_METHOD: lambda value: _encode_utf8(value),
    _PropertyIdentifier.AUTHENTICATION_DATA: lambda value: _encode_bd(value),
    _PropertyIdentifier.REQUEST_PROBLEM_INFORMATION: lambda value: _encode_b(value),
    _PropertyIdentifier.WILL_DELAY_INTERVAL: lambda value: _encode_b(value, 4),
    _PropertyIdentifier.REQUEST_RESPONSE_INFORMATION: lambda value: _encode_b(value),
    _PropertyIdentifier.RESPONSE_INFORMATION: lambda value: _encode_utf8(value),
    _PropertyIdentifier.SERVER_REFERENCE: lambda value: _encode_utf8(value),
    _PropertyIdentifier.REASON_STRING: lambda value: _encode_utf8(value),
    _PropertyIdentifier.RECEIVE_MAXIMUM: lambda value: _encode_b(value, 2),
    _PropertyIdentifier.TOPIC_ALIAS_MAXIMUM: lambda value: _encode_b(value, 2),
    _PropertyIdentifier.TOPIC_ALIAS: lambda value: _encode_b(value, 2),
    _PropertyIdentifier.MAXIMUM_QOS: lambda value: _encode_b(value),
    _PropertyIdentifier.RETAIN_AVAILABLE: lambda value: _encode_b(value),
    _PropertyIdentifier.USER_PROPERTY: lambda value: _encode_utf8_pair(value[0], value[1]),
    _PropertyIdentifier.MAXIMUM_PACKET_SIZE: lambda value: _encode_b(value, 4),
    _PropertyIdentifier.WILDCARD_SUBSCRIPTION_AVAILABLE: lambda value: _encode_b(value),
    _PropertyIdentifier.SUBSCRIPTION_IDENTIFIER_AVAILABLE: lambda value: _encode_b(value),
    _PropertyIdentifier.SHARED_SUBSCRIPTION_AVAILABLE: lambda value: _encode_b(value)
}

_PROPERTY_DECODING = {
    _PropertyIdentifier.PAYLOAD_FORMAT_INDICATOR: lambda bytedata: (bool(_decode_b(bytedata)), 1),
    _PropertyIdentifier.MESSAGE_EXPIRY_INTERVAL: lambda bytedata: (_decode_b(bytedata, 4), 4),
    _PropertyIdentifier.CONTENT_TYPE: lambda bytedata: _decode_utf8(bytedata),
    _PropertyIdentifier.RESPONSE_TOPIC: lambda bytedata: _decode_utf8(bytedata),
    _PropertyIdentifier.CORRELATION_DATA: lambda bytedata: _decode_bd(bytedata),
    _PropertyIdentifier.SUBSCRIPTION_IDENTIFIER: lambda bytedata: _decode_vbi(bytedata),
    _PropertyIdentifier.SESSION_EXPIRY_INTERVAL: lambda bytedata: (_decode_b(bytedata, 4), 4),
    _PropertyIdentifier.ASSIGNED_CLIENT_IDENTIFIER: lambda bytedata: _decode_utf8(bytedata),
    _PropertyIdentifier.SERVER_KEEP_ALIVE: lambda bytedata: (_decode_b(bytedata, 2), 2),
    _PropertyIdentifier.AUTHENTICATION_METHOD: lambda bytedata: _decode_utf8(bytedata),
    _PropertyIdentifier.AUTHENTICATION_DATA: lambda bytedata: _decode_bd(bytedata),
    _PropertyIdentifier.REQUEST_PROBLEM_INFORMATION: lambda bytedata: (bool(_decode_b(bytedata)), 1),
    _PropertyIdentifier.WILL_DELAY_INTERVAL: lambda bytedata: (_decode_b(bytedata, 4), 4),
    _PropertyIdentifier.REQUEST_RESPONSE_INFORMATION: lambda bytedata: (bool(_decode_b(bytedata)), 1),
    _PropertyIdentifier.RESPONSE_INFORMATION: lambda bytedata: _decode_utf8(bytedata),
    _PropertyIdentifier.SERVER_REFERENCE: lambda bytedata: _decode_utf8(bytedata),
    _PropertyIdentifier.REASON_STRING: lambda bytedata: _decode_utf8(bytedata),
    _PropertyIdentifier.RECEIVE_MAXIMUM: lambda bytedata: (_decode_b(bytedata, 2), 2),
    _PropertyIdentifier.TOPIC_ALIAS_MAXIMUM: lambda bytedata: (_decode_b(bytedata, 2), 2),
    _PropertyIdentifier.TOPIC_ALIAS: lambda bytedata: (_decode_b(bytedata, 2), 2),
    _PropertyIdentifier.MAXIMUM_QOS: lambda bytedata: (_decode_b(bytedata), 1),
    _PropertyIdentifier.RETAIN_AVAILABLE: lambda bytedata: (bool(_decode_b(bytedata)), 1),
    _PropertyIdentifier.USER_PROPERTY: lambda bytedata: _decode_utf8_pair(bytedata),
    _PropertyIdentifier.MAXIMUM_PACKET_SIZE: lambda bytedata: (_decode_b(bytedata, 4), 4),
    _PropertyIdentifier.WILDCARD_SUBSCRIPTION_AVAILABLE: lambda bytedata: (bool(_decode_b(bytedata)), 1),
    _PropertyIdentifier.SUBSCRIPTION_IDENTIFIER_AVAILABLE: lambda bytedata: (bool(_decode_b(bytedata)), 1),
    _PropertyIdentifier.SHARED_SUBSCRIPTION_AVAILABLE: lambda bytedata: (bool(_decode_b(bytedata)), 1)
}


class _Property:
    def __init__(self, identifier: int, value):
        self.identifier = identifier
        self.value = value


def _pack_properties(packet) -> tuple:
    properties = ()
    match packet.type:
        case Type.CONNECT:
            properties = (
                _Property(_PropertyIdentifier.SESSION_EXPIRY_INTERVAL, packet.sessionExpiryInterval),
                _Property(_PropertyIdentifier.RECEIVE_MAXIMUM, packet.receiveMaximum),
                _Property(_PropertyIdentifier.MAXIMUM_PACKET_SIZE, packet.maximumPacketSize),
                _Property(_PropertyIdentifier.TOPIC_ALIAS_MAXIMUM, packet.topicAliasMaximum),
                _Property(_PropertyIdentifier.REQUEST_RESPONSE_INFORMATION, packet.requestResponseInformation),
                _Property(_PropertyIdentifier.REQUEST_PROBLEM_INFORMATION, packet.requestProblemInformation),
                _Property(_PropertyIdentifier.AUTHENTICATION_METHOD, packet.authenticationMethod),
                _Property(_PropertyIdentifier.AUTHENTICATION_DATA, packet.authenticationData),
            )
        case Type.CONNACK:
            properties = (
                _Property(_PropertyIdentifier.SESSION_EXPIRY_INTERVAL, packet.sessionExpiryInterval),
                _Property(_PropertyIdentifier.RECEIVE_MAXIMUM, packet.receiveMaximum),
                _Property(_PropertyIdentifier.MAXIMUM_QOS, packet.maximumQoS),
                _Property(_PropertyIdentifier.RETAIN_AVAILABLE, packet.retainAvailable),
                _Property(_PropertyIdentifier.MAXIMUM_PACKET_SIZE, packet.maximumPacketSize),
                _Property(_PropertyIdentifier.ASSIGNED_CLIENT_IDENTIFIER, packet.assignedClientIdentifier),
                _Property(_PropertyIdentifier.TOPIC_ALIAS_MAXIMUM, packet.topicAliasMaximum),
                _Property(_PropertyIdentifier.REASON_STRING, packet.reasonString),
                _Property(_PropertyIdentifier.WILDCARD_SUBSCRIPTION_AVAILABLE, packet.wildcardSubscriptionAvailable),
                _Property(
                    _PropertyIdentifier.SUBSCRIPTION_IDENTIFIER_AVAILABLE, packet.subscriptionIdentifiersAvailable
                ),
                _Property(_PropertyIdentifier.SHARED_SUBSCRIPTION_AVAILABLE, packet.sharedSubscriptionAvailable),
                _Property(_PropertyIdentifier.SERVER_KEEP_ALIVE, packet.serverKeepAlive),
                _Property(_PropertyIdentifier.RESPONSE_INFORMATION, packet.responseInformation),
                _Property(_PropertyIdentifier.SERVER_REFERENCE, packet.serverReference),
                _Property(_PropertyIdentifier.AUTHENTICATION_METHOD, packet.authenticationMethod),
                _Property(_PropertyIdentifier.AUTHENTICATION_DATA, packet.authenticationData),
            )
        case Type.PUBLISH:
            properties = (
                _Property(_PropertyIdentifier.PAYLOAD_FORMAT_INDICATOR, packet.payloadFormatIndicator),
                _Property(_PropertyIdentifier.MESSAGE_EXPIRY_INTERVAL, packet.messageExpiryInterval),
                _Property(_PropertyIdentifier.TOPIC_ALIAS, packet.topicAlias),
                _Property(_PropertyIdentifier.RESPONSE_TOPIC, packet.responseTopic),
                _Property(_PropertyIdentifier.CORRELATION_DATA, packet.correlationData),
                _Property(_PropertyIdentifier.SUBSCRIPTION_IDENTIFIER, packet.subscriptionIdentifier),
                _Property(_PropertyIdentifier.CONTENT_TYPE, packet.contentType),
            )
        case Type.PUBACK | Type.PUBREC | Type.PUBREL | Type.PUBCOMP:
            properties = (
                _Property(_PropertyIdentifier.REASON_STRING, packet.reasonString),
            )
        case Type.SUBSCRIBE:
            properties = (
                _Property(_PropertyIdentifier.SUBSCRIPTION_IDENTIFIER, packet.subscriptionIdentifier),
            )
        case Type.SUBACK:
            properties = (
                _Property(_PropertyIdentifier.REASON_STRING, packet.reasonString),
            )
        case Type.UNSUBACK:
            properties = (
                _Property(_PropertyIdentifier.REASON_STRING, packet.reasonString),
            )
        case Type.DISCONNECT:
            properties = (
                _Property(_PropertyIdentifier.SESSION_EXPIRY_INTERVAL, packet.sessionExpiryInterval),
                _Property(_PropertyIdentifier.REASON_STRING, packet.reasonString),
                _Property(_PropertyIdentifier.SERVER_REFERENCE, packet.serverReference),
            )
        case Type.AUTH:
            properties = (
                _Property(_PropertyIdentifier.AUTHENTICATION_METHOD, packet.authenticationMethod),
                _Property(_PropertyIdentifier.AUTHENTICATION_DATA, packet.authenticationData),
                _Property(_PropertyIdentifier.REASON_STRING, packet.reasonString),
            )

    properties = tuple(item for item in properties if item.value is not None)
    if hasattr(packet, 'userProperties') and packet.userProperties is not None:
        properties += _pack_user_properties(packet.userProperties)
    return properties


def _pack_will_properties(will: Will) -> tuple:
    properties = (
        _Property(_PropertyIdentifier.WILL_DELAY_INTERVAL, will.willDelayInterval),
        _Property(_PropertyIdentifier.PAYLOAD_FORMAT_INDICATOR, will.payloadFormatIndicator),
        _Property(_PropertyIdentifier.MESSAGE_EXPIRY_INTERVAL, will.messageExpiryInterval),
        _Property(_PropertyIdentifier.RESPONSE_TOPIC, will.responseTopic),
        _Property(_PropertyIdentifier.CORRELATION_DATA, will.correlationData),
        _Property(_PropertyIdentifier.CONTENT_TYPE, will.contentType),
    )

    properties = tuple(item for item in properties if item.value is not None)
    if will.userProperties is not None:
        properties += _pack_user_properties(will.userProperties)
    return properties


def _pack_user_properties(user_properties: UserProperties) -> tuple:
    return tuple(
        _Property(_PropertyIdentifier.USER_PROPERTY, user_property) for user_property in user_properties
    )


def _unpack_properties(obj, properties: tuple) -> bool:
    try:
        for value in properties:
            if value.identifier != _PropertyIdentifier.USER_PROPERTY:
                if hasattr(obj, _PROPERTY_NAME[value.identifier]):
                    if getattr(obj, _PROPERTY_NAME[value.identifier]) is None:
                        setattr(obj, _PROPERTY_NAME[value.identifier], value.value)
                    else:
                        return False

        if hasattr(obj, 'userProperties'):
            obj.userProperties = _unpack_user_properties(properties)
        return True
    except KeyError:
        raise MalformedPacket


def _unpack_user_properties(properties: tuple) -> UserProperties:
    user_properties = ()
    for value in properties:
        if value.identifier == _PropertyIdentifier.USER_PROPERTY:
            user_properties += (value.value,)

    return UserProperties(*user_properties)


def encode(packet) -> bytes:
    bytedata = bytes()
    properties = _pack_properties(packet)

    # Fixed Header
    flag = packet.flag
    if callable(packet.flag) and packet.type == Type.PUBLISH:
        flag = flag(dup=packet.DUP, qos=packet.QoS, retain=packet.RETAIN)
    fixed_header = _encode_b(((packet.type & 0b1111) << 4) + (flag & 0b1111))

    # Variable Header
    match packet.type:
        case Type.CONNECT:
            bytedata += _encode_utf8(PROTOCOL_NAME)
            bytedata += _encode_b(PROTOCOL_VERSION)
            flags = (
                    (int(packet.username is not None) << 7) +
                    (int(packet.password is not None) << 6) +
                    (int(packet.cleanStart) << 1)
            )
            if packet.will is not None:
                flags += 1 << 2
                flags += int(packet.will.retain) << 5
                flags += (packet.will.QoS & 0b11) << 3
            bytedata += _encode_b(flags)
            bytedata += _encode_b(packet.keepAlive, 2)

        case Type.CONNACK:
            flags = int(packet.sessionPresent)
            bytedata += _encode_b(flags)
            bytedata += _encode_b(packet.reasonCode)

        case Type.PUBLISH:
            bytedata += _encode_utf8(packet.topic)
            if packet.QoS in (1, 2):
                bytedata += _encode_packet_identifier(packet.packetIdentifier)

        case (
            Type.PUBACK | Type.PUBREC | Type.PUBREL | Type.PUBCOMP |
            Type.SUBSCRIBE | Type.SUBACK | Type.UNSUBSCRIBE | Type.UNSUBACK
        ):
            bytedata += _encode_packet_identifier(packet.packetIdentifier)

    # packets with reason code not required
    if packet.type in (Type.PUBACK, Type.PUBREC, Type.PUBREL, Type.PUBCOMP, Type.DISCONNECT, Type.AUTH):
        if packet.reasonCode is None:
            if len(properties) > 0:
                bytedata += _encode_b(ReasonCode.SUCCESS)
        else:
            bytedata += _encode_b(packet.reasonCode)

    # Properties
    if packet.type not in (Type.PINGREQ, Type.PINGRESP):

        # packets with properties not required
        if (
            packet.type not in (Type.PUBACK, Type.PUBREC, Type.PUBREL, Type.PUBCOMP, Type.DISCONNECT, Type.AUTH) or
            len(properties) > 0
        ):
            bytedata += _encode_properties(properties)

    # Payload
    match packet.type:
        case Type.CONNECT:
            bytedata += _encode_utf8(packet.clientID)
            if packet.will is not None:
                bytedata += _encode_properties(_pack_will_properties(packet.will))
                bytedata += _encode_utf8(packet.will.topic)
                bytedata += _encode_bd(packet.will.payload)
            if packet.username is not None:
                bytedata += _encode_utf8(packet.username)
            if packet.password is not None:
                bytedata += _encode_bd(packet.password)

        case Type.PUBLISH:
            if packet.payload is not None:
                bytedata += packet.payload

        case Type.SUBSCRIBE:
            bytedata += _encode_subscriptions(packet.subscriptions)

        case Type.SUBACK | Type.UNSUBACK:
            for reasonCode in packet.reasonCodes:
                bytedata += _encode_b(reasonCode)

        case Type.UNSUBSCRIBE:
            for topic in packet.topics:
                bytedata += _encode_utf8(topic)

    return fixed_header + _encode_vbi(len(bytedata)) + bytedata


def decode(bytedata: bytes):
    packet_type = (bytedata[0] >> 4) & 0b1111
    packet = _PACKET_CLASS[packet_type]()

    if packet_type == Type.PUBLISH:
        packet.DUP = bool((bytedata[0] >> 3) & 0b1)
        packet.QoS = (bytedata[0] >> 1) & 0b11
        packet.RETAIN = bool(bytedata[0] & 0b1)
    else:
        if (bytedata[0] & 0b1111) != packet.flag:
            raise MalformedPacket

    length, size = _decode_vbi(bytedata[1:])
    fixed_header_length = size + 1

    packet_data = bytedata[fixed_header_length: fixed_header_length + length]
    byte = 0

    # Variable Header
    match packet.type:
        case Type.CONNECT:
            protocol_name, size = _decode_utf8(packet_data[byte:])
            byte += size
            if protocol_name != PROTOCOL_NAME:
                raise MalformedPacket
            protocol_version = _decode_b(packet_data[byte:])
            byte += 1
            if protocol_version != PROTOCOL_VERSION:
                raise MalformedPacket

            if (packet_data[byte] >> 7) & 0b1 == 1:
                packet.username = ''
            if (packet_data[byte] >> 6) & 0b1 == 1:
                packet.password = ''
            packet.cleanStart = bool((packet_data[byte] >> 1) & 0b1)
            if (packet_data[byte] >> 2) & 0b1 == 1:
                packet.will = Will()
                packet.will.retain = (packet_data[byte] >> 5) & 0b1
                packet.will.QoS = (packet_data[byte] >> 3) & 0b11
            byte += 1

            packet.keepAlive = _decode_b(packet_data[byte:], 2)
            byte += 2

        case Type.CONNACK:
            packet.sessionPresent = bool(packet_data[byte] & 0b1)
            byte += 1
            packet.reasonCode = _decode_b(packet_data[byte:])
            byte += 1

        case Type.PUBLISH:
            packet.topic, size = _decode_utf8(packet_data[byte:])
            byte += size
            if packet.QoS in (1, 2):
                packet.packetIdentifier, size = _decode_packet_identifier(packet_data[byte:])
                byte += size

        case (
            Type.PUBACK | Type.PUBREC | Type.PUBREL | Type.PUBCOMP |
            Type.SUBSCRIBE | Type.SUBACK | Type.UNSUBSCRIBE | Type.UNSUBACK
        ):
            packet.packetIdentifier, size = _decode_packet_identifier(packet_data[byte:])
            byte += size

    if packet.type in (Type.PUBACK, Type.PUBREC, Type.PUBREL, Type.PUBCOMP, Type.DISCONNECT, Type.AUTH):
        if length == 2:
            packet.reasonCode = 0
        else:
            packet.reasonCode = _decode_b(packet_data[byte:])
            byte += 1

    # Properties
    if packet.type not in (Type.PINGREQ, Type.PINGRESP):
        if packet.type not in (
            Type.PUBACK, Type.PUBREC, Type.PUBREL, Type.PUBCOMP, Type.DISCONNECT, Type.AUTH
        ) or length >= 4:
            properties, size = _decode_properties(packet_data[byte:])
            byte += size
            _unpack_properties(packet, properties)

    # Payload
    match packet.type:
        case Type.CONNECT:
            packet.clientID, size = _decode_utf8(packet_data[byte:])
            byte += size

            if packet.will is not None:
                properties, size = _decode_properties(packet_data[byte:])
                _unpack_properties(packet.will, properties)
                byte += size
                packet.will.topic, size = _decode_utf8(packet_data[byte:])
                byte += size
                packet.will.payload, size = _decode_bd(packet_data[byte:])
                byte += size
            if packet.username is not None:
                packet.username, size = _decode_utf8(packet_data[byte:])
                byte += size
            if packet.password is not None:
                packet.password, size = _decode_bd(packet_data[byte:])
                byte += size

        case Type.PUBLISH:
            packet.payload = packet_data[byte:]

        case Type.SUBSCRIBE:
            packet.subscriptions, _ = _decode_subscriptions(packet_data[byte:], length - byte)

        case Type.SUBACK | Type.UNSUBACK:
            while byte < length:
                packet.reasonCodes += (_decode_b(packet_data[byte:]),)
                byte += 1

        case Type.UNSUBSCRIBE:
            while byte < length:
                topic, size = _decode_utf8(packet_data[byte:])
                packet.topics += (topic,)
                byte += size

    return packet, fixed_header_length + length


def _encode_properties(properties: tuple) -> bytes:
    bytedata = bytes()
    for value in properties:
        bytedata += _encode_vbi(value.identifier)
        bytedata += _PROPERTY_ENCODING[value.identifier](value.value)
    return _encode_vbi(len(bytedata)) + bytedata


def _encode_subscriptions(subscriptions: tuple) -> bytes:
    bytedata = bytes()
    for subscription in subscriptions:
        bytedata += _encode_utf8(subscription.topic)
        bytedata += _encode_b(
            ((subscription.retainHandling & 0b11) << 4) +
            (int(subscription.RAP) << 3) +
            (int(subscription.NL) << 2) +
            (subscription.QoS & 0b11)
        )
    return bytedata


def _encode_packet_identifier(packet_identifier: int) -> bytes:
    return _encode_b(packet_identifier, 2)


def _encode_b(value: int, length: int = 1) -> bytes:
    return int.to_bytes(value, length, 'big')


def _encode_utf8(string: str) -> bytes:
    return len(string).to_bytes(2, 'big') + string.encode()


def _encode_utf8_pair(key: str, value: str) -> bytes:
    return _encode_utf8(key) + _encode_utf8(value)


def _encode_bd(data: bytes) -> bytes:
    return len(data).to_bytes(2, 'big') + data


def _encode_vbi(value: int) -> bytes:
    bytedata = bytes()
    while True:
        byte = value % 128
        value //= 128
        if value > 0:
            byte = byte | 128
        bytedata += byte.to_bytes(1)
        if value <= 0:
            break
    return bytedata


def _decode_properties(bytedata: bytes) -> (tuple, int):
    properties = ()
    length, offset = _decode_vbi(bytedata)
    byte = offset
    while byte < length + offset:
        property_id, size = _decode_vbi(bytedata[byte:])
        value, value_size = _PROPERTY_DECODING[property_id](bytedata[byte + size:])
        byte += size + value_size

        properties += (_Property(property_id, value),)

    return properties, byte


def _decode_subscriptions(bytedata: bytes, length: int) -> (tuple, int):
    subscriptions = ()
    byte = 0
    while byte < length:
        topic, topic_len = _decode_utf8(bytedata[byte:])
        options = _decode_b(bytedata[byte + topic_len:])
        byte += topic_len + 1

        subscriptions += (
            Subscription(
                topic=topic,
                qos=(options & 0b11),
                nl=bool((options >> 2) & 0b1),
                rap=bool((options >> 3) & 0b1),
                retain_handling=((options >> 4) & 0b11)
            ),
        )

    return subscriptions, byte


def _decode_packet_identifier(bytedata: bytes) -> (int, int):
    return _decode_b(bytedata, 2), 2


def _decode_b(bytedata: bytes, length: int = 1) -> int:
    return int.from_bytes(bytedata[:length], 'big', signed=False)


def _decode_utf8(bytedata: bytes) -> (str, int):
    length = _decode_b(bytedata, 2)
    return bytedata[2:length + 2].decode(), length + 2


def _decode_utf8_pair(bytedata: bytes) -> ((str, str), int):
    key, key_len = _decode_utf8(bytedata)
    value, value_len = _decode_utf8(bytedata[key_len:])
    return (key, value), key_len + value_len


def _decode_bd(bytedata: bytes) -> (bytes, int):
    length = _decode_b(bytedata, 2)
    return bytedata[2: length + 2], length + 2


def _decode_vbi(bytedata: bytes) -> (int, int):
    multiplier = 1
    value = 0
    byte = 0
    while True:
        value += (bytedata[byte] & 127) * multiplier
        multiplier *= 128
        if not (bytedata[byte] & 128):
            break
        byte += 1
    size = byte + 1
    if size > 4:
        raise MalformedPacket
    return value, size
