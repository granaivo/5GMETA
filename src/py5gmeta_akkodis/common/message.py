class EventMessage(object):
    """
    Message record
    Args:
        message_timestamp
        correlation_id
        redelivered
        reply_to
        destination
        message_id
        mode
        type
        priority
        payload
        properties: {}
    """
    def __init__(self,  correlation_id='',
                    redelivered=False, reply_to='', destination='', message_id='',
                    mode=4, otype='', priority=1, payload='', properties=None): #{'source_id': 's0'}
        self.correlation_id = correlation_id
        self.redelivered = redelivered
        self.reply_to = reply_to
        self.destination = destination
        self.message_id = message_id
        self.mode = mode
        self.type   = otype
        self.priority = priority
        self.payload = payload
        self.properties = properties
        print(self.payload)
        print(self.properties)