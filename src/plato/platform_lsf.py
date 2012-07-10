from plato.metashell import Program


class BSub(Program):
    '''Job submission'''

    def __init__(self, lines, queue='1:00'):
        self.name = 'bsub'
        self.queue = queue
        self.lines = lines

    def compose(self):
        if not self.is_composed:
            pass
        return super(BSub, self).compose()