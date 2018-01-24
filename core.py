

class SubscriptionDefinition:
    
    # valid fields
    FIELDS = {
        "trades_speed",
        "order_book_speed",
        "order_book_levels",
        "emit_quotes"
    }
    
    def __init__(self, **kwargs):
        self.trades_speed = kwargs.get("trades_speed", 0)
        self.order_book_speed = kwargs.get("order_book_speed", 0)
        self.order_book_levels = kwargs.get("order_book_levels", 0)
        self.emit_quotes = kwargs.get("emit_quotes", 0)
    
    def __getitem__(self, key):
        return self.__dict__[key]
    
    def update(self, d):
        if isinstance(d, SubscriptionDefinition):
            d = d.__dict__
        for k in d.keys():
            if k in self.FIELDS:
                self.__dict__[k] = d[k]
        
    def empty(self):
        if self.trades_speed > 0:
            return False
        if self.order_book_speed > 0:
            return False
        if self.order_book_levels > 0:
            return False
        if self.emit_quotes:
            return False
        return True
    
    def __eq__(self, other):
        if not isinstance(other, SubscriptionDefinition):
            return False
        for k in self.FIELDS:
            if self.__dict__[k] != other.__dict__[k]:
                return False
        return True
        
    def __repr__(self):
        return "<SD {}>".format(repr(self.__dict__))
