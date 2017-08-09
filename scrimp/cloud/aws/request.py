from scrimp import SimpleStringifiable


class Request(SimpleStringifiable):
    """
    Store the details of what is being requested.
    """

    def __init__(self, instance, instance_type, zone, ami, count, bid=0,
                 ondemand=False, odp=0, price=0, DrAFTS=0, AvgPrice=0,
                 OraclePrice=0):
        self.instance = instance
        self.instance_type = instance_type
        self.zone = zone
        self.ami = ami
        self.count = count
        self.bid = bid
        self.ondemand = ondemand
        self.odp = odp
        self.price = price
        self.DrAFTS = DrAFTS
        self.AvgPrice = AvgPrice
        self.OraclePrice = OraclePrice
