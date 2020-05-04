@staticmethod
    def send_lineage(operator, inlets, outlets, context):
        client = # connection object
        
        ## create types on atlas        

        ## create inlets list
        ## create inlets on atlas
        inlet_list = []
        inlet_list = ## function call
        
        ## create outlets list
        ## create outlets on atlas

        outlet_list = []
        outlet_list = ## function call

        ## same with operator


## abstract classes
class Connection:
    def __init__(self):
        _username = conf.get("atlan", "username")
        _password = conf.get("atlan", "password")
        _port = conf.get("atlan", "port")
        _host = conf.get("atlan", "host")
        Atlas(_host, port=_port, username=_username, password=_password)


## 'snowflake://<user_login_name>:<password>@<account_name>/<database_name>/<schema_name>?warehouse=<warehouse_name>&role=<role_name>'

class Inlet:
    def __init__(self, inlets):
        self.inlets = inlets


class Outlet:
    pass

class Operators:
    pass