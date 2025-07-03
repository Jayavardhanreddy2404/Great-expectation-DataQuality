from great_expectations.data_context import DataContext

connection_string = "mssql+pyodbc://jayavardhan:Password%40244466666@localhost/DB1?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes"

datasource_config = {
    "name": "sql_server_localhost",
    "class_name": "Datasource",
    "execution_engine": {
        "class_name": "SqlAlchemyExecutionEngine",
        "connection_string": connection_string,
    },
    "data_connectors": {
        "default_runtime_data_connector_name": {
            "class_name": "RuntimeDataConnector",
            "batch_identifiers": ["default_identifier_name"],
        }
    }
}

context = DataContext()

# Add the datasource
context.add_datasource(**datasource_config)

print("Datasource added successfully!")
