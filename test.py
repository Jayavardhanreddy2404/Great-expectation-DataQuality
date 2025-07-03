from great_expectations.data_context import DataContext
from great_expectations.core.batch import RuntimeBatchRequest

# DB connection info
SERVER = "localhost"
DATABASE = "DB1"
USERNAME = "jayavardhan"
PASSWORD = "Password@244466666"
DRIVER = "ODBC Driver 18 for SQL Server"

# SQLAlchemy connection string
connection_string = (
    f"mssql+pyodbc://{USERNAME}:{PASSWORD}@{SERVER}/{DATABASE}"
    f"?driver={DRIVER.replace(' ', '+')}&TrustServerCertificate=yes"
)

# GE root directory
GE_ROOT_DIR = "./great_expectations"

# Initialize context
context = DataContext(GE_ROOT_DIR)

# Expectation suite name
expectation_suite_name = "bank_customer_churn_suite"
try:
    context.get_expectation_suite(expectation_suite_name)
    print(f"Loaded existing expectation suite '{expectation_suite_name}'.")
except Exception:
    context.create_expectation_suite(expectation_suite_name)
    print(f"Created new expectation suite '{expectation_suite_name}'.")

# Table name (no database prefix and no outer brackets)
table_name = "Bank Customer Churn Prediction"

# Compose SQL query with schema and properly bracketed table name
table_name = "Bank Customer Churn Prediction"

query = f"""
SELECT 
    customer_id,
    credit_score,
    country,
    gender,
    age,
    tenure,
    CAST(balance AS FLOAT) AS balance,
    products_number,
    credit_card,
    active_member,
    CAST(estimated_salary AS FLOAT) AS estimated_salary,
    churn
FROM dbo.[{table_name}]
"""

# Create batch request using SQL query
batch_request = RuntimeBatchRequest(
    datasource_name="sql_server_localhost",
    data_connector_name="default_runtime_data_connector_name",
    data_asset_name="bank_customer_churn_data",
    runtime_parameters={"query": query},
    batch_identifiers={"default_identifier_name": "default_batch"},
)
# Get validator
validator = context.get_validator(
    batch_request=batch_request,
    expectation_suite_name=expectation_suite_name,
)

print("Validator loaded, adding expectations...")

# Add expectations
validator.expect_column_values_to_not_be_null("customer_id")
validator.expect_column_values_to_be_unique("customer_id")
validator.expect_column_values_to_be_between("credit_score", min_value=300, max_value=850)
validator.expect_column_values_to_not_be_null("country")
validator.expect_column_values_to_be_in_set("gender", ["Male", "Female"])
validator.expect_column_values_to_be_between("age", min_value=18, max_value=100)
validator.expect_column_values_to_be_between("tenure", min_value=0)
validator.expect_column_values_to_be_between("balance", min_value=0)
validator.expect_column_values_to_be_between("products_number", min_value=1, max_value=4)
validator.expect_column_values_to_be_in_set("credit_card", [0, 1])
validator.expect_column_values_to_be_in_set("active_member", [0, 1])
validator.expect_column_values_to_be_between("estimated_salary", min_value=0)
validator.expect_column_values_to_be_in_set("churn", [0, 1])

# Save expectations
validator.save_expectation_suite()
print("Expectation suite saved.")

# Run checkpoint directly (no YAML)
checkpoint_result = context.run_checkpoint(
    checkpoint_name="bank_churn_checkpoint"
)


# Generate data docs
context.build_data_docs()
print(f"\n✅ Validation complete.\n📄 Open: {GE_ROOT_DIR}/uncommitted/data_docs/local_site/index.html to view results.")
