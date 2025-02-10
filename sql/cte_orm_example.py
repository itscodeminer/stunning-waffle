from sqlalchemy.orm import aliased
from sqlalchemy.sql import func

# Alias for Employee (Manager Table)
manager_alias = aliased(Employee)

# Step 1: Define the CTE with Filters on NewHire and Employee
cte_query = session.query(
    NewHire.id.label('emp_id'),
    func.concat(Employee.first_name, ' ', Employee.last_name).label('emp_full_name'),
    NewHire.manager_id.label('emp_manager_id')
).outerjoin(
    Employee, Employee.id == NewHire.old_emp_id  # Join Employee inside CTE
)

# 🔹 Apply filter on NewHire inside CTE
filter_conditions = [NewHire.department == "HR"]
cte_query = cte_query.filter(*filter_conditions)

# 🔹 Apply filter on Employee inside CTE
employee_filters = [Employee.last_name.like("Smith%")]
cte_query = cte_query.filter(*employee_filters)

cte = cte_query.cte()  # Create the CTE

# Step 2: Query CTE + Apply Additional Filters Outside CTE (Manager)
query = session.query(
    *[cte.c[column.name] for column in cte.columns],  # Select all CTE columns
    func.concat(manager_alias.first_name, ' ', manager_alias.last_name).label('manager_name')
).join(
    manager_alias, manager_alias.id == cte.c.emp_manager_id
)

# 🔹 Apply filter outside CTE on Manager
external_filters = [manager_alias.first_name.like("John%")]
query = query.filter(*external_filters)

# Execute Query
results = query.all()
