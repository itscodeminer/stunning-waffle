from sqlalchemy.orm import aliased
from sqlalchemy.sql import func, or_, and_

# Alias for Employee (Manager Table)
manager_alias = aliased(Employee)

# 🔹 Sample filters & search text (Inline)
search_text = "John HR"
filters = {
    "department": "HR",
    "employee_last_name": "Smith",
    "manager_first_name": "John"
}

# Step 1: Define the CTE
cte_query = session.query(
    NewHire.id.label('emp_id'),
    func.concat(Employee.first_name, ' ', Employee.last_name).label('emp_full_name'),
    NewHire.manager_id.label('emp_manager_id')
).outerjoin(
    Employee, Employee.id == NewHire.old_emp_id  # Join Employee inside CTE
)

# 🔹 Apply dynamic filters inside CTE
cte_filters = {
    "department": (NewHire.department, "eq"),
    "employee_last_name": (Employee.last_name, "like"),
}

for key, (column, filter_type) in cte_filters.items():
    if key in filters:
        if filter_type == "eq":
            cte_query = cte_query.filter(column == filters[key])
        elif filter_type == "like":
            cte_query = cte_query.filter(column.like(f"{filters[key]}%"))

# 🔹 Apply search text dynamically
search_conditions = [
    or_(
        NewHire.name.ilike(f"%{term}%"),
        NewHire.position.ilike(f"%{term}%"),
        Employee.first_name.ilike(f"%{term}%"),
        Employee.last_name.ilike(f"%{term}%")
    )
    for term in search_text.split()
]

if search_conditions:
    cte_query = cte_query.filter(or_(*search_conditions))

cte = cte_query.cte()  # Create the CTE

# Step 2: Query CTE + Apply Additional Filters Outside CTE (Manager)
query = session.query(
    *[cte.c[column.name] for column in cte.columns],  # Select all CTE columns
    func.concat(manager_alias.first_name, ' ', manager_alias.last_name).label('manager_name')
).join(
    manager_alias, manager_alias.id == cte.c.emp_manager_id
)

# 🔹 Apply dynamic filters outside CTE (Manager)
manager_filters = {
    "manager_first_name": (manager_alias.first_name, "like")
}

for key, (column, filter_type) in manager_filters.items():
    if key in filters:
        if filter_type == "eq":
            query = query.filter(column == filters[key])
        elif filter_type == "like":
            query = query.filter(column.like(f"{filters[key]}%"))

# Execute Query
results = query.all()
