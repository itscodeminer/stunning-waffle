from pydantic import BaseModel
from sqlalchemy.orm import aliased
from sqlalchemy import func

# Define a Pydantic model for your response
class EmployeeResponse(BaseModel):
    emp_id: int
    emp_full_name: str
    manager_name: str

# Step 1: Create an alias for the Employee table to refer to the manager
manager_alias = aliased(Employee)

# Step 2: Define the CTE for selecting emp_id, emp_full_name, and emp_manager_id
cte = session.query(
    NewHire.id.label('emp_id'),  # New Hire ID
    func.concat(Employee.first_name, ' ', Employee.last_name).label('emp_full_name'),  # Full name of old employee (if exists)
    NewHire.manager_id.label('emp_manager_id')  # Manager ID from the NewHire table
).outerjoin(  # LEFT JOIN to Employee to get old employee details
    Employee, 
    Employee.id == NewHire.old_emp_id  # Join condition for old_emp_id
).cte()  # Create CTE

# Step 3: Query all columns from CTE and manager's full name from manager_alias
results = session.query(
    *[cte.c[column.name] for column in cte.columns],  # Dynamically select all columns from the CTE
    func.concat(manager_alias.first_name, ' ', manager_alias.last_name).label('manager_name')  # Explicitly select manager's full name
).join(  # INNER JOIN with the manager using the manager_id from CTE
    manager_alias, 
    manager_alias.id == cte.c.emp_manager_id  # Join condition on manager_id
).all()

# Step 4: Convert the results to Pydantic model
employee_responses = [EmployeeResponse(**row._asdict()) for row in results]

# Now employee_responses is a list of Pydantic models
