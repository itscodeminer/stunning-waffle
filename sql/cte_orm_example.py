from sqlalchemy import func
from sqlalchemy.orm import aliased

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

# Step 3: Join the CTE with Employee to get manager details
results = session.query(
    cte.c.emp_id,  # emp_id from CTE
    cte.c.emp_full_name,  # Full name of old employee from CTE
    func.concat(manager_alias.first_name, ' ', manager_alias.last_name).label('manager_name')  # Full name of manager
).join(  # INNER JOIN with the manager using the manager_id from CTE
    manager_alias, 
    manager_alias.id == cte.c.emp_manager_id  # Join condition on manager_id
).all()

# Step 4: Output the results
for result in results:
    print(f"New Hire ID: {result.emp_id}, Employee Full Name: {result.emp_full_name}, Manager Full Name: {result.manager_name}")
