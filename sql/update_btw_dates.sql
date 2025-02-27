WITH allocation_data AS (
    SELECT
        vah.van_number,
        vah.emp_id,
        vah.start_date,
        vah.end_date
    FROM
        van_allocation_history vah
    WHERE
        vah.is_active = TRUE -- You can change this condition if you need to include inactive allocations
)
UPDATE
    van_mileage vm
SET
    vm.emp_id = ad.emp_id
FROM
    allocation_data ad
WHERE
    vm.van_number = ad.van_number
    AND vm.mileage_date >= ad.start_date
    AND vm.mileage_date <= ad.end_date;
