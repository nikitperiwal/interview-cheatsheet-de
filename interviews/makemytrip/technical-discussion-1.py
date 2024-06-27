# Problem 1: Execution Schedule for Maximum Number of Non-Conflicting Activities

# Problem Description:
# The objective is to compute an execution schedule having the maximum number of non-conflicting activities.

# Job        Start Time    End Time
# J1             5             9
# J2             1             2
# J3             3             4
# J4             0             6
# J5             5             7
# J6             8             9

def get_max_concurrent_jobs(job_list):
    # Sort jobs by their end time
    job_list.sort(key=lambda x: x[2])

    # List to hold the names of the selected jobs
    selected_jobs = []

    # The end time of the last selected job
    last_end_time = -1

    for job in job_list:
        j_name, start, end = job

        # If the current job starts after the last selected job ends, select it
        if start >= last_end_time:
            selected_jobs.append(j_name)
            last_end_time = end

    return selected_jobs


# Example usage
list_of_jobs = [
    ["J1", 5, 9],
    ["J2", 1, 2],
    ["J3", 3, 4],
    ["J4", 0, 6],
    ["J5", 5, 7],
    ["J6", 8, 9],
]

result = get_max_concurrent_jobs(list_of_jobs)
print(result)
