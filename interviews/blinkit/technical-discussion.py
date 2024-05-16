# Given an array and a target value, find the count of all sub-arrays whose sum is equal to the target value.
# arr = [-3, 5, 6, -1, 0, 5, 5, 2, -2, -9]
# target value = 10
#
# count = 7
# sub array satisfying -> [5,6,-1], [5,6,-1,0], [6,-1,0,5], [0,5,5], [5,5], [0,5,5,2,-2], [5,5,2,-2]


def count_with_sum(arr: list[int], target: int):
    count = 0

    # Iterating over all the numbers (size of sub array)
    for start in range(len(arr)):
        for end in range(start, len(arr)):
            current_arr = arr[start:end + 1]
            if sum(current_arr) == target:
                count += 1
    return count


def count_with_sum_optimised(arr: list[int], target: int):
    count = 0
    cumulative_sum_counts = {}

    cumulative_sum = 0
    for num in arr:
        cumulative_sum += num
        complement = cumulative_sum - target

        # Update cumulative sum count in hash
        cumulative_sum_counts[cumulative_sum] = cumulative_sum_counts.get(cumulative_sum, 0) + 1

        # If complement of cumulative sum is present in hash - add its value to count
        if complement in cumulative_sum_counts:
            count += cumulative_sum_counts[complement]

    return count


array = [-3, 5, 6, -1, 0, 5, 5, 2, -2, -9]
target_num = 10
print(count_with_sum(array, target_num))  # Output: 7
print(count_with_sum_optimised(array, target_num))  # Output: 7
