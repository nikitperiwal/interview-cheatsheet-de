# Problem 2: Next Higher Stock Price

# Problem Description:
# Given an array of stock prices, for each stock price, provide the next higher stock price in a dictionary.

# Input:
# arr = [2, 3, 7, 6, 4, 5, 8, 13, 11, 12, 14, 16]

# Sample Output:
# {
#   2: 3,
#   3: 7,
#   7: 8,
#   6: 8,
#   ...
#   16: -1  # or 0 to indicate no higher price available
# }

def return_next_largest(arr):
    result = {num: -1 for num in arr}  # Initialize the result dictionary with -1 (or 0)
    stack = []

    for num in arr:
        # While stack is not empty and the current number is greater than the stack's top element
        while stack and num > stack[-1]:
            result[stack.pop()] = num
        stack.append(num)

    return result


# Example usage
arr = [2, 3, 7, 6, 4, 5, 8, 13, 11, 12, 14, 16]
result = return_next_largest(arr)
print(result)
