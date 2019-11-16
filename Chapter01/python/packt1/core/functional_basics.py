from typing import List, Optional

words: List[str] = ["Settlements", "some", "centuries", "old", "and", "still", "no", "bigger", "than", "pinheads", "on", "the", "untouched", "expanse", "of", "their", "background"]
# mapped1 = list(map(lambda token: len(token), words))  # lambda
# mapped2 = [*(map(lambda token: len(token), words))]  # unpacking generalization

length_lambda = lambda token: len(token)  # assigning a lambda expression
def length_def(token):  # named function
    return len(token)

mapped1 = list(map(length_lambda, words))
mapped2 = list(map(length_def, words))

print(mapped1)
print(mapped2)
